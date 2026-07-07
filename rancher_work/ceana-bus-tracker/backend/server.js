const express = require("express");
const http = require("http");
const { WebSocketServer } = require("ws");
const Redis = require("ioredis");

const app = express();
app.use(express.json());

const redis = new Redis({ host: process.env.REDIS_HOST || "redis", port: 6379 });
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/ws" });

// Track connected rider clients
const clients = new Set();
wss.on("connection", (ws) => {
  clients.add(ws);
  ws.on("close", () => clients.delete(ws));
});

function broadcast(data) {
  const msg = JSON.stringify(data);
  for (const ws of clients) {
    if (ws.readyState === ws.OPEN) ws.send(msg);
  }
}

// --- GPS ingestion endpoint (replaces "GPS Ingest Lambda") ---
app.post("/bus/location", async (req, res) => {
  const { busId, lat, lng, route } = req.body;
  if (!busId || lat == null || lng == null) {
    return res.status(400).json({ error: "Missing busId, lat, or lng" });
  }

  // TODO: verify JWT here against Keycloak public key, confirm busId matches token claim

  const record = {
    busId,
    lat,
    lng,
    route: route || "unknown",
    status: "ON_TIME",
    lastUpdated: Date.now(),
  };

  await redis.set(`bus:${busId}`, JSON.stringify(record), "EX", 60); // auto-expire after 60s of no updates
  broadcast(record); // replaces "Broadcast Lambda"

  res.json({ ok: true });
});

// --- Read all current bus positions (for rider app initial load) ---
app.get("/buses", async (req, res) => {
  const keys = await redis.keys("bus:*");
  const buses = await Promise.all(
    keys.map(async (k) => JSON.parse(await redis.get(k)))
  );
  res.json(buses);
});

app.get("/health", (req, res) => res.json({ status: "ok" }));

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Ceana backend running on :${PORT}`));
