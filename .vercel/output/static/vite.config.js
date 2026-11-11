import fs from "node:fs";

/**
 * Plugin de Vite que incluye:
 * 1. Bus WebSocket/SSE/REST en tiempo real (/api/state) para sincronizar celular (sensor.html),
 *    panel de juez (admin.html) y pantalla LED (dashboard.html) por Wi-Fi sin necesidad de llaves externas.
 * 2. Soporte HTTPS automático para que Chrome/Safari en móviles habiliten `navigator.mediaDevices.getUserMedia`
 *    y `navigator.wakeLock` al entrar por IP local (`https://192.168.0.6:5174`).
 */
const DEFAULT_STATE = {
  config: {
    sensorMode: "vision",
    motionThreshold: 28,
    audioThreshold: 65,
    lightThreshold: 40,
    cooldownMs: 2500,
    targetLaps: 1,
    propellerFilter: true,
    useTorchFlash: true,
    voiceAnnouncer: true,
    calibrateCommandAt: 0
  },
  race: {
    status: "IDLE",
    pilotName: "MAVERICK FPV",
    nextPilotName: "VALKYRIE",
    roster: ["MAVERICK FPV", "VALKYRIE", "NITRO DRONE", "SKY HUNTER"],
    rosterIndex: 0,
    countdownValue: 3,
    countdownStartedAt: 0,
    startTime: 0,
    finishTime: 0,
    elapsedMs: 0,
    currentLap: 1,
    targetLaps: 1,
    lapSplits: [],
    bestLapMs: 0,
    photoFinishDataUrl: "",
    falseStartReason: "",
    lastUpdated: Date.now()
  },
  telemetry: {
    liveMotion: 0,
    liveAudio: 0,
    liveLight: 50,
    sensorOnline: false,
    wakeLockActive: false,
    calibrating: false,
    updatedAt: Date.now()
  },
  leaderboard: {
    demo_1: { pilotName: "MAVERICK FPV", elapsedMs: 14285, bestLapMs: 14285, laps: 1, sensorMode: "vision", timestamp: Date.now() - 360000 },
    demo_2: { pilotName: "VALKYRIE", elapsedMs: 15890, bestLapMs: 15890, laps: 1, sensorMode: "light", timestamp: Date.now() - 240000 },
    demo_3: { pilotName: "NITRO DRONE", elapsedMs: 17420, bestLapMs: 17420, laps: 1, sensorMode: "audio", timestamp: Date.now() - 120000 }
  }
};

let serverRaceState = structuredClone(DEFAULT_STATE);
const sseClients = new Set();

function broadcastSSE() {
  const payload = `data: ${JSON.stringify(serverRaceState)}\n\n`;
  for (const res of sseClients) {
    try {
      res.write(payload);
    } catch (e) {
      sseClients.delete(res);
    }
  }
}

function setByPath(obj, path, value, merge = false) {
  const parts = path.split("/").filter(Boolean);
  if (parts.length === 0) return;
  let curr = obj;
  for (let i = 0; i < parts.length - 1; i++) {
    const p = parts[i];
    if (typeof curr[p] !== "object" || curr[p] === null) curr[p] = {};
    curr = curr[p];
  }
  const last = parts[parts.length - 1];
  if (merge && typeof curr[last] === "object" && curr[last] !== null && typeof value === "object" && value !== null) {
    curr[last] = { ...curr[last], ...value };
  } else {
    curr[last] = value;
  }
}

export default {
  server: {
    host: true,
    port: 5174,
    https: {
      pfx: fs.readFileSync("./certs/cert.pfx"),
      passphrase: "drone123"
    }
  },
  plugins: [
    {
      name: "drone-race-lan-realtime-hub",
      configureServer(server) {
        server.middlewares.use("/api/events", (req, res) => {
          res.writeHead(200, {
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*"
          });
          res.write(`data: ${JSON.stringify(serverRaceState)}\n\n`);
          sseClients.add(res);
          req.on("close", () => sseClients.delete(res));
        });

        server.middlewares.use("/api/state", (req, res) => {
          res.setHeader("Access-Control-Allow-Origin", "*");
          res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
          res.setHeader("Access-Control-Allow-Headers", "Content-Type");

          if (req.method === "OPTIONS") {
            res.statusCode = 204;
            res.end();
            return;
          }

          if (req.method === "GET") {
            res.setHeader("Content-Type", "application/json");
            res.end(JSON.stringify(serverRaceState));
            return;
          }

          if (req.method === "POST") {
            let body = "";
            req.on("data", (chunk) => (body += chunk));
            req.on("end", () => {
              try {
                const msg = JSON.parse(body || "{}");
                if (msg.action === "update") {
                  setByPath(serverRaceState, msg.path, msg.data, true);
                } else if (msg.action === "set") {
                  setByPath(serverRaceState, msg.path, msg.data, false);
                } else if (msg.action === "pushLeaderboard") {
                  if (!serverRaceState.leaderboard) serverRaceState.leaderboard = {};
                  const id = "run_" + Date.now() + "_" + Math.random().toString(36).slice(2, 6);
                  serverRaceState.leaderboard[id] = msg.data;
                } else if (msg.action === "clearLeaderboard") {
                  serverRaceState.leaderboard = {};
                }
                broadcastSSE();
                res.setHeader("Content-Type", "application/json");
                res.end(JSON.stringify({ ok: true }));
              } catch (e) {
                res.statusCode = 400;
                res.end(JSON.stringify({ error: e.message }));
              }
            });
          }
        });
      }
    }
  ]
};
