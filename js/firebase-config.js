/**
 * Configuración Modular de Sincronización en Tiempo Real
 * Sincronización Híbrida:
 *  1. Cloud Realtime Bus vía MQTT sobre WebSockets (WSS TLS) a brokers públicos globales (HiveMQ / EMQX).
 *     - Funciona entre múltiples dispositivos físicos (celular de pista, laptop de juez, pantalla LED).
 *     - Funciona en Vercel, Wi-Fi, 4G/5G con latencia ultrabaja (<40ms).
 *  2. Firebase Realtime Database (opcional si se configuran credenciales).
 *  3. BroadcastChannel + localStorage (para sincronización instantánea entre pestañas de un mismo equipo).
 */

import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-app.js";
import {
  getDatabase,
  ref,
  set,
  update,
  push,
  onValue,
  remove
} from "https://www.gstatic.com/firebasejs/10.12.2/firebase-database.js";

// ============================================================================
// Credenciales opcionales de Firebase (si deseas persistencia permanente en Firebase)
// ============================================================================
export const firebaseConfig = {
  apiKey: "TU_API_KEY",
  authDomain: "tu-proyecto-drones.firebaseapp.com",
  databaseURL: "https://tu-proyecto-drones-default-rtdb.firebaseio.com",
  projectId: "tu-proyecto-drones",
  storageBucket: "tu-proyecto-drones.appspot.com",
  messagingSenderId: "1234567890",
  appId: "1:1234567890:web:abcdef123456"
};

export const DEFAULT_RACE_STATE = {
  config: {
    sensorMode: "vision",     // 'vision' | 'audio' | 'light' | 'fusion'
    motionThreshold: 28,      // % de píxeles con cambio brusco (1 - 100)
    audioThreshold: 62,       // Nivel acústico de motores (1 - 100)
    lightThreshold: 40,       // Umbral de luminosidad (1 - 100)
    cooldownMs: 2500,         // Tiempo mínimo por vuelta / despegue (anti-rebote)
    targetLaps: 1,            // 1 = Sprint, 2..5 = Circuito Multi-Vuelta
    propellerFilter: true,    // Filtra voces y escucha hélices (1200-4500 Hz)
    useTorchFlash: true,      // Flash LED en celular
    voiceAnnouncer: true,     // Locutor en Dashboard LED
    calibrateCommandAt: 0     // Timestamp para auto-calibración
  },
  race: {
    status: "IDLE",           // 'IDLE' | 'COUNTDOWN' | 'RUNNING' | 'FINISHED' | 'FALSE_START'
    pilotName: "MAVERICK FPV",
    nextPilotName: "VALKYRIE",
    roster: ["MAVERICK FPV", "VALKYRIE", "NITRO DRONE", "SKYHAWK", "PHANTOM X"],
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

/**
 * Carga dinámica y resiliente de Paho MQTT
 */
function ensurePahoLoaded() {
  if (typeof window !== "undefined" && window.Paho && window.Paho.MQTT) {
    return Promise.resolve(window.Paho);
  }
  return new Promise((resolve) => {
    if (typeof document === "undefined") return resolve(null);

    const s = document.createElement("script");
    s.src = "js/paho-mqtt.min.js";
    s.onload = () => resolve(window.Paho);
    s.onerror = () => {
      // Fallback a CDN público si falla carga local
      const s2 = document.createElement("script");
      s2.src = "https://cdnjs.cloudflare.com/ajax/libs/paho-mqtt/1.0.1/mqttws31.min.js";
      s2.onload = () => resolve(window.Paho);
      s2.onerror = () => resolve(null);
      document.head.appendChild(s2);
    };
    document.head.appendChild(s);
  });
}

class DroneRaceSyncEngine {
  constructor() {
    this.isFirebaseConfigured =
      firebaseConfig.apiKey !== "TU_API_KEY" &&
      !firebaseConfig.databaseURL.includes("tu-proyecto-drones");

    this.db = null;
    this.channel = typeof BroadcastChannel !== "undefined" ? new BroadcastChannel("drone_chrono_realtime_bus") : null;
    this.listeners = new Map();
    this.cloudStatusListeners = [];
    this.memoryState = this.getLocalState();

    // Detección de Sala (Room)
    const urlParams = typeof window !== "undefined" ? new URLSearchParams(window.location.search) : new URLSearchParams();
    this.roomId = (urlParams.get("sala") || urlParams.get("room") || (typeof localStorage !== "undefined" && localStorage.getItem("drone_chrono_room")) || "pista_fpv").trim().toLowerCase().replace(/[^a-z0-9_-]/g, "_");
    if (typeof localStorage !== "undefined") {
      localStorage.setItem("drone_chrono_room", this.roomId);
    }

    // Identificador único de este cliente (máximo 20 caracteres para estándar MQTT 3.1)
    this.myClientId = "c_" + Math.random().toString(36).substring(2, 8) + "_" + Date.now().toString(36).slice(-4);
    this.mqttClient = null;
    this.isCloudConnected = false;
    this.lastTelemetryPublish = 0;

    // Inicializar Firebase si está configurado
    if (this.isFirebaseConfigured) {
      try {
        const app = initializeApp(firebaseConfig);
        this.db = getDatabase(app);
        console.log("[DroneChrono] Conectado a Firebase Realtime Database.");
      } catch (err) {
        console.warn("[DroneChrono] Error inicializando Firebase:", err);
        this.isFirebaseConfigured = false;
      }
    }

    // Inicializar Bus Nube MQTT (para sincronizar entre distintos celulares/PCs en Vercel)
    this.initCloudMqtt();

    // Bus local entre pestañas del mismo navegador
    if (this.channel) {
      this.channel.onmessage = (ev) => {
        if (ev.data && ev.data.type === "STATE_UPDATED") {
          this.memoryState = this.getLocalState();
          this.notifyLocalListeners();
        }
      };
    }

    if (typeof window !== "undefined") {
      window.addEventListener("storage", (ev) => {
        if (ev.key === "drone_chrono_state") {
          this.memoryState = this.getLocalState();
          this.notifyLocalListeners();
        }
      });
    }
  }

  getRoomId() {
    return this.roomId;
  }

  setRoomId(newRoom) {
    const clean = (newRoom || "pista_fpv").trim().toLowerCase().replace(/[^a-z0-9_-]/g, "_");
    if (clean === this.roomId) return;
    this.roomId = clean;
    if (typeof localStorage !== "undefined") {
      localStorage.setItem("drone_chrono_room", clean);
    }
    if (this.mqttClient && this.isCloudConnected) {
      try {
        this.mqttClient.disconnect();
      } catch (e) {}
    }
    this.connectMqttBroker(0);
  }

  onCloudStatus(callback) {
    this.cloudStatusListeners.push(callback);
    callback({ connected: this.isCloudConnected, room: this.roomId });
  }

  notifyCloudStatus(connected, message) {
    this.isCloudConnected = connected;
    this.cloudStatusListeners.forEach((cb) => {
      try { cb({ connected, message, room: this.roomId }); } catch (e) {}
    });
  }

  async initCloudMqtt() {
    const Paho = await ensurePahoLoaded();
    if (!Paho || !Paho.MQTT) {
      console.warn("[CloudBus] Paho MQTT no disponible, operando en modo local.");
      return;
    }
    this.connectMqttBroker(0);
  }

  connectMqttBroker(brokerIndex = 0) {
    const brokers = [
      { host: "broker.hivemq.com", port: 8884, path: "/mqtt", ssl: true },
      { host: "broker.emqx.io", port: 8084, path: "/mqtt", ssl: true }
    ];

    const currentBroker = brokers[brokerIndex % brokers.length];
    this.notifyCloudStatus(false, "Conectando a Nube...");

    try {
      const client = new window.Paho.MQTT.Client(
        currentBroker.host,
        currentBroker.port,
        currentBroker.path,
        this.myClientId
      );
      this.mqttClient = client;

      client.onConnectionLost = (resp) => {
        this.notifyCloudStatus(false, "Desconectado de la Nube");
        setTimeout(() => {
          this.connectMqttBroker((brokerIndex + 1) % brokers.length);
        }, 3000);
      };

      client.onMessageArrived = (message) => {
        try {
          const payload = JSON.parse(message.payloadString);
          if (payload.sender === this.myClientId) return; // Ignorar propio eco

          this.handleIncomingCloudMessage(message.destinationName, payload);
        } catch (e) {
          console.warn("[CloudBus] Error decodificando mensaje:", e);
        }
      };

      client.connect({
        useSSL: currentBroker.ssl,
        timeout: 6,
        keepAliveInterval: 30,
        cleanSession: true,
        onSuccess: () => {
          console.log(`[CloudBus] Conectado exitosamente a ${currentBroker.host} en sala [${this.roomId}]`);
          this.notifyCloudStatus(true, `En línea (Nube: ${this.roomId})`);

          // Suscribirse a todos los eventos de la sala
          client.subscribe(`drone_chrono/${this.roomId}/#`, {
            qos: 0,
            onSuccess: () => {
              // Pedir estado actual si nos acabamos de conectar
              this.publishCloud("sync_request", { type: "REQUEST_STATE" });
            }
          });
        },
        onFailure: (err) => {
          console.warn(`[CloudBus] Intento fallido en ${currentBroker.host}:`, err);
          this.notifyCloudStatus(false, "Reintentando Nube...");
          setTimeout(() => {
            this.connectMqttBroker((brokerIndex + 1) % brokers.length);
          }, 3500);
        }
      });
    } catch (e) {
      console.error("[CloudBus] Error al instanciar cliente MQTT:", e);
    }
  }

  handleIncomingCloudMessage(topic, payload) {
    const parts = topic.split("/");
    const subpath = parts.slice(2).join("/");

    if (subpath === "sync_request") {
      const state = this.memoryState || this.getLocalState();
      // Si tenemos carrera activa o datos, respondemos a los nuevos dispositivos
      if (state.race && (state.race.status === "RUNNING" || state.race.status === "COUNTDOWN" || state.race.status === "FINISHED")) {
        this.publishCloud("sync_response", state);
      }
      return;
    }

    if (subpath === "sync_response") {
      if (payload.data && typeof payload.data === "object") {
        this.memoryState = {
          config: { ...DEFAULT_RACE_STATE.config, ...(payload.data.config || {}) },
          race: { ...DEFAULT_RACE_STATE.race, ...(payload.data.race || {}) },
          telemetry: { ...DEFAULT_RACE_STATE.telemetry, ...(payload.data.telemetry || {}) },
          leaderboard: payload.data.leaderboard || {}
        };
        if (typeof localStorage !== "undefined") {
          localStorage.setItem("drone_chrono_state", JSON.stringify(this.memoryState));
        }
        this.notifyLocalListeners();
      }
      return;
    }

    if (subpath === "race" || subpath === "config" || subpath === "telemetry" || subpath === "leaderboard") {
      const state = this.memoryState || this.getLocalState();

      if (payload.action === "update") {
        const curr = this.getValueByPath(state, subpath) || {};
        this.setValueByPath(state, subpath, { ...curr, ...payload.data });
      } else if (payload.action === "set") {
        this.setValueByPath(state, subpath, payload.data);
      } else if (payload.action === "pushLeaderboard") {
        if (!state.leaderboard) state.leaderboard = {};
        const id = "run_" + (payload.timestamp || Date.now()) + "_" + Math.random().toString(36).slice(2, 6);
        state.leaderboard[id] = payload.data;
      } else if (payload.action === "clearLeaderboard") {
        state.leaderboard = {};
      }

      this.memoryState = state;
      if (typeof localStorage !== "undefined") {
        localStorage.setItem("drone_chrono_state", JSON.stringify(state));
      }
      this.notifyLocalListenersForPath(subpath);
    }
  }

  publishCloud(subpath, data, action = "update") {
    if (!this.isCloudConnected || !this.mqttClient) return;

    // Control de frecuencia para telemetría (máx ~9 veces por segundo para optimizar ancho de banda)
    if (subpath === "telemetry") {
      const now = Date.now();
      if (now - this.lastTelemetryPublish < 110) return;
      this.lastTelemetryPublish = now;
    }

    try {
      const topic = `drone_chrono/${this.roomId}/${subpath}`;
      const payload = JSON.stringify({
        sender: this.myClientId,
        action,
        data,
        timestamp: Date.now()
      });

      const message = new window.Paho.MQTT.Message(payload);
      message.destinationName = topic;
      message.qos = 0;
      this.mqttClient.send(message);
    } catch (e) {
      console.warn("[CloudBus] Error publicando mensaje:", e);
    }
  }

  getLocalState() {
    try {
      if (typeof localStorage === "undefined") return structuredClone(DEFAULT_RACE_STATE);
      const raw = localStorage.getItem("drone_chrono_state");
      if (!raw) return structuredClone(DEFAULT_RACE_STATE);
      const parsed = JSON.parse(raw);
      return {
        config: { ...DEFAULT_RACE_STATE.config, ...(parsed.config || {}) },
        race: { ...DEFAULT_RACE_STATE.race, ...(parsed.race || {}) },
        telemetry: { ...DEFAULT_RACE_STATE.telemetry, ...(parsed.telemetry || {}) },
        leaderboard: parsed.leaderboard || structuredClone(DEFAULT_RACE_STATE.leaderboard)
      };
    } catch (e) {
      return structuredClone(DEFAULT_RACE_STATE);
    }
  }

  saveLocalState(fullState) {
    this.memoryState = fullState;
    if (typeof localStorage !== "undefined") {
      localStorage.setItem("drone_chrono_state", JSON.stringify(fullState));
    }
    if (this.channel) {
      this.channel.postMessage({ type: "STATE_UPDATED", timestamp: Date.now() });
    }
    this.notifyLocalListeners();
  }

  notifyLocalListeners() {
    const state = this.memoryState || this.getLocalState();
    for (const [path, callbacks] of this.listeners.entries()) {
      const val = this.getValueByPath(state, path);
      callbacks.forEach((cb) => {
        try { cb(val); } catch (e) { console.error(e); }
      });
    }
  }

  notifyLocalListenersForPath(subpath) {
    const state = this.memoryState || this.getLocalState();
    for (const [path, callbacks] of this.listeners.entries()) {
      if (path === subpath || path === "/" || !path) {
        const val = this.getValueByPath(state, path);
        callbacks.forEach((cb) => {
          try { cb(val); } catch (e) { console.error(e); }
        });
      }
    }
  }

  getValueByPath(obj, path) {
    if (!path || path === "/") return obj;
    return path
      .split("/")
      .filter(Boolean)
      .reduce((acc, part) => (acc && acc[part] !== undefined ? acc[part] : null), obj);
  }

  setValueByPath(obj, path, value) {
    const parts = path.split("/").filter(Boolean);
    let curr = obj;
    for (let i = 0; i < parts.length - 1; i++) {
      const p = parts[i];
      if (typeof curr[p] !== "object" || curr[p] === null) curr[p] = {};
      curr = curr[p];
    }
    curr[parts[parts.length - 1]] = value;
  }

  subscribe(path, callback) {
    if (this.isFirebaseConfigured && this.db) {
      const nodeRef = ref(this.db, path);
      onValue(nodeRef, (snapshot) => {
        const data = snapshot.val();
        if (data !== null) {
          callback(data);
        } else {
          const fallback = this.getValueByPath(DEFAULT_RACE_STATE, path);
          set(nodeRef, fallback);
          callback(fallback);
        }
      });
      return;
    }

    if (!this.listeners.has(path)) {
      this.listeners.set(path, []);
    }
    this.listeners.get(path).push(callback);
    callback(this.getValueByPath(this.memoryState, path));
  }

  async updateNode(path, partialData) {
    if (this.isFirebaseConfigured && this.db) {
      try {
        const nodeRef = ref(this.db, path);
        await update(nodeRef, partialData);
      } catch (e) {}
    }

    const state = this.memoryState || this.getLocalState();
    const currentTarget = this.getValueByPath(state, path) || {};
    this.setValueByPath(state, path, { ...currentTarget, ...partialData });
    this.saveLocalState(state);

    // Transmitir a la Nube para los otros dispositivos
    this.publishCloud(path, partialData, "update");
  }

  async setNode(path, data) {
    if (this.isFirebaseConfigured && this.db) {
      try {
        const nodeRef = ref(this.db, path);
        await set(nodeRef, data);
      } catch (e) {}
    }

    const state = this.memoryState || this.getLocalState();
    this.setValueByPath(state, path, data);
    this.saveLocalState(state);

    // Transmitir a la Nube
    this.publishCloud(path, data, "set");
  }

  async pushLeaderboardEntry(entry) {
    if (this.isFirebaseConfigured && this.db) {
      try {
        const listRef = ref(this.db, "leaderboard");
        await push(listRef, entry);
      } catch (e) {}
    }

    const state = this.memoryState || this.getLocalState();
    if (!state.leaderboard) state.leaderboard = {};
    const id = "run_" + Date.now() + "_" + Math.random().toString(36).slice(2, 6);
    state.leaderboard[id] = entry;
    this.saveLocalState(state);

    // Transmitir a la Nube
    this.publishCloud("leaderboard", entry, "pushLeaderboard");
  }

  async clearLeaderboard() {
    if (this.isFirebaseConfigured && this.db) {
      try {
        await remove(ref(this.db, "leaderboard"));
      } catch (e) {}
    }
    const state = this.memoryState || this.getLocalState();
    state.leaderboard = {};
    this.saveLocalState(state);

    // Transmitir a la Nube
    this.publishCloud("leaderboard", {}, "clearLeaderboard");
  }
}

export const rtdb = new DroneRaceSyncEngine();

export function formatRaceTime(ms) {
  const safeMs = Math.max(0, Math.floor(ms));
  const minutes = Math.floor(safeMs / 60000);
  const seconds = Math.floor((safeMs % 60000) / 1000);
  const centis = Math.floor((safeMs % 1000) / 10);

  return `${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}.${String(centis).padStart(2, "0")}`;
}
