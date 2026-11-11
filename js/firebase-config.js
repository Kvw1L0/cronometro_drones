/**
 * Configuración Modular de Firebase Realtime Database (SDK v10 ESM)
 * + Bus Wi-Fi LAN (Server-Sent Events + REST `/api/state`) + BroadcastChannel
 * Incluye soporte para:
 *  - Modos de Sensor Simples + Fusión Dual ('fusion') + Filtro FFT de Hélices (1.2kHz-4.5kHz)
 *  - Auto-Calibración Inteligente de Ambiente (3 segundos)
 *  - Modo Multi-Vueltas (1 a 5 vueltas) con Split Times y Fastest Lap
 *  - Roster / Cola de Pilotos con "Siguiente en Pista"
 *  - Captura de Foto-Finish automática en base64
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
// 1. REEMPLAZA AQUÍ TUS CREDENCIALES REALES DE FIREBASE REALTIME DATABASE
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
    sensorMode: "vision",     // 'vision' | 'audio' | 'light' | 'fusion' (Visión + Audio simultáneos)
    motionThreshold: 28,      // % de píxeles con cambio brusco (1 - 100)
    audioThreshold: 62,       // Nivel acústico de motores (1 - 100)
    lightThreshold: 40,       // Umbral de luminosidad (1 - 100)
    cooldownMs: 2500,         // Tiempo mínimo por vuelta / despegue (anti-rebote)
    targetLaps: 1,            // 1 = Sprint, 2..5 = Circuito Multi-Vuelta
    propellerFilter: true,    // Filtra la voz humana/música y escucha solo frecuencias de hélices (1200-4500 Hz)
    useTorchFlash: true,      // Parpadea el flash LED físico del celular en GO! y Meta
    voiceAnnouncer: true,     // Locución en vivo en el Dashboard LED
    calibrateCommandAt: 0     // Timestamp para ordenar auto-calibración de 3s al sensor
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
    lapSplits: [],            // Array de tiempos por vuelta en ms
    bestLapMs: 0,
    photoFinishDataUrl: "",   // Snapshot JPEG base64 del instante exacto del cruce de meta
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

class DroneRaceSyncEngine {
  constructor() {
    this.isFirebaseConfigured =
      firebaseConfig.apiKey !== "TU_API_KEY" &&
      !firebaseConfig.databaseURL.includes("tu-proyecto-drones");

    this.db = null;
    this.channel = new BroadcastChannel("drone_chrono_realtime_bus");
    this.listeners = new Map();
    this.memoryState = this.getLocalState();

    if (this.isFirebaseConfigured) {
      try {
        const app = initializeApp(firebaseConfig);
        this.db = getDatabase(app);
        console.log("[DroneChrono] Conectado a Firebase Realtime Database.");
      } catch (err) {
        console.warn("[DroneChrono] Error inicializando Firebase, usando bus LAN:", err);
        this.isFirebaseConfigured = false;
      }
    }

    try {
      const evtSource = new EventSource("/api/events");
      evtSource.onmessage = (e) => {
        try {
          const remoteState = JSON.parse(e.data);
          this.memoryState = remoteState;
          localStorage.setItem("drone_chrono_state", JSON.stringify(remoteState));
          this.notifyLocalListeners();
        } catch (err) {}
      };
    } catch (e) {}

    this.channel.onmessage = (ev) => {
      if (ev.data && ev.data.type === "STATE_UPDATED") {
        this.memoryState = this.getLocalState();
        this.notifyLocalListeners();
      }
    };

    window.addEventListener("storage", (ev) => {
      if (ev.key === "drone_chrono_state") {
        this.memoryState = this.getLocalState();
        this.notifyLocalListeners();
      }
    });
  }

  getLocalState() {
    try {
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
    localStorage.setItem("drone_chrono_state", JSON.stringify(fullState));
    this.channel.postMessage({ type: "STATE_UPDATED", timestamp: Date.now() });
    this.notifyLocalListeners();
  }

  async postToLanHub(payload) {
    try {
      await fetch("/api/state", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
    } catch (e) {}
  }

  notifyLocalListeners() {
    const state = this.memoryState || this.getLocalState();
    for (const [path, callbacks] of this.listeners.entries()) {
      const val = this.getValueByPath(state, path);
      callbacks.forEach((cb) => cb(val));
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
      const nodeRef = ref(this.db, path);
      await update(nodeRef, partialData);
    }

    const state = this.memoryState || this.getLocalState();
    const currentTarget = this.getValueByPath(state, path) || {};
    this.setValueByPath(state, path, { ...currentTarget, ...partialData });
    this.saveLocalState(state);
    this.postToLanHub({ action: "update", path, data: partialData });
  }

  async setNode(path, data) {
    if (this.isFirebaseConfigured && this.db) {
      const nodeRef = ref(this.db, path);
      await set(nodeRef, data);
    }

    const state = this.memoryState || this.getLocalState();
    this.setValueByPath(state, path, data);
    this.saveLocalState(state);
    this.postToLanHub({ action: "set", path, data });
  }

  async pushLeaderboardEntry(entry) {
    if (this.isFirebaseConfigured && this.db) {
      const listRef = ref(this.db, "leaderboard");
      await push(listRef, entry);
    }

    const state = this.memoryState || this.getLocalState();
    if (!state.leaderboard) state.leaderboard = {};
    const id = "run_" + Date.now() + "_" + Math.random().toString(36).slice(2, 6);
    state.leaderboard[id] = entry;
    this.saveLocalState(state);
    this.postToLanHub({ action: "pushLeaderboard", data: entry });
  }

  async clearLeaderboard() {
    if (this.isFirebaseConfigured && this.db) {
      await remove(ref(this.db, "leaderboard"));
    }
    const state = this.memoryState || this.getLocalState();
    state.leaderboard = {};
    this.saveLocalState(state);
    this.postToLanHub({ action: "clearLeaderboard" });
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
