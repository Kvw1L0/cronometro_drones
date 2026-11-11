/**
 * Motor de Sincronización Multi-Dispositivo en Tiempo Real (Triple Redundancia)
 * Capas de transporte activas simultáneamente con deduplicación por msgId:
 *  1. MQTT sobre WebSockets TLS (HiveMQ:8884 -> EMQX:8084 -> Mosquitto:8081)
 *     - Ideal para telemetría continua de sensores (~8-10 FPS) y eventos instantáneos.
 *  2. Bus Universal Puerto 443 HTTPS/WSS (`ntfy.sh`)
 *     - Funciona incluso detrás de redes móviles 4G/5G o routers que bloqueen puertos 8884/8084.
 *     - Garantiza entrega de estados críticos de carrera (COUNTDOWN, RUNNING, FINISHED, config).
 *  3. Bus Local LAN (`/api/events` SSE + `/api/state` REST en Vite dev) + BroadcastChannel + localStorage
 *     - Para operación offline en red local o entre pestañas del mismo equipo.
 */

// Nota: El SDK remoto de Firebase se carga dinámicamente SOLO si firebaseConfig tiene credenciales reales,
// evitando bloquear la carga offline o en redes móviles cuando opera por MQTT WSS / HTTPS-443.

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
    sensorMode: "vision",
    motionThreshold: 28,
    audioThreshold: 62,
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
    lastUpdated: 0
  },
  telemetry: {
    liveMotion: 0,
    liveAudio: 0,
    liveLight: 50,
    sensorOnline: false,
    wakeLockActive: false,
    calibrating: false,
    updatedAt: 0
  },
  leaderboard: {
    demo_1: { pilotName: "MAVERICK FPV", elapsedMs: 14285, bestLapMs: 14285, laps: 1, sensorMode: "vision", timestamp: Date.now() - 360000 },
    demo_2: { pilotName: "VALKYRIE", elapsedMs: 15890, bestLapMs: 15890, laps: 1, sensorMode: "light", timestamp: Date.now() - 240000 },
    demo_3: { pilotName: "NITRO DRONE", elapsedMs: 17420, bestLapMs: 17420, laps: 1, sensorMode: "audio", timestamp: Date.now() - 120000 }
  }
};

function ensurePahoLoaded() {
  if (typeof window !== "undefined" && window.Paho && window.Paho.MQTT) {
    return Promise.resolve(window.Paho);
  }
  return new Promise((resolve) => {
    if (typeof document === "undefined") return resolve(null);

    const s = document.createElement("script");
    s.src = "./js/paho-mqtt.min.js";
    s.onload = () => resolve(window.Paho);
    s.onerror = () => {
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

    // Sala compartida por defecto
    const urlParams = typeof window !== "undefined" ? new URLSearchParams(window.location.search) : new URLSearchParams();
    this.roomId = (
      urlParams.get("sala") ||
      urlParams.get("room") ||
      (typeof localStorage !== "undefined" && localStorage.getItem("drone_chrono_room")) ||
      "pista_fpv"
    ).trim().toLowerCase().replace(/[^a-z0-9_-]/g, "_") || "pista_fpv";

    if (typeof localStorage !== "undefined") {
      localStorage.setItem("drone_chrono_room", this.roomId);
    }

    // Identificador corto y único por pestaña (<= 18 chars para MQTT 3.1)
    this.myClientId = "dc_" + Math.random().toString(36).substring(2, 8) + Date.now().toString(36).slice(-4);
    this.role = (typeof window !== "undefined" && window.location.pathname.includes("admin")) ? "admin" : "client";

    // Estado de conectividades
    this.mqttClient = null;
    this.isMqttConnected = false;
    this.isMqttConnecting = false;
    this.mqttReconnectTimer = null;

    this.ntfyWs = null;
    this.isNtfyConnected = false;
    this.ntfyReconnectTimer = null;

    this.isLanHubConnected = false;

    // Deduplicación de mensajes entre múltiples canales
    this.seenMsgIds = new Set();
    this.lastTelemetryMqttAt = 0;
    this.lastTelemetryNtfyAt = 0;

    this.fb = null;
    if (this.isFirebaseConfigured) {
      this.initFirebaseDynamic();
    }

    // Capa 1: Bus local Vite (solo si estamos sirviendo en puerto local / LAN Vite)
    this.initLanHubIfLocal();

    // Capa 2: MQTT WSS Multi-Broker
    this.initCloudMqtt();

    // Capa 3: Puerto 443 HTTPS/WSS Universal (ntfy.sh)
    this.initPort443Channel();

    // Sincronización instantánea entre pestañas del mismo navegador
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
    const clean = (newRoom || "pista_fpv").trim().toLowerCase().replace(/[^a-z0-9_-]/g, "_") || "pista_fpv";
    if (clean === this.roomId) return;
    this.roomId = clean;
    if (typeof localStorage !== "undefined") {
      localStorage.setItem("drone_chrono_room", clean);
    }
    this.resetCloudConnections();
  }

  resetCloudConnections() {
    clearTimeout(this.mqttReconnectTimer);
    clearTimeout(this.ntfyReconnectTimer);

    if (this.mqttClient) {
      try {
        this.mqttClient.onConnectionLost = null;
        if (this.isMqttConnected) this.mqttClient.disconnect();
      } catch (e) {}
      this.mqttClient = null;
    }
    this.isMqttConnected = false;
    this.isMqttConnecting = false;

    if (this.ntfyWs) {
      try {
        this.ntfyWs.onclose = null;
        this.ntfyWs.close();
      } catch (e) {}
      this.ntfyWs = null;
    }
    this.isNtfyConnected = false;

    this.connectMqttBroker(0);
    this.connectPort443Ws();
  }

  onCloudStatus(callback) {
    this.cloudStatusListeners.push(callback);
    this.emitCloudStatus();
  }

  emitCloudStatus() {
    const connected = this.isMqttConnected || this.isNtfyConnected || this.isLanHubConnected;
    const transports = [];
    if (this.isMqttConnected) transports.push("MQTT");
    if (this.isNtfyConnected) transports.push("HTTPS-443");
    if (this.isLanHubConnected) transports.push("LAN");

    const info = {
      connected,
      mqtt: this.isMqttConnected,
      https443: this.isNtfyConnected,
      lan: this.isLanHubConnected,
      transports,
      room: this.roomId,
      message: connected
        ? `Nube OK (${transports.join("+")}): ${this.roomId}`
        : `Conectando Nube: ${this.roomId}`
    };

    this.cloudStatusListeners.forEach((cb) => {
      try { cb(info); } catch (e) {}
    });
  }

  // ============================================================================
  // CAPA 1: LAN HUB (Solo cuando corre en dev local Vite)
  // ============================================================================
  initLanHubIfLocal() {
    if (typeof window === "undefined") return;
    const h = window.location.hostname;
    const isLocalHost =
      h === "localhost" ||
      h === "127.0.0.1" ||
      h.startsWith("192.168.") ||
      h.startsWith("10.") ||
      window.location.port === "5173" ||
      window.location.port === "5174";

    if (!isLocalHost) return;

    try {
      const evtSource = new EventSource("/api/events");
      evtSource.onopen = () => {
        this.isLanHubConnected = true;
        this.emitCloudStatus();
      };
      evtSource.onmessage = (e) => {
        try {
          const remoteState = JSON.parse(e.data);
          this.memoryState = remoteState;
          if (typeof localStorage !== "undefined") {
            localStorage.setItem("drone_chrono_state", JSON.stringify(remoteState));
          }
          this.notifyLocalListeners();
        } catch (err) {}
      };
      evtSource.onerror = () => {
        this.isLanHubConnected = false;
        this.emitCloudStatus();
      };
    } catch (e) {}
  }

  async postToLanHub(payload) {
    if (!this.isLanHubConnected) return;
    try {
      await fetch("/api/state", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
    } catch (e) {}
  }

  // ============================================================================
  // CAPA 2: MQTT WSS MULTI-BROKER (HiveMQ -> EMQX -> Mosquitto)
  // ============================================================================
  async initCloudMqtt() {
    const Paho = await ensurePahoLoaded();
    if (!Paho || !Paho.MQTT) {
      console.warn("[CloudBus] Paho MQTT no disponible; operando por Puerto 443.");
      return;
    }
    this.connectMqttBroker(0);
  }

  connectMqttBroker(brokerIndex = 0) {
    if (this.isMqttConnecting) return;
    clearTimeout(this.mqttReconnectTimer);

    const brokers = [
      { host: "broker.hivemq.com", port: 8884, path: "/mqtt", ssl: true },
      { host: "broker.emqx.io", port: 8084, path: "/mqtt", ssl: true },
      { host: "test.mosquitto.org", port: 8081, path: "/mqtt", ssl: true }
    ];

    const currentBroker = brokers[brokerIndex % brokers.length];

    // Limpiar cliente anterior completamente para no chocar con el mismo clientId
    if (this.mqttClient) {
      try {
        this.mqttClient.onConnectionLost = null;
        this.mqttClient.onMessageArrived = null;
        if (this.isMqttConnected) this.mqttClient.disconnect();
      } catch (e) {}
      this.mqttClient = null;
    }

    this.isMqttConnected = false;
    this.isMqttConnecting = true;
    this.emitCloudStatus();

    // Generar un socketClientId fresco por cada intento para evitar colisiones de reconexión en HiveMQ
    const socketClientId = `${this.myClientId}_${Math.random().toString(36).slice(2, 5)}`;

    try {
      const client = new window.Paho.MQTT.Client(
        currentBroker.host,
        currentBroker.port,
        currentBroker.path,
        socketClientId
      );
      this.mqttClient = client;

      client.onConnectionLost = () => {
        if (this.mqttClient !== client) return;
        this.isMqttConnected = false;
        this.isMqttConnecting = false;
        this.emitCloudStatus();
        clearTimeout(this.mqttReconnectTimer);
        this.mqttReconnectTimer = setTimeout(() => {
          this.connectMqttBroker((brokerIndex + 1) % brokers.length);
        }, 2500);
      };

      client.onMessageArrived = (message) => {
        try {
          const payload = JSON.parse(message.payloadString);
          this.handleIncomingPayload(message.destinationName, payload);
        } catch (e) {}
      };

      client.connect({
        useSSL: currentBroker.ssl,
        timeout: 10,
        keepAliveInterval: 25,
        cleanSession: true,
        onSuccess: () => {
          if (this.mqttClient !== client) {
            try { client.disconnect(); } catch (e) {}
            return;
          }
          this.isMqttConnecting = false;
          this.isMqttConnected = true;
          console.log(`[CloudBus] MQTT conectado a ${currentBroker.host} · Sala: [${this.roomId}]`);
          this.emitCloudStatus();

          client.subscribe(`drone_chrono/${this.roomId}/#`, {
            qos: 0,
            onSuccess: () => {
              this.onCloudChannelReady();
            }
          });
        },
        onFailure: (err) => {
          if (this.mqttClient !== client) return;
          this.isMqttConnecting = false;
          this.isMqttConnected = false;
          console.warn(`[CloudBus] Falló MQTT ${currentBroker.host}:`, err?.errorMessage || err);
          this.emitCloudStatus();
          clearTimeout(this.mqttReconnectTimer);
          this.mqttReconnectTimer = setTimeout(() => {
            this.connectMqttBroker((brokerIndex + 1) % brokers.length);
          }, 2500);
        }
      });
    } catch (e) {
      this.isMqttConnecting = false;
      this.isMqttConnected = false;
      this.emitCloudStatus();
    }
  }

  // ============================================================================
  // CAPA 3: CANAL UNIVERSAL PUERTO 443 (ntfy.sh WSS + HTTPS POST)
  // Nunca bloqueado por redes móviles 4G/5G ni firewalls corporativos
  // ============================================================================
  getNtfyTopic() {
    return `drone_chrono_v2_${this.roomId}`;
  }

  initPort443Channel() {
    this.connectPort443Ws();
  }

  connectPort443Ws() {
    if (typeof WebSocket === "undefined") return;
    clearTimeout(this.ntfyReconnectTimer);

    if (this.ntfyWs) {
      try {
        this.ntfyWs.onclose = null;
        this.ntfyWs.onerror = null;
        this.ntfyWs.close();
      } catch (e) {}
      this.ntfyWs = null;
    }

    const topic = this.getNtfyTopic();
    try {
      const ws = new WebSocket(`wss://ntfy.sh/${topic}/ws`);
      this.ntfyWs = ws;

      ws.onopen = () => {
        if (this.ntfyWs !== ws) return;
        this.isNtfyConnected = true;
        console.log(`[CloudBus] Canal Puerto 443 (WSS) conectado · Sala: [${this.roomId}]`);
        this.emitCloudStatus();
        this.onCloudChannelReady();
      };

      ws.onmessage = (ev) => {
        try {
          const envelope = JSON.parse(ev.data);
          if (envelope.event !== "message" || !envelope.message) return;
          const payload = JSON.parse(envelope.message);
          if (payload && payload.path) {
            this.handleIncomingPayload(`drone_chrono/${this.roomId}/${payload.path}`, payload);
          }
        } catch (e) {}
      };

      ws.onclose = () => {
        if (this.ntfyWs !== ws) return;
        this.isNtfyConnected = false;
        this.emitCloudStatus();
        clearTimeout(this.ntfyReconnectTimer);
        this.ntfyReconnectTimer = setTimeout(() => this.connectPort443Ws(), 4000);
      };

      ws.onerror = () => {
        if (this.ntfyWs !== ws) return;
        this.isNtfyConnected = false;
        this.emitCloudStatus();
      };
    } catch (e) {
      this.isNtfyConnected = false;
    }
  }

  async publishPort443(subpath, payloadObj) {
    if (typeof fetch === "undefined") return;

    // Clonar para omitir photoFinishDataUrl si supera límite de mensaje rápido 443 (~3.5 KB)
    const compact = { ...payloadObj, path: subpath };
    let strippedPhoto = false;
    if (compact.data && typeof compact.data === "object" && compact.data.photoFinishDataUrl) {
      const testStr = JSON.stringify(compact);
      if (testStr.length > 3500) {
        const cloned = { ...compact.data };
        delete cloned.photoFinishDataUrl;
        compact.data = cloned;
        strippedPhoto = true;
      }
    }
    if (subpath === "sync_response" && compact.data && compact.data.race && compact.data.race.photoFinishDataUrl) {
      const clonedRace = { ...compact.data.race };
      delete clonedRace.photoFinishDataUrl;
      compact.data = {
        ...compact.data,
        race: clonedRace
      };
      strippedPhoto = true;
    }
    if (strippedPhoto && compact.msgId) {
      compact.baseMsgId = compact.msgId;
      compact.msgId = `${compact.msgId}_noimg`;
    }

    const bodyStr = JSON.stringify(compact);
    if (bodyStr.length > 3800) return;

    try {
      await fetch(`https://ntfy.sh/${this.getNtfyTopic()}`, {
        method: "POST",
        body: bodyStr
      });
    } catch (e) {}
  }

  // ============================================================================
  // LÓGICA UNIFICADA DE ENVÍO, RECEPCIÓN Y DEDUPLICACIÓN
  // ============================================================================
  onCloudChannelReady() {
    if (this.role === "admin") {
      // El administrador comparte su config y estado inicial con cualquier sensor conectado
      const state = this.memoryState || this.getLocalState();
      this.publishCloud("sync_response", { config: state.config, race: state.race, leaderboard: state.leaderboard }, "sync");
    } else {
      // Sensores y pantallas piden el estado vivo actual
      this.publishCloud("sync_request", { type: "REQUEST_STATE" }, "sync_request");
    }
  }

  markMessageSeen(msgId) {
    if (!msgId) return false;
    if (this.seenMsgIds.has(msgId)) return true;
    this.seenMsgIds.add(msgId);
    if (this.seenMsgIds.size > 250) {
      const first = this.seenMsgIds.values().next().value;
      this.seenMsgIds.delete(first);
    }
    return false;
  }

  handleIncomingPayload(topic, payload) {
    if (!payload || payload.sender === this.myClientId) return;
    if (payload.baseMsgId && this.seenMsgIds.has(payload.baseMsgId)) return;
    if (payload.msgId && this.markMessageSeen(payload.msgId)) return;

    const parts = topic.split("/");
    const subpath = payload.path || parts.slice(2).join("/");

    if (subpath === "sync_request") {
      if (this.role === "admin") {
        const state = this.memoryState || this.getLocalState();
        this.publishCloud("sync_response", {
          config: state.config,
          race: state.race,
          leaderboard: state.leaderboard
        }, "sync");
      }
      return;
    }

    if (subpath === "sync_response") {
      if (payload.data && typeof payload.data === "object") {
        const current = this.memoryState || this.getLocalState();
        this.memoryState = {
          config: { ...DEFAULT_RACE_STATE.config, ...(payload.data.config || current.config || {}) },
          race: { ...DEFAULT_RACE_STATE.race, ...(payload.data.race || current.race || {}) },
          telemetry: current.telemetry || structuredClone(DEFAULT_RACE_STATE.telemetry),
          leaderboard: payload.data.leaderboard || current.leaderboard || {}
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
    const now = Date.now();
    const isTelemetry = subpath === "telemetry";

    if (isTelemetry) {
      // Rate limit MQTT telemetría (~8-9 FPS)
      const mqttReady = this.isMqttConnected && this.mqttClient;
      const sendMqtt = mqttReady && (now - this.lastTelemetryMqttAt >= 110);
      // En Puerto 443 solo enviar telemetría cada 1.2s si MQTT NO está conectado (para no saturar rate-limit)
      const sendNtfy = !mqttReady && this.isNtfyConnected && (now - this.lastTelemetryNtfyAt >= 1200);

      if (!sendMqtt && !sendNtfy) return;

      const msgId = `${this.myClientId}_${now}_${Math.random().toString(36).slice(2, 6)}`;
      this.markMessageSeen(msgId);
      const envelope = { sender: this.myClientId, msgId, action, path: subpath, data, timestamp: now };

      if (sendMqtt) {
        this.lastTelemetryMqttAt = now;
        try {
          const message = new window.Paho.MQTT.Message(JSON.stringify(envelope));
          message.destinationName = `drone_chrono/${this.roomId}/${subpath}`;
          message.qos = 0;
          this.mqttClient.send(message);
        } catch (e) {}
      }
      if (sendNtfy) {
        this.lastTelemetryNtfyAt = now;
        this.publishPort443(subpath, envelope);
      }
      return;
    }

    // Eventos CRÍTICOS de carrera / configuración: enviar por MQTT Y por Puerto 443 en paralelo
    const msgId = `${this.myClientId}_${now}_${Math.random().toString(36).slice(2, 6)}`;
    this.markMessageSeen(msgId);
    const envelope = { sender: this.myClientId, msgId, action, path: subpath, data, timestamp: now };

    if (this.isMqttConnected && this.mqttClient) {
      try {
        const message = new window.Paho.MQTT.Message(JSON.stringify(envelope));
        message.destinationName = `drone_chrono/${this.roomId}/${subpath}`;
        message.qos = 0;
        this.mqttClient.send(message);
      } catch (e) {}
    }

    // Siempre respaldar eventos críticos por HTTPS Puerto 443
    this.publishPort443(subpath, envelope);
  }

  // ============================================================================
  // API PÚBLICA COMPATIBLE (Firebase / Local / Cloud)
  // ============================================================================
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

  async initFirebaseDynamic() {
    try {
      const [appMod, dbMod] = await Promise.all([
        import("https://www.gstatic.com/firebasejs/10.12.2/firebase-app.js"),
        import("https://www.gstatic.com/firebasejs/10.12.2/firebase-database.js")
      ]);
      const app = appMod.initializeApp(firebaseConfig);
      this.db = dbMod.getDatabase(app);
      this.fb = dbMod;
    } catch (err) {
      console.warn("[DroneChrono] Firebase lazy-load error:", err);
      this.isFirebaseConfigured = false;
    }
  }

  subscribe(path, callback) {
    if (this.isFirebaseConfigured && this.db && this.fb) {
      const { ref, set, onValue } = this.fb;
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
    if (this.isFirebaseConfigured && this.db && this.fb) {
      try {
        await this.fb.update(this.fb.ref(this.db, path), partialData);
      } catch (e) {}
    }

    const state = this.memoryState || this.getLocalState();
    const currentTarget = this.getValueByPath(state, path) || {};
    this.setValueByPath(state, path, { ...currentTarget, ...partialData });
    this.saveLocalState(state);
    this.publishCloud(path, partialData, "update");
    this.postToLanHub({ action: "update", path, data: partialData });
  }

  async setNode(path, data) {
    if (this.isFirebaseConfigured && this.db && this.fb) {
      try {
        await this.fb.set(this.fb.ref(this.db, path), data);
      } catch (e) {}
    }

    const state = this.memoryState || this.getLocalState();
    this.setValueByPath(state, path, data);
    this.saveLocalState(state);
    this.publishCloud(path, data, "set");
    this.postToLanHub({ action: "set", path, data });
  }

  async pushLeaderboardEntry(entry) {
    if (this.isFirebaseConfigured && this.db && this.fb) {
      try {
        await this.fb.push(this.fb.ref(this.db, "leaderboard"), entry);
      } catch (e) {}
    }

    const state = this.memoryState || this.getLocalState();
    if (!state.leaderboard) state.leaderboard = {};
    const id = "run_" + Date.now() + "_" + Math.random().toString(36).slice(2, 6);
    state.leaderboard[id] = entry;
    this.saveLocalState(state);
    this.publishCloud("leaderboard", entry, "pushLeaderboard");
    this.postToLanHub({ action: "pushLeaderboard", data: entry });
  }

  async clearLeaderboard() {
    if (this.isFirebaseConfigured && this.db && this.fb) {
      try {
        await this.fb.remove(this.fb.ref(this.db, "leaderboard"));
      } catch (e) {}
    }
    const state = this.memoryState || this.getLocalState();
    state.leaderboard = {};
    this.saveLocalState(state);
    this.publishCloud("leaderboard", {}, "clearLeaderboard");
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
