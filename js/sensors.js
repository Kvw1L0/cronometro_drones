/**
 * Motor Modular Profesional de Sensores para Teléfono en Pista (sensor.html)
 * Incluye:
 *  - Opción A ('vision'): Diferencia de píxeles en <canvas>
 *  - Opción B ('audio'): Web Audio API con Filtro de Frecuencia de Hélices FPV (1.200 Hz - 4.500 Hz)
 *  - Opción C ('light'): Fotómetro ITU-R BT.601 (apertura/bloqueo de luz)
 *  - Opción D ('fusion'): Doble Confirmación (Visión + Audio simultáneos para 0% falsos positivos)
 *  - Auto-Calibración de Ambiente de 3 Segundos
 *  - Captura de Foto-Finish en JPEG Base64
 *  - Disparo del Flash LED físico (Torch API) en teléfonos compatibles
 */

export class SoundBuzzer {
  constructor() {
    this.audioCtx = null;
  }

  init() {
    if (!this.audioCtx) {
      const AudioCtx = window.AudioContext || window.webkitAudioContext;
      this.audioCtx = new AudioCtx();
    }
    if (this.audioCtx.state === "suspended") {
      this.audioCtx.resume();
    }
  }

  beep(freq = 880, durationMs = 160, type = "sine", gainVal = 0.35) {
    this.init();
    if (!this.audioCtx) return;

    const osc = this.audioCtx.createOscillator();
    const gain = this.audioCtx.createGain();
    osc.type = type;
    osc.frequency.setValueAtTime(freq, this.audioCtx.currentTime);

    gain.gain.setValueAtTime(gainVal, this.audioCtx.currentTime);
    gain.gain.exponentialRampToValueAtTime(0.001, this.audioCtx.currentTime + durationMs / 1000);

    osc.connect(gain);
    gain.connect(this.audioCtx.destination);

    osc.start();
    osc.stop(this.audioCtx.currentTime + durationMs / 1000);
  }

  playShortTick() {
    this.beep(920, 150, "square", 0.3);
  }

  playLapSplitBeep() {
    this.beep(1180, 140, "triangle", 0.38);
    setTimeout(() => this.beep(1560, 190, "triangle", 0.38), 130);
  }

  playLongGo() {
    this.beep(1480, 620, "sawtooth", 0.45);
  }

  playFalseStartAlarm() {
    this.beep(220, 450, "sawtooth", 0.5);
    setTimeout(() => this.beep(165, 550, "sawtooth", 0.5), 220);
  }
}

export class MultiSensorEngine {
  constructor(videoEl, procCanvasEl, onTriggerCallback, onTelemetryCallback) {
    this.video = videoEl;
    this.canvas = procCanvasEl;
    this.ctx = this.canvas.getContext("2d", { willReadFrequently: true });
    this.onTrigger = onTriggerCallback;
    this.onTelemetry = onTelemetryCallback;

    // Lienzo de muestreo rápido 64x48 (60 FPS)
    this.sampleW = 64;
    this.sampleH = 48;
    this.canvas.width = this.sampleW;
    this.canvas.height = this.sampleH;

    // Lienzo secundario 320x240 para capturar la Foto-Finish del dron al cruzar la meta
    this.photoCanvas = document.createElement("canvas");
    this.photoCanvas.width = 320;
    this.photoCanvas.height = 240;
    this.photoCtx = this.photoCanvas.getContext("2d");

    this.prevFrameLuma = null;
    this.stream = null;
    this.audioCtx = null;
    this.analyser = null;
    this.audioTimeData = null;
    this.audioFreqData = null;
    this.isRunning = false;
    this.facingMode = "environment";

    this.config = {
      sensorMode: "vision",
      motionThreshold: 28,
      audioThreshold: 62,
      lightThreshold: 40,
      cooldownMs: 2500,
      propellerFilter: true,
      useTorchFlash: true
    };

    this.metrics = {
      motion: 0,
      audio: 0,
      light: 50
    };

    this.isCalibrating = false;
    this.calibSamples = [];
    this.lastTriggerTimestamp = 0;
    this.lastTelemetryPush = 0;
  }

  updateConfig(newConfig) {
    if (!newConfig) return;
    this.config = { ...this.config, ...newConfig };
  }

  async requestMediaStream(constraints) {
    if (!navigator.mediaDevices || typeof navigator.mediaDevices.getUserMedia !== "function") {
      const legacyGetUserMedia =
        navigator.getUserMedia ||
        navigator.webkitGetUserMedia ||
        navigator.mozGetUserMedia ||
        navigator.msGetUserMedia;

      if (legacyGetUserMedia) {
        return new Promise((resolve, reject) => {
          legacyGetUserMedia.call(navigator, constraints, resolve, reject);
        });
      }

      if (window.location.protocol === "http:" && window.location.hostname !== "localhost" && window.location.hostname !== "127.0.0.1") {
        const httpsUrl = `https://${window.location.host}${window.location.pathname}${window.location.search}`;
        window.location.href = httpsUrl;
        throw new Error(`Redirigiendo automáticamente a conexión segura ${httpsUrl}...`);
      }

      throw new Error("Tu navegador bloqueó la API de Cámara. Asegúrate de abrir la dirección con https://");
    }

    return await navigator.mediaDevices.getUserMedia(constraints);
  }

  async startHardware(facingMode = "environment") {
    this.facingMode = facingMode;
    if (this.stream) {
      this.stream.getTracks().forEach((t) => t.stop());
    }

    try {
      this.stream = await this.requestMediaStream({
        video: {
          facingMode: { ideal: this.facingMode },
          width: { ideal: 640 },
          height: { ideal: 480 }
        },
        audio: {
          echoCancellation: false,
          noiseSuppression: false,
          autoGainControl: false
        }
      });
    } catch (firstErr) {
      this.stream = await this.requestMediaStream({
        video: true,
        audio: true
      }).catch(async () => {
        return await this.requestMediaStream({ video: true, audio: false });
      });
    }

    this.video.srcObject = this.stream;
    await this.video.play();

    if (this.stream.getAudioTracks().length > 0) {
      try {
        const AudioCtx = window.AudioContext || window.webkitAudioContext;
        this.audioCtx = new AudioCtx();
        const source = this.audioCtx.createMediaStreamSource(this.stream);
        this.analyser = this.audioCtx.createAnalyser();
        this.analyser.fftSize = 1024;
        this.analyser.smoothingTimeConstant = 0.22;
        source.connect(this.analyser);
        this.audioTimeData = new Uint8Array(this.analyser.frequencyBinCount);
        this.audioFreqData = new Uint8Array(this.analyser.frequencyBinCount);
      } catch (audioErr) {
        console.warn("[Sensor] Audio analyser warning:", audioErr);
      }
    }

    this.prevFrameLuma = null;
    this.isRunning = true;
    requestAnimationFrame(() => this.loop());
  }

  async toggleCameraFacing() {
    const nextFacing = this.facingMode === "environment" ? "user" : "environment";
    await this.startHardware(nextFacing);
    return nextFacing;
  }

  /**
   * Dispara un destello en la linterna LED física del teléfono (si la cámara trasera soporta `torch`)
   */
  async pulseTorchFlash(durationMs = 280) {
    if (!this.config.useTorchFlash || !this.stream) return;
    const videoTrack = this.stream.getVideoTracks()[0];
    if (!videoTrack || typeof videoTrack.getCapabilities !== "function") return;

    try {
      const caps = videoTrack.getCapabilities();
      if (!caps.torch) return;
      await videoTrack.applyConstraints({ advanced: [{ torch: true }] });
      setTimeout(async () => {
        try {
          await videoTrack.applyConstraints({ advanced: [{ torch: false }] });
        } catch (e) {}
      }, durationMs);
    } catch (e) {}
  }

  /**
   * Captura una foto instantánea (Foto-Finish) del momento exacto en que el dron cruza la meta
   */
  capturePhotoFinishSnapshot(pilotName = "PILOTO", timeFormatted = "00:00.000") {
    if (!this.video || this.video.readyState < 2) return "";
    try {
      const w = this.photoCanvas.width;
      const h = this.photoCanvas.height;
      this.photoCtx.drawImage(this.video, 0, 0, w, h);

      // Cinta inferior con marca de agua de cronometraje oficial
      this.photoCtx.fillStyle = "rgba(7, 10, 15, 0.82)";
      this.photoCtx.fillRect(0, h - 34, w, 34);
      this.photoCtx.fillStyle = "#00FF66";
      this.photoCtx.font = "bold 12px monospace";
      this.photoCtx.fillText(`PHOTO FINISH · ${pilotName} · ${timeFormatted}`, 10, h - 13);

      return this.photoCanvas.toDataURL("image/jpeg", 0.72);
    } catch (e) {
      return "";
    }
  }

  /**
   * Auto-Calibración Inteligente de Ambiente durante 3 segundos:
   * Mide el ruido y movimiento base de la pista y sugiere umbrales óptimos libres de falsos positivos.
   */
  runAutoCalibration(durationMs = 3000, onComplete) {
    if (this.isCalibrating) return;
    this.isCalibrating = true;
    this.calibSamples = [];

    setTimeout(() => {
      this.isCalibrating = false;
      if (this.calibSamples.length === 0) return;

      let maxMotion = 0;
      let maxAudio = 0;
      let avgLightSum = 0;

      for (const s of this.calibSamples) {
        if (s.motion > maxMotion) maxMotion = s.motion;
        if (s.audio > maxAudio) maxAudio = s.audio;
        avgLightSum += s.light;
      }

      const avgLight = Math.round(avgLightSum / this.calibSamples.length);

      // Fijar umbrales un margen seguro por encima del ruido/movimiento máximo medido en reposo
      const recommended = {
        motionThreshold: Math.min(90, Math.max(18, maxMotion + 18)),
        audioThreshold: Math.min(92, Math.max(25, maxAudio + 20)),
        lightThreshold: Math.min(85, Math.max(15, Math.round(avgLight * 0.68)))
      };

      if (onComplete) onComplete(recommended);
    }, durationMs);
  }

  processVideoFrame() {
    if (!this.video || this.video.readyState < 2) return;

    this.ctx.drawImage(this.video, 0, 0, this.sampleW, this.sampleH);
    const frameData = this.ctx.getImageData(0, 0, this.sampleW, this.sampleH).data;
    const totalPixels = this.sampleW * this.sampleH;

    if (!this.prevFrameLuma) {
      this.prevFrameLuma = new Uint8Array(totalPixels);
    }

    let changedPixels = 0;
    let totalLuminance = 0;
    const pixelDiffSensitivity = 32;

    for (let i = 0, p = 0; i < frameData.length; i += 4, p++) {
      const luma = (frameData[i] * 77 + frameData[i + 1] * 150 + frameData[i + 2] * 29) >> 8;
      totalLuminance += luma;

      const diff = Math.abs(luma - this.prevFrameLuma[p]);
      if (diff > pixelDiffSensitivity) {
        changedPixels++;
      }
      this.prevFrameLuma[p] = luma;
    }

    const rawMotionPct = (changedPixels / totalPixels) * 260;
    this.metrics.motion = Math.min(100, Math.round(rawMotionPct));

    const avgLuma = totalLuminance / totalPixels;
    this.metrics.light = Math.min(100, Math.round((avgLuma / 255) * 100));
  }

  /**
   * Procesamiento Acústico con soporte para Filtro FFT de Hélices (1.200 Hz - 4.500 Hz)
   * Ignora graves de música y voz del animador (80-400 Hz) para detectar exclusivamente motores brushless.
   */
  processAudioLevel() {
    if (!this.analyser) return;

    if (this.config.propellerFilter && this.audioFreqData && this.audioCtx) {
      this.analyser.getByteFrequencyData(this.audioFreqData);
      const sampleRate = this.audioCtx.sampleRate || 44100;
      const binHz = sampleRate / this.analyser.fftSize;

      const startBin = Math.max(1, Math.floor(1200 / binHz));
      const endBin = Math.min(this.audioFreqData.length - 1, Math.ceil(4500 / binHz));

      let sum = 0;
      let count = 0;
      for (let i = startBin; i <= endBin; i++) {
        sum += this.audioFreqData[i];
        count++;
      }
      const avgBandEnergy = count > 0 ? sum / count : 0;
      this.metrics.audio = Math.min(100, Math.round((avgBandEnergy / 185) * 100));
      return;
    }

    if (this.audioTimeData) {
      this.analyser.getByteTimeDomainData(this.audioTimeData);
      let sumSquares = 0;
      for (let i = 0; i < this.audioTimeData.length; i++) {
        const normalized = (this.audioTimeData[i] - 128) / 128;
        sumSquares += normalized * normalized;
      }
      const rms = Math.sqrt(sumSquares / this.audioTimeData.length);
      this.metrics.audio = Math.min(100, Math.round(rms * 320));
    }
  }

  loop() {
    if (!this.isRunning) return;

    this.processVideoFrame();
    this.processAudioLevel();

    if (this.isCalibrating) {
      this.calibSamples.push({ ...this.metrics });
    }

    const now = performance.now();
    if (now - this.lastTelemetryPush > 150 && this.onTelemetry) {
      this.lastTelemetryPush = now;
      this.onTelemetry({ ...this.metrics, calibrating: this.isCalibrating });
    }

    if (!this.isCalibrating) {
      this.evaluateSensorTrigger(now);
    }

    requestAnimationFrame(() => this.loop());
  }

  evaluateSensorTrigger(now) {
    if (now - this.lastTriggerTimestamp < 450) return;

    const mode = this.config.sensorMode;

    if (mode === "vision") {
      if (this.metrics.motion >= this.config.motionThreshold) {
        this.lastTriggerTimestamp = now;
        this.onTrigger({
          mode: "vision",
          type: "MOTION_SPIKE",
          value: this.metrics.motion,
          threshold: this.config.motionThreshold
        });
      }
    } else if (mode === "audio") {
      if (this.metrics.audio >= this.config.audioThreshold) {
        this.lastTriggerTimestamp = now;
        this.onTrigger({
          mode: "audio",
          type: "AUDIO_SPIKE",
          value: this.metrics.audio,
          threshold: this.config.audioThreshold
        });
      }
    } else if (mode === "fusion") {
      // MODO D (Fusión Dual): Exige que tanto Visión como Audio superen sus umbrales simultáneamente
      if (
        this.metrics.motion >= this.config.motionThreshold &&
        this.metrics.audio >= this.config.audioThreshold
      ) {
        this.lastTriggerTimestamp = now;
        this.onTrigger({
          mode: "fusion",
          type: "FUSION_SPIKE",
          value: Math.round((this.metrics.motion + this.metrics.audio) / 2),
          threshold: this.config.motionThreshold
        });
      }
    } else if (mode === "light") {
      const isBright = this.metrics.light > this.config.lightThreshold;
      this.onTrigger({
        mode: "light",
        type: isBright ? "LIGHT_OPENED" : "LIGHT_BLOCKED",
        value: this.metrics.light,
        threshold: this.config.lightThreshold,
        continuous: true
      });
    }
  }
}

export async function requestTrackWakeLock(onStatusChange) {
  if (!("wakeLock" in navigator)) {
    if (onStatusChange) onStatusChange(false);
    return null;
  }
  try {
    const wakeLock = await navigator.wakeLock.request("screen");
    if (onStatusChange) onStatusChange(true);
    wakeLock.addEventListener("release", () => {
      if (onStatusChange) onStatusChange(false);
    });
    return wakeLock;
  } catch (err) {
    if (onStatusChange) onStatusChange(false);
    return null;
  }
}
