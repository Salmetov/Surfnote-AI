// Lightweight payload for Yandex Telemost: captures incoming audio tracks and sends mixed audio frames to the backend.

class WebSocketClient {
  static MESSAGE_TYPES = {
    JSON: 1,
    VIDEO: 2,
    AUDIO: 3,
    ENCODED_MP4_CHUNK: 4,
    PER_PARTICIPANT_AUDIO: 5,
  };

  constructor() {
    const initData =
      window.__attendeeInitialData ||
      window.initialData ||
      (window.top && window.top.__attendeeInitialData) ||
      (window.parent && window.parent.__attendeeInitialData) ||
      {};
    const port = initData.websocketPort;
    if (!port) {
      console.error("Telemost payload: websocketPort missing on window");
      this.ws = null;
      this.mediaSendingEnabled = false;
      return;
    }

    // Use "localhost" (not 127.0.0.1) to match other adapters and avoid edge-case mixed-content/localhost trust issues.
    const url = `ws://localhost:${port}`;
    console.log("Telemost payload boot, connecting to", url, "with initData", initData);
    this.ws = new WebSocket(url);
    this.ws.binaryType = "arraybuffer";
    this.mediaSendingEnabled = false;

    this.ws.onopen = () => {
      console.log("Telemost WS connected");
      // Non-Diag so backend logs it (useful for debugging connectivity).
      this.sendJson({ type: "TelemostWsConnected", message: "Telemost WS connected" });
    };
    this.ws.onerror = (e) => console.error("Telemost WS error", e?.message || e);
    this.ws.onclose = () => console.log("Telemost WS closed");
  }

  sendJson(data) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    const jsonBytes = new TextEncoder().encode(JSON.stringify(data));
    const message = new Uint8Array(4 + jsonBytes.length);
    new DataView(message.buffer).setInt32(0, WebSocketClient.MESSAGE_TYPES.JSON, true);
    message.set(jsonBytes, 4);
    this.ws.send(message.buffer);
  }

  sendMixedAudio(audioData) {
    if (!this.mediaSendingEnabled || !this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    const message = new Uint8Array(4 + audioData.buffer.byteLength);
    const view = new DataView(message.buffer);
    view.setInt32(0, WebSocketClient.MESSAGE_TYPES.AUDIO, true);
    message.set(new Uint8Array(audioData.buffer), 4);
    this.ws.send(message.buffer);
  }

  sendPerParticipantAudio(participantId, audioData) {
    if (!this.mediaSendingEnabled || !this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    try {
      const participantIdBytes = new TextEncoder().encode(participantId || "unknown");
      const message = new Uint8Array(4 + 1 + participantIdBytes.length + audioData.buffer.byteLength);
      const view = new DataView(message.buffer);

      view.setInt32(0, WebSocketClient.MESSAGE_TYPES.PER_PARTICIPANT_AUDIO, true);
      view.setUint8(4, participantIdBytes.length);
      message.set(participantIdBytes, 5);
      message.set(new Uint8Array(audioData.buffer), 5 + participantIdBytes.length);

      this.ws.send(message.buffer);
    } catch (err) {
      console.error("Error sending per-participant audio", err);
    }
  }
}

class AudioInterceptor {
  constructor(ws) {
    this.ws = ws;
    this.audioContext = null; // Lazily created to avoid autoplay blocks
    this.mixedDestination = null; // Mixed mono destination for all sources
    this.mediaElementSources = new Map(); // Map<HTMLElement, MediaElementAudioSourceNode>
    this.trackSources = new Map(); // Map<trackId, MediaStreamAudioSourceNode>
    this.trackProcessors = new Map(); // Map<trackId, AbortController> for cleanup
    this.processorNode = null; // ScriptProcessorNode that taps mixedDestination
    this.sentFormat = false;
    this.peerConnections = new Set();
    // Map<participantId, fullName>. participantId can be a track.id or a contributing source id (SSRC) depending on platform.
    this.trackNames = new Map();
    this.pendingTrackNameResolutions = new Set(); // Set<participantId>
    // WeakMap<RTCRtpReceiver, Array<RTCRtpContributingSource>>
    this.receiverToContributingSources = new WeakMap();
    // WeakMap<RTCRtpReceiver, number> last time we refreshed contributing sources (ms)
    this.receiverToContribRefreshedAt = new WeakMap();
    this.trackIdDomAttrHits = new Set(); // Set<trackId> where we saw the id in DOM attributes
    this.botName =
      (window.initialData && window.initialData.botName) ||
      (window.__attendeeInitialData && window.__attendeeInitialData.botName) ||
      null;
    this._cachedNonBotName = null;
    this._cachedNonBotNameAt = 0;
    this._lastAudioStatsAt = 0;
    this._lastActiveSpeakerDiagAt = 0;
  }

  fnv1a32(str) {
    // Deterministic small hash for stable participant ids (avoid non-ascii / very long ids).
    let h = 0x811c9dc5;
    for (let i = 0; i < str.length; i++) {
      h ^= str.charCodeAt(i);
      // 32-bit FNV-1a
      h = (h + ((h << 1) + (h << 4) + (h << 7) + (h << 8) + (h << 24))) >>> 0;
    }
    return h >>> 0;
  }

  getUiParticipantIdForName(fullName) {
    const name = (fullName || "").trim();
    if (!name) return null;
    const hash = this.fnv1a32(name).toString(16).padStart(8, "0");
    return `ui_${hash}`;
  }

  updateContributingSources(receiver, sources) {
    try {
      if (!receiver) return;
      if (!Array.isArray(sources)) return;
      this.receiverToContributingSources.set(receiver, sources);
      this.receiverToContribRefreshedAt.set(receiver, Date.now());
    } catch (_) {}
  }

  getContributingSourcesSnapshot(receiver, { forceRefresh = false } = {}) {
    try {
      if (!receiver) return null;
      if (typeof receiver.getContributingSources !== "function") return null;

      const now = Date.now();
      const last = this.receiverToContribRefreshedAt.get(receiver) || 0;
      const shouldRefresh = forceRefresh || now - last > 250;

      if (shouldRefresh) {
        const sources = receiver.getContributingSources();
        if (Array.isArray(sources)) {
          this.receiverToContributingSources.set(receiver, sources);
          this.receiverToContribRefreshedAt.set(receiver, now);
          return sources;
        }
      }

      const cached = this.receiverToContributingSources.get(receiver);
      return Array.isArray(cached) ? cached : null;
    } catch (_) {
      return null;
    }
  }

  getDominantSourceId(receiver) {
    try {
      const sources = this.getContributingSourcesSnapshot(receiver, { forceRefresh: true });
      if (!sources || !sources.length) return null;
      // Spec: RTCRtpContributingSource has {source, audioLevel, timestamp, ...}
      let best = null;
      for (const s of sources) {
        if (!s || typeof s.source === "undefined") continue;
        if (!best) {
          best = s;
          continue;
        }
        const a = typeof s.audioLevel === "number" ? s.audioLevel : -1;
        const b = typeof best.audioLevel === "number" ? best.audioLevel : -1;
        if (a > b) best = s;
      }
      return best ? String(best.source) : null;
    } catch (_) {
      return null;
    }
  }

  isLikelyHumanName(txt) {
    if (!txt) return false;
    if (txt.length < 2 || txt.length > 80) return false;
    // Filter out obvious UI strings (RU/EN) that may appear in containers.
    const badSubstrings = [
      "ваше имя",
      "подключ",
      "создать",
      "запланир",
      "настройк",
      "выйти",
      "микрофон",
      "камера",
      "встреч", // встреча / встрече / встречу
      "звонок",
      "организатор",
      "copy",
      "share",
      "mute",
      "camera",
      "microphone",
      "your name",
    ];
    const lower = txt.toLowerCase();
    if (badSubstrings.some((s) => lower.includes(s))) return false;
    return true;
  }

  findNonBotParticipantNameFromDom() {
    const now = Date.now();
    if (this._cachedNonBotName && now - this._cachedNonBotNameAt < 5000) {
      return this._cachedNonBotName;
    }

    try {
      const botName = (this.botName || "").trim();
      const selectors = [
        '[class*="DisplayName_"]',
        '[class*="peerDisplayName"]',
        '[class*="userDisplayName"]',
        '[class*="UserName_"]',
        '[class*="TextName_"]',
        '[class*="Name_"]',
      ];
      const nodes = Array.from(document.querySelectorAll(selectors.join(", ")));
      const counts = new Map();

      for (const el of nodes) {
        const txt = (el?.textContent || "").trim();
        if (!this.isLikelyHumanName(txt)) continue;
        if (botName && txt === botName) continue;
        counts.set(txt, (counts.get(txt) || 0) + 1);
      }

      let best = null;
      let bestCount = 0;
      for (const [name, count] of counts.entries()) {
        if (count > bestCount) {
          best = name;
          bestCount = count;
        }
      }

      this._cachedNonBotName = best;
      this._cachedNonBotNameAt = now;
      return best;
    } catch (e) {
      return null;
    }
  }

  collectNonBotParticipantNamesFromDom() {
    try {
      const botName = (this.botName || "").trim();
      const selectors = [
        '[class*="DisplayName_"]',
        '[class*="peerDisplayName"]',
        '[class*="userDisplayName"]',
        '[class*="UserName_"]',
        '[class*="TextName_"]',
        '[class*="Name_"]',
      ];
      const nodes = Array.from(document.querySelectorAll(selectors.join(", ")));
      const counts = new Map();

      for (const el of nodes) {
        const txt = (el?.textContent || "").trim();
        if (!this.isLikelyHumanName(txt)) continue;
        if (botName && txt === botName) continue;
        counts.set(txt, (counts.get(txt) || 0) + 1);
      }

      return Array.from(counts.entries())
        .sort((a, b) => b[1] - a[1])
        .map(([name]) => name);
    } catch (e) {
      return [];
    }
  }

  extractNameFromElement(el, trackId) {
    if (!el) return null;

    // Find a nearby participant container
    const container =
      el.closest?.('[class*="Participant"]') ||
      el.closest?.('[class*="participant"]') ||
      el.parentElement ||
      el;

    const nameSelectors = [
      '[class*="DisplayName_"]',
      '[class*="Name_"]',
      '[class*="participant-name"]',
      '[data-testid*="name"]',
      '[data-testid*="participant"]',
    ];

    const uniqueLikelyNamesIn = (root) => {
      try {
        const out = new Set();
        const nodes = [];
        try {
          nodes.push(...Array.from(root.querySelectorAll(nameSelectors.join(", "))));
        } catch (_) {}
        nodes.push(root);

        for (const n of nodes) {
          const txt = (n?.textContent || "").trim();
          if (!txt) continue;
          if (!this.isLikelyHumanName(txt)) continue;
          if (trackId && txt.includes(trackId)) continue;
          if (this.botName && txt === this.botName) continue;
          out.add(txt);
          if (out.size > 2) break; // we only care about 0/1 vs ambiguous
        }
        return Array.from(out);
      } catch (_) {
        return [];
      }
    };

    // Prefer a container that yields exactly one plausible name.
    const candidates = [];
    candidates.push(container);
    // Also try immediate ancestors (often the tile wrapper), but keep this bounded.
    let p = container?.parentElement;
    for (let i = 0; i < 4 && p; i++) {
      candidates.push(p);
      p = p.parentElement;
    }

    for (const c of candidates) {
      const names = uniqueLikelyNamesIn(c);
      if (names.length === 1) return names[0];
    }

    return null;
  }

  tryResolveNameFromDom(trackId) {
    try {
      const root = document.body;
      if (!root) return null;

      // Heuristic 1: look for any element containing trackId in attributes
      const walker = document.createTreeWalker(root, NodeFilter.SHOW_ELEMENT);
      let steps = 0;
      let node = walker.currentNode;
      while (node && steps < 8000) {
        steps++;
        try {
          if (node.attributes) {
            for (const attr of node.attributes) {
              const v = attr?.value;
              if (v && (v === trackId || v.includes(trackId))) {
                try {
                  if (!this.trackIdDomAttrHits.has(trackId)) {
                    this.trackIdDomAttrHits.add(trackId);
                    this.ws.sendJson({ type: "Diag", message: "TrackIdDomAttributeHit", trackId });
                  }
                } catch (_) {}
                const name = this.extractNameFromElement(node, trackId);
                if (name) return name;
              }
            }
          }
        } catch (_) {}
        node = walker.nextNode();
      }

      // Heuristic 2: active speaker label (if present)
      const activeSpeakerEl =
        document.querySelector('video[class*="ActiveVideo_"]') ||
        document.querySelector('[class*="ActiveVideo_"] video') ||
        document.querySelector('[aria-label*="говорит" i]') ||
        document.querySelector('[aria-label*="speaking" i]') ||
        document.querySelector('[class*="speaking" i]');
      const activeName = this.extractNameFromElement(activeSpeakerEl, trackId);
      if (activeName) return activeName;
    } catch (err) {
      console.error("Error resolving name from DOM", err);
    }
    return null;
  }

  maybeSendTrackNameUpdate(trackId, fullName, reason) {
    if (!trackId || !fullName) return;
    const current = this.trackNames.get(trackId);
    if (current === fullName) return;
    this.trackNames.set(trackId, fullName);
    this.ws.sendJson({ type: "ParticipantNameUpdate", trackId, fullName, reason: reason || "unknown" });
  }

  findActiveSpeakerNameFromDom() {
    try {
      const parseRgb = (s) => {
        if (!s) return null;
        const str = String(s).trim();
        const m = str.match(/rgba?\(([^)]+)\)/i);
        if (m) {
          const parts = m[1]
            .split(",")
            .map((p) => p.trim())
            .map((p) => (p.endsWith("%") ? Math.round((parseFloat(p) / 100) * 255) : parseFloat(p)));
          if (parts.length >= 3) return { r: parts[0] || 0, g: parts[1] || 0, b: parts[2] || 0 };
        }
        const hex = str.match(/^#([0-9a-f]{3}|[0-9a-f]{6})$/i);
        if (hex) {
          const h = hex[1];
          if (h.length === 3) {
            const r = parseInt(h[0] + h[0], 16);
            const g = parseInt(h[1] + h[1], 16);
            const b = parseInt(h[2] + h[2], 16);
            return { r, g, b };
          }
          const r = parseInt(h.slice(0, 2), 16);
          const g = parseInt(h.slice(2, 4), 16);
          const b = parseInt(h.slice(4, 6), 16);
          return { r, g, b };
        }
        return null;
      };

      const isGreenish = (rgb) => {
        if (!rgb) return false;
        const { r, g, b } = rgb;
        return g >= 140 && g > r * 1.35 && g > b * 1.35;
      };

      const hasGreenAccent = (el) => {
        try {
          if (!el) return false;
          const stylesToCheck = [];
          try {
            stylesToCheck.push(getComputedStyle(el));
          } catch (_) {}
          // Telemost often draws the green outline on a pseudo-element.
          try {
            stylesToCheck.push(getComputedStyle(el, "::before"));
          } catch (_) {}
          try {
            stylesToCheck.push(getComputedStyle(el, "::after"));
          } catch (_) {}

          for (const st of stylesToCheck) {
            if (!st) continue;
            const borderW = Math.max(
              parseFloat(st.borderTopWidth) || 0,
              parseFloat(st.borderRightWidth) || 0,
              parseFloat(st.borderBottomWidth) || 0,
              parseFloat(st.borderLeftWidth) || 0
            );
            const outlineW = parseFloat(st.outlineWidth) || 0;
            const borderColor = parseRgb(st.borderTopColor) || parseRgb(st.borderColor);
            const outlineColor = parseRgb(st.outlineColor);
            const shadow = (st.boxShadow || "").toLowerCase();

            if (borderW > 0 && isGreenish(borderColor)) return true;
            if (outlineW > 0 && isGreenish(outlineColor)) return true;

            // Some UIs render the speaking highlight as a green box-shadow.
            if (shadow && shadow.includes("rgb(")) {
              const matches = shadow.match(/rgba?\([^)]+\)/g) || [];
              for (const m of matches) {
                if (isGreenish(parseRgb(m))) return true;
              }
            }
          }
        } catch (_) {}
        return false;
      };

      const findGreenTileFromNameNode = (nameNode) => {
        let cur = nameNode;
        for (let i = 0; i < 15 && cur; i++) {
          if (hasGreenAccent(cur)) return cur;
          cur = cur.parentElement;
        }
        return null;
      };

      // Telemost визуально подсвечивает говорящего зелёной рамкой вокруг плитки.
      // Начинаем с текстовых нод имён и ищем ближайшего родителя с зелёным акцентом.
      const nameNodes = Array.from(
        document.querySelectorAll(
          [
            '[class*="DisplayName_"]',
            '[class*="peerDisplayName"]',
            '[class*="userDisplayName"]',
            '[class*="UserName_"]',
            '[class*="TextName_"]',
            '[class*="Name_"]',
            '[class*="participant-name"]',
            '[data-testid*="name"]',
          ].join(", ")
        )
      );

      for (const node of nameNodes) {
        const txt = (node?.textContent || "").trim();
        if (!txt) continue;
        if (!this.isLikelyHumanName(txt)) continue;
        if (this.botName && txt === this.botName) continue;
        const tile = findGreenTileFromNameNode(node);
        if (!tile) continue;
        // Double-check: extracting from the tile should resolve to the same unique name
        const name = this.extractNameFromElement(tile);
        if (!name) return txt;
        if (this.botName && name === this.botName) continue;
        return name;
      }

      // Fallback: legacy selectors (some layouts mark an active video element/class)
      const el =
        document.querySelector('video[class*="ActiveVideo_"]') ||
        document.querySelector('[class*="ActiveVideo_"] video') ||
        document.querySelector('[class*="ActiveSpeaker"]') ||
        document.querySelector('[class*="activeSpeaker"]') ||
        document.querySelector('[class*="Speaking"]') ||
        document.querySelector('[class*="speaking"]') ||
        document.querySelector('[aria-label*="говорит" i]') ||
        document.querySelector('[aria-label*="speaking" i]') ||
        document.querySelector('[class*="speaking" i]');

      const fallbackName = this.extractNameFromElement(el);
      if (!fallbackName) return null;
      if (this.botName && fallbackName === this.botName) return null;
      return fallbackName;
    } catch (_) {
      return null;
    }
  }

  scheduleNameResolution(trackId, reason) {
    if (!trackId) return;
    if (this.trackNames.has(trackId)) return;
    if (this.pendingTrackNameResolutions.has(trackId)) return;

    this.pendingTrackNameResolutions.add(trackId);

    const maxAttempts = 15; // ~30s at 2s interval
    let attempts = 0;
    const tick = () => {
      attempts++;
      const name = this.tryResolveNameFromDom(trackId);
      if (name) {
        this.maybeSendTrackNameUpdate(trackId, name, reason || "dom_scan");
        this.pendingTrackNameResolutions.delete(trackId);
        return;
      }
      if (attempts >= maxAttempts) {
        this.pendingTrackNameResolutions.delete(trackId);
        return;
      }
      setTimeout(tick, 2000);
    };
    setTimeout(tick, 1000);
  }

  ensureAudioGraph() {
    if (!this.audioContext) {
      try {
        this.audioContext = new AudioContext({ sampleRate: 48000 });
      } catch (err) {
        console.error("Failed to create AudioContext at 48kHz, falling back to default", err);
        this.audioContext = new AudioContext();
      }
    }
    if (!this.mixedDestination) {
      this.mixedDestination = this.audioContext.createMediaStreamDestination();
    }
    if (!this.processorNode) {
      // Tap the mixed destination with a lightweight ScriptProcessor to get PCM data
      const mixedSource = this.audioContext.createMediaStreamSource(this.mixedDestination.stream);
      const processor = this.audioContext.createScriptProcessor(4096, 1, 1);
      processor.onaudioprocess = (event) => {
        try {
          const input = event.inputBuffer;
          const samples = input.getChannelData(0);
          const mono = new Float32Array(samples.length);
          mono.set(samples);

          if (!this.sentFormat) {
            this.ws.sendJson({
              type: "AudioFormatUpdate",
              format: {
                numberOfChannels: 1,
                originalNumberOfChannels: input.numberOfChannels || 1,
                numberOfFrames: input.length,
                sampleRate: input.sampleRate,
                format: "f32",
                duration: input.length / input.sampleRate,
              },
            });
            this.sentFormat = true;
          }

          this.ws.sendMixedAudio(mono);
          this.ws.sendJson({ type: "AudioFrameSeen", samples: mono.length, sampleRate: input.sampleRate });
        } catch (err) {
          console.error("Error in mixed audio processor", err);
        }
      };

      mixedSource.connect(processor);
      // Do not connect to destination to avoid echo
      this.processorNode = processor;
    }
  }

  async resumeAudioContext() {
    if (!this.audioContext) return;
    if (this.audioContext.state === "suspended") {
      try {
        await this.audioContext.resume();
      } catch (err) {
        console.error("Failed to resume AudioContext", err);
      }
    }
  }

  processAudioTrack(event) {
    const trackId = event.track?.id || `track_${Math.random().toString(36).slice(2, 8)}`;

    try {
      this.ensureAudioGraph();
      const stream = event.streams?.[0] || new MediaStream([event.track]);
      if (this.trackSources.has(trackId)) {
        return;
      }

      console.log("Telemost audio track seen:", {
        id: trackId,
        kind: event.track.kind,
        label: event.track.label,
        enabled: event.track.enabled,
        muted: event.track.muted,
        readyState: event.track.readyState,
      });
      this.ws.sendJson({
        type: "AudioTrackSeen",
        trackId,
        label: event.track.label,
        enabled: event.track.enabled,
        muted: event.track.muted,
        readyState: event.track.readyState,
        hasReceiver: Boolean(event.receiver),
      });
      // Attempt to resolve a friendly participant name for this track (fallback path).
      this.scheduleNameResolution(trackId, "track_seen");

      const source = this.audioContext.createMediaStreamSource(stream);
      source.connect(this.mixedDestination); // feed mixed graph; no playback
      this.trackSources.set(trackId, source);

      // Per-track PCM pipeline
      try {
        const audioInterceptor = this;
        const wsClient = this.ws;
        const processor = new MediaStreamTrackProcessor({ track: event.track });
        const abortController = new AbortController();
        const resolveNameOnAudio = () => {
          // If this track is producing audio, it's a good moment to attempt DOM-based name resolution.
          try {
            if (!audioInterceptor.trackNames.has(trackId)) {
              const name = audioInterceptor.tryResolveNameFromDom(trackId);
              if (name) audioInterceptor.maybeSendTrackNameUpdate(trackId, name, "audio_activity");
            }
          } catch (_) {}
        };

        // NOTE: AudioData frames must be closed. Using a reader loop avoids leaks/backpressure issues.
        const reader = processor.readable.getReader();

        const computeRms = (arr) => {
          let sum = 0;
          for (let i = 0; i < arr.length; i++) sum += arr[i] * arr[i];
          return Math.sqrt(sum / Math.max(1, arr.length));
        };

        const run = async () => {
          let framesSeen = 0;
          try {
            while (!abortController.signal.aborted) {
              const { value: frame, done } = await reader.read();
              if (done) break;
              if (!frame) continue;
              framesSeen++;
              try {
                const numChannels = frame.numberOfChannels;
                const numSamples = frame.numberOfFrames;
                const audioData = new Float32Array(numSamples);

                if (numChannels > 1) {
                  const channelData = new Float32Array(numSamples);
                  for (let ch = 0; ch < numChannels; ch++) {
                    frame.copyTo(channelData, { planeIndex: ch });
                    for (let i = 0; i < numSamples; i++) audioData[i] += channelData[i];
                  }
                  for (let i = 0; i < numSamples; i++) audioData[i] /= numChannels;
                } else {
                  frame.copyTo(audioData, { planeIndex: 0 });
                }

                const rms = computeRms(audioData);
                // Skip near-silence to save bandwidth. (VAD will also run server-side.)
                if (rms > 1e-6) {
                  // Prefer UI-based attribution: for mixed audio, tag chunks by active speaker name from DOM.
                  // This doesn't separate audio streams; it assigns a speaker label to the mixed PCM.
                  let participantId = null;
                  try {
                    const activeName = audioInterceptor.findActiveSpeakerNameFromDom();
                    const uiId = activeName ? audioInterceptor.getUiParticipantIdForName(activeName) : null;
                    if (uiId) {
                      participantId = uiId;
                      audioInterceptor.maybeSendTrackNameUpdate(uiId, activeName, "active_speaker_ui");
                    }
                  } catch (_) {}

                  // Fallback: use the actual MediaStreamTrack id so ParticipantNameUpdate(trackId, fullName)
                  // maps onto the same participant that receives audio.
                  if (!participantId) participantId = trackId;

                  // Optional diagnostic fallback: contributing sources (SSRC). We keep this only for debugging,
                  // because SSRC/source ids do not reliably map to participant display names.
                  // If you want to re-enable SSRC-based diarization later, do it behind a feature flag.
                  // if (!participantId) {
                  //   try {
                  //     const dominantSourceId = audioInterceptor.getDominantSourceId(event.receiver);
                  //     if (dominantSourceId) participantId = `src_${dominantSourceId}`;
                  //   } catch (_) {}
                  // }

                  wsClient.sendPerParticipantAudio(participantId, audioData);
                  resolveNameOnAudio();
                }

                // Lightweight diag every ~200 frames per track
                if (framesSeen % 200 === 0) {
                  const sources = audioInterceptor.getContributingSourcesSnapshot(event.receiver, { forceRefresh: true }) || [];
                  wsClient.sendJson({
                    type: "Diag",
                    message: "PerTrackAudio",
                    trackId,
                    dominantSourceId: audioInterceptor.getDominantSourceId(event.receiver),
                    contributingSources: sources
                      .slice(0, 5)
                      .map((s) => ({
                        source: s?.source,
                        audioLevel: typeof s?.audioLevel === "number" ? s.audioLevel : null,
                        timestamp: typeof s?.timestamp === "number" ? s.timestamp : null,
                      })),
                    framesSeen,
                    rms,
                  });
                }
              } catch (err) {
                console.error("Error processing per-track audio frame", err);
              } finally {
                try {
                  frame.close();
                } catch (_) {}
              }
            }
          } catch (err) {
            if (err?.name !== "AbortError") {
              console.error("Telemost per-track audio reader error", err);
            }
          } finally {
            try {
              await reader.cancel();
            } catch (_) {}
          }
        };

        run();

        this.trackProcessors.set(trackId, abortController);
      } catch (err) {
        console.error("Error setting up per-track audio processor", err);
      }

      event.track.addEventListener("ended", () => {
        try {
          source.disconnect();
        } catch (err) {
          console.error("Error disconnecting track source", err);
        }
        const controller = this.trackProcessors.get(trackId);
        if (controller) {
          try {
            controller.abort();
          } catch (e) {
            console.error("Error aborting track processor", e);
          }
          this.trackProcessors.delete(trackId);
        }
        this.trackSources.delete(trackId);
      });

      this.resumeAudioContext();
    } catch (err) {
      console.error("Error setting up audio interceptor", err);
    }
  }

  setupWebAudioElementCapture() {
    try {
      this.ensureAudioGraph();
      this.resumeAudioContext();

      const mediaEls = Array.from(document.querySelectorAll("audio, video"));
      mediaEls.forEach((el) => {
        if (this.mediaElementSources.has(el)) return;
        try {
          const source = this.audioContext.createMediaElementSource(el);
          source.connect(this.mixedDestination);
          this.mediaElementSources.set(el, source);
          console.log("Attached AudioContext source to element", el.tagName);
        } catch (e) {
          console.error("Error attaching media element source", e);
        }
      });

      // Try to map media elements (and their MediaStreams) to participant names.
      // Many RTC apps attach a dedicated MediaStream to an <audio>/<video> element inside a participant tile.
      // We can resolve track.id -> displayed name without requiring the trackId to appear in DOM attributes.
      mediaEls.forEach((el) => {
        try {
          const stream = el.srcObject;
          if (!stream || typeof stream.getAudioTracks !== "function") return;
          const audioTracks = stream.getAudioTracks() || [];
          if (!audioTracks.length) return;
          // If an element contains multiple remote audio tracks, we cannot safely map one UI name to all of them.
          // Telemost (and other RTC apps) sometimes attach a "multi-track" stream to a single hidden <audio>.
          if (audioTracks.length !== 1) return;

          const aria = (el.getAttribute && (el.getAttribute("aria-label") || el.getAttribute("title"))) || "";
          const nameFromAria = aria && aria.length < 80 ? aria.trim() : null;
          const nameFromDom = this.extractNameFromElement(el);
          let fullName = nameFromDom || nameFromAria;
          if (!fullName) return;
          if (this.botName && fullName === this.botName) return;

          const t = audioTracks[0];
          if (t?.id) this.maybeSendTrackNameUpdate(t.id, fullName, "media_element");
        } catch (e) {
          // Ignore per-element failures
        }
      });
    } catch (err) {
      console.error("Error in setupWebAudioElementCapture", err);
    }
  }

  pollPeerConnectionsForReceivers() {
    this.peerConnections.forEach((pc) => {
      try {
        pc
          .getReceivers?.()
          ?.filter((r) => r.track && r.track.kind === "audio")
          .forEach((r) => {
            const t = r.track;
            if (t) {
              this.processAudioTrack({ track: t, receiver: r, streams: r.getStreams?.() || [] });
            }
          });
      } catch (err) {
        console.error("Error polling receivers", err);
      }
    });
  }

  logDiag() {
    try {
      this.ws.sendJson({
        type: "Diag",
        pcs: this.peerConnections.size,
        trackSources: this.trackSources.size,
        trackProcessors: this.trackProcessors.size,
        mediaEls: this.mediaElementSources.size,
        href: location.href,
      });
    } catch (err) {
      console.error("Error sending diag", err);
    }
  }

  sendActiveSpeakerDiag() {
    try {
      const now = Date.now();
      if (now - this._lastActiveSpeakerDiagAt < 2000) return;
      this._lastActiveSpeakerDiagAt = now;
      const activeName = this.findActiveSpeakerNameFromDom();
      const uiId = activeName ? this.getUiParticipantIdForName(activeName) : null;
      this.ws.sendJson({
        type: "Diag",
        message: "ActiveSpeaker",
        activeName,
        uiId,
      });
    } catch (_) {
      // ignore
    }
  }

  async sendAudioReceiverStats() {
    try {
      const now = Date.now();
      if (now - this._lastAudioStatsAt < 5000) return;
      this._lastAudioStatsAt = now;

      const receiversOut = [];
      for (const pc of this.peerConnections) {
        let receivers = [];
        try {
          receivers = (pc.getReceivers?.() || []).filter((r) => r?.track?.kind === "audio");
        } catch (_) {}

        for (const r of receivers) {
          const trackId = r?.track?.id || null;
          const syncSources = [];
          try {
            if (typeof r.getSynchronizationSources === "function") {
              const ss = r.getSynchronizationSources() || [];
              for (const s of ss.slice(0, 5)) {
                syncSources.push({
                  source: s?.source,
                  audioLevel: typeof s?.audioLevel === "number" ? s.audioLevel : null,
                  timestamp: typeof s?.timestamp === "number" ? s.timestamp : null,
                });
              }
            }
          } catch (_) {}

          const contributingSources = [];
          try {
            const cs = this.getContributingSourcesSnapshot(r, { forceRefresh: true }) || [];
            for (const s of cs.slice(0, 5)) {
              contributingSources.push({
                source: s?.source,
                audioLevel: typeof s?.audioLevel === "number" ? s.audioLevel : null,
                timestamp: typeof s?.timestamp === "number" ? s.timestamp : null,
              });
            }
          } catch (_) {}

          let inbound = [];
          try {
            const stats = await pc.getStats(r);
            stats.forEach((s) => {
              const kind = s.kind || s.mediaType || null;
              if (s.type !== "inbound-rtp") return;
              if (kind && kind !== "audio") return;
              inbound.push({
                id: s.id,
                ssrc: s.ssrc,
                packetsReceived: s.packetsReceived,
                bytesReceived: s.bytesReceived,
                jitter: s.jitter,
                audioLevel: typeof s.audioLevel === "number" ? s.audioLevel : null,
                trackIdentifier: s.trackIdentifier,
              });
            });
          } catch (_) {
            // ignore
          }
          inbound = inbound.slice(0, 5);

          receiversOut.push({
            trackId,
            hasReceiver: Boolean(r),
            trackEnabled: r?.track?.enabled,
            trackMuted: r?.track?.muted,
            trackReadyState: r?.track?.readyState,
            contributingSources,
            syncSources,
            inbound,
          });
        }
      }

      this.ws.sendJson({
        type: "Diag",
        message: "AudioReceiverStats",
        receivers: receiversOut,
      });
    } catch (err) {
      // Never break the bot on diag
    }
  }
}

// Main bootstrap
(() => {
  const bootstrapInWindow = (win) => {
    try {
      if (!win || !win.RTCPeerConnection) return;
      if (win.__attendeeAudioInterceptorInitialized) return;
      win.__attendeeAudioInterceptorInitialized = true;

      const ws = new WebSocketClient();
      const audioInterceptor = new AudioInterceptor(ws);

      try {
        ws.sendJson({
          type: "Diag",
          message: "Bootstrapping Telemost interceptor",
          href: win.location?.href,
          origin: win.location?.origin,
          iframeCount: (win.document && win.document.querySelectorAll("iframe").length) || 0,
        });
      } catch (_) {}

      const OriginalRTCPeerConnection = win.RTCPeerConnection;

      // Intercept contributing sources like we do in Google Meet to attribute audio to the dominant speaker.
      try {
        const orig = win.RTCRtpReceiver?.prototype?.getContributingSources;
        if (orig && !win.__attendeeTelemostContributingSourcesWrapped) {
          win.__attendeeTelemostContributingSourcesWrapped = true;
          win.RTCRtpReceiver.prototype.getContributingSources = function (...args) {
            const result = orig.apply(this, args);
            try {
              audioInterceptor.updateContributingSources(this, result);
            } catch (_) {}
            return result;
          };
        }
      } catch (e) {
        // ignore
      }

      const makeWrappedPeerConnection = (...args) => {
        const pc = new OriginalRTCPeerConnection(...args);
        audioInterceptor.peerConnections.add(pc);
        try {
          ws.sendJson({ type: "Diag", message: "PC created", pcs: audioInterceptor.peerConnections.size });
        } catch (_) {}

        pc.addEventListener("track", (event) => {
          if (event.track && event.track.kind === "audio") {
            audioInterceptor.processAudioTrack(event);
          }
        });

        // Poll once after negotiation
        setTimeout(() => {
          try {
            pc
              .getReceivers?.()
              ?.filter((r) => r.track && r.track.kind === "audio")
              .forEach((r) => audioInterceptor.processAudioTrack({ track: r.track, receiver: r, streams: r.getStreams?.() || [] }));
          } catch (err) {
            console.error("Error enumerating receivers", err);
          }
        }, 1000);

        return pc;
      };

      win.RTCPeerConnection = function (...args) {
        return makeWrappedPeerConnection(...args);
      };
      if (win.webkitRTCPeerConnection) {
        win.webkitRTCPeerConnection = function (...args) {
          return makeWrappedPeerConnection(...args);
        };
      }

      win.ws = ws;
      ws.enableMediaSending = async () => {
        ws.mediaSendingEnabled = true;
        await audioInterceptor.resumeAudioContext();
      };
      ws.disableMediaSending = async () => {
        ws.mediaSendingEnabled = false;
      };
      win.enableMediaSending = ws.enableMediaSending;
      win.disableMediaSending = ws.disableMediaSending;

      // Try WebAudio fallback a couple of times after join
      setTimeout(() => {
        try {
          audioInterceptor.setupWebAudioElementCapture();
        } catch (err) {
          console.error("Error in media element capture (t1)", err);
        }
      }, 2000);

      setTimeout(() => {
        try {
          audioInterceptor.setupWebAudioElementCapture();
        } catch (err) {
          console.error("Error in media element capture (t2)", err);
        }
      }, 5000);

      // Continuous polling for late-added receivers and media elements + diag
      setInterval(() => {
        try {
          audioInterceptor.pollPeerConnectionsForReceivers();
          audioInterceptor.setupWebAudioElementCapture();
          audioInterceptor.logDiag();
          audioInterceptor.sendAudioReceiverStats();
          audioInterceptor.sendActiveSpeakerDiag();
        } catch (err) {
          console.error("Error in periodic poll", err);
        }
      }, 2000);
    } catch (err) {
      console.error("Error bootstrapping audio interceptor", err);
    }
  };

  // Bootstrap in current window
  bootstrapInWindow(window);

  // Attempt to bootstrap in same-origin iframes
  const bootstrapIframes = () => {
    document.querySelectorAll("iframe").forEach((iframe) => {
      try {
        const win = iframe.contentWindow;
        if (win && win.location && win.location.origin === window.location.origin) {
          bootstrapInWindow(win);
        }
      } catch (e) {
        // Cross-origin; ignore
      }
    });
  };

  // Initial and periodic attempts (Telemost may inject iframes later)
  bootstrapIframes();
  setInterval(bootstrapIframes, 2000);
})();
