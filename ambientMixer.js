// ambientMixer.js — Yaub Voice Stream
// Mezcla ruido de fondo (ambient noise) con el audio TTS antes de mandarlo a Twilio.
//
// Funcionamiento:
// - Se cargan loops de audio en formato μ-law 8kHz mono al startup (una sola vez).
// - Por cada chunk de TTS que va a Twilio, se mezcla con el siguiente fragmento del loop.
// - El loop avanza un cursor por llamada (cada llamada tiene su propio offset).
// - El volumen del ambiente es configurable por asistente (0.0 a 0.30).
//
// Formato μ-law:
// - 1 byte por sample, 8kHz, mono → 8000 bytes/segundo
// - Twilio manda chunks de 640 bytes (80ms de audio)
// - Para mezclar, hay que decodear μ-law → PCM int16, sumar, y re-encodear
//
// Performance:
// - Las tablas de conversión están pre-calculadas en memoria (256 entradas)
// - Cada chunk de 640 samples toma ~0.1ms en mezclarse, despreciable

const fs = require('fs');
const path = require('path');

// ─── μ-law ↔ PCM int16 lookup tables ────────────────────────────────────────
// Pre-calculamos las 256 conversiones posibles para evitar matemática en runtime.
const MULAW_TO_PCM = new Int16Array(256);
const PCM_TO_MULAW = new Uint8Array(65536); // index = pcm + 32768

(function buildLookupTables() {
  // μ-law → PCM
  for (let i = 0; i < 256; i++) {
    let mulaw = ~i & 0xFF;
    const sign = (mulaw & 0x80) ? -1 : 1;
    const exponent = (mulaw >> 4) & 0x07;
    const mantissa = mulaw & 0x0F;
    let magnitude = ((mantissa << 3) + 0x84) << exponent;
    magnitude -= 0x84;
    MULAW_TO_PCM[i] = sign * magnitude;
  }
  // PCM → μ-law
  const BIAS = 0x84;
  const CLIP = 32635;
  for (let pcm = -32768; pcm <= 32767; pcm++) {
    let sample = pcm;
    const sign = (sample < 0) ? 0x80 : 0x00;
    if (sample < 0) sample = -sample;
    if (sample > CLIP) sample = CLIP;
    sample += BIAS;
    let exponent = 7;
    for (let mask = 0x4000; (sample & mask) === 0 && exponent > 0; mask >>= 1) exponent--;
    const mantissa = (sample >> (exponent + 3)) & 0x0F;
    PCM_TO_MULAW[pcm + 32768] = ~(sign | (exponent << 4) | mantissa) & 0xFF;
  }
})();

// ─── Ambient loops library ──────────────────────────────────────────────────
// Carga los archivos .ulaw (μ-law raw) en memoria al inicio.
// Si un archivo no existe, se loguea pero no se rompe el server.

const AMBIENT_TYPES = ['call_center', 'office', 'retail_store', 'restaurant', 'home_office', 'cafe'];
const ambientLoops = {}; // type → Buffer (μ-law bytes)

function loadAmbientLibrary(audioDir) {
  const baseDir = audioDir || path.join(__dirname, 'ambient');
  console.log(`[ambient] Loading library from ${baseDir}`);
  let loadedCount = 0;
  for (const type of AMBIENT_TYPES) {
    const filePath = path.join(baseDir, `${type}.ulaw`);
    try {
      if (fs.existsSync(filePath)) {
        ambientLoops[type] = fs.readFileSync(filePath);
        const seconds = (ambientLoops[type].length / 8000).toFixed(1);
        console.log(`[ambient] Loaded ${type}: ${ambientLoops[type].length} bytes (${seconds}s loop)`);
        loadedCount++;
      } else {
        console.warn(`[ambient] Missing file: ${filePath} — type "${type}" will be disabled`);
      }
    } catch (err) {
      console.error(`[ambient] Error loading ${type}: ${err.message}`);
    }
  }
  console.log(`[ambient] Library loaded: ${loadedCount}/${AMBIENT_TYPES.length} types ready`);
  return loadedCount;
}

// ─── Per-call ambient state ─────────────────────────────────────────────────
// Cada llamada tiene su propio cursor en el loop, así dos llamadas simultáneas
// no quedan sincronizadas (sonaría artificial).

function createAmbientState(type, volume) {
  if (!type || !ambientLoops[type]) {
    return null; // type disabled or not loaded
  }
  // Volumen seguro: clamp entre 0.0 y 0.30
  // Importante: usar isFinite + parseFloat en vez de `|| 0.08` porque
  // el `||` trataría 0.0 como falsy y nos daría el default no deseado.
  const parsed = parseFloat(volume);
  const safeVolume = Number.isFinite(parsed)
    ? Math.max(0.0, Math.min(0.30, parsed))
    : 0.08;
  // Offset aleatorio para que no todas las llamadas empiecen igual
  const initialOffset = Math.floor(Math.random() * ambientLoops[type].length);
  return {
    type,
    volume: safeVolume,
    cursor: initialOffset,
    loopLength: ambientLoops[type].length,
  };
}

// ─── Mixing function ────────────────────────────────────────────────────────
// Recibe: chunk μ-law del TTS (Buffer) + estado ambient
// Retorna: chunk μ-law mezclado (Buffer)
//
// Si ambientState es null → retorna el chunk original sin tocar (zero overhead).
// Si volumen es 0 → retorna el chunk original.

function mixChunk(ttsChunk, ambientState) {
  // Early return: si no hay ambient o volumen es 0, retorna el chunk SIN modificar
  // Esto evita el round-trip μ-law→PCM→μ-law que puede introducir diferencias de 1 bit
  if (!ambientState) return ttsChunk;
  if (ambientState.volume <= 0) return ttsChunk;
  const ambientLoop = ambientLoops[ambientState.type];
  if (!ambientLoop) return ttsChunk;

  const len = ttsChunk.length;
  const output = Buffer.allocUnsafe(len);
  const volume = ambientState.volume;
  let cursor = ambientState.cursor;
  const loopLen = ambientState.loopLength;

  for (let i = 0; i < len; i++) {
    // Decodear ambos samples a PCM int16
    const ttsPcm = MULAW_TO_PCM[ttsChunk[i]];
    const ambientPcm = MULAW_TO_PCM[ambientLoop[cursor]];

    // Mezclar: voz al 100%, ambiente al volumen configurado
    let mixed = ttsPcm + Math.round(ambientPcm * volume);

    // Clamp a int16 range
    if (mixed > 32767) mixed = 32767;
    else if (mixed < -32768) mixed = -32768;

    // Re-encodear a μ-law
    output[i] = PCM_TO_MULAW[mixed + 32768];

    // Avanzar cursor del loop con wrap-around
    cursor = (cursor + 1) % loopLen;
  }

  // Persistir el cursor para el siguiente chunk
  ambientState.cursor = cursor;

  return output;
}

// ─── Standalone ambient streaming (for silence between turns) ───────────────
// OPCIONAL: Si quieres mandar ambiente de fondo INCLUSO cuando el bot no
// está hablando (gap entre cliente y respuesta del bot), llamas a esto.
// Twilio no soporta dos streams paralelos, así que esto solo se usa si quieres
// un "filler" después de que el cliente termina de hablar pero antes de que
// llegue la primera respuesta del bot.
//
// Por ahora NO lo activamos en producción, pero lo dejo listo para que Jesús
// lo prenda después si Rodrigo lo pide.

function generateAmbientChunk(ambientState, sizeBytes = 640) {
  if (!ambientState) return null;
  const ambientLoop = ambientLoops[ambientState.type];
  if (!ambientLoop) return null;

  const output = Buffer.allocUnsafe(sizeBytes);
  const volume = ambientState.volume * 0.6; // un poco más bajo cuando NO hay voz
  let cursor = ambientState.cursor;
  const loopLen = ambientState.loopLength;

  for (let i = 0; i < sizeBytes; i++) {
    const ambientPcm = MULAW_TO_PCM[ambientLoop[cursor]];
    let attenuated = Math.round(ambientPcm * volume);
    if (attenuated > 32767) attenuated = 32767;
    else if (attenuated < -32768) attenuated = -32768;
    output[i] = PCM_TO_MULAW[attenuated + 32768];
    cursor = (cursor + 1) % loopLen;
  }

  ambientState.cursor = cursor;
  return output;
}

// ─── Public API ─────────────────────────────────────────────────────────────
module.exports = {
  loadAmbientLibrary,
  createAmbientState,
  mixChunk,
  generateAmbientChunk,
  AMBIENT_TYPES,
  // Para debugging
  getStatus: () => ({
    loaded: Object.keys(ambientLoops),
    sizes: Object.fromEntries(
      Object.entries(ambientLoops).map(([k, v]) => [k, `${(v.length / 8000).toFixed(1)}s`])
    ),
  }),
};
