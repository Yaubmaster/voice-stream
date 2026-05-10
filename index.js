require("dotenv").config();
const http = require('http');
const WebSocket = require('ws');
const { createClient } = require('@supabase/supabase-js');
const { spawn } = require('child_process');
const fs = require('fs');
const { validateRequest } = require('twilio');
const keyManager = require('./keyManager');

// ─── NEW: Ambient noise mixer ────────────────────────────────────────────────
const ambientMixer = require('./ambientMixer');
// ─────────────────────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 8080;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const GOOGLE_TTS_KEY_PATH = process.env.GOOGLE_TTS_KEY_PATH;
const TWILIO_ACCOUNT_SID = process.env.TWILIO_ACCOUNT_SID;
const TWILIO_AUTH_TOKEN = process.env.TWILIO_AUTH_TOKEN;
const AZURE_OPENAI_ENDPOINT = process.env.AZURE_OPENAI_ENDPOINT;
const AZURE_OPENAI_DEPLOYMENT = process.env.AZURE_OPENAI_DEPLOYMENT;
const AZURE_OPENAI_API_VERSION = process.env.AZURE_OPENAI_API_VERSION;

const KATUZ_ENGINE_URL = `${SUPABASE_URL}/functions/v1/katuz-engine`;

// ─── NEW: Load ambient library at startup ────────────────────────────────────
ambientMixer.loadAmbientLibrary();
// ─────────────────────────────────────────────────────────────────────────────

let googleAccessToken = null;
let googleTokenExpiry = 0;

async function getGoogleAccessToken() {
  if (googleAccessToken && Date.now() < googleTokenExpiry - 60000) return googleAccessToken;
  const keyFile = JSON.parse(fs.readFileSync(GOOGLE_TTS_KEY_PATH, 'utf8'));
  const now = Math.floor(Date.now() / 1000);
  const payload = { iss: keyFile.client_email, scope: 'https://www.googleapis.com/auth/cloud-platform', aud: 'https://oauth2.googleapis.com/token', exp: now + 3600, iat: now };
  const crypto = require('crypto');
  const header = Buffer.from(JSON.stringify({ alg: 'RS256', typ: 'JWT' })).toString('base64url');
  const body = Buffer.from(JSON.stringify(payload)).toString('base64url');
  const signingInput = `${header}.${body}`;
  const sign = crypto.createSign('RSA-SHA256');
  sign.update(signingInput);
  const signature = sign.sign(keyFile.private_key, 'base64url');
  const jwt = `${signingInput}.${signature}`;
  const res = await fetch('https://oauth2.googleapis.com/token', { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: `grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=${jwt}` });
  const data = await res.json();
  if (!data.access_token) throw new Error(`Google auth failed: ${JSON.stringify(data)}`);
  googleAccessToken = data.access_token;
  googleTokenExpiry = Date.now() + (data.expires_in * 1000);
  console.log('[Google TTS] Token renovado');
  return googleAccessToken;
}

// ─── HTTP server + health endpoint ───────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════
// v3: Mini-proxy para Gabssa (bypass TLS que Supabase Edge no soporta)
// Llamado desde external-api-proxy cuando necesita llegar a dev.gabssa.app
// Auth: requiere X-Proxy-Secret header que matchee PROXY_SHARED_SECRET
// ═══════════════════════════════════════════════════════════════
const GABSSA_API_KEY = '0eb457fb-557c-4cbf-b227-b0c9407bf9ea';
const GABSSA_BASE = 'https://dev.gabssa.app/seleccion';
const PROXY_SHARED_SECRET = process.env.PROXY_SHARED_SECRET || 'yaub-proxy-2026-xyz789';

async function handleGabssaProxy(req, res, body) {
  try {
    const secret = req.headers['x-proxy-secret'];
    if (secret !== PROXY_SHARED_SECRET) {
      res.writeHead(401, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ error: 'Unauthorized' }));
    }

    const { tool, params = {} } = JSON.parse(body || '{}');
    console.log(`[gabssa-proxy] tool=${tool}`);

    let url, method, payload;

    if (tool === 'verificar_curp') {
      const curp = (params.curp || '').toUpperCase().trim();
      if (curp.length !== 18) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ success: false, error: 'CURP inválido' }));
      }
      url = `${GABSSA_BASE}/postulante/buscarPorCurpSiEstaEnListaNegra`;
      method = 'POST';
      payload = JSON.stringify({ curp });
    } else if (tool === 'obtener_vacantes') {
      let qp = [];
      if (params.limit) qp.push(`limit=${params.limit}`);
      if (params.offset) qp.push(`offset=${params.offset}`);
      url = `${GABSSA_BASE}/requisicion/getVacantesApi${qp.length ? '?' + qp.join('&') : ''}`;
      method = 'GET';
    } else if (tool === 'crear_postulante') {
      url = `${GABSSA_BASE}/postulante/crearPostulanteApi`;
      method = 'POST';
      payload = JSON.stringify(params);
    } else {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ error: `Tool ${tool} not supported` }));
    }

    const fetchOpts = {
      method,
      headers: { 'Content-Type': 'application/json', 'x-api-key': GABSSA_API_KEY }
    };
    if (payload) fetchOpts.body = payload;

    const gabssaRes = await fetch(url, fetchOpts);
    const data = await gabssaRes.json();

    // Transform for each tool
    let result;
    if (tool === 'verificar_curp') {
      const enListaNegra = Array.isArray(data) && data.length > 0;
      result = {
        success: true,
        curp: JSON.parse(payload).curp,
        es_apto: !enListaNegra,
        en_lista_negra: enListaNegra,
        datos: enListaNegra ? data[0] : null,
        mensaje: enListaNegra ? 'No es posible continuar con el proceso.' : 'Apto para continuar.'
      };
    } else if (tool === 'obtener_vacantes') {
      if (!data.success) {
        result = { success: false, error: data.message || 'Error vacantes' };
      } else {
        // Filter only ACTIVE/OPEN vacancies (PROCESANDO/SOLICITUD/ASIGNADA)
        const ACTIVE_STATUSES = ['PROCESANDO', 'SOLICITUD', 'ASIGNADA'];
        const activeVacantes = (data.data || []).filter(v => ACTIVE_STATUSES.includes(v.DESCRIPCION_ESTATUS));

        // Deduplicate by id_vacante (API returns duplicates)
        const seenIds = new Set();
        const uniqueVacantes = activeVacantes.filter(v => {
          if (seenIds.has(v.ID_VACANTE)) return false;
          seenIds.add(v.ID_VACANTE);
          return true;
        });

        const vacantes = uniqueVacantes.map(v => ({
          id_vacante: v.ID_VACANTE, titulo: v.TITULO_VACANTE, puesto: v.NOMBRE_PUESTO,
          plaza: v.PLAZA, sueldo_neto: v.SUELDO_NETO_MENSUAL, horario: v.HORARIO,
          prestaciones: v.PRESTACIONES, requisitos: v.REQUISITOS, funciones: v.FUNCIONES,
          vacantes_disponibles: v.VACANTES_PENDIENTES, estatus: v.DESCRIPCION_ESTATUS
        }));
        result = { success: true, total: vacantes.length, total_raw: data.total, vacantes };
      }
    } else if (tool === 'crear_postulante') {
      result = data.success
        ? { success: true, postulante_id: data.data?.postulante_id || null, mensaje: 'Registrado.' }
        : { success: false, error: data.message || 'Error al registrar' };
    }

    console.log(`[gabssa-proxy] ${tool} OK`);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify(result));
  } catch (err) {
    console.error(`[gabssa-proxy] Error:`, err.message);
    res.writeHead(500, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ success: false, error: err.message }));
  }
}

const server = http.createServer((req, res) => {
  // v3: Gabssa proxy endpoint (for Supabase Edge Functions)
  if (req.method === 'POST' && req.url === '/api/gabssa-proxy') {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => handleGabssaProxy(req, res, body));
    return;
  }

  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'ok',
      uptime: Math.round(process.uptime()),
      keys: keyManager.getStatus(),
      ambient: ambientMixer.getStatus(), // NEW: visibility en /health
    }));
    return;
  }
  res.writeHead(200);
  res.end('voice-stream ok');
});

const wss = new WebSocket.Server({ server, perMessageDeflate: false });

function trimForTTS(text, maxChars = 250) {
  if (!text || text.length <= maxChars) return text;
  const cutoff = text.lastIndexOf('.', maxChars);
  if (cutoff > 60) return text.slice(0, cutoff + 1).trim();
  const space = text.lastIndexOf(' ', maxChars);
  return space > 60 ? text.slice(0, space).trim() : text.slice(0, maxChars).trim();
}

function convertMp3ToMulaw(inputBuffer) {
  return new Promise((resolve, reject) => {
    const ffmpeg = spawn('ffmpeg', ['-f', 'mp3', '-i', 'pipe:0', '-ar', '8000', '-ac', '1', '-acodec', 'pcm_mulaw', '-f', 'mulaw', 'pipe:1']);
    const chunks = [];
    ffmpeg.stdout.on('data', chunk => chunks.push(chunk));
    ffmpeg.stdout.on('end', () => resolve(Buffer.concat(chunks)));
    ffmpeg.stderr.on('data', () => {});
    ffmpeg.on('error', reject);
    ffmpeg.stdin.write(inputBuffer);
    ffmpeg.stdin.end();
  });
}

// ─── MODIFIED: streamTTSToTwilio ahora pasa ambientState y isBrowser a los providers ─────
async function streamTTSToTwilio(text, va, streamSid, twilioWs, signal, ambientState, isBrowser = false) {
  const provider = va.tts_provider ?? 'elevenlabs';
  console.log(`[TTS] Proveedor: ${provider}${ambientState ? ` ambient=${ambientState.type}@${(ambientState.volume * 100).toFixed(0)}%` : ''} isBrowser=${isBrowser}`);
  if (provider === 'deepgram') await streamDeepgramTTSToTwilio(text, va.deepgram_aura_voice ?? 'aura-2-carina-es', streamSid, twilioWs, signal, ambientState, isBrowser);
  else if (provider === 'openai') await streamOpenAITTSToTwilio(text, va.openai_voice ?? 'alloy', va.openai_tts_model ?? 'tts-1', streamSid, twilioWs, signal, ambientState, isBrowser);
  else if (provider === 'google') await streamGoogleTTSToTwilio(text, va.google_tts_voice ?? 'es-US-Wavenet-B', va.google_tts_language ?? 'es-US', streamSid, twilioWs, signal, ambientState, isBrowser);
  else await streamElevenLabsToTwilio(text, va.elevenlabs_voice_id, va.elevenlabs_model, streamSid, twilioWs, signal, ambientState, isBrowser);
}

function buildToolsFromIntegrations(integrations) {
  if (!integrations || integrations.length === 0) return [];
  const valid = integrations.filter(i => i.name && i.name.trim().length > 0);
  if (valid.length === 0) return [];
  return valid.map(integration => {
    const properties = {};
    const required = [];
    if (integration.parameters && integration.parameters.length > 0) {
      for (const param of integration.parameters) {
        properties[param.name] = { type: param.type ?? 'string', description: param.description ?? param.name };
        if (param.required) required.push(param.name);
      }
    }
    return { type: 'function', function: { name: integration.name, description: integration.description, parameters: { type: 'object', properties, required } } };
  });
}

async function callDynamicIntegration(integration, params, callSid = null, supabaseClient = null) {
  const toolName = integration.name;
  console.log(`[integration] Llamando: ${toolName}`);
  try {
    // v2: Route specific tools via external-api-proxy (handles auth, token refresh, etc.)
    const PROXY_TOOLS = new Set([
      'consultar_pedidos_activos', 'consultar_pedido',
      'registrar_queja'
    ]);
    // Gabssa tools (validar_cobertura, verificar_curp, obtener_vacantes, crear_postulante)
    // llaman directo desde Node. NODE_TLS_REJECT_UNAUTHORIZED=0 bypassa el cert malo.

    let data;
    if (PROXY_TOOLS.has(toolName)) {
      console.log(`[integration] Routing ${toolName} via external-api-proxy`);
      const proxyUrl = `${process.env.SUPABASE_URL}/functions/v1/external-api-proxy`;
      const proxyRes = await fetch(proxyUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${process.env.SUPABASE_SERVICE_ROLE_KEY}`,
        },
        body: JSON.stringify({ tool: toolName, params }),
      });
      data = await proxyRes.json();
      console.log(`[integration] Proxy result ${toolName}:`, JSON.stringify(data).slice(0, 300));
    } else {
      // Direct call for non-proxied integrations
      const headers = { 'Content-Type': 'application/json', ...(integration.headers ?? {}) };
      const method = (integration.method ?? 'POST').toUpperCase();

      // Support path params like {phone_number}
      let url = integration.url;
      if (url && url.includes('{')) {
        for (const [key, val] of Object.entries(params)) {
          url = url.replace(`{${key}}`, encodeURIComponent(val));
        }
      }
      const body = method === 'GET' ? undefined : JSON.stringify(params);
      if (method === 'GET' && !integration.url.includes('{')) {
        url = `${url}?${new URLSearchParams(params).toString()}`;
      }
      const res = await fetch(url, { method, headers, body });
      data = await res.json();
      console.log(`[integration] Respuesta ${toolName}:`, JSON.stringify(data).slice(0, 300));
    }

    // PRESERVED: cobertura funnel_stage tracking (works for both proxy + direct)
    if (toolName === 'validar_cobertura' && callSid && supabaseClient) {
      const esExitoso = data?.success === true && (data?.tiene_cobertura === true || data?.message?.success === true);
      const cobertura = esExitoso ? 'coverage_validated' : 'coverage_failed';
      supabaseClient.from('voice_calls').update({
        funnel_stage: cobertura,
        outcome_variables: { cobertura_positiva: esExitoso, cobertura_negativa: !esExitoso }
      }).eq('call_sid', callSid).then(() => {});
      console.log(`[cobertura] funnel_stage=${cobertura} callSid=${callSid}`);
    }

    return { result: data, interpretation_guide: integration.response_mapping ?? '' };
  } catch (err) {
    console.error(`[integration] Error en ${toolName}:`, err.message);
    return { error: err.message };
  }
}

async function inferCallOutcome(transcript, dashboardType, outcomeVariables) {
  if (!transcript || transcript.length < 2) return { outcome: null, variables: {}, quality_score: null, sentiment: null };
  const lastTurns = transcript.slice(-8).map(t => `${t.role}: ${t.text}`).join('\n');
  const outcomeByType = {
    ventas: '"completed" si se tomó un pedido o venta exitosa, "coverage_failed" si no hay cobertura, "abandoned" si el cliente colgó sin comprar, "escalated" si se transfirió',
    cobranza: '"promise" si el cliente prometió pagar, "refused" si se negó, "wrong_contact" si no era la persona correcta, "callback" si pidió que lo llamen después, "abandoned" si colgó',
    atencion: '"resolved" si se resolvió el problema, "escalated" si se transfirió a humano, "unresolved" si no se pudo resolver, "abandoned" si colgó sin resolver',
  };
  const outcomeOptions = outcomeByType[dashboardType ?? 'atencion'];
  let variableInstructions = '';
  if (outcomeVariables && outcomeVariables.length > 0) {
    variableInstructions = `\nTambién extrae estas variables de la conversación (null si no se mencionaron):\n` +
      outcomeVariables.map(v => `- "${v.key}": ${v.description}`).join('\n');
  }
  const systemPrompt = `Analiza esta conversación de contact center.
Responde SOLO con JSON válido sin markdown:
{
  "outcome": "<una de: ${outcomeOptions}>",
  "outcome_reason": "frase corta explicando por qué",
  "quality_score": <numero del 0 al 100 basado en empatia, resolucion y adherencia al flujo>,
  "sentiment": "<positive|neutral|negative>",
  "analysis": "<parrafo de 2-3 oraciones describiendo que paso en la llamada, que queria el cliente y como respondio el asistente>"${outcomeVariables?.length > 0 ? `,
  "variables": {${outcomeVariables?.map(v => `"${v.key}": null`).join(', ')}}` : ''}
}${variableInstructions}`;

  // Usar keyManager para inferCallOutcome — fallback a OpenAI directo si Azure falla
  const { key, endpoint, isAzure, onSuccess, onFailure } = keyManager.getLLMKey();
  const apiUrl = isAzure
    ? `${endpoint}/openai/deployments/${AZURE_OPENAI_DEPLOYMENT}/chat/completions?api-version=${AZURE_OPENAI_API_VERSION}`
    : `${endpoint}/chat/completions`;
  const apiHeaders = isAzure
    ? { 'Content-Type': 'application/json', 'api-key': key }
    : { 'Content-Type': 'application/json', 'Authorization': `Bearer ${key}` };
  const reqBody = { max_tokens: 200, temperature: 0.1, messages: [{ role: 'system', content: systemPrompt }, { role: 'user', content: lastTurns }] };
  if (!isAzure) reqBody.model = 'gpt-4o-mini';

  try {
    const res = await fetch(apiUrl, { method: 'POST', headers: apiHeaders, body: JSON.stringify(reqBody) });
    if (!res.ok) { onFailure(res.status); throw new Error(`LLM ${res.status}`); }
    const data = await res.json();
    onSuccess();
    const raw = data.choices?.[0]?.message?.content?.trim() ?? '{}';
    const parsed = JSON.parse(raw);
    console.log(`[outcome] Detectado: ${parsed.outcome} — ${parsed.outcome_reason} | score: ${parsed.quality_score} | sentiment: ${parsed.sentiment}`);
    return { outcome: parsed.outcome, variables: parsed.variables ?? {}, reason: parsed.outcome_reason, quality_score: parsed.quality_score ?? null, sentiment: parsed.sentiment ?? null, analysis: parsed.analysis ?? null };
  } catch (err) {
    console.error('[outcome] Error infiriendo outcome:', err.message);
    return { outcome: null, variables: {} };
  }
}

// ─── Deepgram connection usando keyManager ────────────────────────────────────
function createDeepgramConnection() {
  const { key, onSuccess, onFailure } = keyManager.getDeepgramKey();
  const dgUrl = 'wss://api.deepgram.com/v1/listen?' + new URLSearchParams({
    model: 'nova-2', language: 'es', encoding: 'mulaw', sample_rate: '8000', smart_format: 'true', filler_words: 'true', keywords: 'uno:2,dos:2,tres:2,cuatro:2,cinco:2,seis:2,siete:2,ocho:2,nueve:2,cero:2',
    channels: '1', interim_results: 'true', endpointing: '400'
  }).toString();
  const ws = new WebSocket(dgUrl, { headers: { Authorization: `Token ${key}` } });
  ws.on('open', () => { onSuccess(); });
  ws.on('error', (e) => {
    const status = e?.status || 500;
    if (status === 429 || status >= 500) onFailure(status);
    console.error('[Deepgram] Error:', e.message);
  });
  return ws;
}

wss.on('connection', (twilioWs, req) => {
  const url = new URL(req.url, 'http://localhost');
  const source = url.searchParams.get('source') ?? 'twilio';
  const isBrowser = source === 'browser';

  if (!isBrowser) {
    const twilioSignature = req.headers['x-twilio-signature'] ?? '';
    const fullUrl = `https://stream.yaub.ai${req.url}`;
    const isValid = validateRequest(TWILIO_AUTH_TOKEN, twilioSignature, fullUrl, {});
    if (false && !isValid) { console.warn('[Security] MONITOR - firma invalida:', req.url); }
    console.log('[Security] Firma Twilio validada OK');
  } else {
    console.log('[Security] Conexión de navegador (bypass Twilio signature)');
  }
  console.log('[voice-stream] Nueva conexión WS recibida:', req.url);

  const assistantId = url.searchParams.get('assistant_id') ?? '';
  const callSid = url.searchParams.get('call_sid') ?? (isBrowser ? `browser_${Math.random().toString(36).slice(2, 10)}` : '');
  const phoneParam = normalizePhone(url.searchParams.get('phone') ?? '');
  let callerPhone = normalizePhone(url.searchParams.get('from') ?? (isBrowser ? 'browser_user' : ''));
  console.log(`[voice-stream] source=${source} callSid=${callSid} to=${phoneParam} from=${callerPhone} assistantId=${assistantId}`);

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
  let streamSid = '';
  let deepgramWs = null;
  let isDeepgramReady = false;
  let audioBuffer = [];
  let va = null;
  let resolvedCallSid = callSid;
  let resolvedPhone = phoneParam;
  let currentAbortController = null;
  let isSpeaking = false;
  let pendingMark = false;
  let callFinalized = false;
  let recordingSid = null;
  const callStartTime = Date.now();

  // ─── NEW: Per-call ambient state ───────────────────────────────────────────
  let ambientState = null;
  // ───────────────────────────────────────────────────────────────────────────

  if (isBrowser) {
    console.log('[voice-stream] Browser connection: Triggering automatic start');
    loadAssistant(resolvedPhone, assistantId).then(() => {
      setTimeout(async () => {
        if (!va) { console.error('[voice-stream] va sigue null (browser)'); return; }
        const greeting = va.greeting;
        if (greeting) {
          console.log(`[voice-stream] Saludo (browser): "${greeting}"`);
          isSpeaking = true; pendingMark = true;
          sendAudio(twilioWs, { event: 'transcript', role: 'assistant', text: greeting, isFinal: true }, true);
          _browserHistory.push({ role: 'assistant', text: greeting, ts: new Date().toISOString() });
          await streamTTSToTwilio(greeting, va, streamSid, twilioWs, null, ambientState, true);
        }
      }, 500);
    });
  }

  let katuzSessionId = null;
  let katuzEnabled = false;
  let katuzTurnCount = 0;
  let katuzTenantId = null;

  // Browser playground sessions don't have a row in voice_calls (Twilio is the
  // only writer). Without persisted history every turn arrives with empty
  // context and the LLM resets the conversation. Keep an in-memory transcript
  // for the lifetime of the WS connection and use it instead of the DB read.
  const _browserHistory = [];

  async function katuzCreateSession(voiceCallId, tenantId, assistantId) {
    try {
      const { data, error } = await supabase.from('katuz_sessions').insert({ call_sid: resolvedCallSid, voice_call_id: voiceCallId ?? null, tenant_id: tenantId, assistant_id: assistantId ?? null, phone_from: callerPhone || resolvedPhone, status: 'active', started_at: new Date().toISOString() }).select('id').single();
      if (error) { console.error('[Katuz] Error creando sesión:', error.message); return; }
      katuzSessionId = data.id; katuzEnabled = true; katuzTenantId = tenantId;
      console.log(`[Katuz] Sesión creada: ${katuzSessionId} tenant: ${tenantId}`);
    } catch (err) { console.error('[Katuz] katuzCreateSession error:', err.message); }
  }

  async function katuzEmitTranscript(speaker, text) {
    if (!katuzEnabled || !katuzSessionId) return;
    try {
      await supabase.from('katuz_events').insert({ session_id: katuzSessionId, tenant_id: katuzTenantId, event_type: 'transcript', speaker, content: text, ts_offset_ms: Date.now() - callStartTime, metadata: {} });
    } catch (err) { console.error('[Katuz] emit transcript error:', err.message); }
  }

  async function katuzEmitToolCall(toolCalls) {
    if (!katuzEnabled || !katuzSessionId) return;
    try {
      await supabase.from('katuz_events').insert({
        session_id: katuzSessionId,
        tenant_id: katuzTenantId,
        event_type: 'tool_call',
        speaker: 'asesor',
        content: `Ejecutando ${toolCalls.length} herramientas`,
        metadata: { tool_calls: toolCalls },
        ts_offset_ms: Date.now() - callStartTime,
      });
    } catch (err) { console.error('[Katuz] emit tool call error:', err.message); }
  }

  async function katuzAnalyze(speaker, text) {
    if (!katuzEnabled || !katuzSessionId) return;
    fetch(KATUZ_ENGINE_URL, { method: 'POST', headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${SUPABASE_SERVICE_ROLE_KEY}` }, body: JSON.stringify({ session_id: katuzSessionId, tenant_id: katuzTenantId, speaker, text, turn: katuzTurnCount, ts_offset_ms: Date.now() - callStartTime }) }).catch(err => console.error('[Katuz] analyze error:', err.message));
  }

  async function katuzFinalizeSession() {
    if (!katuzEnabled || !katuzSessionId) return;
    try {
      await supabase.from('katuz_sessions').update({ status: 'completed', ended_at: new Date().toISOString(), duration_seconds: Math.round((Date.now() - callStartTime) / 1000) }).eq('id', katuzSessionId);
      console.log(`[Katuz] Sesión finalizada: ${katuzSessionId}`);
    } catch (err) { console.error('[Katuz] finalizeSession error:', err.message); }
  }

  function normalizePhone(phone) {
    return decodeURIComponent(phone).replace(/\s/g, '').replace(/\+/g, '+');
  }

  function interruptSpeaking() {
    if (!isSpeaking && !currentAbortController) return;
    console.log('[barge-in] Usuario interrumpió — cancelando respuesta en curso');
    if (streamSid && twilioWs.readyState === WebSocket.OPEN) { twilioWs.send(JSON.stringify({ event: 'clear', streamSid })); console.log('[barge-in] Clear enviado a Twilio'); }
    if (currentAbortController) { currentAbortController.abort(); currentAbortController = null; }
    isSpeaking = false; pendingMark = false;
  }

  async function finalizeCall() {
    if (callFinalized) return;
    callFinalized = true;
    const durationSeconds = Math.round((Date.now() - callStartTime) / 1000);
    console.log(`[voice-stream] Finalizando llamada — duración: ${durationSeconds}s`);

    try {
      const { data: callData } = await supabase.from('voice_calls').select('transcript').eq('call_sid', resolvedCallSid).single();
      const transcript = callData?.transcript ?? [];
      const dashboardType = va?.assistants?.dashboard_type ?? 'atencion';
      const outcomeVariables = va?.assistants?.outcome_variables ?? [];
      const { outcome, variables, quality_score, sentiment, analysis } = await inferCallOutcome(transcript, dashboardType, outcomeVariables);

      await supabase.from('voice_calls').update({
        status: 'completed',
        ended_at: new Date().toISOString(),
        duration_seconds: durationSeconds,
        outcome: outcome,
        quality_score: quality_score,
        sentiment: sentiment,
        ai_analysis: { outcome, quality_score, sentiment, analysis },
        ...(Object.keys(variables).length > 0 ? { outcome_variables: variables } : {}),
      }).eq('call_sid', resolvedCallSid);

      console.log(`[voice-stream] Llamada finalizada — outcome: ${outcome} variables: ${JSON.stringify(variables)}`);

      if (recordingSid && durationSeconds >= 20) {
        setTimeout(async () => {
          try {
            console.log(`[Recording] Descargando ${recordingSid}...`);
            await new Promise(r => setTimeout(r, 5000));
            const audioRes = await fetch(`https://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Recordings/${recordingSid}.mp3`, {
              headers: { 'Authorization': 'Basic ' + Buffer.from(`${TWILIO_ACCOUNT_SID}:${TWILIO_AUTH_TOKEN}`).toString('base64') }
            });
            if (audioRes.ok) {
              const audioBuffer = Buffer.from(await audioRes.arrayBuffer());
              const fileName = `${resolvedCallSid}.mp3`;
              const { error: uploadError } = await supabase.storage.from('call-recordings').upload(fileName, audioBuffer, { contentType: 'audio/mpeg', upsert: true });
              if (!uploadError) {
                const { data: urlData } = supabase.storage.from('call-recordings').getPublicUrl(fileName);
                await supabase.from('voice_calls').update({ recording_sid: recordingSid, recording_url: urlData?.publicUrl }).eq('call_sid', resolvedCallSid);
                console.log(`[Recording] Subida exitosa: ${fileName}`);
              } else { console.error('[Recording] Error subiendo:', uploadError.message); }
            } else { console.error('[Recording] Error descargando de Twilio:', audioRes.status); }
          } catch(e) { console.error('[Recording] Error:', e.message); }
        }, 3000);
      }
    } catch (err) {
      console.error('[finalizeCall] Error:', err.message);
      await supabase.from('voice_calls').update({ status: 'completed', ended_at: new Date().toISOString(), duration_seconds: durationSeconds }).eq('call_sid', resolvedCallSid);
    }

    await katuzFinalizeSession();
  }

  function hangupCall() {
    console.log('[voice-stream] Colgando llamada...');
    if (twilioWs.readyState === WebSocket.OPEN) {
      fetch(`https://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Calls/${resolvedCallSid}.json`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Authorization': 'Basic ' + Buffer.from(`${TWILIO_ACCOUNT_SID}:${TWILIO_AUTH_TOKEN}`).toString('base64') },
        body: 'Status=completed'
      }).then(() => console.log('[voice-stream] Llamada colgada via Twilio API'))
        .catch(err => console.error('[voice-stream] Error colgando:', err.message));
    }
  }

  function loadAssistant(phone, assistantId = null) {
    console.log(`[loadAssistant] buscando phone="${phone}" assistantId="${assistantId}"`);
    let query = supabase.from('voice_assistants').select('*, assistants(id, name, prompt, llm_model, tenant_id, dashboard_type, outcome_variables)').eq('is_active', true);
    
    if (assistantId) {
      query = query.eq('assistant_id', assistantId);
    } else {
      query = query.eq('twilio_phone_number', phone);
    }

    return query.single()
      .then(({ data, error }) => {
        va = data;
        console.log(`[loadAssistant] resultado: ${va?.assistants?.name ?? 'null'} tipo: ${va?.assistants?.dashboard_type ?? 'atencion'} integraciones: ${va?.integrations?.length ?? 0} error: ${error?.message ?? 'none'}`);
        // ... (ambient noise logic remains same)

        // ─── NEW: Initialize ambient state if enabled for this assistant ─────
        if (va?.ambient_noise_enabled === true) {
          ambientState = ambientMixer.createAmbientState(
            va.ambient_noise_type ?? 'call_center',
            va.ambient_noise_volume ?? 0.08
          );
          if (ambientState) {
            console.log(`[ambient] Activado para esta llamada: type=${ambientState.type} volume=${(ambientState.volume * 100).toFixed(1)}%`);
          } else {
            console.warn(`[ambient] Solicitado pero no disponible: type=${va.ambient_noise_type} (archivo no cargado?)`);
          }
        } else {
          console.log('[ambient] Desactivado para este asistente');
        }
        // ─────────────────────────────────────────────────────────────────────

        if (va?.katuz_enabled && va?.assistants?.tenant_id) {
          supabase.from('voice_calls').select('id').eq('call_sid', resolvedCallSid).single()
            .then(({ data: callData }) => { katuzCreateSession(callData?.id, va.assistants.tenant_id, va.assistants.id); });
        }
      });
  }

  async function runPipeline(transcript, signal) {
    try {
      if (!va) { console.error('[pipeline] va es null'); return; }
      if (signal?.aborted) return;

      katuzTurnCount++;
      katuzEmitTranscript('cliente', transcript);
      katuzAnalyze('cliente', transcript);

      let history;
      let turnCount;
      if (isBrowser) {
        history = _browserHistory;
        turnCount = Math.floor(_browserHistory.length / 2) + 1;
      } else {
        const { data: call } = await supabase.from('voice_calls').select('transcript, turn_count').eq('call_sid', resolvedCallSid).single();
        history = call?.transcript ?? [];
        turnCount = (call?.turn_count ?? 0) + 1;
      }
      const historyMessages = history.map(t => ({ role: t.role === 'user' ? 'user' : 'assistant', content: t.text }));
      if (signal?.aborted) return;

      const rawPrompt = va.assistants?.prompt ?? 'Eres un asistente útil.';
      // Strip international prefix (+52, +1, etc.) to get clean 10-digit number
      const cleanPhone = (callerPhone || '').replace(/\D/g, '').slice(-10);
      const phoneLast4 = cleanPhone.length >= 4 ? cleanPhone.slice(-4) : cleanPhone;
      const phoneLast4Pairs = phoneLast4.length === 4 ? `${phoneLast4.slice(0,2)}, ${phoneLast4.slice(2,4)}` : phoneLast4;
      const systemPrompt = rawPrompt
        .replace(/\{\{phone\}\}/g, cleanPhone || 'desconocido')
        .replace(/\{\{phone_last4\}\}/g, phoneLast4)
        .replace(/\{\{phone_last4_pairs\}\}/g, phoneLast4Pairs)
        .replace(/\{\{call_sid\}\}/g, resolvedCallSid || '');

      const model = va.assistants?.llm_model ?? 'gpt-4o-mini';
      const integrations = va.integrations ?? [];
      const dynamicTools = buildToolsFromIntegrations(integrations);
      console.log(`[pipeline] caller=${callerPhone} tools: ${dynamicTools.map(t => t.function.name).join(', ') || 'ninguna'}`);

      const messages = [
        { role: 'system', content: systemPrompt + '\n\nIMPORTANTE: Responde de forma CORTA y NATURAL para una llamada telefónica. Máximo 2-3 oraciones cortas. Sin listas ni bullets.\n\nCuando la conversación haya terminado (el cliente se despidió, completó su objetivo o indicó que no necesita más ayuda), incluye la frase exacta: [HANGUP] al final de tu respuesta.' },
        ...historyMessages,
        { role: 'user', content: transcript },
      ];

      const aiReply = await callOpenAIWithDynamicTools(model, messages, dynamicTools, integrations, signal);
      if (!aiReply || signal?.aborted) return;

      const shouldHangup = aiReply.includes('[HANGUP]');
      const cleanReply = aiReply.replace('[HANGUP]', '').trim();
      console.log(`[AI] "${cleanReply}"${shouldHangup ? ' [COLGANDO]' : ''}`);

      katuzEmitTranscript('asesor', cleanReply);
      katuzAnalyze('asesor', cleanReply);

      if (isBrowser) sendAudio(twilioWs, { event: 'transcript', role: 'assistant', text: cleanReply, isFinal: true }, true);

      const ttsText = trimForTTS(cleanReply, 250);

      history.push({ role: 'user', text: transcript, ts: new Date().toISOString() }, { role: 'assistant', text: cleanReply, ts: new Date().toISOString() });
      if (!isBrowser) {
        supabase.from('voice_calls').update({ transcript: history, turn_count: turnCount, last_activity_at: new Date().toISOString() }).eq('call_sid', resolvedCallSid).then(() => {});
      }

      if (signal?.aborted) return;
      pendingMark = true;
      // ─── MODIFIED: Pasamos ambientState al stream ────────────────────────
      await streamTTSToTwilio(ttsText, va, streamSid, twilioWs, signal, ambientState, isBrowser);
      // ─────────────────────────────────────────────────────────────────────
      if (signal?.aborted) pendingMark = false;

      if (shouldHangup) {
        const despedidaMs = Math.max(4000, (ttsText.length / 15) * 1000);
        console.log(`[voice-stream] Esperando ${Math.round(despedidaMs/1000)}s antes de colgar...`);
        setTimeout(() => { console.log('[voice-stream] Colgando después de despedida...'); hangupCall(); }, despedidaMs);
      }
    } catch (err) {
      if (err.name !== 'AbortError') console.error('[pipeline] Error:', err.message);
      pendingMark = false;
    }
  }

  // ─── LLM con keyManager: round-robin Azure → OpenAI fallback ─────────────
  async function callOpenAIWithDynamicTools(model, messages, tools, integrations, signal, retryCount = 0) {
    try {
      while (true) {
        if (signal?.aborted) return null;

        const { key, endpoint, isAzure, onSuccess, onFailure } = keyManager.getLLMKey();
        const apiUrl = isAzure
          ? `${endpoint}/openai/deployments/${AZURE_OPENAI_DEPLOYMENT}/chat/completions?api-version=${AZURE_OPENAI_API_VERSION}`
          : `${endpoint}/chat/completions`;
        const apiHeaders = isAzure
          ? { 'Content-Type': 'application/json', 'api-key': key }
          : { 'Content-Type': 'application/json', 'Authorization': `Bearer ${key}` };

        const requestBody = { max_tokens: 180, messages };
        if (!isAzure) requestBody.model = model;
        if (tools.length > 0) { requestBody.tools = tools; requestBody.tool_choice = 'auto'; }

        let res, data;
        res = await fetch(apiUrl, { method: 'POST', headers: apiHeaders, body: JSON.stringify(requestBody), signal });
        data = await res.json();

        if (res.status === 429 || res.status === 503) {
          onFailure(res.status);
          if (retryCount < 2) {
            console.warn(`[LLM] Rate limit/503 (${res.status}), rotando key (intento ${retryCount + 1}/2)...`);
            await new Promise(r => setTimeout(r, 1500));
            return callOpenAIWithDynamicTools(model, messages, tools, integrations, signal, retryCount + 1);
          }
          return 'Lo siento, ocurrió un error.';
        }

        if (!res.ok || data.error) {
          onFailure(res.status);
          console.error('[LLM] API error:', JSON.stringify(data.error ?? data));
          return 'Lo siento, ocurrió un error.';
        }

        onSuccess();
        const msg = data.choices?.[0]?.message;
        if (!msg) { console.error('[LLM] msg null, choices:', JSON.stringify(data.choices)); return 'Lo siento, ocurrió un error.'; }
        if (!msg.tool_calls || msg.tool_calls.length === 0) return msg.content?.trim() ?? 'Lo siento, ocurrió un error.';

        console.log(`[function-calling] LLM solicitó ${msg.tool_calls.length} tool(s)`);
        messages.push(msg);
        katuzEmitToolCall(msg.tool_calls);
        for (const toolCall of msg.tool_calls) {
          if (signal?.aborted) return null;
          const toolName = toolCall.function.name;
          const toolParams = JSON.parse(toolCall.function.arguments);
          const integration = integrations.find(i => i.name === toolName);
          
          if (isBrowser) sendAudio(twilioWs, { event: 'tool_call', name: toolName, params: toolParams }, true);
          const toolResult = integration ? await callDynamicIntegration(integration, toolParams, resolvedCallSid, supabase) : { error: `Integración "${toolName}" no encontrada` };
          if (isBrowser) sendAudio(twilioWs, { event: 'tool_result', name: toolName, result: toolResult }, true);

          messages.push({ role: 'tool', tool_call_id: toolCall.id, content: JSON.stringify(toolResult) });
        }
      }
    } catch (err) {
      if (err.name === 'AbortError') return null;
      console.error('[LLM tools] Exception:', err.message);
      return 'Lo siento, ocurrió un error.';
    }
  }

  function connectDeepgram() {
    deepgramWs = createDeepgramConnection();
    deepgramWs.on('open', () => { console.log('[Deepgram] Conectado'); isDeepgramReady = true; for (const chunk of audioBuffer) deepgramWs.send(Buffer.from(chunk, 'base64')); audioBuffer = []; });
    deepgramWs.on('message', async (data) => {
      const msg = JSON.parse(data.toString());
      const transcript = msg?.channel?.alternatives?.[0]?.transcript ?? '';
      const isFinal = msg?.is_final === true;
      if (!transcript.trim()) return;
      if (!isFinal) { 
        if (isSpeaking) { interruptSpeaking(); console.log('[barge-in] Interim — bot cortado'); } 
        if (isBrowser) sendAudio(twilioWs, { event: 'transcript', text: transcript, isFinal: false }, true);
        return; 
      }
      console.log(`[Deepgram] Transcript final: "${transcript}"`);
      if (isBrowser) sendAudio(twilioWs, { event: 'transcript', text: transcript, isFinal: true }, true);
      if (isSpeaking) { interruptSpeaking(); await new Promise(r => setTimeout(r, 150)); }
      isSpeaking = true;
      const controller = new AbortController();
      currentAbortController = controller;
      try { await runPipeline(transcript, controller.signal); }
      catch (err) { if (err.name !== 'AbortError') console.error('[Deepgram handler] Error:', err.message); }
      finally { if (currentAbortController === controller) currentAbortController = null; }
    });
    deepgramWs.on('error', (e) => console.error('[Deepgram] Error:', e.message));
    deepgramWs.on('close', () => console.log('[Deepgram] Cerrado'));
  }

  connectDeepgram();

  twilioWs.on('message', async (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return; }
    if (msg.event === 'connected') console.log('[Twilio] connected');
    if (msg.event === 'mark') {
      if (msg.mark?.name === 'end-of-response' && pendingMark) { pendingMark = false; isSpeaking = false; console.log('[Twilio] Mark confirmado — bot terminó de hablar'); }
    }
    if (msg.event === 'start') {
      streamSid = msg.start?.streamSid ?? '';
      const params = msg.start?.customParameters ?? {};
      if (params.callSid) resolvedCallSid = params.callSid;
      if (params.phone) resolvedPhone = normalizePhone(params.phone);
      else if (resolvedPhone === '') resolvedPhone = normalizePhone(url.searchParams.get('phone') ?? '');
      if (params.from) callerPhone = normalizePhone(params.from);
      console.log(`[Twilio] start streamSid=${streamSid} callSid=${resolvedCallSid} to="${resolvedPhone}" from="${callerPhone}"`);

      fetch(`https://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Calls/${resolvedCallSid}/Recordings.json`, {
        method: 'POST',
        headers: { 'Authorization': 'Basic ' + Buffer.from(`${TWILIO_ACCOUNT_SID}:${TWILIO_AUTH_TOKEN}`).toString('base64'), 'Content-Type': 'application/x-www-form-urlencoded' },
        body: 'RecordingChannels=dual'
      }).then(async (recRes) => {
        const recData = await recRes.json();
        if (recData.sid) { recordingSid = recData.sid; console.log(`[Recording] Iniciada: ${recData.sid}`); }
        else { console.error('[Recording] Error iniciando:', JSON.stringify(recData)); }
      }).catch(e => console.error('[Recording] fetch error:', e.message));

      loadAssistant(resolvedPhone, assistantId).then(() => {
        setTimeout(async () => {
          if (!va) { console.error('[voice-stream] va sigue null después de cargar'); return; }
          const greeting = va.greeting;
          if (greeting) {
            console.log(`[voice-stream] Saludo: "${greeting}"`);
            isSpeaking = true;
            pendingMark = true;
            if (isBrowser) sendAudio(twilioWs, { event: 'transcript', role: 'assistant', text: greeting, isFinal: true }, true);
            // ─── MODIFIED: ambientState y isBrowser también van en el saludo ──────────────
            await streamTTSToTwilio(greeting, va, streamSid, twilioWs, null, ambientState, isBrowser);
            // ─────────────────────────────────────────────────────────────────
          }
          else { console.log('[voice-stream] Sin greeting — esperando al usuario'); isSpeaking = false; pendingMark = false; }
        }, 300);
      });
    }
    if (msg.event === 'media') {
      const payload = msg.media?.payload ?? '';
      if (isDeepgramReady && deepgramWs?.readyState === WebSocket.OPEN) {
        deepgramWs.send(Buffer.from(payload, 'base64'));
      }
      else audioBuffer.push(payload);
    }
    // NOTE: A previous "binary audio passthrough" branch lived here. It was
    // unconditionally re-sending the raw WebSocket frame (always a Buffer of
    // the JSON string) to Deepgram alongside the actual mulaw payload, which
    // poisoned Deepgram's input with garbage bytes and prevented any speech
    // detection from browser sources. Removed.
    if (msg.event === 'stop') { console.log('[Twilio] stop'); interruptSpeaking(); deepgramWs?.close(); await finalizeCall(); }
  });

  twilioWs.on('error', (e) => console.error('[Twilio WS] Error:', e.message));
  twilioWs.on('close', () => { console.log('[Twilio WS] Cerrado'); interruptSpeaking(); deepgramWs?.close(); finalizeCall(); });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[voice-stream] Listening on port ${PORT}`);
  console.log(`[KeyManager] Estado inicial:`, JSON.stringify(keyManager.getStatus(), null, 2));
  console.log(`[ambient] Estado inicial:`, JSON.stringify(ambientMixer.getStatus(), null, 2));
});

// ─── TTS providers ────────────────────────────────────────────────────────────
// Todos reciben ambientState y isBrowser como últimos parámetros.

function sendAudio(ws, eventData, isBrowser) {
  if (ws.readyState !== WebSocket.OPEN) return;
  // El navegador recibe el mismo formato JSON que Twilio para simplicidad
  ws.send(JSON.stringify(eventData));
}

async function streamDeepgramTTSToTwilio(text, voice = 'aura-2-carina-es', streamSid, twilioWs, signal, ambientState, isBrowser = false) {
  const { key, onSuccess, onFailure } = keyManager.getDeepgramKey();
  try {
    console.log(`[Deepgram TTS] voz=${voice} texto="${text.slice(0, 60)}..."`);
    const res = await fetch(`https://api.deepgram.com/v1/speak?model=${voice}&encoding=mulaw&sample_rate=8000&container=none`, {
      method: 'POST',
      headers: { 'Authorization': `Token ${key}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ text }),
      signal
    });
    if (!res.ok) { onFailure(res.status); console.error(`[Deepgram TTS] Error ${res.status}:`, await res.text()); return; }
    onSuccess();
    const reader = res.body.getReader();
    let leftover = Buffer.alloc(0);
    const chunkSize = 640;
    try {
      while (true) {
        if (signal?.aborted) { await reader.cancel(); return; }
        const { done, value } = await reader.read();
        if (done) break;
        leftover = Buffer.concat([leftover, Buffer.from(value)]);
        while (leftover.length >= chunkSize) {
          if (signal?.aborted) { await reader.cancel(); return; }
          const toSend = leftover.slice(0, chunkSize); leftover = leftover.slice(chunkSize);
          const mixed = ambientMixer.mixChunk(toSend, ambientState);
          sendAudio(twilioWs, { event: 'media', streamSid, media: { payload: mixed.toString('base64') } }, isBrowser);
        }
      }
    } finally { reader.releaseLock(); }
    if (!signal?.aborted && leftover.length > 0) {
      const mixed = ambientMixer.mixChunk(leftover, ambientState);
      sendAudio(twilioWs, { event: 'media', streamSid, media: { payload: mixed.toString('base64') } }, isBrowser);
    }
    if (!signal?.aborted) sendAudio(twilioWs, { event: 'mark', streamSid, mark: { name: 'end-of-response' } }, isBrowser);
  } catch (err) { if (err.name !== 'AbortError') console.error('[Deepgram TTS] Error:', err.message); }
}

async function streamElevenLabsToTwilio(text, voiceId, model = 'eleven_turbo_v2_5', streamSid, twilioWs, signal, ambientState, isBrowser = false) {
  const { key, onSuccess, onFailure } = keyManager.getElevenLabsKey();
  try {
    const res = await fetch(`https://api.elevenlabs.io/v1/text-to-speech/${voiceId}/stream?output_format=ulaw_8000`, {
      method: 'POST',
      headers: { 'xi-api-key': key, 'Content-Type': 'application/json' },
      body: JSON.stringify({ text, model_id: model, voice_settings: { stability: 0.5, similarity_boost: 0.75, style: 0.0, use_speaker_boost: true } }),
      signal
    });
    if (!res.ok) { onFailure(res.status); console.error(`[ElevenLabs] Error ${res.status}:`, await res.text()); return; }
    onSuccess();
    const reader = res.body.getReader();
    let leftover = Buffer.alloc(0);
    const chunkSize = 640;
    try {
      while (true) {
        if (signal?.aborted) { await reader.cancel(); return; }
        const { done, value } = await reader.read();
        if (done) break;
        leftover = Buffer.concat([leftover, Buffer.from(value)]);
        while (leftover.length >= chunkSize) {
          if (signal?.aborted) { await reader.cancel(); return; }
          const toSend = leftover.slice(0, chunkSize); leftover = leftover.slice(chunkSize);
          const mixed = ambientMixer.mixChunk(toSend, ambientState);
          sendAudio(twilioWs, { event: 'media', streamSid, media: { payload: mixed.toString('base64') } }, isBrowser);
        }
      }
    } finally { reader.releaseLock(); }
    if (!signal?.aborted && leftover.length > 0) {
      const mixed = ambientMixer.mixChunk(leftover, ambientState);
      sendAudio(twilioWs, { event: 'media', streamSid, media: { payload: mixed.toString('base64') } }, isBrowser);
    }
    if (!signal?.aborted) sendAudio(twilioWs, { event: 'mark', streamSid, mark: { name: 'end-of-response' } }, isBrowser);
  } catch (err) { if (err.name !== 'AbortError') console.error('[ElevenLabs stream] Error:', err.message); }
}

async function streamOpenAITTSToTwilio(text, voice = 'alloy', model = 'tts-1', streamSid, twilioWs, signal, ambientState, isBrowser = false) {
  const { key, onSuccess, onFailure } = keyManager.getLLMKey();
  try {
    console.log(`[OpenAI TTS] voz=${voice} modelo=${model}`);
    const res = await fetch('https://api.openai.com/v1/audio/speech', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${key}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ model, input: text, voice, response_format: 'mp3', speed: 1.0 }),
      signal
    });
    if (!res.ok) { onFailure(res.status); console.error(`[OpenAI TTS] Error ${res.status}:`, await res.text()); return; }
    onSuccess();
    if (signal?.aborted) return;
    const mp3Buffer = Buffer.from(await res.arrayBuffer());
    if (signal?.aborted) return;
    const mulawBuffer = await convertMp3ToMulaw(mp3Buffer);
    if (signal?.aborted) return;
    const chunkSize = 640;
    for (let i = 0; i < mulawBuffer.length; i += chunkSize) {
      if (signal?.aborted) return;
      const chunk = mulawBuffer.slice(i, i + chunkSize);
      const mixed = ambientMixer.mixChunk(chunk, ambientState);
      sendAudio(twilioWs, { event: 'media', streamSid, media: { payload: mixed.toString('base64') } }, isBrowser);
    }
    if (!signal?.aborted) sendAudio(twilioWs, { event: 'mark', streamSid, mark: { name: 'end-of-response' } }, isBrowser);
  } catch (err) { if (err.name !== 'AbortError') console.error('[OpenAI TTS] Error:', err.message); }
}

async function streamGoogleTTSToTwilio(text, voice = 'es-US-Wavenet-B', languageCode = 'es-US', streamSid, twilioWs, signal, ambientState, isBrowser = false) {
  const { key: googleKey, onSuccess, onFailure } = keyManager.getGoogleKey();
  try {
    console.log(`[Google TTS] voz=${voice} idioma=${languageCode}`);
    const accessToken = await getGoogleAccessToken();
    if (signal?.aborted) return;
    const res = await fetch('https://texttospeech.googleapis.com/v1/text:synthesize', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${accessToken}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ input: { text }, voice: { languageCode, name: voice }, audioConfig: { audioEncoding: 'MP3', speakingRate: 1.0 } }),
      signal
    });
    if (!res.ok) { onFailure(res.status); console.error(`[Google TTS] Error ${res.status}:`, await res.text()); return; }
    onSuccess();
    if (signal?.aborted) return;
    const data = await res.json();
    const mp3Buffer = Buffer.from(data.audioContent, 'base64');
    console.log(`[Google TTS] MP3: ${mp3Buffer.length} bytes`);
    if (signal?.aborted) return;
    const mulawBuffer = await convertMp3ToMulaw(mp3Buffer);
    console.log(`[Google TTS] mulaw: ${mulawBuffer.length} bytes`);
    if (signal?.aborted) return;
    const chunkSize = 640;
    for (let i = 0; i < mulawBuffer.length; i += chunkSize) {
      if (signal?.aborted) return;
      const chunk = mulawBuffer.slice(i, i + chunkSize);
      const mixed = ambientMixer.mixChunk(chunk, ambientState);
      sendAudio(twilioWs, { event: 'media', streamSid, media: { payload: mixed.toString('base64') } }, isBrowser);
    }
    if (!signal?.aborted) sendAudio(twilioWs, { event: 'mark', streamSid, mark: { name: 'end-of-response' } }, isBrowser);
  } catch (err) { if (err.name !== 'AbortError') console.error('[Google TTS] Error:', err.message); }
}
