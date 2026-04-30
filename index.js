require("dotenv").config();
const http = require('http');
const WebSocket = require('ws');
const { createClient } = require('@supabase/supabase-js');
const { spawn } = require('child_process');
const fs = require('fs');
const { validateRequest } = require('twilio');
const keyManager = require('./keyManager');

const PORT = process.env.PORT || 8080;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const GOOGLE_TTS_KEY_PATH = process.env.GOOGLE_TTS_KEY_PATH;
const TWILIO_ACCOUNT_SID = process.env.TWILIO_ACCOUNT_SID;
const TWILIO_AUTH_TOKEN = process.env.TWILIO_AUTH_TOKEN;
const twilioClient = require('twilio')(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN);
const AZURE_OPENAI_ENDPOINT = process.env.AZURE_OPENAI_ENDPOINT;
const AZURE_OPENAI_DEPLOYMENT = process.env.AZURE_OPENAI_DEPLOYMENT;
const AZURE_OPENAI_API_VERSION = process.env.AZURE_OPENAI_API_VERSION;

const KATUZ_ENGINE_URL = `${SUPABASE_URL}/functions/v1/katuz-engine`;

let googleAccessToken = null;
let googleTokenExpiry = 0;

// ─── Self-service limit enforcement ──────────────────────────────────────────
const activeCalls = new Map(); // tenantId -> Set of callSids

function trackCallStart(tenantId, callSid) {
  if (!tenantId) return;
  if (!activeCalls.has(tenantId)) activeCalls.set(tenantId, new Set());
  activeCalls.get(tenantId).add(callSid);
}

function trackCallEnd(tenantId, callSid) {
  if (!tenantId || !activeCalls.has(tenantId)) return;
  activeCalls.get(tenantId).delete(callSid);
  if (activeCalls.get(tenantId).size === 0) activeCalls.delete(tenantId);
}

function getActiveCalls(tenantId) {
  return activeCalls.has(tenantId) ? activeCalls.get(tenantId).size : 0;
}

async function checkTenantLimits(supabaseClient, tenantId) {
  try {
    const { data, error } = await supabaseClient.rpc('check_voice_limits', { p_tenant_id: tenantId });
    if (error) {
      console.error('[limits] RPC error:', error.message);
      return { allowed: true, reason: 'error_fallback', max_duration_seconds: 0, max_concurrent: 999, minutes_remaining: 999 };
    }
    if (data && data.length > 0) return data[0];
    // No subscription = legacy/enterprise client, allow
    return { allowed: true, reason: 'no_subscription', max_duration_seconds: 0, max_concurrent: 999, minutes_remaining: 999 };
  } catch (err) {
    console.error('[limits] Exception:', err.message);
    return { allowed: true, reason: 'exception_fallback', max_duration_seconds: 0, max_concurrent: 999, minutes_remaining: 999 };
  }
}

async function reportVoiceUsage(supabaseClient, tenantId, durationSeconds) {
  if (!tenantId || durationSeconds <= 0) return;
  const minutesUsed = Math.ceil(durationSeconds / 60);
  try {
    await supabaseClient.rpc('increment_voice_usage', { p_tenant_id: tenantId, p_minutes: minutesUsed });
    console.log(`[usage] Reported ${minutesUsed}min for tenant=${tenantId}`);
  } catch (err) {
    console.error(`[usage] Error reporting: ${err.message}`);
  }
}
// ─── End limit enforcement ───────────────────────────────────────────────────

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
const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', uptime: Math.round(process.uptime()), keys: keyManager.getStatus(), activeTenants: activeCalls.size }));
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
    ffmpeg.stderr.on('data', () => { });
    ffmpeg.on('error', reject);
    ffmpeg.stdin.write(inputBuffer);
    ffmpeg.stdin.end();
  });
}

async function streamTTSToTwilio(text, va, streamSid, twilioWs, signal) {
  const provider = va.tts_provider ?? 'elevenlabs';
  console.log(`[TTS] Proveedor: ${provider}`);
  if (provider === 'openai') await streamOpenAITTSToTwilio(text, va.openai_voice ?? 'alloy', va.openai_tts_model ?? 'tts-1', streamSid, twilioWs, signal);
  else if (provider === 'google') await streamGoogleTTSToTwilio(text, va.google_tts_voice ?? 'es-US-Wavenet-B', va.google_tts_language ?? 'es-US', streamSid, twilioWs, signal);
  else await streamElevenLabsToTwilio(text, va.elevenlabs_voice_id, va.elevenlabs_model, streamSid, twilioWs, signal);
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
  console.log(`[integration] Llamando: ${integration.name} → ${integration.url}`);
  try {
    const headers = { 'Content-Type': 'application/json', ...(integration.headers ?? {}) };
    const method = (integration.method ?? 'POST').toUpperCase();
    const body = method === 'GET' ? undefined : JSON.stringify(params);
    const url = method === 'GET' ? `${integration.url}?${new URLSearchParams(params).toString()}` : integration.url;
    const res = await fetch(url, { method, headers, body });
    const data = await res.json();
    console.log(`[integration] Respuesta ${integration.name}:`, JSON.stringify(data).slice(0, 300));
    if (integration.name === 'validar_cobertura' && callSid && supabaseClient) {
      const cobertura = data?.success === true ? 'coverage_validated' : 'coverage_failed';
      supabaseClient.from('voice_calls').update({
        funnel_stage: cobertura,
        outcome_variables: { cobertura_positiva: data?.success === true, cobertura_negativa: data?.success !== true }
      }).eq('call_sid', callSid).then(() => { });
      console.log(`[cobertura] funnel_stage=${cobertura} callSid=${callSid}`);
    }
    return { result: data, interpretation_guide: integration.response_mapping ?? '' };
  } catch (err) {
    console.error(`[integration] Error en ${integration.name}:`, err.message);
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
    model: 'nova-2', language: 'es', encoding: 'mulaw', sample_rate: '8000',
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
  const twilioSignature = req.headers['x-twilio-signature'] ?? '';
  const fullUrl = `https://stream.yaub.ai${req.url}`;
  const isValid = validateRequest(TWILIO_AUTH_TOKEN, twilioSignature, fullUrl, {});
  if (false && !isValid) { console.warn('[Security] MONITOR - firma invalida:', req.url); }
  console.log('[Security] Firma Twilio validada OK');
  console.log('[voice-stream] Nueva conexión WS recibida:', req.url);

  const url = new URL(req.url, 'http://localhost');
  const callSid = url.searchParams.get('call_sid') ?? '';
  const phoneParam = normalizePhone(url.searchParams.get('phone') ?? '');
  let callerPhone = normalizePhone(url.searchParams.get('from') ?? '');
  console.log(`[voice-stream] callSid=${callSid} to=${phoneParam} from=${callerPhone}`);

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

  // ─── Limit enforcement state (per-connection) ─────────
  let tenantId = null;
  let callLimits = null;
  let maxDurationTimer = null;
  // ───────────────────────────────────────────────────────

  let katuzSessionId = null;
  let katuzEnabled = false;
  let katuzTurnCount = 0;
  let katuzTenantId = null;

  async function katuzCreateSession(voiceCallId, tId, assistantId) {
    try {
      const { data, error } = await supabase.from('katuz_sessions').insert({ call_sid: resolvedCallSid, voice_call_id: voiceCallId ?? null, tenant_id: tId, assistant_id: assistantId ?? null, phone_from: callerPhone || resolvedPhone, status: 'active', started_at: new Date().toISOString() }).select('id').single();
      if (error) { console.error('[Katuz] Error creando sesión:', error.message); return; }
      katuzSessionId = data.id; katuzEnabled = true; katuzTenantId = tId;
      console.log(`[Katuz] Sesión creada: ${katuzSessionId} tenant: ${tId}`);
    } catch (err) { console.error('[Katuz] katuzCreateSession error:', err.message); }
  }

  async function katuzEmitTranscript(speaker, text) {
    if (!katuzEnabled || !katuzSessionId) return;
    try {
      await supabase.from('katuz_events').insert({ session_id: katuzSessionId, tenant_id: katuzTenantId, event_type: 'transcript', speaker, content: text, ts_offset_ms: Date.now() - callStartTime, metadata: {} });
    } catch (err) { console.error('[Katuz] emit transcript error:', err.message); }
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

    // ─── Clear max duration timer ────────────────────────
    if (maxDurationTimer) { clearTimeout(maxDurationTimer); maxDurationTimer = null; }

    // ─── Track call end + report usage ───────────────────
    trackCallEnd(tenantId, resolvedCallSid);
    reportVoiceUsage(supabase, tenantId, durationSeconds);
    // ─────────────────────────────────────────────────────

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
          } catch (e) { console.error('[Recording] Error:', e.message); }
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

  function loadAssistant(phone) {
    console.log(`[loadAssistant] buscando phone="${phone}"`);
    return supabase.from('voice_assistants').select('*, assistants(id, name, prompt, llm_model, tenant_id, dashboard_type, outcome_variables)').eq('twilio_phone_number', phone).eq('is_active', true).single()
      .then(({ data, error }) => {
        va = data;
        // ─── Capture tenantId for limit enforcement ──────
        tenantId = va?.assistants?.tenant_id ?? null;
        // ─────────────────────────────────────────────────
        console.log(`[loadAssistant] resultado: ${va?.assistants?.name ?? 'null'} tenant: ${tenantId} tipo: ${va?.assistants?.dashboard_type ?? 'atencion'} integraciones: ${va?.integrations?.length ?? 0} error: ${error?.message ?? 'none'}`);
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

      const { data: call } = await supabase.from('voice_calls').select('transcript, turn_count').eq('call_sid', resolvedCallSid).single();
      const history = call?.transcript ?? [];
      const turnCount = (call?.turn_count ?? 0) + 1;
      const historyMessages = history.map(t => ({ role: t.role === 'user' ? 'user' : 'assistant', content: t.text }));
      if (signal?.aborted) return;

      const rawPrompt = va.assistants?.prompt ?? 'Eres un asistente útil.';
      const systemPrompt = rawPrompt
        .replace(/\{\{phone\}\}/g, callerPhone || 'desconocido')
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

      const ttsText = trimForTTS(cleanReply, 250);

      history.push({ role: 'user', text: transcript, ts: new Date().toISOString() }, { role: 'assistant', text: cleanReply, ts: new Date().toISOString() });
      supabase.from('voice_calls').update({ transcript: history, turn_count: turnCount, last_activity_at: new Date().toISOString() }).eq('call_sid', resolvedCallSid).then(() => { });

      if (signal?.aborted) return;
      pendingMark = true;
      await streamTTSToTwilio(ttsText, va, streamSid, twilioWs, signal);
      if (signal?.aborted) pendingMark = false;

      if (shouldHangup) {
        const despedidaMs = Math.max(4000, (ttsText.length / 15) * 1000);
        console.log(`[voice-stream] Esperando ${Math.round(despedidaMs / 1000)}s antes de colgar...`);
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
        for (const toolCall of msg.tool_calls) {
          if (signal?.aborted) return null;
          const toolName = toolCall.function.name;
          const toolParams = JSON.parse(toolCall.function.arguments);
          const integration = integrations.find(i => i.name === toolName);
          const toolResult = integration ? await callDynamicIntegration(integration, toolParams, resolvedCallSid, supabase) : { error: `Integración "${toolName}" no encontrada` };
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
      if (!isFinal) { if (isSpeaking) { interruptSpeaking(); console.log('[barge-in] Interim — bot cortado'); } return; }
      console.log(`[Deepgram] Transcript final: "${transcript}"`);
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

      loadAssistant(resolvedPhone).then(async () => {
        // ─── LIMIT ENFORCEMENT: check after assistant loads ──
        if (tenantId) {
          callLimits = await checkTenantLimits(supabase, tenantId);

          if (!callLimits.allowed) {
            console.log(`[limits] BLOCKED tenant=${tenantId} reason=${callLimits.reason}`);
            // Play a short message then hang up
            if (va) {
              isSpeaking = true;
              pendingMark = true;
              await streamTTSToTwilio('Lo siento, el servicio no está disponible en este momento. Por favor intenta más tarde.', va, streamSid, twilioWs, null);
            }
            setTimeout(() => hangupCall(), 4000);
            return;
          }

          // Check concurrent calls
          const currentActive = getActiveCalls(tenantId);
          if (callLimits.max_concurrent > 0 && callLimits.max_concurrent < 999 && currentActive >= callLimits.max_concurrent) {
            console.log(`[limits] CONCURRENT LIMIT tenant=${tenantId} active=${currentActive} max=${callLimits.max_concurrent}`);
            if (va) {
              isSpeaking = true;
              pendingMark = true;
              await streamTTSToTwilio('Todas nuestras líneas están ocupadas en este momento. Por favor intenta en unos minutos.', va, streamSid, twilioWs, null);
            }
            setTimeout(() => hangupCall(), 4000);
            return;
          }

          // Track this call
          trackCallStart(tenantId, resolvedCallSid);

          // Set max duration timer (0 = unlimited for enterprise)
          if (callLimits.max_duration_seconds > 0) {
            const warningAt = Math.max(0, callLimits.max_duration_seconds - 15) * 1000;
            // Warning 15s before cutoff
            setTimeout(async () => {
              if (callFinalized) return;
              console.log(`[limits] WARNING: 15s remaining for tenant=${tenantId}`);
              // Inject a system-level nudge on next turn — the bot will wrap up naturally
            }, warningAt);

            maxDurationTimer = setTimeout(() => {
              if (callFinalized) return;
              console.log(`[limits] MAX DURATION reached tenant=${tenantId} limit=${callLimits.max_duration_seconds}s — hanging up`);
              hangupCall();
            }, callLimits.max_duration_seconds * 1000);
          }

          console.log(`[limits] OK tenant=${tenantId} maxDur=${callLimits.max_duration_seconds}s concurrent=${currentActive + 1}/${callLimits.max_concurrent} remaining=${Math.round(callLimits.minutes_remaining)}min`);
        }
        // ─── END LIMIT ENFORCEMENT ───────────────────────────

        setTimeout(async () => {
          if (!va) { console.error('[voice-stream] va sigue null después de cargar'); return; }
          const greeting = va.greeting;
          if (greeting) { console.log(`[voice-stream] Saludo: "${greeting}"`); isSpeaking = true; pendingMark = true; await streamTTSToTwilio(trimForTTS(greeting, 250), va, streamSid, twilioWs, null); }
          else { console.log('[voice-stream] Sin greeting — esperando al usuario'); isSpeaking = false; pendingMark = false; }
        }, 300);
      });
    }
    if (msg.event === 'media') {
      const payload = msg.media?.payload ?? '';
      if (isDeepgramReady && deepgramWs?.readyState === WebSocket.OPEN) deepgramWs.send(Buffer.from(payload, 'base64'));
      else audioBuffer.push(payload);
    }
    if (msg.event === 'stop') { console.log('[Twilio] stop'); interruptSpeaking(); deepgramWs?.close(); await finalizeCall(); }
  });

  twilioWs.on('error', (e) => console.error('[Twilio WS] Error:', e.message));
  twilioWs.on('close', () => { console.log('[Twilio WS] Cerrado'); interruptSpeaking(); deepgramWs?.close(); finalizeCall(); });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[voice-stream] Listening on port ${PORT}`);
  console.log(`[KeyManager] Estado inicial:`, JSON.stringify(keyManager.getStatus(), null, 2));
});

// ─── TTS providers ────────────────────────────────────────────────────────────

async function streamElevenLabsToTwilio(text, voiceId, model = 'eleven_turbo_v2_5', streamSid, twilioWs, signal) {
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
          if (twilioWs.readyState === WebSocket.OPEN) twilioWs.send(JSON.stringify({ event: 'media', streamSid, media: { payload: toSend.toString('base64') } }));
        }
      }
    } finally { reader.releaseLock(); }
    if (!signal?.aborted && leftover.length > 0 && twilioWs.readyState === WebSocket.OPEN) twilioWs.send(JSON.stringify({ event: 'media', streamSid, media: { payload: leftover.toString('base64') } }));
    if (!signal?.aborted && twilioWs.readyState === WebSocket.OPEN) twilioWs.send(JSON.stringify({ event: 'mark', streamSid, mark: { name: 'end-of-response' } }));
  } catch (err) { if (err.name !== 'AbortError') console.error('[ElevenLabs stream] Error:', err.message); }
}

async function streamOpenAITTSToTwilio(text, voice = 'alloy', model = 'tts-1', streamSid, twilioWs, signal) {
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
      if (twilioWs.readyState === WebSocket.OPEN) twilioWs.send(JSON.stringify({ event: 'media', streamSid, media: { payload: mulawBuffer.slice(i, i + chunkSize).toString('base64') } }));
    }
    if (!signal?.aborted && twilioWs.readyState === WebSocket.OPEN) twilioWs.send(JSON.stringify({ event: 'mark', streamSid, mark: { name: 'end-of-response' } }));
  } catch (err) { if (err.name !== 'AbortError') console.error('[OpenAI TTS] Error:', err.message); }
}

async function streamGoogleTTSToTwilio(text, voice = 'es-US-Wavenet-B', languageCode = 'es-US', streamSid, twilioWs, signal) {
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
      if (twilioWs.readyState === WebSocket.OPEN) twilioWs.send(JSON.stringify({ event: 'media', streamSid, media: { payload: mulawBuffer.slice(i, i + chunkSize).toString('base64') } }));
    }
    if (!signal?.aborted && twilioWs.readyState === WebSocket.OPEN) twilioWs.send(JSON.stringify({ event: 'mark', streamSid, mark: { name: 'end-of-response' } }));
  } catch (err) { if (err.name !== 'AbortError') console.error('[Google TTS] Error:', err.message); }
}
