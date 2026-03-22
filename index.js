require("dotenv").config();
const http = require('http');
const WebSocket = require('ws');
const { createClient } = require('@supabase/supabase-js');
const { spawn } = require('child_process');
const fs = require('fs');

const PORT = process.env.PORT || 8080;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const ELEVENLABS_API_KEY = process.env.ELEVENLABS_API_KEY;
const DEEPGRAM_API_KEY = process.env.DEEPGRAM_API_KEY;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const GOOGLE_TTS_KEY_PATH = process.env.GOOGLE_TTS_KEY_PATH;

const KATUZ_ENGINE_URL = `${SUPABASE_URL}/functions/v1/katuz-engine`;

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

const server = http.createServer((req, res) => { res.writeHead(200); res.end('voice-stream ok'); });
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

async function streamTTSToTwilio(text, va, streamSid, twilioWs, signal) {
  const provider = va.tts_provider ?? 'elevenlabs';
  console.log(`[TTS] Proveedor: ${provider}`);
  if (provider === 'openai') await streamOpenAITTSToTwilio(text, va.openai_voice ?? 'alloy', va.openai_tts_model ?? 'tts-1', streamSid, twilioWs, signal);
  else if (provider === 'google') await streamGoogleTTSToTwilio(text, va.google_tts_voice ?? 'es-US-Wavenet-B', va.google_tts_language ?? 'es-US', streamSid, twilioWs, signal);
  else await streamElevenLabsToTwilio(text, va.elevenlabs_voice_id, va.elevenlabs_model, streamSid, twilioWs, signal);
}

function buildToolsFromIntegrations(integrations) {
  if (!integrations || integrations.length === 0) return [];
  return integrations.map(integration => {
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

async function callDynamicIntegration(integration, params) {
  console.log(`[integration] Llamando: ${integration.name} → ${integration.url}`);
  try {
    const headers = { 'Content-Type': 'application/json', ...(integration.headers ?? {}) };
    const method = (integration.method ?? 'POST').toUpperCase();
    const body = method === 'GET' ? undefined : JSON.stringify(params);
    const url = method === 'GET' ? `${integration.url}?${new URLSearchParams(params).toString()}` : integration.url;
    const res = await fetch(url, { method, headers, body });
    const data = await res.json();
    console.log(`[integration] Respuesta ${integration.name}:`, JSON.stringify(data).slice(0, 300));
    return { result: data, interpretation_guide: integration.response_mapping ?? '' };
  } catch (err) {
    console.error(`[integration] Error en ${integration.name}:`, err.message);
    return { error: err.message };
  }
}

wss.on('connection', (twilioWs, req) => {
  console.log('[voice-stream] Nueva conexión WS recibida:', req.url);
  const url = new URL(req.url, 'http://localhost');
  const callSid = url.searchParams.get('call_sid') ?? '';
  const phoneParam = normalizePhone(url.searchParams.get('phone') ?? '');
  // callerPhone = número de quien llama (From), phoneParam = número del asistente (To)
  const callerPhone = normalizePhone(url.searchParams.get('from') ?? '');
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
  const callStartTime = Date.now();

  let katuzSessionId = null;
  let katuzEnabled = false;
  let katuzTurnCount = 0;
  let katuzTenantId = null;

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
    const durationSeconds = Math.round((Date.now() - callStartTime) / 1000);
    console.log(`[voice-stream] Finalizando llamada — duración: ${durationSeconds}s`);
    if (resolvedCallSid) await supabase.from('voice_calls').update({ status: 'completed', ended_at: new Date().toISOString(), duration_seconds: durationSeconds }).eq('call_sid', resolvedCallSid);
    await katuzFinalizeSession();
  }

  function loadAssistant(phone) {
    console.log(`[loadAssistant] buscando phone="${phone}"`);
    return supabase.from('voice_assistants').select('*, assistants(id, name, prompt, llm_model, tenant_id)').eq('twilio_phone_number', phone).eq('is_active', true).single()
      .then(({ data, error }) => {
        va = data;
        console.log(`[loadAssistant] resultado: ${va?.assistants?.name ?? 'null'} proveedor: ${va?.tts_provider ?? 'elevenlabs'} integraciones: ${va?.integrations?.length ?? 0} error: ${error?.message ?? 'none'}`);
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

      // Inyectar variables dinámicas — {{phone}} = número del cliente (From)
      const rawPrompt = va.assistants?.prompt ?? 'Eres un asistente útil.';
      const systemPrompt = rawPrompt
        .replace(/\{\{phone\}\}/g, callerPhone || 'desconocido')
        .replace(/\{\{call_sid\}\}/g, resolvedCallSid || '');

      const model = va.assistants?.llm_model ?? 'gpt-4o-mini';
      const integrations = va.integrations ?? [];
      const dynamicTools = buildToolsFromIntegrations(integrations);
      console.log(`[pipeline] caller=${callerPhone} tools: ${dynamicTools.map(t => t.function.name).join(', ') || 'ninguna'}`);

      const messages = [
        { role: 'system', content: systemPrompt + '\n\nIMPORTANTE: Responde de forma CORTA y NATURAL para una llamada telefónica. Máximo 2-3 oraciones cortas. Sin listas ni bullets.' },
        ...historyMessages,
        { role: 'user', content: transcript },
      ];

      const aiReply = await callOpenAIWithDynamicTools(model, messages, dynamicTools, integrations, signal);
      if (!aiReply || signal?.aborted) return;

      console.log(`[AI] "${aiReply}"`);
      katuzEmitTranscript('asesor', aiReply);
      katuzAnalyze('asesor', aiReply);

      const ttsText = trimForTTS(aiReply, 250);
      if (ttsText !== aiReply) console.log(`[TTS] Texto recortado de ${aiReply.length} a ${ttsText.length} chars`);

      history.push({ role: 'user', text: transcript, ts: new Date().toISOString() }, { role: 'assistant', text: aiReply, ts: new Date().toISOString() });
      supabase.from('voice_calls').update({ transcript: history, turn_count: turnCount, last_activity_at: new Date().toISOString() }).eq('call_sid', resolvedCallSid).then(() => {});

      if (signal?.aborted) return;
      pendingMark = true;
      await streamTTSToTwilio(ttsText, va, streamSid, twilioWs, signal);
      if (signal?.aborted) pendingMark = false;
    } catch (err) {
      if (err.name !== 'AbortError') console.error('[pipeline] Error:', err.message);
      pendingMark = false;
    }
  }

  async function callOpenAIWithDynamicTools(model, messages, tools, integrations, signal) {
    try {
      while (true) {
        if (signal?.aborted) return null;
        const requestBody = { model, max_tokens: 150, messages };
        if (tools.length > 0) { requestBody.tools = tools; requestBody.tool_choice = 'auto'; }
        const res = await fetch('https://api.openai.com/v1/chat/completions', { method: 'POST', headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${OPENAI_API_KEY}` }, body: JSON.stringify(requestBody), signal });
        const data = await res.json();
        const msg = data.choices?.[0]?.message;
        if (!msg) return 'Lo siento, ocurrió un error.';
        if (!msg.tool_calls || msg.tool_calls.length === 0) return msg.content?.trim() ?? 'Lo siento, ocurrió un error.';
        console.log(`[function-calling] OpenAI solicitó ${msg.tool_calls.length} tool(s)`);
        messages.push(msg);
        for (const toolCall of msg.tool_calls) {
          if (signal?.aborted) return null;
          const toolName = toolCall.function.name;
          const toolParams = JSON.parse(toolCall.function.arguments);
          const integration = integrations.find(i => i.name === toolName);
          const toolResult = integration ? await callDynamicIntegration(integration, toolParams) : { error: `Integración "${toolName}" no encontrada` };
          messages.push({ role: 'tool', tool_call_id: toolCall.id, content: JSON.stringify(toolResult) });
        }
      }
    } catch (err) {
      if (err.name === 'AbortError') return null;
      console.error('[OpenAI tools] Exception:', err.message);
      return 'Lo siento, ocurrió un error.';
    }
  }

  function connectDeepgram() {
    const dgUrl = 'wss://api.deepgram.com/v1/listen?' + new URLSearchParams({ model: 'nova-2', language: 'es', encoding: 'mulaw', sample_rate: '8000', channels: '1', interim_results: 'true', endpointing: '400' }).toString();
    deepgramWs = new WebSocket(dgUrl, { headers: { Authorization: `Token ${DEEPGRAM_API_KEY}` } });
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
      console.log(`[Twilio] start streamSid=${streamSid} callSid=${resolvedCallSid} to="${resolvedPhone}" from="${callerPhone}"`);
      loadAssistant(resolvedPhone).then(() => {
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

server.listen(PORT, '0.0.0.0', () => { console.log(`[voice-stream] Listening on port ${PORT}`); });

async function streamElevenLabsToTwilio(text, voiceId, model = 'eleven_turbo_v2_5', streamSid, twilioWs, signal) {
  try {
    const res = await fetch(`https://api.elevenlabs.io/v1/text-to-speech/${voiceId}/stream?output_format=ulaw_8000`, { method: 'POST', headers: { 'xi-api-key': ELEVENLABS_API_KEY, 'Content-Type': 'application/json' }, body: JSON.stringify({ text, model_id: model, voice_settings: { stability: 0.5, similarity_boost: 0.75, style: 0.0, use_speaker_boost: true } }), signal });
    if (!res.ok) { console.error(`[ElevenLabs] Error ${res.status}:`, await res.text()); return; }
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
  try {
    console.log(`[OpenAI TTS] voz=${voice} modelo=${model}`);
    const res = await fetch('https://api.openai.com/v1/audio/speech', { method: 'POST', headers: { 'Authorization': `Bearer ${OPENAI_API_KEY}`, 'Content-Type': 'application/json' }, body: JSON.stringify({ model, input: text, voice, response_format: 'mp3', speed: 1.0 }), signal });
    if (!res.ok) { console.error(`[OpenAI TTS] Error ${res.status}:`, await res.text()); return; }
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
  try {
    console.log(`[Google TTS] voz=${voice} idioma=${languageCode}`);
    const accessToken = await getGoogleAccessToken();
    if (signal?.aborted) return;
    const res = await fetch('https://texttospeech.googleapis.com/v1/text:synthesize', { method: 'POST', headers: { 'Authorization': `Bearer ${accessToken}`, 'Content-Type': 'application/json' }, body: JSON.stringify({ input: { text }, voice: { languageCode, name: voice }, audioConfig: { audioEncoding: 'MP3', speakingRate: 1.0 } }), signal });
    if (!res.ok) { console.error(`[Google TTS] Error ${res.status}:`, await res.text()); return; }
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
