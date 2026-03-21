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

const EXTERNAL_API_PROXY = `${SUPABASE_URL}/functions/v1/external-api-proxy`;

// ─── Google Auth token cache ──────────────────────────────────────────────────
let googleAccessToken = null;
let googleTokenExpiry = 0;

async function getGoogleAccessToken() {
  if (googleAccessToken && Date.now() < googleTokenExpiry - 60000) return googleAccessToken;
  const keyFile = JSON.parse(fs.readFileSync(GOOGLE_TTS_KEY_PATH, 'utf8'));
  const now = Math.floor(Date.now() / 1000);
  const payload = {
    iss: keyFile.client_email,
    scope: 'https://www.googleapis.com/auth/cloud-platform',
    aud: 'https://oauth2.googleapis.com/token',
    exp: now + 3600,
    iat: now,
  };
  const crypto = require('crypto');
  const header = Buffer.from(JSON.stringify({ alg: 'RS256', typ: 'JWT' })).toString('base64url');
  const body = Buffer.from(JSON.stringify(payload)).toString('base64url');
  const signingInput = `${header}.${body}`;
  const sign = crypto.createSign('RSA-SHA256');
  sign.update(signingInput);
  const signature = sign.sign(keyFile.private_key, 'base64url');
  const jwt = `${signingInput}.${signature}`;
  const res = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: `grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=${jwt}`,
  });
  const data = await res.json();
  if (!data.access_token) throw new Error(`Google auth failed: ${JSON.stringify(data)}`);
  googleAccessToken = data.access_token;
  googleTokenExpiry = Date.now() + (data.expires_in * 1000);
  console.log('[Google TTS] Token renovado');
  return googleAccessToken;
}

const server = http.createServer((req, res) => {
  res.writeHead(200);
  res.end('voice-stream ok');
});

const wss = new WebSocket.Server({ server, perMessageDeflate: false });

// ─── Trim texto para TTS ──────────────────────────────────────────────────────
function trimForTTS(text, maxChars = 250) {
  if (!text || text.length <= maxChars) return text;
  const cutoff = text.lastIndexOf('.', maxChars);
  if (cutoff > 60) return text.slice(0, cutoff + 1).trim();
  const space = text.lastIndexOf(' ', maxChars);
  return space > 60 ? text.slice(0, space).trim() : text.slice(0, maxChars).trim();
}

// ─── Convertir MP3 → mulaw 8kHz (para OpenAI TTS) ────────────────────────────
function convertMp3ToMulaw(inputBuffer) {
  return new Promise((resolve, reject) => {
    const ffmpeg = spawn('ffmpeg', [
      '-f', 'mp3', '-i', 'pipe:0',
      '-ar', '8000', '-ac', '1',
      '-acodec', 'pcm_mulaw', '-f', 'mulaw',
      'pipe:1',
    ]);
    const chunks = [];
    ffmpeg.stdout.on('data', chunk => chunks.push(chunk));
    ffmpeg.stdout.on('end', () => resolve(Buffer.concat(chunks)));
    ffmpeg.stderr.on('data', () => {});
    ffmpeg.on('error', reject);
    ffmpeg.stdin.write(inputBuffer);
    ffmpeg.stdin.end();
  });
}

// ─── Convertir LINEAR16 16kHz → mulaw 8kHz (para Google TTS) ─────────────────
// CRÍTICO: el audio de Google viene en LINEAR16 a 16000 Hz
// Hay que especificar -ar 16000 ANTES del -i para que ffmpeg sepa el rate de entrada
function convertLinear16ToMulaw(inputBuffer) {
  return new Promise((resolve, reject) => {
    const ffmpeg = spawn('ffmpeg', [
      '-f', 's16le',     // formato signed 16-bit little-endian
      '-ar', '16000',    // sample rate de ENTRADA (Google devuelve 16kHz)
      '-ac', '1',        // mono
      '-i', 'pipe:0',    // input desde stdin
      '-ar', '8000',     // sample rate de SALIDA (Twilio necesita 8kHz)
      '-ac', '1',
      '-acodec', 'pcm_mulaw',
      '-f', 'mulaw',
      'pipe:1',          // output a stdout
    ]);
    const chunks = [];
    ffmpeg.stdout.on('data', chunk => chunks.push(chunk));
    ffmpeg.stdout.on('end', () => resolve(Buffer.concat(chunks)));
    ffmpeg.stderr.on('data', () => {});
    ffmpeg.on('error', reject);
    ffmpeg.stdin.write(inputBuffer);
    ffmpeg.stdin.end();
  });
}

// ─── Router TTS ───────────────────────────────────────────────────────────────
async function streamTTSToTwilio(text, va, streamSid, twilioWs, signal) {
  const provider = va.tts_provider ?? 'elevenlabs';
  console.log(`[TTS] Proveedor: ${provider}`);
  if (provider === 'openai') {
    await streamOpenAITTSToTwilio(text, va.openai_voice ?? 'alloy', va.openai_tts_model ?? 'tts-1', streamSid, twilioWs, signal);
  } else if (provider === 'google') {
    await streamGoogleTTSToTwilio(text, va.google_tts_voice ?? 'es-US-Wavenet-B', va.google_tts_language ?? 'es-US', streamSid, twilioWs, signal);
  } else {
    await streamElevenLabsToTwilio(text, va.elevenlabs_voice_id, va.elevenlabs_model, streamSid, twilioWs, signal);
  }
}

wss.on('connection', (twilioWs, req) => {
  console.log('[voice-stream] Nueva conexión WS recibida:', req.url);
  const url = new URL(req.url, 'http://localhost');
  const callSid = url.searchParams.get('call_sid') ?? '';
  const phoneParam = normalizePhone(url.searchParams.get('phone') ?? '');
  console.log(`[voice-stream] callSid=${callSid} phone=${phoneParam}`);

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

  function normalizePhone(phone) {
    return decodeURIComponent(phone).replace(/\s/g, '').replace(/\+/g, '+');
  }

  function interruptSpeaking() {
    if (!isSpeaking && !currentAbortController) return;
    console.log('[barge-in] Usuario interrumpió — cancelando respuesta en curso');
    if (streamSid && twilioWs.readyState === WebSocket.OPEN) {
      twilioWs.send(JSON.stringify({ event: 'clear', streamSid }));
      console.log('[barge-in] Clear enviado a Twilio');
    }
    if (currentAbortController) {
      currentAbortController.abort();
      currentAbortController = null;
    }
    isSpeaking = false;
    pendingMark = false;
  }

  async function finalizeCall() {
    const durationSeconds = Math.round((Date.now() - callStartTime) / 1000);
    console.log(`[voice-stream] Finalizando llamada — duración: ${durationSeconds}s`);
    if (resolvedCallSid) {
      await supabase.from('voice_calls').update({
        status: 'completed',
        ended_at: new Date().toISOString(),
        duration_seconds: durationSeconds,
      }).eq('call_sid', resolvedCallSid);
    }
  }

  function loadAssistant(phone) {
    console.log(`[loadAssistant] buscando phone="${phone}"`);
    return supabase
      .from('voice_assistants')
      .select('*, assistants(id, name, prompt, llm_model)')
      .eq('twilio_phone_number', phone)
      .eq('is_active', true)
      .single()
      .then(({ data, error }) => {
        va = data;
        console.log(`[loadAssistant] resultado: ${va?.assistants?.name ?? 'null'} proveedor: ${va?.tts_provider ?? 'elevenlabs'} error: ${error?.message ?? 'none'}`);
      });
  }

  async function callExternalTool(toolName, params) {
    console.log(`[function-calling] Llamando tool: ${toolName}`, params);
    try {
      const res = await fetch(EXTERNAL_API_PROXY, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${SUPABASE_SERVICE_ROLE_KEY}` },
        body: JSON.stringify({ tool: toolName, params }),
      });
      const data = await res.json();
      console.log(`[function-calling] Resultado ${toolName}:`, JSON.stringify(data));
      return data;
    } catch (err) {
      console.error(`[function-calling] Error en ${toolName}:`, err.message);
      return { error: err.message };
    }
  }

  const OPENAI_TOOLS = [
    {
      type: 'function',
      function: {
        name: 'validar_cobertura',
        description: 'Valida si una dirección tiene cobertura de entrega de KFC y retorna la sucursal más cercana.',
        parameters: { type: 'object', properties: { direccion: { type: 'string', description: 'Dirección completa del cliente' } }, required: ['direccion'] },
      },
    },
    {
      type: 'function',
      function: {
        name: 'consultar_pedido',
        description: 'Consulta si el cliente tiene un pedido reciente en base al número de teléfono.',
        parameters: { type: 'object', properties: { telefono: { type: 'string', description: 'Número de teléfono del cliente' } }, required: ['telefono'] },
      },
    },
  ];

  async function runPipeline(transcript, signal) {
    try {
      if (!va) { console.error('[pipeline] va es null'); return; }
      if (signal?.aborted) return;

      const { data: call } = await supabase.from('voice_calls').select('transcript, turn_count').eq('call_sid', resolvedCallSid).single();
      const history = call?.transcript ?? [];
      const turnCount = (call?.turn_count ?? 0) + 1;
      const historyMessages = history.map(t => ({ role: t.role === 'user' ? 'user' : 'assistant', content: t.text }));

      if (signal?.aborted) return;

      const systemPrompt = va.assistants?.prompt ?? 'Eres un asistente útil.';
      const model = va.assistants?.llm_model ?? 'gpt-4o-mini';
      const messages = [
        { role: 'system', content: systemPrompt + '\n\nIMPORTANTE: Responde de forma CORTA y NATURAL para una llamada telefónica. Máximo 2-3 oraciones cortas. Sin listas ni bullets.' },
        ...historyMessages,
        { role: 'user', content: transcript },
      ];

      const aiReply = await callOpenAIWithTools(model, messages, OPENAI_TOOLS, signal);
      if (!aiReply || signal?.aborted) return;

      console.log(`[AI] "${aiReply}"`);
      const ttsText = trimForTTS(aiReply, 250);
      if (ttsText !== aiReply) console.log(`[TTS] Texto recortado de ${aiReply.length} a ${ttsText.length} chars`);

      history.push(
        { role: 'user', text: transcript, ts: new Date().toISOString() },
        { role: 'assistant', text: aiReply, ts: new Date().toISOString() }
      );
      supabase.from('voice_calls').update({
        transcript: history, turn_count: turnCount, last_activity_at: new Date().toISOString()
      }).eq('call_sid', resolvedCallSid).then(() => {});

      if (signal?.aborted) return;
      pendingMark = true;
      await streamTTSToTwilio(ttsText, va, streamSid, twilioWs, signal);
      if (signal?.aborted) pendingMark = false;
    } catch (err) {
      if (err.name !== 'AbortError') console.error('[pipeline] Error:', err.message);
      pendingMark = false;
    }
  }

  async function callOpenAIWithTools(model, messages, tools, signal) {
    try {
      while (true) {
        if (signal?.aborted) return null;
        const res = await fetch('https://api.openai.com/v1/chat/completions', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${OPENAI_API_KEY}` },
          body: JSON.stringify({ model, max_tokens: 150, messages, tools, tool_choice: 'auto' }),
          signal,
        });
        const data = await res.json();
        const msg = data.choices?.[0]?.message;
        if (!msg) return 'Lo siento, ocurrió un error.';
        if (!msg.tool_calls || msg.tool_calls.length === 0) return msg.content?.trim() ?? 'Lo siento, ocurrió un error.';
        console.log(`[function-calling] OpenAI solicitó ${msg.tool_calls.length} tool(s)`);
        messages.push(msg);
        for (const toolCall of msg.tool_calls) {
          if (signal?.aborted) return null;
          const toolResult = await callExternalTool(toolCall.function.name, JSON.parse(toolCall.function.arguments));
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
    const dgUrl = 'wss://api.deepgram.com/v1/listen?' + new URLSearchParams({
      model: 'nova-2', language: 'es', encoding: 'mulaw', sample_rate: '8000',
      channels: '1', interim_results: 'true', endpointing: '200',
    }).toString();
    deepgramWs = new WebSocket(dgUrl, { headers: { Authorization: `Token ${DEEPGRAM_API_KEY}` } });
    deepgramWs.on('open', () => {
      console.log('[Deepgram] Conectado');
      isDeepgramReady = true;
      for (const chunk of audioBuffer) deepgramWs.send(Buffer.from(chunk, 'base64'));
      audioBuffer = [];
    });
    deepgramWs.on('message', async (data) => {
      const msg = JSON.parse(data.toString());
      const transcript = msg?.channel?.alternatives?.[0]?.transcript ?? '';
      const isFinal = msg?.is_final === true;
      if (!transcript.trim()) return;
      if (!isFinal) {
        if (isSpeaking) { interruptSpeaking(); console.log('[barge-in] Interim — bot cortado'); }
        return;
      }
      console.log(`[Deepgram] Transcript final: "${transcript}"`);
      if (isSpeaking) { interruptSpeaking(); await new Promise(r => setTimeout(r, 150)); }
      isSpeaking = true;
      const controller = new AbortController();
      currentAbortController = controller;
      try {
        await runPipeline(transcript, controller.signal);
      } catch (err) {
        if (err.name !== 'AbortError') console.error('[Deepgram handler] Error:', err.message);
      } finally {
        if (currentAbortController === controller) currentAbortController = null;
      }
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
      if (msg.mark?.name === 'end-of-response' && pendingMark) {
        pendingMark = false;
        isSpeaking = false;
        console.log('[Twilio] Mark confirmado — bot terminó de hablar');
      }
    }
    if (msg.event === 'start') {
      streamSid = msg.start?.streamSid ?? '';
      const params = msg.start?.customParameters ?? {};
      if (params.callSid) resolvedCallSid = params.callSid;
      if (params.phone) resolvedPhone = normalizePhone(params.phone);
      else if (resolvedPhone === '') resolvedPhone = normalizePhone(url.searchParams.get('phone') ?? '');
      console.log(`[Twilio] start streamSid=${streamSid} callSid=${resolvedCallSid} phone="${resolvedPhone}"`);
      loadAssistant(resolvedPhone).then(() => {
        setTimeout(async () => {
          if (!va) { console.error('[voice-stream] va sigue null después de cargar'); return; }
          const greeting = va.greeting;
          if (greeting) {
            console.log(`[voice-stream] Saludo: "${greeting}"`);
            isSpeaking = true;
            pendingMark = true;
            await streamTTSToTwilio(trimForTTS(greeting, 250), va, streamSid, twilioWs, null);
          } else {
            console.log('[voice-stream] Sin greeting — esperando al usuario');
            isSpeaking = false;
            pendingMark = false;
          }
        }, 300);
      });
    }
    if (msg.event === 'media') {
      const payload = msg.media?.payload ?? '';
      if (isDeepgramReady && deepgramWs?.readyState === WebSocket.OPEN) deepgramWs.send(Buffer.from(payload, 'base64'));
      else audioBuffer.push(payload);
    }
    if (msg.event === 'stop') {
      console.log('[Twilio] stop');
      interruptSpeaking();
      deepgramWs?.close();
      await finalizeCall();
    }
  });

  twilioWs.on('error', (e) => console.error('[Twilio WS] Error:', e.message));
  twilioWs.on('close', () => {
    console.log('[Twilio WS] Cerrado');
    interruptSpeaking();
    deepgramWs?.close();
    finalizeCall();
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[voice-stream] Listening on port ${PORT}`);
});

// ─── ElevenLabs TTS — streaming verdadero ────────────────────────────────────
async function streamElevenLabsToTwilio(text, voiceId, model = 'eleven_turbo_v2_5', streamSid, twilioWs, signal) {
  try {
    const res = await fetch(
      `https://api.elevenlabs.io/v1/text-to-speech/${voiceId}/stream?output_format=ulaw_8000`,
      {
        method: 'POST',
        headers: { 'xi-api-key': ELEVENLABS_API_KEY, 'Content-Type': 'application/json' },
        body: JSON.stringify({ text, model_id: model, voice_settings: { stability: 0.5, similarity_boost: 0.75, style: 0.0, use_speaker_boost: true } }),
        signal,
      }
    );
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
          const toSend = leftover.slice(0, chunkSize);
          leftover = leftover.slice(chunkSize);
          if (twilioWs.readyState === WebSocket.OPEN)
            twilioWs.send(JSON.stringify({ event: 'media', streamSid, media: { payload: toSend.toString('base64') } }));
        }
      }
    } finally { reader.releaseLock(); }
    if (!signal?.aborted && leftover.length > 0 && twilioWs.readyState === WebSocket.OPEN)
      twilioWs.send(JSON.stringify({ event: 'media', streamSid, media: { payload: leftover.toString('base64') } }));
    if (!signal?.aborted && twilioWs.readyState === WebSocket.OPEN)
      twilioWs.send(JSON.stringify({ event: 'mark', streamSid, mark: { name: 'end-of-response' } }));
  } catch (err) {
    if (err.name !== 'AbortError') console.error('[ElevenLabs stream] Error:', err.message);
  }
}

// ─── OpenAI TTS — MP3 via ffmpeg → mulaw ─────────────────────────────────────
async function streamOpenAITTSToTwilio(text, voice = 'alloy', model = 'tts-1', streamSid, twilioWs, signal) {
  try {
    console.log(`[OpenAI TTS] voz=${voice} modelo=${model}`);
    const res = await fetch('https://api.openai.com/v1/audio/speech', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${OPENAI_API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ model, input: text, voice, response_format: 'mp3', speed: 1.0 }),
      signal,
    });
    if (!res.ok) { console.error(`[OpenAI TTS] Error ${res.status}:`, await res.text()); return; }
    if (signal?.aborted) return;
    const mp3Buffer = Buffer.from(await res.arrayBuffer());
    console.log(`[OpenAI TTS] MP3: ${mp3Buffer.length} bytes`);
    if (signal?.aborted) return;
    const mulawBuffer = await convertMp3ToMulaw(mp3Buffer);
    console.log(`[OpenAI TTS] mulaw: ${mulawBuffer.length} bytes`);
    if (signal?.aborted) return;
    const chunkSize = 640;
    for (let i = 0; i < mulawBuffer.length; i += chunkSize) {
      if (signal?.aborted) return;
      if (twilioWs.readyState === WebSocket.OPEN)
        twilioWs.send(JSON.stringify({ event: 'media', streamSid, media: { payload: mulawBuffer.slice(i, i + chunkSize).toString('base64') } }));
    }
    if (!signal?.aborted && twilioWs.readyState === WebSocket.OPEN)
      twilioWs.send(JSON.stringify({ event: 'mark', streamSid, mark: { name: 'end-of-response' } }));
  } catch (err) {
    if (err.name !== 'AbortError') console.error('[OpenAI TTS] Error:', err.message);
  }
}

// ─── Google WaveNet TTS ───────────────────────────────────────────────────────
// Google devuelve LINEAR16 a 16000Hz. ffmpeg DEBE saber el sample rate de entrada
// para no interpretar el audio a velocidad incorrecta (efecto "ardilla").
async function streamGoogleTTSToTwilio(text, voice = 'es-US-Wavenet-B', languageCode = 'es-US', streamSid, twilioWs, signal) {
  try {
    console.log(`[Google TTS] voz=${voice} idioma=${languageCode}`);
    const accessToken = await getGoogleAccessToken();
    if (signal?.aborted) return;

    const res = await fetch('https://texttospeech.googleapis.com/v1/text:synthesize', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${accessToken}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        input: { text },
        voice: { languageCode, name: voice },
        audioConfig: {
          audioEncoding: 'LINEAR16',
          sampleRateHertz: 16000,
          speakingRate: 1.0,
        },
      }),
      signal,
    });

    if (!res.ok) { console.error(`[Google TTS] Error ${res.status}:`, await res.text()); return; }
    if (signal?.aborted) return;

    const data = await res.json();
    const rawBuffer = Buffer.from(data.audioContent, 'base64');
    console.log(`[Google TTS] LINEAR16 raw: ${rawBuffer.length} bytes`);
    if (signal?.aborted) return;

    // Convertir LINEAR16 16kHz → mulaw 8kHz con sample rate correcto en entrada
    const mulawBuffer = await convertLinear16ToMulaw(rawBuffer);
    console.log(`[Google TTS] mulaw: ${mulawBuffer.length} bytes`);
    if (signal?.aborted) return;

    const chunkSize = 640;
    for (let i = 0; i < mulawBuffer.length; i += chunkSize) {
      if (signal?.aborted) return;
      if (twilioWs.readyState === WebSocket.OPEN)
        twilioWs.send(JSON.stringify({ event: 'media', streamSid, media: { payload: mulawBuffer.slice(i, i + chunkSize).toString('base64') } }));
    }
    if (!signal?.aborted && twilioWs.readyState === WebSocket.OPEN)
      twilioWs.send(JSON.stringify({ event: 'mark', streamSid, mark: { name: 'end-of-response' } }));
  } catch (err) {
    if (err.name !== 'AbortError') console.error('[Google TTS] Error:', err.message);
  }
}
