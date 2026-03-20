require("dotenv").config();
const http = require('http');
const WebSocket = require('ws');
const { createClient } = require('@supabase/supabase-js');
 
const PORT = process.env.PORT || 8080;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const ELEVENLABS_API_KEY = process.env.ELEVENLABS_API_KEY;
const DEEPGRAM_API_KEY = process.env.DEEPGRAM_API_KEY;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
 
const server = http.createServer((req, res) => {
  res.writeHead(200);
  res.end('voice-stream ok');
});
 
const wss = new WebSocket.Server({ server, perMessageDeflate: false });
 
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
 
  // isSpeaking = true desde que empieza el TTS
  // se pone false SOLO cuando Twilio manda el mark de confirmación
  // Así el barge-in siempre tiene una ventana para dispararse
  let isSpeaking = false;
  let pendingMark = false;
 
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
        console.log(`[loadAssistant] resultado: ${va?.assistants?.name ?? 'null'} error: ${error?.message ?? 'none'}`);
      });
  }
 
  // Pipeline completo: OpenAI + ElevenLabs, dentro del scope de la conexión
  // para tener acceso a isSpeaking y pendingMark
  async function runPipeline(transcript, signal) {
    try {
      if (!va) { console.error('[pipeline] va es null'); return; }
      if (signal?.aborted) return;
 
      const { data: call } = await supabase
        .from('voice_calls')
        .select('transcript, turn_count')
        .eq('call_sid', resolvedCallSid)
        .single();
 
      const history = call?.transcript ?? [];
      const turnCount = (call?.turn_count ?? 0) + 1;
      const historyMessages = history.map(t => ({
        role: t.role === 'user' ? 'user' : 'assistant',
        content: t.text,
      }));
 
      if (signal?.aborted) return;
 
      const aiReply = await callOpenAI(
        va.assistants?.prompt ?? 'Eres un asistente útil.',
        va.assistants?.llm_model ?? 'gpt-4o-mini',
        transcript,
        historyMessages,
        signal
      );
 
      if (!aiReply || signal?.aborted) return;
 
      console.log(`[AI] "${aiReply}"`);
 
      history.push(
        { role: 'user', text: transcript, ts: new Date().toISOString() },
        { role: 'assistant', text: aiReply, ts: new Date().toISOString() }
      );
 
      supabase.from('voice_calls').update({
        transcript: history,
        turn_count: turnCount,
        last_activity_at: new Date().toISOString(),
      }).eq('call_sid', resolvedCallSid).then(() => {});
 
      if (signal?.aborted) return;
 
      // Marcar pendingMark ANTES de enviar el audio
      pendingMark = true;
 
      await streamElevenLabsToTwilio(
        aiReply,
        va.elevenlabs_voice_id,
        va.elevenlabs_model,
        streamSid,
        twilioWs,
        signal
      );
 
      // Si el signal fue abortado durante TTS, limpiar pendingMark
      if (signal?.aborted) {
        pendingMark = false;
      }
 
    } catch (err) {
      if (err.name !== 'AbortError') {
        console.error('[pipeline] Error:', err.message);
      }
      pendingMark = false;
    }
  }
 
  function connectDeepgram() {
    const dgUrl = 'wss://api.deepgram.com/v1/listen?' + new URLSearchParams({
      model: 'nova-2',
      language: 'es',
      encoding: 'mulaw',
      sample_rate: '8000',
      channels: '1',
      interim_results: 'true',
      endpointing: '200',
    }).toString();
 
    deepgramWs = new WebSocket(dgUrl, {
      headers: { Authorization: `Token ${DEEPGRAM_API_KEY}` },
    });
 
    deepgramWs.on('open', () => {
      console.log('[Deepgram] Conectado');
      isDeepgramReady = true;
      for (const chunk of audioBuffer) {
        deepgramWs.send(Buffer.from(chunk, 'base64'));
      }
      audioBuffer = [];
    });
 
    deepgramWs.on('message', async (data) => {
      const msg = JSON.parse(data.toString());
      const transcript = msg?.channel?.alternatives?.[0]?.transcript ?? '';
      const isFinal = msg?.is_final === true;
 
      if (!transcript.trim()) return;
 
      // ── INTERIM: usuario habla → cortar bot inmediato ─────────────────────
      if (!isFinal) {
        if (isSpeaking) {
          interruptSpeaking();
          console.log('[barge-in] Interim — bot cortado');
        }
        return;
      }
 
      // ── FINAL: procesar y responder ───────────────────────────────────────
      console.log(`[Deepgram] Transcript final: "${transcript}"`);
 
      if (isSpeaking) {
        interruptSpeaking();
        await new Promise(r => setTimeout(r, 150));
      }
 
      isSpeaking = true;
      const controller = new AbortController();
      currentAbortController = controller;
 
      try {
        await runPipeline(transcript, controller.signal);
      } catch (err) {
        if (err.name !== 'AbortError') {
          console.error('[Deepgram handler] Error:', err.message);
        }
      } finally {
        if (currentAbortController === controller) currentAbortController = null;
        // NO ponemos isSpeaking = false aquí
        // Lo hace el handler del mark de Twilio
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
 
    // ── MARK: Twilio confirma que el teléfono terminó de reproducir el audio ─
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
 
      if (params.phone) {
        resolvedPhone = normalizePhone(params.phone);
      } else if (resolvedPhone === '') {
        resolvedPhone = normalizePhone(url.searchParams.get('phone') ?? '');
      }
 
      console.log(`[Twilio] start streamSid=${streamSid} callSid=${resolvedCallSid} phone="${resolvedPhone}" params=${JSON.stringify(params)}`);
 
      loadAssistant(resolvedPhone).then(() => {
        setTimeout(async () => {
          if (va) {
            const name = va.assistants?.name ?? 'Asistente';
            isSpeaking = true;
            pendingMark = true;
            await streamElevenLabsToTwilio(
              `Hola, soy ${name}. ¿En qué puedo ayudarte?`,
              va.elevenlabs_voice_id,
              va.elevenlabs_model,
              streamSid,
              twilioWs,
              null
            );
            // isSpeaking se apaga cuando llega el mark
          } else {
            console.error('[voice-stream] va sigue null después de cargar');
          }
        }, 300);
      });
    }
 
    if (msg.event === 'media') {
      const payload = msg.media?.payload ?? '';
      if (isDeepgramReady && deepgramWs?.readyState === WebSocket.OPEN) {
        deepgramWs.send(Buffer.from(payload, 'base64'));
      } else {
        audioBuffer.push(payload);
      }
    }
 
    if (msg.event === 'stop') {
      console.log('[Twilio] stop');
      interruptSpeaking();
      deepgramWs?.close();
      await supabase.from('voice_calls').update({
        status: 'completed',
        ended_at: new Date().toISOString(),
      }).eq('call_sid', resolvedCallSid);
    }
  });
 
  twilioWs.on('error', (e) => console.error('[Twilio WS] Error:', e.message));
  twilioWs.on('close', () => {
    console.log('[Twilio WS] Cerrado');
    interruptSpeaking();
    deepgramWs?.close();
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
        headers: {
          'xi-api-key': ELEVENLABS_API_KEY,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          text,
          model_id: model,
          voice_settings: { stability: 0.5, similarity_boost: 0.75, style: 0.0, use_speaker_boost: true },
        }),
        signal,
      }
    );
 
    if (!res.ok) {
      console.error(`[ElevenLabs] Error ${res.status}:`, await res.text());
      return;
    }
 
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
          if (twilioWs.readyState === WebSocket.OPEN) {
            twilioWs.send(JSON.stringify({
              event: 'media',
              streamSid,
              media: { payload: toSend.toString('base64') },
            }));
          }
        }
      }
    } finally {
      reader.releaseLock();
    }
 
    if (!signal?.aborted && leftover.length > 0 && twilioWs.readyState === WebSocket.OPEN) {
      twilioWs.send(JSON.stringify({
        event: 'media',
        streamSid,
        media: { payload: leftover.toString('base64') },
      }));
    }
 
    if (!signal?.aborted && twilioWs.readyState === WebSocket.OPEN) {
      twilioWs.send(JSON.stringify({ event: 'mark', streamSid, mark: { name: 'end-of-response' } }));
    }
  } catch (err) {
    if (err.name !== 'AbortError') {
      console.error('[ElevenLabs stream] Error:', err.message);
    }
  }
}
 
// ─── OpenAI — abortable ───────────────────────────────────────────────────────
async function callOpenAI(systemPrompt, model, userMessage, history = [], signal) {
  try {
    const res = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${OPENAI_API_KEY}`,
      },
      body: JSON.stringify({
        model,
        max_tokens: 150,
        messages: [
          {
            role: 'system',
            content: systemPrompt + '\n\nIMPORTANTE: Responde de forma CORTA y NATURAL para una llamada telefónica. Máximo 2-3 oraciones. Sin listas ni bullets.',
          },
          ...history,
          { role: 'user', content: userMessage },
        ],
      }),
      signal,
    });
 
    const data = await res.json();
    return data.choices?.[0]?.message?.content?.trim() ?? 'Lo siento, ocurrió un error. ¿Puedes repetir?';
  } catch (err) {
    if (err.name === 'AbortError') return null;
    console.error('[OpenAI] Exception:', err.message);
    return 'Lo siento, ocurrió un error.';
  }
}
