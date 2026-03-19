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
  const phoneParam = decodeURIComponent(url.searchParams.get('phone') ?? '').replace(/\s/g, '+');

  console.log(`[voice-stream] callSid=${callSid} phone=${phoneParam}`);

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

  let streamSid = '';
  let deepgramWs = null;
  let isDeepgramReady = false;
  let audioBuffer = [];
  let isSpeaking = false;
  let va = null;
  let resolvedCallSid = callSid;
  let resolvedPhone = phoneParam;

  function loadAssistant(phone) {
    return supabase
      .from('voice_assistants')
      .select('*, assistants(id, name, prompt, llm_model)')
      .eq('twilio_phone_number', phone)
      .eq('is_active', true)
      .single()
      .then(({ data, error }) => {
        va = data;
        console.log(`[voice-stream] Asistente: ${va?.assistants?.name} error: ${error?.message}`);
      });
  }

  function connectDeepgram() {
    const dgUrl = 'wss://api.deepgram.com/v1/listen?' + new URLSearchParams({
      model: 'nova-2',
      language: 'es',
      encoding: 'mulaw',
      sample_rate: '8000',
      channels: '1',
      interim_results: 'false',
      endpointing: '300',
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
      if (isFinal && transcript.trim() && !isSpeaking) {
        console.log(`[Deepgram] Transcript: "${transcript}"`);
        isSpeaking = true;
        await handleUserSpeech(transcript, resolvedCallSid, va, streamSid, twilioWs, supabase);
        isSpeaking = false;
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

    if (msg.event === 'start') {
      streamSid = msg.start?.streamSid ?? '';
      const params = msg.start?.customParameters ?? {};
      if (params.callSid) resolvedCallSid = params.callSid;
      if (params.phone) {
        resolvedPhone = decodeURIComponent(params.phone).replace(/\s/g, '+');
      }
      console.log(`[Twilio] start streamSid=${streamSid} callSid=${resolvedCallSid} phone=${resolvedPhone}`);

      loadAssistant(resolvedPhone).then(() => {
        setTimeout(async () => {
          if (va) {
            const name = va.assistants?.name ?? 'Asistente';
            isSpeaking = true;
            await streamElevenLabsToTwilio(`Hola, soy ${name}. ¿En qué puedo ayudarte?`, va.elevenlabs_voice_id, va.elevenlabs_model, streamSid, twilioWs);
            isSpeaking = false;
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
      deepgramWs?.close();
      await supabase.from('voice_calls').update({
        status: 'completed', ended_at: new Date().toISOString(),
      }).eq('call_sid', resolvedCallSid);
    }
  });

  twilioWs.on('error', (e) => console.error('[Twilio WS] Error:', e.message));
  twilioWs.on('close', () => { console.log('[Twilio WS] Cerrado'); deepgramWs?.close(); });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[voice-stream] Listening on port ${PORT}`);
});

async function handleUserSpeech(transcript, callSid, va, streamSid, twilioWs, supabase) {
  try {
    if (!va) { console.error('[handleUserSpeech] va es null'); return; }
    const { data: call } = await supabase.from('voice_calls').select('transcript, turn_count').eq('call_sid', callSid).single();
    const history = call?.transcript ?? [];
    const turnCount = (call?.turn_count ?? 0) + 1;
    const historyMessages = history.map(t => ({ role: t.role === 'user' ? 'user' : 'assistant', content: t.text }));
    const aiReply = await callOpenAI(va.assistants?.prompt ?? 'Eres un asistente útil.', va.assistants?.llm_model ?? 'gpt-4o-mini', transcript, historyMessages);
    console.log(`[AI] "${aiReply}"`);
    history.push({ role: 'user', text: transcript, ts: new Date().toISOString() }, { role: 'assistant', text: aiReply, ts: new Date().toISOString() });
    await supabase.from('voice_calls').update({ transcript: history, turn_count: turnCount, last_activity_at: new Date().toISOString() }).eq('call_sid', callSid);
    await streamElevenLabsToTwilio(aiReply, va.elevenlabs_voice_id, va.elevenlabs_model, streamSid, twilioWs);
  } catch (err) { console.error('[handleUserSpeech] Error:', err.message); }
}

async function streamElevenLabsToTwilio(text, voiceId, model = 'eleven_turbo_v2_5', streamSid, twilioWs) {
  try {
    const res = await fetch(`https://api.elevenlabs.io/v1/text-to-speech/${voiceId}/stream?output_format=ulaw_8000`, {
      method: 'POST',
      headers: { 'xi-api-key': ELEVENLABS_API_KEY, 'Content-Type': 'application/json' },
      body: JSON.stringify({ text, model_id: model, voice_settings: { stability: 0.5, similarity_boost: 0.75, style: 0.0, use_speaker_boost: true } }),
    });
    if (!res.ok) { console.error(`[ElevenLabs] Error ${res.status}:`, await res.text()); return; }
    const chunks = [];
    for await (const chunk of res.body) { chunks.push(chunk); }
    const audio = Buffer.concat(chunks);
    const chunkSize = 640;
    for (let i = 0; i < audio.length; i += chunkSize) {
      const chunk = audio.slice(i, i + chunkSize);
      if (twilioWs.readyState === WebSocket.OPEN) {
        twilioWs.send(JSON.stringify({ event: 'media', streamSid, media: { payload: chunk.toString('base64') } }));
      }
    }
    if (twilioWs.readyState === WebSocket.OPEN) {
      twilioWs.send(JSON.stringify({ event: 'mark', streamSid, mark: { name: 'end-of-response' } }));
    }
  } catch (err) { console.error('[ElevenLabs stream] Error:', err.message); }
}

async function callOpenAI(systemPrompt, model, userMessage, history = []) {
  try {
    const res = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${OPENAI_API_KEY}` },
      body: JSON.stringify({ model, max_tokens: 150, messages: [{ role: 'system', content: systemPrompt + '\n\nIMPORTANTE: Responde de forma CORTA y NATURAL para una llamada telefónica. Máximo 2-3 oraciones. Sin listas ni bullets.' }, ...history, { role: 'user', content: userMessage }] }),
    });
    const data = await res.json();
    return data.choices?.[0]?.message?.content?.trim() ?? 'Lo siento, ocurrió un error. ¿Puedes repetir?';
  } catch (err) { console.error('[OpenAI] Exception:', err.message); return 'Lo siento, ocurrió un error.'; }
}
