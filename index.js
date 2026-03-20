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

const EXTERNAL_API_PROXY = `${SUPABASE_URL}/functions/v1/external-api-proxy`;

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

  async function callExternalTool(toolName, params) {
    console.log(`[function-calling] Llamando tool: ${toolName}`, params);
    try {
      const res = await fetch(EXTERNAL_API_PROXY, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
        },
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
        parameters: {
          type: 'object',
          properties: {
            direccion: {
              type: 'string',
              description: 'Dirección completa del cliente, ej: "Calle Canarias 424, Portales Norte, Benito Juárez, CDMX"',
            },
          },
          required: ['direccion'],
        },
      },
    },
    {
      type: 'function',
      function: {
        name: 'consultar_pedido',
        description: 'Consulta si el cliente tiene un pedido reciente en base al número de teléfono.',
        parameters: {
          type: 'object',
          properties: {
            telefono: {
              type: 'string',
              description: 'Número de teléfono del cliente',
            },
          },
          required: ['telefono'],
        },
      },
    },
  ];

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

      const systemPrompt = va.assistants?.prompt ?? 'Eres un asistente útil.';
      const model = va.assistants?.llm_model ?? 'gpt-4o-mini';

      const messages = [
        {
          role: 'system',
          content: systemPrompt + '\n\nIMPORTANTE: Responde de forma CORTA y NATURAL para una llamada telefónica. Máximo 2-3 oraciones. Sin listas ni bullets.',
        },
        ...historyMessages,
        { role: 'user', content: transcript },
      ];

      const aiReply = await callOpenAIWithTools(model, messages, OPENAI_TOOLS, signal);
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

      pendingMark = true;
      await streamElevenLabsToTwilio(aiReply, va.elevenlabs_voice_id, va.elevenlabs_model, streamSid, twilioWs, signal);
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
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${OPENAI_API_KEY}`,
          },
          body: JSON.stringify({
            model,
            max_tokens: 200,
            messages,
            tools,
            tool_choice: 'auto',
          }),
          signal,
        });

        const data = await res.json();
        const choice = data.choices?.[0];
        const msg = choice?.message;

        if (!msg) return 'Lo siento, ocurrió un error.';

        if (!msg.tool_calls || msg.tool_calls.length === 0) {
          return msg.content?.trim() ?? 'Lo siento, ocurrió un error.';
        }

        console.log(`[function-calling] OpenAI solicitó ${msg.tool_calls.length} tool(s)`);
        messages.push(msg);

        for (const toolCall of msg.tool_calls) {
          if (signal?.aborted) return null;
          const toolName = toolCall.function.name;
          const toolParams = JSON.parse(toolCall.function.arguments);
          const toolResult = await callExternalTool(toolName, toolParams);
          messages.push({
            role: 'tool',
            tool_call_id: toolCall.id,
            content: JSON.stringify(toolResult),
          });
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

      if (!isFinal) {
        if (isSpeaking) {
          interruptSpeaking();
          console.log('[barge-in] Interim — bot cortado');
        }
        return;
      }

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

      if (params.phone) {
        resolvedPhone = normalizePhone(params.phone);
      } else if (resolvedPhone === '') {
        resolvedPhone = normalizePhone(url.searchParams.get('phone') ?? '');
      }

      console.log(`[Twilio] start streamSid=${streamSid} callSid=${resolvedCallSid} phone="${resolvedPhone}"`);

      loadAssistant(resolvedPhone).then(() => {
        setTimeout(async () => {
          if (!va) {
            console.error('[voice-stream] va sigue null después de cargar');
            return;
          }

          // ── Saludo dinámico ────────────────────────────────────────────────
          // Si el asistente tiene greeting configurado → lo dice al contestar (inbound)
          // Si no tiene greeting → espera al usuario (outbound o sin saludo)
          const greeting = va.greeting;
          if (greeting) {
            console.log(`[voice-stream] Saludo: "${greeting}"`);
            isSpeaking = true;
            pendingMark = true;
            await streamElevenLabsToTwilio(
              greeting,
              va.elevenlabs_voice_id,
              va.elevenlabs_model,
              streamSid,
              twilioWs,
              null
            );
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
          if (twilioWs.readyState === WebSocket.OPEN) {
            twilioWs.send(JSON.stringify({ event: 'media', streamSid, media: { payload: toSend.toString('base64') } }));
          }
        }
      }
    } finally {
      reader.releaseLock();
    }

    if (!signal?.aborted && leftover.length > 0 && twilioWs.readyState === WebSocket.OPEN) {
      twilioWs.send(JSON.stringify({ event: 'media', streamSid, media: { payload: leftover.toString('base64') } }));
    }

    if (!signal?.aborted && twilioWs.readyState === WebSocket.OPEN) {
      twilioWs.send(JSON.stringify({ event: 'mark', streamSid, mark: { name: 'end-of-response' } }));
    }
  } catch (err) {
    if (err.name !== 'AbortError') console.error('[ElevenLabs stream] Error:', err.message);
  }
}
