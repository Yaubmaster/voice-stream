const fs = require('fs');
let code = fs.readFileSync('index.js', 'utf8');
const deepgramFn = `
async function streamDeepgramTTSToTwilio(text, voice = 'aura-2-carina-es', streamSid, twilioWs, signal) {
  const { key, onSuccess, onFailure } = keyManager.getDeepgramKey();
  try {
    console.log(\`[Deepgram TTS] voz=\${voice} texto="\${text.slice(0, 60)}..."\`);
    const res = await fetch(\`https://api.deepgram.com/v1/speak?model=\${voice}&encoding=mulaw&sample_rate=8000&container=none\`, {
      method: 'POST',
      headers: { 'Authorization': \`Token \${key}\`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ text }),
      signal
    });
    if (!res.ok) { onFailure(res.status); console.error(\`[Deepgram TTS] Error \${res.status}:\`, await res.text()); return; }
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
  } catch (err) { if (err.name !== 'AbortError') console.error('[Deepgram TTS] Error:', err.message); }
}

`;
code = code.replace('async function streamElevenLabsToTwilio(', deepgramFn + 'async function streamElevenLabsToTwilio(');
code = code.replace(
  "if (provider === 'openai') await streamOpenAITTSToTwilio(text, va.openai_voice ?? 'alloy', va.openai_tts_model ?? 'tts-1', streamSid, twilioWs, signal);",
  "if (provider === 'deepgram') await streamDeepgramTTSToTwilio(text, va.deepgram_aura_voice ?? 'aura-2-carina-es', streamSid, twilioWs, signal);\n  else if (provider === 'openai') await streamOpenAITTSToTwilio(text, va.openai_voice ?? 'alloy', va.openai_tts_model ?? 'tts-1', streamSid, twilioWs, signal);"
);
fs.writeFileSync('index.js', code);
console.log('Patch aplicado');
const p = fs.readFileSync('index.js', 'utf8');
console.log('fn:', p.includes('streamDeepgramTTSToTwilio'));
console.log('branch:', p.includes("provider === 'deepgram'"));
