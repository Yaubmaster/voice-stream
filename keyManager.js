class KeyPool {
  constructor(name, keys) {
    this.name = name;
    this.keys = keys.filter(Boolean);
    this.index = 0;
    this.failures = {};
    if (this.keys.length === 0) console.warn(`[KeyManager] WARN: pool "${name}" tiene 0 keys configuradas`);
  }
  getKey() {
    const now = Date.now();
    const total = this.keys.length;
    for (let i = 0; i < total; i++) {
      const candidate = this.keys[(this.index + i) % total];
      const failure = this.failures[candidate];
      if (!failure || now > failure.cooldownUntil) { this.index = (this.index + i + 1) % total; return candidate; }
    }
    console.error(`[KeyManager] CRIT: todas las keys de "${this.name}" en cooldown`);
    const key = this.keys[this.index % total];
    this.index = (this.index + 1) % total;
    return key;
  }
  reportFailure(key, statusCode) {
    if (!this.failures[key]) this.failures[key] = { count: 0, cooldownUntil: 0 };
    this.failures[key].count++;
    const cooldownMs = Math.min(30000 * Math.pow(2, this.failures[key].count - 1), 300000);
    this.failures[key].cooldownUntil = Date.now() + cooldownMs;
    console.warn(`[KeyManager] Key fallida en "${this.name}" (status ${statusCode}). Fallo #${this.failures[key].count}. Cooldown: ${cooldownMs/1000}s`);
  }
  reportSuccess(key) { if (this.failures[key]) delete this.failures[key]; }
  getStatus() {
    const now = Date.now();
    return { name: this.name, total: this.keys.length, available: this.keys.filter(k => !this.failures[k] || now > this.failures[k].cooldownUntil).length, failures: Object.entries(this.failures).map(([k,v]) => ({ key: k.slice(0,8)+'...', count: v.count, cooldownRemaining: Math.max(0,Math.round((v.cooldownUntil-now)/1000))+'s' })) };
  }
}

function parseKeys(envVar) {
  const raw = process.env[envVar] || '';
  return raw.split(',').map(k => k.trim()).filter(Boolean);
}

const pools = {
  deepgram:   new KeyPool('Deepgram',        parseKeys('DEEPGRAM_API_KEYS')),
  azure:      new KeyPool('Azure OpenAI',    parseKeys('AZURE_OPENAI_KEYS')),
  openai:     new KeyPool('OpenAI fallback', parseKeys('OPENAI_API_KEYS')),
  google:     new KeyPool('Google TTS',      parseKeys('GOOGLE_TTS_KEYS')),
  elevenlabs: new KeyPool('ElevenLabs',      parseKeys('ELEVENLABS_API_KEYS')),
};

const keyManager = {
  getKey(provider) { if (!pools[provider]) throw new Error(`[KeyManager] Provider desconocido: ${provider}`); return pools[provider].getKey(); },
  reportFailure(provider, key, statusCode) { if (pools[provider]) pools[provider].reportFailure(key, statusCode); },
  reportSuccess(provider, key) { if (pools[provider]) pools[provider].reportSuccess(key); },
  getDeepgramKey() { const key = pools.deepgram.getKey(); return { key, onSuccess: () => keyManager.reportSuccess('deepgram', key), onFailure: (s) => keyManager.reportFailure('deepgram', key, s) }; },
  getLLMKey() {
    const azureAvailable = pools.azure.keys.length > 0;
    const openaiAvailable = pools.openai.keys.length > 0;
    if (azureAvailable) {
      const key = pools.azure.getKey();
      return { key, endpoint: process.env.AZURE_OPENAI_ENDPOINT, isAzure: true, onSuccess: () => keyManager.reportSuccess('azure', key), onFailure: (s) => { keyManager.reportFailure('azure', key, s); if (openaiAvailable) console.warn('[KeyManager] Azure falló, fallback a OpenAI disponible'); } };
    }
    if (openaiAvailable) {
      const key = pools.openai.getKey();
      return { key, endpoint: 'https://api.openai.com/v1', isAzure: false, onSuccess: () => keyManager.reportSuccess('openai', key), onFailure: (s) => keyManager.reportFailure('openai', key, s) };
    }
    throw new Error('[KeyManager] CRIT: sin keys LLM disponibles');
  },
  getGoogleKey() { const key = pools.google.getKey(); return { key, onSuccess: () => keyManager.reportSuccess('google', key), onFailure: (s) => keyManager.reportFailure('google', key, s) }; },
  getElevenLabsKey() { const key = pools.elevenlabs.getKey(); return { key, onSuccess: () => keyManager.reportSuccess('elevenlabs', key), onFailure: (s) => keyManager.reportFailure('elevenlabs', key, s) }; },
  getStatus() { return Object.values(pools).map(p => p.getStatus()); },
};

module.exports = keyManager;
