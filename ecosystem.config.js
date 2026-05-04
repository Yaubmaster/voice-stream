module.exports = {
  apps: [{
    name: 'voice-stream',
    script: 'index.js',
    instances: 'max',
    exec_mode: 'cluster',
    env_file: '.env'
  }]
}
