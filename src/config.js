/**
 * Pipeline configuration management.
 * Supports environment-based overrides and hot-reload of pipeline definitions.
 */

require('dotenv').config();

const path = require('path');
const fs = require('fs');

// ─── Pipeline Stage Configuration ────────────────────────────────────────────

const defaultPipelineConfig = {
  stages: [
    { name: 'validate', enabled: true },
    { name: 'enrich',    enabled: true },
    { name: 'filter',    enabled: true },
    { name: 'transform', enabled: true },
    { name: 'route',     enabled: true },
    { name: 'sink',      enabled: true }
  ],

  // Sink targets for processed events
  sinks: {
    file: {
      enabled: true,
      outputDir: process.env.SINK_FILE_DIR || path.join(__dirname, '..', 'data', 'events'),
      rotationIntervalMs: 24 * 60 * 60 * 1000 // daily rotation
    },
    webhook: {
      enabled: true,
      targets: [
        { url: process.env.WEBHOOK_URL_1 || '', secret: process.env.WEBHOOK_SECRET_1 || '', retryAttempts: 3 },
        { url: process.env.WEBHOOK_URL_2 || '', secret: process.env.WEBHOOK_SECRET_2 || '', retryAttempts: 2 }
      ]
    },
    console: {
      enabled: process.env.NODE_ENV === 'development'
    }
  },

  // Routing rules: event type glob → processor names
  routes: [
    { pattern: 'user.*',        processors: ['logProcessor', 'analyticsProcessor'] },
    { pattern: 'order.*',       processors: ['logProcessor', 'alertProcessor'] },
    { pattern: 'payment.*',     processors: ['logProcessor', 'analyticsProcessor', 'alertProcessor'] },
    { pattern: 'system.alert',  processors: ['alertProcessor'] },
    { pattern: '*',             processors: ['logProcessor'] } // default fallback
  ],

  // Dead-letter queue configuration
  deadLetter: {
    maxRetries: parseInt(process.env.DLQ_MAX_RETRIES || '3', 10),
    outputDir: process.env.DLQ_DIR || path.join(__dirname, '..', 'data', 'dead-letter'),
    alertThreshold: parseInt(process.env.DLQ_ALERT_THRESHOLD || '10', 10) // alert after N DLQ entries
  },

  // Backpressure configuration
  backpressure: {
    maxQueueSize: parseInt(process.env.BP_MAX_QUEUE || '10000', 10),
    maxConcurrency: parseInt(process.env.BP_MAX_CONCURRENCY || '50', 10),
    drainIntervalMs: parseInt(process.env.BP_DRAIN_INTERVAL || '100', 10),
    highWatermark: 0.8,  // 80% → start shedding
    lowWatermark:  0.5   // 50% → resume normal
  },

  // Replay configuration
  replay: {
    batchSize: parseInt(process.env.REPLAY_BATCH_SIZE || '100', 10),
    maxLookbackDays: parseInt(process.env.REPLAY_MAX_DAYS || '30', 10)
  },

  // Validation
  validation: {
    schemaDir: path.join(__dirname, '..', 'schemas'),
    strictMode: process.env.VALIDATION_STRICT === 'true'
  }
};

// ─── Config Loader ───────────────────────────────────────────────────────────

let activeConfig = { ...defaultPipelineConfig };

function loadConfig() {
  const configPath = process.env.PIPELINE_CONFIG_PATH;
  if (configPath && fs.existsSync(configPath)) {
    try {
      const override = JSON.parse(fs.readFileSync(configPath, 'utf-8'));
      activeConfig = deepMerge(defaultPipelineConfig, override);
      return activeConfig;
    } catch (err) {
      console.warn(`[Config] Failed to load override config: ${err.message}`);
    }
  }
  activeConfig = { ...defaultPipelineConfig };
  return activeConfig;
}

function getConfig() {
  return activeConfig;
}

function updateConfig(updates) {
  activeConfig = deepMerge(activeConfig, updates);
  return activeConfig;
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

function deepMerge(target, source) {
  const out = { ...target };
  for (const key of Object.keys(source)) {
    if (source[key] && typeof source[key] === 'object' && !Array.isArray(source[key])) {
      out[key] = deepMerge(target[key] || {}, source[key]);
    } else {
      out[key] = source[key];
    }
  }
  return out;
}

module.exports = {
  loadConfig,
  getConfig,
  updateConfig,
  defaultPipelineConfig,
  port: parseInt(process.env.PORT || '3000', 10),
  nodeEnv: process.env.NODE_ENV || 'development',
  logLevel: process.env.LOG_LEVEL || 'info'
};