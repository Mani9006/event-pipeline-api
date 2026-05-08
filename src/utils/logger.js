/**
 * Structured logging utility.
 * Outputs JSON-formatted logs with severity levels, correlation IDs,
 * and optional pretty-printing for development.
 */

const { logLevel, nodeEnv } = require('../config');

const LEVELS = { debug: 0, info: 1, warn: 2, error: 3, fatal: 4 };
const CURRENT_LEVEL = LEVELS[logLevel] ?? LEVELS.info;

function shouldLog(level) {
  return LEVELS[level] >= CURRENT_LEVEL;
}

function formatLog(level, message, meta = {}) {
  const entry = {
    timestamp: new Date().toISOString(),
    level,
    message,
    ...meta
  };
  return nodeEnv === 'development'
    ? JSON.stringify(entry, null, 2)
    : JSON.stringify(entry);
}

function log(level, message, meta) {
  if (!shouldLog(level)) return;
  const output = formatLog(level, message, meta);
  switch (level) {
    case 'error':
    case 'fatal':
      process.stderr.write(output + '\n');
      break;
    default:
      process.stdout.write(output + '\n');
  }
}

const logger = {
  debug: (msg, meta) => log('debug', msg, meta),
  info:  (msg, meta) => log('info',  msg, meta),
  warn:  (msg, meta) => log('warn',  msg, meta),
  error: (msg, meta) => log('error', msg, meta),
  fatal: (msg, meta) => log('fatal', msg, meta),

  child(meta) {
    return {
      debug: (msg, m) => log('debug', msg, { ...meta, ...m }),
      info:  (msg, m) => log('info',  msg, { ...meta, ...m }),
      warn:  (msg, m) => log('warn',  msg, { ...meta, ...m }),
      error: (msg, m) => log('error', msg, { ...meta, ...m }),
      fatal: (msg, m) => log('fatal', msg, { ...meta, ...m }),
    };
  }
};

module.exports = logger;