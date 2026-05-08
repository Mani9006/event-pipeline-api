/**
 * Request logging middleware.
 * Logs incoming requests with timing, status code, and correlation IDs.
 */

const { v4: uuidv4 } = require('uuid');
const logger = require('../utils/logger');

function requestLogger(req, res, next) {
  // Attach correlation ID
  const correlationId = req.headers['x-correlation-id'] || uuidv4();
  req.correlationId = correlationId;
  res.setHeader('X-Correlation-Id', correlationId);

  const startTime = process.hrtime.bigint();
  const requestLog = {
    correlationId,
    method: req.method,
    path: req.path,
    query: req.query,
    clientIp: req.ip || req.connection.remoteAddress,
    userAgent: req.headers['user-agent'],
    contentType: req.headers['content-type']
  };

  logger.info('Request started', requestLog);

  // Capture response finish
  res.on('finish', () => {
    const endTime = process.hrtime.bigint();
    const durationMs = Number(endTime - startTime) / 1_000_000;

    const level = res.statusCode >= 500 ? 'error'
      : res.statusCode >= 400 ? 'warn' : 'info';

    logger[level]('Request completed', {
      ...requestLog,
      statusCode: res.statusCode,
      durationMs: Math.round(durationMs * 100) / 100,
      contentLength: res.getHeader('content-length')
    });
  });

  res.on('close', () => {
    if (!res.writableEnded) {
      logger.warn('Request aborted by client', requestLog);
    }
  });

  next();
}

/**
 * Error logging middleware - must be registered last.
 */
function errorLogger(err, req, res, next) {
  logger.error('Unhandled error', {
    correlationId: req.correlationId,
    message: err.message,
    stack: err.stack,
    method: req.method,
    path: req.path
  });

  res.status(err.status || 500).json({
    success: false,
    error: process.env.NODE_ENV === 'production'
      ? 'Internal server error'
      : err.message
  });
}

module.exports = { requestLogger, errorLogger };