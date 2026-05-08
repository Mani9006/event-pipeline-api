/**
 * Log Processor.
 * Logs event summaries with configurable fields and log levels.
 */

const logger = require('../utils/logger');

class LogProcessor {
  constructor(options = {}) {
    this.fields = options.fields || ['id', 'type', 'source', 'timestamp'];
    this.includePayload = options.includePayload || false;
    this.maxPayloadLength = options.maxPayloadLength || 500;
  }

  async process(event) {
    const logEntry = {};
    for (const field of this.fields) {
      logEntry[field] = event[field];
    }

    if (this.includePayload && event.payload) {
      const payloadStr = JSON.stringify(event.payload);
      logEntry.payload = payloadStr.length > this.maxPayloadLength
        ? payloadStr.substring(0, this.maxPayloadLength) + '...'
        : payloadStr;
    }

    logger.info('Event processed', {
      processor: 'logProcessor',
      ...logEntry,
      enrichmentCount: event._enrichment?.enrichmentCount || 0,
      routingResults: event._routing?.results?.map(r => ({
        processor: r.processor,
        status: r.status
      })) || []
    });

    return { ...event, _logProcessed: true };
  }
}

// Factory for registration
async function logProcessor(event) {
  const processor = new LogProcessor();
  return processor.process(event);
}

module.exports = { LogProcessor, logProcessor };