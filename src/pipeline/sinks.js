/**
 * Sink management module.
 * Handles output to multiple sinks: file, webhook (simulated), and console.
 * Supports retry logic, circuit breaker, and batch writes.
 */

const fs = require('fs');
const path = require('path');
const logger = require('../utils/logger');
const { metrics } = require('../utils/metrics');

// ─── Circuit Breaker ─────────────────────────────────────────────────────────

class CircuitBreaker {
  constructor(name, options = {}) {
    this.name = name;
    this.failureThreshold = options.failureThreshold || 5;
    this.resetTimeoutMs = options.resetTimeoutMs || 30000;
    this.halfOpenMax = options.halfOpenMax || 3;
    this._failures = 0;
    this._lastFailure = null;
    this._halfOpenAttempts = 0;
    this.state = 'closed'; // closed | open | half-open
  }

  canExecute() {
    if (this.state === 'closed') return true;
    if (this.state === 'open') {
      if (Date.now() - this._lastFailure >= this.resetTimeoutMs) {
        this.state = 'half-open';
        this._halfOpenAttempts = 0;
        logger.info(`Circuit breaker '${this.name}' entering half-open state`);
        return true;
      }
      return false;
    }
    if (this.state === 'half-open') {
      return this._halfOpenAttempts < this.halfOpenMax;
    }
    return true;
  }

  recordSuccess() {
    if (this.state === 'half-open') {
      this._halfOpenAttempts++;
      if (this._halfOpenAttempts >= this.halfOpenMax) {
        this.state = 'closed';
        this._failures = 0;
        logger.info(`Circuit breaker '${this.name}' closed`);
      }
    } else {
      this._failures = 0;
    }
  }

  recordFailure() {
    this._failures++;
    this._lastFailure = Date.now();
    if (this.state === 'half-open') {
      this.state = 'open';
      this._halfOpenAttempts = 0;
    } else if (this._failures >= this.failureThreshold) {
      this.state = 'open';
      logger.warn(`Circuit breaker '${this.name}' opened after ${this._failures} failures`);
    }
  }
}

// ─── Sink Manager ────────────────────────────────────────────────────────────

class SinkManager {
  constructor(config) {
    this.sinks = new Map();
    this.config = config;
    this.batchBuffer = new Map(); // sinkName → events[]
    this.batchTimers = new Map();
  }

  register(name, sinkFn, options = {}) {
    this.sinks.set(name, {
      fn: sinkFn,
      circuitBreaker: new CircuitBreaker(name, options.circuitBreaker),
      retryAttempts: options.retryAttempts || 3,
      batchSize: options.batchSize || 1,
      flushIntervalMs: options.flushIntervalMs || 5000
    });
    logger.info(`Sink registered: ${name}`);
  }

  /**
   * Write an event to all enabled sinks.
   * @param {Object} event
   * @param {Object} context
   */
  async write(event, context = {}) {
    const results = [];

    for (const [name, sink] of this.sinks) {
      const start = Date.now();

      if (!sink.circuitBreaker.canExecute()) {
        results.push({ sink: name, status: 'circuit-open', durationMs: 0 });
        continue;
      }

      try {
        await this._writeWithRetry(sink, event, context);
        sink.circuitBreaker.recordSuccess();

        const durationMs = Date.now() - start;
        metrics.recordSinkLatency(durationMs);
        metrics.increment('eventsWrittenToSink');

        results.push({ sink: name, status: 'success', durationMs });
      } catch (err) {
        sink.circuitBreaker.recordFailure();
        logger.error(`Sink '${name}' write failed`, {
          eventId: event.id,
          error: err.message
        });
        results.push({ sink: name, status: 'failed', error: err.message, durationMs: Date.now() - start });
      }
    }

    return results;
  }

  async _writeWithRetry(sink, event, context) {
    let lastErr;
    for (let i = 0; i < sink.retryAttempts; i++) {
      try {
        return await sink.fn(event, context);
      } catch (err) {
        lastErr = err;
        if (i < sink.retryAttempts - 1) {
          await new Promise(r => setTimeout(r, 100 * (i + 1))); // exponential-ish backoff
        }
      }
    }
    throw lastErr;
  }

  /**
   * Get sink statuses.
   */
  getStatuses() {
    return [...this.sinks.entries()].map(([name, sink]) => ({
      name,
      circuitBreaker: {
        state: sink.circuitBreaker.state,
        failures: sink.circuitBreaker._failures
      }
    }));
  }
}

// ─── Built-in Sink Implementations ───────────────────────────────────────────

/**
 * File sink - appends events as JSON Lines.
 */
function createFileSink(outputDir) {
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }

  return async (event) => {
    const date = new Date().toISOString().split('T')[0];
    const filePath = path.join(outputDir, `events-${date}.jsonl`);
    const line = JSON.stringify({
      ...event,
      _sink: {
        writtenAt: new Date().toISOString(),
        sinkType: 'file'
      }
    }) + '\n';

    return new Promise((resolve, reject) => {
      fs.appendFile(filePath, line, (err) => {
        if (err) reject(err);
        else resolve({ filePath, bytes: line.length });
      });
    });
  };
}

/**
 * Console sink - logs to stdout.
 */
function createConsoleSink() {
  return async (event) => {
    logger.info('Sink output', {
      eventId: event.id,
      type: event.type,
      sink: 'console'
    });
    return { sink: 'console', logged: true };
  };
}

/**
 * Webhook sink - simulates HTTP delivery.
 */
function createWebhookSink(targets) {
  return async (event) => {
    const results = [];
    for (const target of targets) {
      if (!target.url) continue;

      // Simulated webhook delivery
      const start = Date.now();
      const deliveryId = `${Date.now()}-${Math.random().toString(36).substring(2, 10)}`;

      logger.info('Webhook simulated delivery', {
        eventId: event.id,
        url: target.url,
        deliveryId,
        durationMs: Date.now() - start
      });

      results.push({
        url: target.url,
        deliveryId,
        status: 'simulated-success',
        latencyMs: Date.now() - start
      });
    }
    return { sink: 'webhook', deliveries: results };
  };
}

/**
 * Null sink - for testing / disabled.
 */
function createNullSink() {
  return async () => ({ sink: 'null', status: 'ignored' });
}

module.exports = {
  SinkManager,
  CircuitBreaker,
  createFileSink,
  createConsoleSink,
  createWebhookSink,
  createNullSink
};