/**
 * Event router module.
 * Routes events to registered processors based on type-matching rules.
 * Supports glob patterns, priority ordering, and fallback handlers.
 */

const logger = require('../utils/logger');
const { metrics } = require('../utils/metrics');

// ─── Route Registry ──────────────────────────────────────────────────────────

class Router {
  constructor(config) {
    this.routes = new Map();      // pattern → processor names
    this.processors = new Map();  // name → processor function
    this.defaultProcessors = [];
    this._loadConfig(config);
  }

  _loadConfig(config) {
    for (const route of config.routes || []) {
      this.routes.set(route.pattern, route.processors);
      if (route.pattern === '*') {
        this.defaultProcessors = route.processors;
      }
    }
  }

  /**
   * Register a processor function.
   * @param {string} name - Processor identifier
   * @param {Function} processor - async (event, context) => processedEvent
   */
  register(name, processor) {
    this.processors.set(name, processor);
    logger.info(`Processor registered: ${name}`);
  }

  /**
   * Unregister a processor.
   */
  unregister(name) {
    this.processors.delete(name);
    logger.info(`Processor unregistered: ${name}`);
  }

  /**
   * Route an event to matching processors.
   * @param {Object} event
   * @param {Object} context - Pipeline context
   * @returns {Object} - Routed event with processing results
   */
  async route(event, context) {
    const matches = this._findMatches(event.type);
    const processorNames = matches.length > 0 ? matches : this.defaultProcessors;
    const results = [];

    logger.debug('Routing event', {
      eventId: event.id,
      type: event.type,
      processors: processorNames
    });

    for (const name of processorNames) {
      const processor = this.processors.get(name);
      if (!processor) {
        logger.warn(`Processor not found: ${name}`);
        results.push({ processor: name, status: 'skipped', reason: 'not_found' });
        continue;
      }

      const start = Date.now();
      try {
        const result = await processor(event, context);
        results.push({
          processor: name,
          status: 'completed',
          durationMs: Date.now() - start,
          result: result
        });
        metrics.increment('eventsRouted');
      } catch (err) {
        logger.error(`Processor '${name}' failed`, {
          eventId: event.id,
          error: err.message
        });
        results.push({
          processor: name,
          status: 'failed',
          durationMs: Date.now() - start,
          error: err.message
        });
      }
    }

    return {
      ...event,
      _routing: {
        processors: processorNames,
        results,
        routedAt: new Date().toISOString()
      }
    };
  }

  /**
   * Find all matching processor names for an event type.
   */
  _findMatches(eventType) {
    const matches = [];
    for (const [pattern, processors] of this.routes) {
      if (pattern === '*') continue; // handled as fallback
      if (this._matchPattern(eventType, pattern)) {
        matches.push(...processors);
      }
    }
    return [...new Set(matches)]; // deduplicate
  }

  /**
   * Match an event type against a glob pattern.
   */
  _matchPattern(eventType, pattern) {
    const regex = new RegExp(
      '^' + pattern.replace(/\./g, '\\.').replace(/\*/g, '[^.]+') + '$'
    );
    return regex.test(eventType);
  }

  /**
   * Get routing configuration.
   */
  getRoutes() {
    return [...this.routes.entries()].map(([pattern, processors]) => ({
      pattern,
      processors
    }));
  }

  /**
   * Get list of registered processors.
   */
  getProcessors() {
    return [...this.processors.keys()];
  }
}

// ─── Singleton Factory ───────────────────────────────────────────────────────

function createRouter(config) {
  return new Router(config);
}

module.exports = { Router, createRouter };