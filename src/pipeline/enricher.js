/**
 * Event enrichment module.
 * Adds contextual data to events from external sources, caches, and computed fields.
 */

const logger = require('../utils/logger');
const { metrics } = require('../utils/metrics');

class Enricher {
  constructor() {
    this.enrichers = new Map(); // name → { fn, cache, cacheTTL }
    this.enrichmentOrder = [];
  }

  /**
   * Register an enrichment function.
   * @param {string} name
   * @param {Function} fn - async (event, context) => enrichedFields
   * @param {Object} options
   * @param {boolean} options.cached - Whether to cache results
   * @param {number} options.cacheTTL - Cache TTL in ms
   * @param {number} options.priority - Enrichment priority (lower = earlier)
   */
  register(name, fn, options = {}) {
    this.enrichers.set(name, {
      fn,
      cached: options.cached || false,
      cache: new Map(),
      cacheTTL: options.cacheTTL || 60_000,
      priority: options.priority || 0
    });
    this.enrichmentOrder = [...this.enrichers.entries()]
      .sort((a, b) => (a[1].priority || 0) - (b[1].priority || 0))
      .map(([name]) => name);
    logger.info(`Enricher registered: ${name}`);
  }

  /**
   * Unregister an enricher.
   */
  unregister(name) {
    this.enrichers.delete(name);
    this.enrichmentOrder = this.enrichmentOrder.filter(n => n !== name);
  }

  /**
   * Enrich an event with all registered enrichers.
   * @param {Object} event
   * @param {Object} context
   * @returns {Object} - Enriched event
   */
  async enrich(event, context = {}) {
    const enriched = { ...event };
    const applied = [];

    for (const name of this.enrichmentOrder) {
      const enricher = this.enrichers.get(name);
      const start = Date.now();

      try {
        let enrichmentData;

        if (enricher.cached) {
          const cacheKey = this._buildCacheKey(event, name);
          const cached = enricher.cache.get(cacheKey);
          if (cached && Date.now() - cached.ts < enricher.cacheTTL) {
            enrichmentData = cached.data;
          } else {
            enrichmentData = await enricher.fn(enriched, context);
            enricher.cache.set(cacheKey, { data: enrichmentData, ts: Date.now() });
          }
        } else {
          enrichmentData = await enricher.fn(enriched, context);
        }

        if (enrichmentData && typeof enrichmentData === 'object') {
          enriched.enrichment = {
            ...(enriched.enrichment || {}),
            ...enrichmentData
          };
        }

        applied.push({
          name,
          durationMs: Date.now() - start,
          status: 'applied',
          fields: Object.keys(enrichmentData || {})
        });

        metrics.increment('eventsEnriched');
      } catch (err) {
        logger.error(`Enricher '${name}' failed`, {
          eventId: event.id,
          error: err.message
        });
        applied.push({
          name,
          durationMs: Date.now() - start,
          status: 'failed',
          error: err.message
        });
      }
    }

    enriched._enrichment = {
      applied,
      enrichedAt: new Date().toISOString(),
      enrichmentCount: applied.filter(a => a.status === 'applied').length
    };

    return enriched;
  }

  _buildCacheKey(event, enricherName) {
    return `${enricherName}:${event.type}:${event.source}:${event.payload?.userId || ''}`;
  }

  /**
   * Get all registered enrichers.
   */
  list() {
    return [...this.enrichers.keys()];
  }

  /**
   * Clear all caches.
   */
  clearCaches() {
    for (const enricher of this.enrichers.values()) {
      enricher.cache.clear();
    }
  }

  /**
   * Cache statistics.
   */
  cacheStats() {
    const stats = {};
    for (const [name, enricher] of this.enrichers) {
      if (enricher.cached) {
        stats[name] = {
          size: enricher.cache.size,
          ttl: enricher.cacheTTL
        };
      }
    }
    return stats;
  }
}

// ─── Built-in Enrichment Functions ───────────────────────────────────────────

/**
 * Add geo-location info (simulated).
 */
async function geoEnrichment(event) {
  const ip = event.metadata?.ipAddress || event.payload?.ipAddress;
  if (!ip) return {};

  // Simulated geo lookup
  return {
    geo: {
      country: ip.startsWith('10.') || ip.startsWith('192.168') ? 'local' : 'US',
      region: 'unknown',
      timezone: 'UTC',
      lookupMethod: 'simulated'
    }
  };
}

/**
 * Add timestamp enrichments.
 */
async function timeEnrichment(event) {
  const ts = new Date(event.timestamp);
  return {
    time: {
      utcHour: ts.getUTCHours(),
      utcDay: ts.getUTCDay(),
      utcMonth: ts.getUTCMonth(),
      iso: ts.toISOString(),
      unix: Math.floor(ts.getTime() / 1000)
    }
  };
}

/**
 * Add source system metadata.
 */
async function sourceEnrichment(event) {
  const source = event.source;
  return {
    source: {
      name: source,
      trustLevel: source === 'internal' ? 'high' : 'medium',
      category: source.includes('mobile') ? 'mobile' : 'web'
    }
  };
}

/**
 * Add hash for deduplication.
 */
async function dedupHash(event) {
  const crypto = require('crypto');
  const content = `${event.type}:${event.source}:${JSON.stringify(event.payload)}:${event.timestamp}`;
  return {
    dedup: {
      hash: crypto.createHash('sha256').update(content).digest('hex').substring(0, 16)
    }
  };
}

module.exports = {
  Enricher,
  geoEnrichment,
  timeEnrichment,
  sourceEnrichment,
  dedupHash
};