/**
 * Event transformation pipeline.
 * Applies registered transforms in sequence with conditional support.
 */

const logger = require('../utils/logger');

class Transformer {
  constructor() {
    this.transforms = new Map(); // name → { fn, condition }
  }

  /**
   * Register a transformation function.
   * @param {string} name - Transform name
   * @param {Function} fn - (event, context) => transformedEvent
   * @param {Function|null} condition - Optional condition function
   */
  register(name, fn, condition = null) {
    this.transforms.set(name, { fn, condition });
    logger.info(`Transform registered: ${name}`);
  }

  /**
   * Unregister a transform.
   */
  unregister(name) {
    this.transforms.delete(name);
  }

  /**
   * Apply all matching transforms to an event.
   * @param {Object} event
   * @param {Object} context
   * @returns {Object} - Transformed event
   */
  async transform(event, context = {}) {
    let current = { ...event };
    const applied = [];
    const skipped = [];

    for (const [name, { fn, condition }] of this.transforms) {
      // Check condition
      if (condition) {
        const shouldApply = await condition(current, context);
        if (!shouldApply) {
          skipped.push(name);
          continue;
        }
      }

      const start = Date.now();
      try {
        current = await fn(current, context);
        applied.push({
          name,
          durationMs: Date.now() - start,
          status: 'applied'
        });
      } catch (err) {
        logger.error(`Transform '${name}' failed`, {
          eventId: event.id,
          error: err.message
        });
        applied.push({
          name,
          durationMs: Date.now() - start,
          status: 'failed',
          error: err.message
        });
        // Continue with the event as-is rather than failing the pipeline
      }
    }

    return {
      ...current,
      _transform: {
        applied,
        skipped,
        transformCount: applied.length
      }
    };
  }

  /**
   * Get registered transforms.
   */
  list() {
    return [...this.transforms.keys()];
  }

  /**
   * Clear all transforms.
   */
  clear() {
    this.transforms.clear();
  }
}

// ─── Built-in Transforms ─────────────────────────────────────────────────────

/**
 * Normalize timestamps to ISO-8601 format.
 */
function normalizeTimestamp(event) {
  if (event.timestamp) {
    try {
      event.timestamp = new Date(event.timestamp).toISOString();
    } catch {
      // keep original
    }
  }
  return event;
}

/**
 * Add computed fields to the event.
 */
function addComputedFields(event) {
  event._computed = {
    ...(event._computed || {}),
    eventHour: new Date(event.timestamp).getUTCHours(),
    eventDay: new Date(event.timestamp).getUTCDay(),
    eventTypeCategory: event.type?.split('.')[0] || 'unknown',
    payloadSize: JSON.stringify(event.payload).length
  };
  return event;
}

/**
 * Anonymize sensitive fields.
 */
function anonymizeFields(fields) {
  return (event) => {
    if (!event.payload) return event;
    const payload = { ...event.payload };
    for (const field of fields) {
      if (payload[field]) {
        const str = String(payload[field]);
        payload[field] = str.substring(0, 4) + '****' + str.slice(-4);
      }
    }
    return { ...event, payload };
  };
}

/**
 * Flatten nested payload objects.
 */
function flattenPayload(event) {
  if (!event.payload || typeof event.payload !== 'object') return event;

  const flattened = {};
  function flatten(obj, prefix = '') {
    for (const [key, value] of Object.entries(obj)) {
      const newKey = prefix ? `${prefix}.${key}` : key;
      if (value && typeof value === 'object' && !Array.isArray(value)) {
        flatten(value, newKey);
      } else {
        flattened[newKey] = value;
      }
    }
  }
  flatten(event.payload);
  return { ...event, payload: flattened, _originalPayload: event.payload };
}

module.exports = {
  Transformer,
  normalizeTimestamp,
  addComputedFields,
  anonymizeFields,
  flattenPayload
};