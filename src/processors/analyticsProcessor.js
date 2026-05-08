/**
 * Analytics Processor.
 * Aggregates event statistics, computes counters, and tracks trends.
 */

const logger = require('../utils/logger');

class AnalyticsProcessor {
  constructor(options = {}) {
    this.windowMs = options.windowMs || 60_000; // 1 minute aggregation window
    this.maxBuckets = options.maxBuckets || 60;
    this.buckets = new Map(); // timestamp → stats
  }

  async process(event) {
    const bucketKey = this._getBucketKey();
    const bucket = this._getOrCreateBucket(bucketKey);

    // Aggregate
    bucket.total++;
    bucket.byType[event.type] = (bucket.byType[event.type] || 0) + 1;
    bucket.bySource[event.source] = (bucket.bySource[event.source] || 0) + 1;

    // Payload size
    const payloadSize = JSON.stringify(event.payload).length;
    bucket.payloadSizes.push(payloadSize);

    // Update running average
    bucket.avgPayloadSize = bucket.payloadSizes.reduce((a, b) => a + b, 0) / bucket.payloadSizes.length;

    // Event type category
    const category = event.type?.split('.')[0] || 'unknown';
    bucket.byCategory[category] = (bucket.byCategory[category] || 0) + 1;

    // Hour distribution
    const hour = new Date(event.timestamp).getUTCHours();
    bucket.hourDistribution[hour] = (bucket.hourDistribution[hour] || 0) + 1;

    logger.debug('Analytics aggregated', {
      eventId: event.id,
      type: event.type,
      bucketKey,
      bucketSize: bucket.total
    });

    return {
      ...event,
      _analytics: {
        bucketKey,
        aggregated: true,
        ...bucket
      }
    };
  }

  _getBucketKey() {
    const now = Date.now();
    return Math.floor(now / this.windowMs) * this.windowMs;
  }

  _getOrCreateBucket(key) {
    if (!this.buckets.has(key)) {
      this.buckets.set(key, {
        total: 0,
        byType: {},
        bySource: {},
        byCategory: {},
        payloadSizes: [],
        avgPayloadSize: 0,
        hourDistribution: {}
      });

      // Cleanup old buckets
      if (this.buckets.size > this.maxBuckets) {
        const oldest = this.buckets.keys().next().value;
        this.buckets.delete(oldest);
      }
    }
    return this.buckets.get(key);
  }

  /**
   * Get analytics summary.
   */
  getSummary() {
    const summaries = [];
    for (const [key, bucket] of this.buckets) {
      summaries.push({
        windowStart: new Date(key).toISOString(),
        windowEnd: new Date(key + this.windowMs).toISOString(),
        ...bucket,
        payloadSizes: bucket.payloadSizes.length // don't expose raw array
      });
    }
    return summaries.sort((a, b) => new Date(b.windowStart) - new Date(a.windowStart));
  }

  /**
   * Get overall totals.
   */
  getTotals() {
    const totals = {
      totalEvents: 0,
      byType: {},
      bySource: {},
      byCategory: {}
    };
    for (const bucket of this.buckets.values()) {
      totals.totalEvents += bucket.total;
      for (const [type, count] of Object.entries(bucket.byType)) {
        totals.byType[type] = (totals.byType[type] || 0) + count;
      }
      for (const [source, count] of Object.entries(bucket.bySource)) {
        totals.bySource[source] = (totals.bySource[source] || 0) + count;
      }
      for (const [cat, count] of Object.entries(bucket.byCategory)) {
        totals.byCategory[cat] = (totals.byCategory[cat] || 0) + count;
      }
    }
    return totals;
  }
}

const processorInstance = new AnalyticsProcessor();

async function analyticsProcessor(event) {
  return processorInstance.process(event);
}

module.exports = { AnalyticsProcessor, analyticsProcessor, processorInstance };