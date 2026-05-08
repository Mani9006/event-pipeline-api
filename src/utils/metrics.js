/**
 * Pipeline metrics collection and reporting.
 * Tracks throughput, latency, error rates, and backpressure indicators.
 * Exposes data for the monitoring endpoint.
 */

class MetricsCollector {
  constructor() {
    this.reset();
  }

  reset() {
    this.counters = {
      eventsIngested: 0,
      eventsProcessed: 0,
      eventsFailed: 0,
      eventsFiltered: 0,
      eventsEnriched: 0,
      eventsRouted: 0,
      eventsWrittenToSink: 0,
      deadLetterQueued: 0,
      backpressureHits: 0,
      replaysInitiated: 0,
      replaysCompleted: 0
    };
    this.histograms = {
      processingLatencyMs: [],
      queueDepth: [],
      sinkLatencyMs: [],
      validationLatencyMs: []
    };
    this.gauges = {
      queueDepth: 0,
      activeWorkers: 0,
      circuitBreakerState: 'closed' // closed | open | half-open
    };
    this.timestamps = {
      startedAt: Date.now()
    };
  }

  // ─── Counters ──────────────────────────────────────────────────────────────

  increment(counter, amount = 1) {
    if (this.counters[counter] !== undefined) {
      this.counters[counter] += amount;
    }
  }

  getCounter(counter) {
    return this.counters[counter] || 0;
  }

  // ─── Histograms ────────────────────────────────────────────────────────────

  observe(histogram, value) {
    if (this.histograms[histogram]) {
      this.histograms[histogram].push(value);
      // Keep last 10,000 samples
      if (this.histograms[histogram].length > 10000) {
        this.histograms[histogram] = this.histograms[histogram].slice(-10000);
      }
    }
  }

  percentile(histogram, p) {
    const arr = this.histograms[histogram];
    if (!arr || arr.length === 0) return 0;
    const sorted = [...arr].sort((a, b) => a - b);
    const idx = Math.floor((p / 100) * (sorted.length - 1));
    return sorted[idx];
  }

  mean(histogram) {
    const arr = this.histograms[histogram];
    if (!arr || arr.length === 0) return 0;
    return arr.reduce((a, b) => a + b, 0) / arr.length;
  }

  // ─── Gauges ────────────────────────────────────────────────────────────────

  setGauge(gauge, value) {
    if (this.gauges[gauge] !== undefined) {
      this.gauges[gauge] = value;
    }
  }

  getGauge(gauge) {
    return this.gauges[gauge];
  }

  // ─── Snapshot ──────────────────────────────────────────────────────────────

  snapshot() {
    const uptimeSeconds = Math.floor((Date.now() - this.timestamps.startedAt) / 1000);
    return {
      counters: { ...this.counters },
      gauges:   { ...this.gauges },
      histograms: {
        processingLatencyMs: {
          p50: this.percentile('processingLatencyMs', 50),
          p95: this.percentile('processingLatencyMs', 95),
          p99: this.percentile('processingLatencyMs', 99),
          mean: this.mean('processingLatencyMs'),
          count: this.histograms.processingLatencyMs.length
        },
        sinkLatencyMs: {
          p50: this.percentile('sinkLatencyMs', 50),
          p95: this.percentile('sinkLatencyMs', 95),
          mean: this.mean('sinkLatencyMs'),
          count: this.histograms.sinkLatencyMs.length
        }
      },
      throughput: {
        eventsPerSecond: this.counters.eventsProcessed / Math.max(uptimeSeconds, 1),
        uptimeSeconds
      }
    };
  }

  // ─── Event Lifecycle ───────────────────────────────────────────────────────

  recordProcessingTime(durationMs) {
    this.observe('processingLatencyMs', durationMs);
  }

  recordSinkLatency(durationMs) {
    this.observe('sinkLatencyMs', durationMs);
  }

  recordValidationLatency(durationMs) {
    this.observe('validationLatencyMs', durationMs);
  }

  recordQueueDepth(depth) {
    this.setGauge('queueDepth', depth);
    this.observe('queueDepth', depth);
  }

  // ─── Health Score ──────────────────────────────────────────────────────────

  healthScore() {
    const total = this.counters.eventsProcessed + this.counters.eventsFailed;
    if (total === 0) return 1.0;
    const errorRate = this.counters.eventsFailed / total;
    const latencyP95 = this.percentile('processingLatencyMs', 95);

    let score = 1.0;
    if (errorRate > 0.1) score -= 0.4;
    else if (errorRate > 0.05) score -= 0.2;

    if (latencyP95 > 5000) score -= 0.3;
    else if (latencyP95 > 1000) score -= 0.15;

    if (this.gauges.circuitBreakerState === 'open') score -= 0.3;

    return Math.max(0, Math.min(1, score));
  }
}

// Singleton instance
const metrics = new MetricsCollector();

module.exports = { MetricsCollector, metrics };