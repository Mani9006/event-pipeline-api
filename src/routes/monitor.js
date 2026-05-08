/**
 * Monitoring and metrics routes.
 * Exposes health checks, metrics, alerts, and pipeline diagnostics.
 */

const express = require('express');
const { metrics } = require('../utils/metrics');
const { processorInstance: analyticsProcessor } = require('../processors/analyticsProcessor');
const { processorInstance: alertProcessor } = require('../processors/alertProcessor');
const { getConfig } = require('../config');

const router = express.Router();

/**
 * Health check endpoint.
 * GET /api/v1/health
 */
router.get('/', (req, res) => {
  const { pipeline } = req.app.locals;
  const score = metrics.healthScore();
  const health = {
    status: score >= 0.9 ? 'healthy' : score >= 0.6 ? 'degraded' : 'unhealthy',
    score,
    uptime: process.uptime(),
    timestamp: new Date().toISOString(),
    version: process.env.npm_package_version || '1.0.0',
    checks: {
      queue: pipeline.queue.size() < pipeline.queue.maxSize * 0.9 ? 'ok' : 'warning',
      memory: process.memoryUsage().heapUsed < 500 * 1024 * 1024 ? 'ok' : 'warning',
      circuitBreakers: pipeline.sinkManager.getStatuses().every(s =>
        s.circuitBreaker.state !== 'open') ? 'ok' : 'warning'
    }
  };

  const statusCode = health.status === 'unhealthy' ? 503 : 200;
  res.status(statusCode).json(health);
});

/**
 * Detailed health with diagnostics.
 * GET /api/v1/health/detailed
 */
router.get('/detailed', (req, res) => {
  const { pipeline } = req.app.locals;
  const memUsage = process.memoryUsage();

  res.json({
    status: metrics.healthScore() >= 0.6 ? 'ok' : 'critical',
    server: {
      uptime: process.uptime(),
      pid: process.pid,
      nodeVersion: process.version,
      platform: process.platform,
      arch: process.arch
    },
    memory: {
      rss: `${(memUsage.rss / 1024 / 1024).toFixed(2)} MB`,
      heapTotal: `${(memUsage.heapTotal / 1024 / 1024).toFixed(2)} MB`,
      heapUsed: `${(memUsage.heapUsed / 1024 / 1024).toFixed(2)} MB`,
      external: `${(memUsage.external / 1024 / 1024).toFixed(2)} MB`
    },
    queue: pipeline.queue.stats(),
    circuitBreakers: pipeline.sinkManager.getStatuses(),
    stages: pipeline.config.stages.map(s => ({
      name: s.name,
      enabled: s.enabled
    })),
    pipelineExecutions: pipeline.registry.stats(),
    deadLetter: pipeline.dlq.stats()
  });
});

/**
 * Metrics endpoint.
 * GET /api/v1/metrics
 */
router.get('/', (req, res) => {
  const snapshot = metrics.snapshot();
  res.json({
    timestamp: new Date().toISOString(),
    ...snapshot
  });
});

/**
 * Prometheus-compatible metrics.
 * GET /api/v1/metrics/prometheus
 */
router.get('/prometheus', (req, res) => {
  const snapshot = metrics.snapshot();
  const lines = [];

  // Counters
  for (const [key, value] of Object.entries(snapshot.counters)) {
    const metricName = `pipeline_${key.replace(/([A-Z])/g, '_$1').toLowerCase()}`;
    lines.push(`# HELP ${metricName} Pipeline ${key}`);
    lines.push(`# TYPE ${metricName} counter`);
    lines.push(`${metricName} ${value}`);
  }

  // Gauges
  for (const [key, value] of Object.entries(snapshot.gauges)) {
    const metricName = `pipeline_${key.replace(/([A-Z])/g, '_$1').toLowerCase()}`;
    lines.push(`# HELP ${metricName} Pipeline ${key}`);
    lines.push(`# TYPE ${metricName} gauge`);
    lines.push(`${metricName} ${typeof value === 'string' ? (value === 'open' ? 1 : 0) : value}`);
  }

  // Latency histogram
  lines.push('# HELP pipeline_processing_latency_ms Event processing latency');
  lines.push('# TYPE pipeline_processing_latency_ms histogram');
  const lat = snapshot.histograms.processingLatencyMs;
  lines.push(`pipeline_processing_latency_ms_p50 ${lat.p50}`);
  lines.push(`pipeline_processing_latency_ms_p95 ${lat.p95}`);
  lines.push(`pipeline_processing_latency_ms_p99 ${lat.p99}`);

  // Throughput
  lines.push('# HELP pipeline_throughput_events_per_second Current event throughput');
  lines.push('# TYPE pipeline_throughput_events_per_second gauge');
  lines.push(`pipeline_throughput_events_per_second ${snapshot.throughput.eventsPerSecond.toFixed(2)}`);

  res.setHeader('Content-Type', 'text/plain');
  res.send(lines.join('\n'));
});

/**
 * Analytics summary.
 * GET /api/v1/metrics/analytics
 */
router.get('/analytics', (req, res) => {
  const summary = analyticsProcessor.getSummary();
  const totals = analyticsProcessor.getTotals();
  res.json({ summary, totals });
});

/**
 * Alert history.
 * GET /api/v1/metrics/alerts
 */
router.get('/alerts', (req, res) => {
  const alerts = alertProcessor.getAlerts();
  res.json({ alerts, count: alerts.length });
});

/**
 * Clear alert history.
 * POST /api/v1/metrics/alerts/clear
 */
router.post('/alerts/clear', (req, res) => {
  alertProcessor.clearAlerts();
  res.json({ success: true, message: 'Alert history cleared' });
});

/**
 * Queue metrics.
 * GET /api/v1/metrics/queue
 */
router.get('/queue', (req, res) => {
  const { pipeline } = req.app.locals;
  res.json({
    stats: pipeline.queue.stats(),
    metrics: {
      enqueued: metrics.getCounter('eventsIngested'),
      processed: metrics.getCounter('eventsProcessed'),
      failed: metrics.getCounter('eventsFailed'),
      dropped: metrics.getCounter('eventsFiltered')
    }
  });
});

module.exports = router;