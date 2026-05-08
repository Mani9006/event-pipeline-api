/**
 * Event-Driven Data Pipeline API
 * Main application entry point.
 *
 * Orchestrates the pipeline engine: queue, router, validator, transformer,
 * filter, enricher, sinks, and dead-letter queue. Provides REST endpoints
 * for ingestion, management, monitoring, and replay.
 */

const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const compression = require('compression');
const path = require('path');

const { loadConfig, getConfig, port, nodeEnv } = require('./config');
const logger = require('./utils/logger');
const { EventQueue } = require('./utils/queue');
const { metrics } = require('./utils/metrics');

const { PipelineValidator } = require('./pipeline/validator');
const { Transformer, normalizeTimestamp, addComputedFields, anonymizeFields, flattenPayload } = require('./pipeline/transformer');
const { Filter } = require('./pipeline/filter');
const { Enricher, geoEnrichment, timeEnrichment, sourceEnrichment, dedupHash } = require('./pipeline/enricher');
const { Router, createRouter } = require('./pipeline/router');
const { SinkManager, createFileSink, createConsoleSink, createWebhookSink } = require('./pipeline/sinks');

const { EventStore, normalizeEvent } = require('./models/Event');
const { PipelineRegistry } = require('./models/Pipeline');
const { DeadLetterQueue } = require('./models/DeadLetter');

const { logProcessor } = require('./processors/logProcessor');
const { analyticsProcessor } = require('./processors/analyticsProcessor');
const { alertProcessor } = require('./processors/alertProcessor');

const { createRateLimiter } = require('./middleware/rateLimiter');
const { requestLogger, errorLogger } = require('./middleware/requestLogger');
const { validateEvent, validateByType, sanitizeBody } = require('./middleware/schemaValidator');

const ingestRoutes = require('./routes/ingest');
const pipelineRoutes = require('./routes/pipeline');
const monitorRoutes = require('./routes/monitor');
const replayRoutes = require('./routes/replay');

// ─── Pipeline Engine ─────────────────────────────────────────────────────────

class PipelineEngine {
  constructor(config) {
    this.config = config;
    this.status = 'initializing';

    // Core components
    this.queue = new EventQueue({
      maxSize: config.backpressure.maxQueueSize,
      highWatermark: config.backpressure.highWatermark,
      lowWatermark: config.backpressure.lowWatermark
    });

    this.validator = new PipelineValidator(
      config.validation.schemaDir,
      config.validation.strictMode
    );

    this.transformer = new Transformer();
    this.filter = new Filter();
    this.enricher = new Enricher();
    this.router = createRouter(config);
    this.sinkManager = new SinkManager(config);
    this.registry = new PipelineRegistry();
    this.dlq = new DeadLetterQueue(config.deadLetter.outputDir, config.deadLetter.alertThreshold);

    this._drainInterval = null;
    this._setupDefaults();
  }

  // ─── Default Configuration ─────────────────────────────────────────────────

  _setupDefaults() {
    // Default transforms
    this.transformer.register('normalizeTimestamp', normalizeTimestamp);
    this.transformer.register('addComputedFields', addComputedFields);
    this.transformer.register('anonymizeEmail', anonymizeFields(['email']), (event) => !!event.payload?.email);
    this.transformer.register('flattenPayload', flattenPayload, (event) => Object.keys(event.payload || {}).length > 5);

    // Default enrichers
    this.enricher.register('geo', geoEnrichment, { cached: true, cacheTTL: 60000, priority: 1 });
    this.enricher.register('time', timeEnrichment, { priority: 2 });
    this.enricher.register('source', sourceEnrichment, { priority: 3 });
    this.enricher.register('dedup', dedupHash, { priority: 4 });

    // Default filters
    this.filter.addRule({
      name: 'drop-empty-payload',
      type: 'expression',
      config: { expression: '!event.payload || Object.keys(event.payload).length === 0' },
      action: 'drop',
      priority: 1
    });

    this.filter.addRule({
      name: 'drop-old-events',
      type: 'expression',
      config: {
        expression: '(Date.now() - new Date(event.timestamp || Date.now()).getTime()) > 86400000'
      },
      action: 'drop',
      priority: 2
    });

    this.filter.addRule({
      name: 'pass-internal',
      type: 'type-match',
      config: { patterns: ['system.*'] },
      action: 'pass',
      priority: 0
    });

    // Register processors
    this.router.register('logProcessor', logProcessor);
    this.router.register('analyticsProcessor', analyticsProcessor);
    this.router.register('alertProcessor', alertProcessor);

    // Register sinks
    if (config.sinks.file.enabled) {
      this.sinkManager.register('file', createFileSink(config.sinks.file.outputDir), {
        retryAttempts: 3,
        circuitBreaker: { failureThreshold: 10, resetTimeoutMs: 30000 }
      });
    }

    if (config.sinks.webhook.enabled) {
      this.sinkManager.register('webhook', createWebhookSink(config.sinks.webhook.targets), {
        retryAttempts: 2,
        circuitBreaker: { failureThreshold: 5, resetTimeoutMs: 60000 }
      });
    }

    if (config.sinks.console.enabled) {
      this.sinkManager.register('console', createConsoleSink());
    }

    // Queue event handlers
    this.queue.on('dropped', (event) => {
      metrics.increment('eventsFailed');
      logger.warn('Event dropped due to backpressure', { eventId: event.id });
    });
  }

  // ─── Event Processing ──────────────────────────────────────────────────────

  async processEvent(event) {
    const start = Date.now();
    const execution = this.registry.register(event._internal?.executionId
      ? { id: event._internal.executionId, eventId: event.id, config: this.config }
      : new (require('./models/Pipeline').PipelineExecution)(event.id, this.config)
    );

    try {
      execution.addStage('validate', 'running');

      // 1. Validate
      const validation = this.validator.validate(event);
      if (!validation.valid) {
        execution.completeStage('validate', 'failed', { errors: validation.errors });
        execution.finalize('failed', new Error(`Validation failed: ${validation.errors.map(e => e.message).join(', ')}`));
        await this._handleFailure(event, new Error('Validation failed'), 'validate');
        return { success: false, stages: execution.stages };
      }
      execution.completeStage('validate', 'completed', { durationMs: validation.durationMs });

      // 2. Enrich
      execution.addStage('enrich', 'running');
      let enriched = event;
      if (this.isStageEnabled('enrich')) {
        enriched = await this.enricher.enrich(event);
      }
      execution.completeStage('enrich', 'completed');

      // 3. Filter
      execution.addStage('filter', 'running');
      if (this.isStageEnabled('filter')) {
        const filterResult = this.filter.evaluate(enriched);
        if (!filterResult.passed) {
          execution.completeStage('filter', 'completed', { filtered: true, reason: filterResult.reason });
          execution.finalize('completed');
          metrics.increment('eventsFiltered');
          logger.info('Event filtered', { eventId: event.id, reason: filterResult.reason });
          return { success: true, stages: execution.stages, filtered: true };
        }
      }
      execution.completeStage('filter', 'completed', { filtered: false });

      // 4. Transform
      execution.addStage('transform', 'running');
      let transformed = enriched;
      if (this.isStageEnabled('transform')) {
        transformed = await this.transformer.transform(enriched);
      }
      execution.completeStage('transform', 'completed');

      // 5. Route
      execution.addStage('route', 'running');
      let routed = transformed;
      if (this.isStageEnabled('route')) {
        routed = await this.router.route(transformed, { executionId: execution.id });
      }
      execution.completeStage('route', 'completed');

      // 6. Sink
      execution.addStage('sink', 'running');
      if (this.isStageEnabled('sink')) {
        await this.sinkManager.write(routed, { executionId: execution.id });
      }
      execution.completeStage('sink', 'completed');

      // Complete
      const durationMs = Date.now() - start;
      execution.finalize('completed');
      metrics.recordProcessingTime(durationMs);
      metrics.increment('eventsProcessed');

      logger.info('Event processed', {
        eventId: event.id,
        type: event.type,
        durationMs,
        stages: execution.stages.length
      });

      return { success: true, stages: execution.stages, executionId: execution.id };
    } catch (err) {
      execution.finalize('failed', err);
      metrics.increment('eventsFailed');
      await this._handleFailure(event, err, execution.stages[execution.stages.length - 1]?.name || 'unknown');

      logger.error('Pipeline processing failed', {
        eventId: event.id,
        stage: execution.stages[execution.stages.length - 1]?.name,
        error: err.message
      });

      return { success: false, stages: execution.stages, error: err.message };
    }
  }

  async _handleFailure(event, error, stage) {
    const retryCount = event._internal?.retryCount || 0;
    const config = getConfig();

    if (retryCount < config.deadLetter.maxRetries) {
      // Re-enqueue for retry with incremented count
      const retryEvent = {
        ...event,
        _internal: {
          ...event._internal,
          retryCount: retryCount + 1,
          lastError: error.message,
          lastFailedStage: stage
        }
      };
      this.queue.enqueue(retryEvent, retryCount + 1);
      logger.info('Event queued for retry', {
        eventId: event.id,
        attempt: retryCount + 1
      });
    } else {
      // Send to dead letter queue
      await this.dlq.add(event, error, stage);
      metrics.increment('deadLetterQueued');
      logger.warn('Event moved to dead letter queue', {
        eventId: event.id,
        stage,
        retries: retryCount
      });
    }
  }

  // ─── Queue Draining ────────────────────────────────────────────────────────

  startDraining() {
    const config = this.config.backpressure;
    this._drainInterval = setInterval(async () => {
      const batch = this.queue.drainBatch(config.maxConcurrency);
      if (batch.length === 0) return;

      metrics.recordQueueDepth(this.queue.size());
      this.status = 'processing';

      const promises = batch.map(event => this.processEvent(event));
      await Promise.all(promises);

      if (this.queue.isEmpty()) {
        this.status = 'idle';
      }
    }, config.drainIntervalMs);

    logger.info('Pipeline draining started', {
      intervalMs: config.drainIntervalMs,
      maxConcurrency: config.maxConcurrency
    });
  }

  stopDraining() {
    if (this._drainInterval) {
      clearInterval(this._drainInterval);
      this._drainInterval = null;
      logger.info('Pipeline draining stopped');
    }
  }

  isStageEnabled(name) {
    const stage = this.config.stages.find(s => s.name === name);
    return stage ? stage.enabled : false;
  }
}

// ─── Application Factory ─────────────────────────────────────────────────────

function createApp() {
  const config = loadConfig();
  const app = express();

  // Global middleware
  app.use(helmet({
    contentSecurityPolicy: false // Allow flexibility for API clients
  }));
  app.use(cors());
  app.use(compression());
  app.use(express.json({ limit: '1mb' }));
  app.use(requestLogger);

  // Rate limiting
  app.use(createRateLimiter({
    windowMs: 60_000,
    maxRequests: 1000,
    burstAllowance: 50
  }));

  // Pipeline engine
  const pipeline = new PipelineEngine(config);
  const eventStore = new EventStore(config.sinks.file.outputDir);

  app.locals = {
    ...app.locals,
    pipeline,
    eventStore,
    config
  };

  // ─── Routes ────────────────────────────────────────────────────────────────

  app.use('/api/v1/ingest', sanitizeBody, validateEvent(), validateByType, ingestRoutes);
  app.use('/api/v1/pipeline', pipelineRoutes);
  app.use('/api/v1/health', monitorRoutes);
  app.use('/api/v1/metrics', monitorRoutes);
  app.use('/api/v1/replay', replayRoutes);

  // Root route
  app.get('/', (req, res) => {
    res.json({
      name: 'Event-Driven Data Pipeline API',
      version: process.env.npm_package_version || '1.0.0',
      status: 'running',
      endpoints: {
        ingest: '/api/v1/ingest',
        pipeline: '/api/v1/pipeline',
        health: '/api/v1/health',
        metrics: '/api/v1/metrics',
        replay: '/api/v1/replay'
      },
      documentation: '/api/v1/docs'
    });
  });

  // Documentation route
  app.get('/api/v1/docs', (req, res) => {
    res.json({
      description: 'Event-driven data processing pipeline API',
      sections: {
        ingestion: {
          'POST /api/v1/ingest': 'Submit a single event',
          'POST /api/v1/ingest/batch': 'Submit a batch of events',
          'POST /api/v1/ingest/validate': 'Validate an event without processing',
          'GET /api/v1/ingest/health': 'Check ingestion endpoint health'
        },
        pipeline: {
          'GET /api/v1/pipeline': 'Get full pipeline status',
          'GET /api/v1/pipeline/routes': 'Get routing configuration',
          'POST /api/v1/pipeline/routes': 'Add a routing rule',
          'GET /api/v1/pipeline/filters': 'Get filter rules',
          'POST /api/v1/pipeline/filters': 'Add a filter rule',
          'PATCH /api/v1/pipeline/stages/:name/toggle': 'Toggle a pipeline stage',
          'PUT /api/v1/pipeline/config': 'Update pipeline configuration'
        },
        monitoring: {
          'GET /api/v1/health': 'Health check',
          'GET /api/v1/health/detailed': 'Detailed health with diagnostics',
          'GET /api/v1/metrics': 'Metrics snapshot',
          'GET /api/v1/metrics/prometheus': 'Prometheus-compatible metrics',
          'GET /api/v1/metrics/analytics': 'Analytics summary',
          'GET /api/v1/metrics/alerts': 'Alert history',
          'GET /api/v1/metrics/queue': 'Queue metrics'
        },
        replay: {
          'POST /api/v1/replay': 'Replay events by time range',
          'POST /api/v1/replay/dead-letter': 'Replay dead-letter events',
          'POST /api/v1/replay/queue': 'Replay queued events',
          'GET /api/v1/replay/status': 'Get replay status'
        }
      }
    });
  });

  // Error handling (must be last)
  app.use(errorLogger);

  return { app, pipeline, eventStore, config };
}

// ─── Server Startup ──────────────────────────────────────────────────────────

function startServer() {
  const { app, pipeline, eventStore } = createApp();

  // Start queue draining
  pipeline.startDraining();

  const server = app.listen(port, () => {
    logger.info('Server started', {
      port,
      env: nodeEnv,
      nodeVersion: process.version
    });
  });

  // Graceful shutdown
  process.on('SIGTERM', () => shutdown(server, pipeline));
  process.on('SIGINT', () => shutdown(server, pipeline));

  return server;
}

function shutdown(server, pipeline) {
  logger.info('Shutdown signal received, starting graceful shutdown...');
  pipeline.stopDraining();
  server.close(() => {
    logger.info('Server closed');
    process.exit(0);
  });

  // Force shutdown after 30 seconds
  setTimeout(() => {
    logger.error('Forced shutdown after timeout');
    process.exit(1);
  }, 30000);
}

// Start if run directly
if (require.main === module) {
  startServer();
}

module.exports = { createApp, startServer, PipelineEngine };