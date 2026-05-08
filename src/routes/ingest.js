/**
 * Event ingestion routes.
 * Handles single and batch event ingestion with validation, backpressure,
 * and immediate or queued processing.
 */

const express = require('express');
const { v4: uuidv4 } = require('uuid');
const { normalizeEvent } = require('../models/Event');
const { metrics } = require('../utils/metrics');
const logger = require('../utils/logger');
const { getConfig } = require('../config');

const router = express.Router();

/**
 * Ingest a single event.
 * POST /api/v1/ingest
 */
router.post('/', async (req, res) => {
  const start = Date.now();
  const { pipeline } = req.app.locals;
  const event = normalizeEvent(req.body);

  try {
    // Check backpressure
    if (pipeline.queue.isBackpressured()) {
      metrics.increment('backpressureHits');
      logger.warn('Backpressure active - event rejected', { eventId: event.id });
      return res.status(503).json({
        success: false,
        error: 'Service temporarily unavailable - backpressure active',
        eventId: event.id,
        queueStats: pipeline.queue.stats()
      });
    }

    metrics.increment('eventsIngested');

    // Process synchronously or enqueue
    const processSync = req.query.sync === 'true';

    if (processSync) {
      const result = await pipeline.processEvent(event);
      res.status(200).json({
        success: result.success,
        eventId: event.id,
        processingTimeMs: Date.now() - start,
        stages: result.stages || [],
        result: result.success ? 'processed' : 'failed'
      });
    } else {
      const accepted = pipeline.queue.enqueue(event, req.body.priority || 5);
      if (!accepted) {
        return res.status(503).json({
          success: false,
          error: 'Queue full - event dropped',
          eventId: event.id
        });
      }

      res.status(202).json({
        success: true,
        eventId: event.id,
        status: 'queued',
        queuePosition: pipeline.queue.size(),
        processingTimeMs: Date.now() - start
      });
    }
  } catch (err) {
    logger.error('Ingest failed', { eventId: event.id, error: err.message });
    res.status(500).json({
      success: false,
      error: 'Processing failed',
      eventId: event.id
    });
  }
});

/**
 * Ingest a batch of events.
 * POST /api/v1/ingest/batch
 */
router.post('/batch', async (req, res) => {
  const start = Date.now();
  const { events } = req.body;

  if (!Array.isArray(events)) {
    return res.status(400).json({
      success: false,
      error: 'Expected array of events in "events" field'
    });
  }

  if (events.length === 0) {
    return res.status(400).json({
      success: false,
      error: 'Empty batch'
    });
  }

  const config = getConfig();
  if (events.length > config.backpressure.maxConcurrency * 2) {
    return res.status(400).json({
      success: false,
      error: `Batch too large. Max ${config.backpressure.maxConcurrency * 2} events.`
    });
  }

  const { pipeline } = req.app.locals;
  const results = [];
  let accepted = 0;
  let rejected = 0;

  try {
    for (const raw of events) {
      const event = normalizeEvent(raw);

      if (pipeline.queue.isBackpressured()) {
        rejected++;
        results.push({ eventId: event.id, status: 'rejected', reason: 'backpressure' });
        continue;
      }

      const enqueued = pipeline.queue.enqueue(event, raw.priority || 5);
      if (enqueued) {
        accepted++;
        metrics.increment('eventsIngested');
        results.push({ eventId: event.id, status: 'queued' });
      } else {
        rejected++;
        results.push({ eventId: event.id, status: 'rejected', reason: 'queue-full' });
      }
    }

    res.status(202).json({
      success: true,
      summary: { total: events.length, accepted, rejected },
      results,
      processingTimeMs: Date.now() - start
    });
  } catch (err) {
    logger.error('Batch ingest failed', { error: err.message });
    res.status(500).json({
      success: false,
      error: 'Batch processing failed'
    });
  }
});

/**
 * Health check for the ingest endpoint.
 * GET /api/v1/ingest/health
 */
router.get('/health', (req, res) => {
  const { pipeline } = req.app.locals;
  res.json({
    status: 'healthy',
    queue: pipeline.queue.stats(),
    canAccept: !pipeline.queue.isBackpressured()
  });
});

/**
 * Validate an event without processing.
 * POST /api/v1/ingest/validate
 */
router.post('/validate', (req, res) => {
  const { pipeline } = req.app.locals;
  const event = normalizeEvent(req.body);
  const result = pipeline.validator.validate(event);

  res.status(result.valid ? 200 : 400).json({
    valid: result.valid,
    errors: result.errors,
    warnings: result.warnings,
    durationMs: result.durationMs
  });
});

module.exports = router;