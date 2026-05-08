/**
 * Event replay routes.
 * Supports replaying events from storage by time range, type, or DLQ.
 */

const express = require('express');
const { metrics } = require('../utils/metrics');
const logger = require('../utils/logger');
const { getConfig } = require('../config');

const router = express.Router();

/**
 * Replay events by time range.
 * POST /api/v1/replay
 */
router.post('/', async (req, res) => {
  const start = Date.now();
  const { pipeline, eventStore } = req.app.locals;
  const { from, to, eventType, batchSize, dryRun } = req.body;

  // Validate time range
  if (!from || !to) {
    return res.status(400).json({
      success: false,
      error: 'Requires "from" and "to" ISO timestamps'
    });
  }

  const startTime = new Date(from).getTime();
  const endTime = new Date(to).getTime();

  if (isNaN(startTime) || isNaN(endTime)) {
    return res.status(400).json({
      success: false,
      error: 'Invalid timestamp format'
    });
  }

  const config = getConfig();
  const maxLookback = config.replay.maxLookbackDays * 86400000;
  if (Date.now() - startTime > maxLookback) {
    return res.status(400).json({
      success: false,
      error: `Lookback exceeds maximum of ${config.replay.maxLookbackDays} days`
    });
  }

  try {
    // Find events
    const events = await eventStore.findByTimeRange(startTime, endTime);
    let filtered = events;
    if (eventType) {
      filtered = events.filter(e => e.type === eventType);
    }

    const bs = Math.min(batchSize || config.replay.batchSize, 500);
    const totalBatches = Math.ceil(filtered.length / bs);

    if (dryRun) {
      return res.json({
        success: true,
        dryRun: true,
        totalEvents: filtered.length,
        totalBatches,
        timeRange: { from, to },
        eventType: eventType || 'all',
        sample: filtered.slice(0, 3).map(e => ({ id: e.id, type: e.type, timestamp: e.timestamp }))
      });
    }

    // Start replay
    metrics.increment('replaysInitiated');
    const replayId = `replay-${Date.now()}`;
    let processed = 0;
    let failed = 0;

    // Process in batches
    for (let i = 0; i < filtered.length; i += bs) {
      const batch = filtered.slice(i, i + bs);
      const promises = batch.map(async (event) => {
        try {
          await pipeline.processEvent(event);
          processed++;
        } catch (err) {
          failed++;
          logger.error('Replay event failed', { eventId: event.id, error: err.message });
        }
      });

      await Promise.all(promises);
      logger.info(`Replay batch ${Math.floor(i / bs) + 1}/${totalBatches} processed`);
    }

    metrics.increment('replaysCompleted');

    res.json({
      success: true,
      replayId,
      totalEvents: filtered.length,
      processed,
      failed,
      totalBatches,
      processingTimeMs: Date.now() - start
    });
  } catch (err) {
    logger.error('Replay failed', { error: err.message });
    res.status(500).json({
      success: false,
      error: 'Replay failed',
      details: err.message
    });
  }
});

/**
 * Replay dead-letter events.
 * POST /api/v1/replay/dead-letter
 */
router.post('/dead-letter', async (req, res) => {
  const start = Date.now();
  const { pipeline } = req.app.locals;
  const { dryRun, maxEvents } = req.body;

  const replayable = pipeline.dlq.getReplayable();
  const toReplay = maxEvents ? replayable.slice(0, maxEvents) : replayable;

  if (dryRun) {
    return res.json({
      success: true,
      dryRun: true,
      totalReplayable: replayable.length,
      willReplay: toReplay.length,
      sample: toReplay.slice(0, 5).map(e => e.id)
    });
  }

  let processed = 0;
  let failed = 0;

  for (const entry of toReplay) {
    try {
      // Increment retry count
      const event = {
        ...entry.originalEvent,
        _internal: {
          ...entry.originalEvent._internal,
          retryCount: (entry.retryCount || 0) + 1,
          replayedAt: new Date().toISOString()
        }
      };

      await pipeline.processEvent(event);
      pipeline.dlq.markReplayed(entry.id);
      processed++;
    } catch (err) {
      failed++;
      logger.error('DLQ replay failed', { eventId: entry.id, error: err.message });
    }
  }

  metrics.increment('replaysCompleted');

  res.json({
    success: true,
    totalReplayed: toReplay.length,
    processed,
    failed,
    remaining: pipeline.dlq.getReplayable().length,
    processingTimeMs: Date.now() - start
  });
});

/**
 * Get replay status/history.
 * GET /api/v1/replay/status
 */
router.get('/status', (req, res) => {
  const { pipeline } = req.app.locals;
  res.json({
    replaysInitiated: metrics.getCounter('replaysInitiated'),
    replaysCompleted: metrics.getCounter('replaysCompleted'),
    deadLetterQueue: {
      total: pipeline.dlq.stats().totalEntries,
      replayable: pipeline.dlq.getReplayable().length
    }
  });
});

/**
 * Replay events from the queue (in-memory).
 * POST /api/v1/replay/queue
 */
router.post('/queue', async (req, res) => {
  const { pipeline } = req.app.locals;
  const batch = pipeline.queue.drainBatch(100);

  if (batch.length === 0) {
    return res.json({
      success: true,
      replayed: 0,
      message: 'No queued events to replay'
    });
  }

  let processed = 0;
  let failed = 0;

  const promises = batch.map(async (event) => {
    try {
      await pipeline.processEvent(event);
      processed++;
    } catch (err) {
      failed++;
      logger.error('Queue replay event failed', { eventId: event.id, error: err.message });
    }
  });

  await Promise.all(promises);

  res.json({
    success: true,
    total: batch.length,
    processed,
    failed,
    queueRemaining: pipeline.queue.size()
  });
});

module.exports = router;