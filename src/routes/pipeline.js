/**
 * Pipeline management routes.
 * Provides endpoints for configuring filters, transforms, enrichers,
 * routing rules, and viewing pipeline state.
 */

const express = require('express');
const logger = require('../utils/logger');

const router = express.Router();

/**
 * Get pipeline status and configuration.
 * GET /api/v1/pipeline
 */
router.get('/', (req, res) => {
  const { pipeline } = req.app.locals;
  res.json({
    status: pipeline.status,
    stages: pipeline.config.stages.map(s => ({
      ...s,
      active: pipeline.isStageEnabled(s.name)
    })),
    routes: pipeline.router.getRoutes(),
    processors: pipeline.router.getProcessors(),
    filters: pipeline.filter.getRules(),
    enrichers: pipeline.enricher.list(),
    transforms: pipeline.transformer.list(),
    sinks: pipeline.sinkManager.getStatuses(),
    backpressure: {
      queueStats: pipeline.queue.stats(),
      config: pipeline.config.backpressure
    }
  });
});

/**
 * Get routing configuration.
 * GET /api/v1/pipeline/routes
 */
router.get('/routes', (req, res) => {
  const { pipeline } = req.app.locals;
  res.json({
    routes: pipeline.router.getRoutes(),
    processors: pipeline.router.getProcessors()
  });
});

/**
 * Add a routing rule.
 * POST /api/v1/pipeline/routes
 */
router.post('/routes', (req, res) => {
  const { pipeline } = req.app.locals;
  const { pattern, processors } = req.body;

  if (!pattern || !Array.isArray(processors)) {
    return res.status(400).json({
      success: false,
      error: 'Requires pattern (string) and processors (array)'
    });
  }

  pipeline.router.routes.set(pattern, processors);
  logger.info('Route added', { pattern, processors });

  res.json({ success: true, pattern, processors });
});

/**
 * Get filter rules.
 * GET /api/v1/pipeline/filters
 */
router.get('/filters', (req, res) => {
  const { pipeline } = req.app.locals;
  res.json({ filters: pipeline.filter.getRules() });
});

/**
 * Add a filter rule.
 * POST /api/v1/pipeline/filters
 */
router.post('/filters', (req, res) => {
  const { pipeline } = req.app.locals;
  const { name, type, config, action, priority } = req.body;

  if (!name || !type || !config) {
    return res.status(400).json({
      success: false,
      error: 'Requires name, type, and config fields'
    });
  }

  pipeline.filter.addRule({ name, type, config, action: action || 'drop', priority: priority || 0 });
  res.json({ success: true, rule: { name, type, action: action || 'drop' } });
});

/**
 * Toggle a filter rule.
 * PATCH /api/v1/pipeline/filters/:name/toggle
 */
router.patch('/filters/:name/toggle', (req, res) => {
  const { pipeline } = req.app.locals;
  const active = pipeline.filter.toggleRule(req.params.name);
  res.json({ success: true, name: req.params.name, active });
});

/**
 * Delete a filter rule.
 * DELETE /api/v1/pipeline/filters/:name
 */
router.delete('/filters/:name', (req, res) => {
  const { pipeline } = req.app.locals;
  pipeline.filter.removeRule(req.params.name);
  res.json({ success: true, name: req.params.name, message: 'Rule removed' });
});

/**
 * Get enricher configuration.
 * GET /api/v1/pipeline/enrichers
 */
router.get('/enrichers', (req, res) => {
  const { pipeline } = req.app.locals;
  res.json({
    enrichers: pipeline.enricher.list(),
    cacheStats: pipeline.enricher.cacheStats()
  });
});

/**
 * Clear all enricher caches.
 * POST /api/v1/pipeline/enrichers/clear-cache
 */
router.post('/enrichers/clear-cache', (req, res) => {
  const { pipeline } = req.app.locals;
  pipeline.enricher.clearCaches();
  res.json({ success: true, message: 'All caches cleared' });
});

/**
 * Get transform configuration.
 * GET /api/v1/pipeline/transforms
 */
router.get('/transforms', (req, res) => {
  const { pipeline } = req.app.locals;
  res.json({ transforms: pipeline.transformer.list() });
});

/**
 * Toggle a pipeline stage.
 * PATCH /api/v1/pipeline/stages/:name/toggle
 */
router.patch('/stages/:name/toggle', (req, res) => {
  const { pipeline } = req.app.locals;
  const stage = pipeline.config.stages.find(s => s.name === req.params.name);
  if (!stage) {
    return res.status(404).json({
      success: false,
      error: `Stage '${req.params.name}' not found`
    });
  }

  stage.enabled = !stage.enabled;
  logger.info(`Stage '${req.params.name}' ${stage.enabled ? 'enabled' : 'disabled'}`);

  res.json({ success: true, stage: req.params.name, enabled: stage.enabled });
});

/**
 * Get sink statuses.
 * GET /api/v1/pipeline/sinks
 */
router.get('/sinks', (req, res) => {
  const { pipeline } = req.app.locals;
  res.json({ sinks: pipeline.sinkManager.getStatuses() });
});

/**
 * Update pipeline configuration.
 * PUT /api/v1/pipeline/config
 */
router.put('/config', (req, res) => {
  const { pipeline } = req.app.locals;
  const updates = req.body;

  if (updates.stages) {
    for (const stage of updates.stages) {
      const existing = pipeline.config.stages.find(s => s.name === stage.name);
      if (existing) {
        existing.enabled = stage.enabled;
      }
    }
  }

  logger.info('Pipeline config updated');
  res.json({ success: true, config: pipeline.config.stages });
});

module.exports = router;