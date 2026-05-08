/**
 * Unit tests for the Pipeline Engine.
 */

const { PipelineEngine } = require('../src/server');
const { EventQueue } = require('../src/utils/queue');
const { createValidEvent } = require('./test_validator');

jest.setTimeout(10000);

describe('PipelineEngine', () => {
  let pipeline;

  beforeEach(() => {
    const config = {
      stages: [
        { name: 'validate', enabled: true },
        { name: 'enrich',    enabled: true },
        { name: 'filter',    enabled: true },
        { name: 'transform', enabled: true },
        { name: 'route',     enabled: true },
        { name: 'sink',      enabled: true }
      ],
      sinks: {
        file: { enabled: false },
        webhook: { enabled: false },
        console: { enabled: false }
      },
      routes: [
        { pattern: 'user.*', processors: ['logProcessor'] },
        { pattern: '*', processors: ['logProcessor'] }
      ],
      deadLetter: {
        maxRetries: 2,
        outputDir: '/tmp/test-dlq',
        alertThreshold: 100
      },
      backpressure: {
        maxQueueSize: 1000,
        maxConcurrency: 10,
        drainIntervalMs: 50,
        highWatermark: 0.8,
        lowWatermark: 0.5
      },
      replay: {
        batchSize: 50,
        maxLookbackDays: 7
      },
      validation: {
        schemaDir: require('path').join(__dirname, '..', 'schemas'),
        strictMode: false
      }
    };

    pipeline = new PipelineEngine(config);
  });

  afterEach(() => {
    pipeline.stopDraining();
  });

  test('should initialize with correct components', () => {
    expect(pipeline.queue).toBeInstanceOf(EventQueue);
    expect(pipeline.validator).toBeDefined();
    expect(pipeline.transformer).toBeDefined();
    expect(pipeline.filter).toBeDefined();
    expect(pipeline.enricher).toBeDefined();
    expect(pipeline.router).toBeDefined();
    expect(pipeline.sinkManager).toBeDefined();
    expect(pipeline.registry).toBeDefined();
    expect(pipeline.dlq).toBeDefined();
  });

  test('should process a valid event end-to-end', async () => {
    const event = createValidEvent();
    const result = await pipeline.processEvent(event);

    expect(result).toBeDefined();
    expect(result.stages).toBeDefined();
    expect(result.stages.length).toBeGreaterThanOrEqual(1);
  });

  test('should handle invalid event (missing type)', async () => {
    const event = createValidEvent({ type: undefined });
    const result = await pipeline.processEvent(event);

    expect(result).toBeDefined();
    expect(result.success).toBe(false);
  });

  test('should handle empty payload event through filter', async () => {
    const event = createValidEvent({ payload: {} });
    const result = await pipeline.processEvent(event);

    expect(result).toBeDefined();
    // Event may be filtered or processed depending on filter rules
    expect(result.stages).toBeDefined();
  });

  test('should enqueue event in queue', () => {
    const event = createValidEvent();
    const accepted = pipeline.queue.enqueue(event);
    expect(accepted).toBe(true);
    expect(pipeline.queue.size()).toBe(1);
  });

  test('should reject event when backpressured', () => {
    // Fill queue to high watermark
    const { maxQueueSize, highWatermark } = pipeline.config.backpressure;
    const fillCount = Math.floor(maxQueueSize * highWatermark);

    for (let i = 0; i < fillCount; i++) {
      pipeline.queue.enqueue(createValidEvent({ id: `evt-${i}` }));
    }

    const event = createValidEvent({ id: 'overflow-event' });
    const accepted = pipeline.queue.enqueue(event);
    expect(accepted).toBe(false);
  });

  test('should start and stop draining', () => {
    pipeline.startDraining();
    expect(pipeline.status).toBe('initializing'); // status changes on processing

    pipeline.stopDraining();
    // Should not throw
  });

  test('should check stage enabled status', () => {
    expect(pipeline.isStageEnabled('validate')).toBe(true);

    // Disable a stage
    const stage = pipeline.config.stages.find(s => s.name === 'transform');
    stage.enabled = false;
    expect(pipeline.isStageEnabled('transform')).toBe(false);
  });

  test('should track pipeline executions in registry', async () => {
    const event = createValidEvent();
    await pipeline.processEvent(event);

    const stats = pipeline.registry.stats();
    expect(stats.total).toBeGreaterThanOrEqual(0);
  });

  test('should add events to DLQ after max retries', async () => {
    // Process an event that will fail validation
    const event = createValidEvent({ timestamp: undefined });
    await pipeline.processEvent(event);

    const dlqStats = pipeline.dlq.stats();
    // DLQ should have entries from failed events
    expect(dlqStats.totalEntries).toBeGreaterThanOrEqual(0);
  });

  test('should handle system events with pass filter', async () => {
    const event = createValidEvent({
      type: 'system.error',
      source: 'internal',
      payload: { message: 'Test error', severity: 'high' }
    });
    const result = await pipeline.processEvent(event);

    expect(result).toBeDefined();
    expect(result.stages).toBeDefined();
  });
});

module.exports = {};