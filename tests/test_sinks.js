/**
 * Unit tests for Sink implementations.
 */

const { SinkManager, CircuitBreaker, createFileSink, createConsoleSink, createWebhookSink } = require('../src/pipeline/sinks');
const fs = require('fs');
const path = require('path');

// ─── Test Helpers ────────────────────────────────────────────────────────────

function createTestEvent(overrides = {}) {
  return {
    id: 'evt-test-001',
    type: 'test.event',
    timestamp: new Date().toISOString(),
    source: 'test-suite',
    payload: { test: true },
    ...overrides
  };
}

const tempDir = path.join('/tmp', 'pipeline-test-sinks');

// ─── Circuit Breaker Tests ───────────────────────────────────────────────────

describe('CircuitBreaker', () => {
  let cb;

  beforeEach(() => {
    cb = new CircuitBreaker('test-sink', {
      failureThreshold: 3,
      resetTimeoutMs: 100,
      halfOpenMax: 2
    });
  });

  test('should start in closed state', () => {
    expect(cb.state).toBe('closed');
    expect(cb.canExecute()).toBe(true);
  });

  test('should open after threshold failures', () => {
    cb.recordFailure();
    cb.recordFailure();
    cb.recordFailure();
    expect(cb.state).toBe('open');
    expect(cb.canExecute()).toBe(false);
  });

  test('should enter half-open after timeout', (done) => {
    cb.recordFailure();
    cb.recordFailure();
    cb.recordFailure();
    expect(cb.state).toBe('open');

    setTimeout(() => {
      expect(cb.canExecute()).toBe(true);
      expect(cb.state).toBe('half-open');
      done();
    }, 150);
  });

  test('should close after successful half-open attempts', () => {
    cb.recordFailure();
    cb.recordFailure();
    cb.recordFailure();
    expect(cb.state).toBe('open');

    // Simulate half-open recovery
    cb.state = 'half-open';
    cb._halfOpenAttempts = 0;
    cb._failures = 0;

    cb.recordSuccess();
    cb.recordSuccess();
    expect(cb.state).toBe('closed');
  });

  test('should reopen on failure during half-open', () => {
    cb.recordFailure();
    cb.recordFailure();
    cb.recordFailure();
    cb.state = 'half-open';
    cb._halfOpenAttempts = 0;

    cb.recordFailure();
    expect(cb.state).toBe('open');
  });
});

// ─── Sink Manager Tests ──────────────────────────────────────────────────────

describe('SinkManager', () => {
  let manager;

  beforeEach(() => {
    manager = new SinkManager({});
  });

  test('should register a sink', () => {
    const fn = jest.fn(async () => ({ status: 'ok' }));
    manager.register('test-sink', fn);
    expect(manager.sinks.has('test-sink')).toBe(true);
  });

  test('should unregister a sink', () => {
    const fn = jest.fn(async () => ({ status: 'ok' }));
    manager.register('temp-sink', fn);
    manager.unregister('temp-sink');
    expect(manager.sinks.has('temp-sink')).toBe(false);
  });

  test('should write to registered sink', async () => {
    const fn = jest.fn(async () => ({ status: 'ok' }));
    manager.register('mock-sink', fn);

    const event = createTestEvent();
    const results = await manager.write(event);

    expect(results).toHaveLength(1);
    expect(results[0].sink).toBe('mock-sink');
    expect(results[0].status).toBe('success');
    expect(fn).toHaveBeenCalledWith(event, expect.any(Object));
  });

  test('should handle sink failures', async () => {
    const fn = jest.fn(async () => {
      throw new Error('Sink write failed');
    });
    manager.register('failing-sink', fn, { retryAttempts: 1 });

    const event = createTestEvent();
    const results = await manager.write(event);

    expect(results).toHaveLength(1);
    expect(results[0].status).toBe('failed');
    expect(results[0].error).toBe('Sink write failed');
  });

  test('should retry failed sinks', async () => {
    let calls = 0;
    const fn = jest.fn(async () => {
      calls++;
      if (calls < 3) throw new Error('Temporary failure');
      return { status: 'ok' };
    });

    manager.register('retry-sink', fn, { retryAttempts: 3 });

    const event = createTestEvent();
    const results = await manager.write(event);

    expect(results[0].status).toBe('success');
    expect(fn).toHaveBeenCalledTimes(3);
  });

  test('should skip open circuit breakers', async () => {
    const fn = jest.fn(async () => ({ status: 'ok' }));
    manager.register('cb-sink', fn, {
      circuitBreaker: { failureThreshold: 1, resetTimeoutMs: 60000 }
    });

    // Open the circuit
    const sink = manager.sinks.get('cb-sink');
    sink.circuitBreaker.recordFailure();

    const event = createTestEvent();
    const results = await manager.write(event);

    expect(results[0].status).toBe('circuit-open');
    expect(fn).not.toHaveBeenCalled();
  });

  test('should get sink statuses', () => {
    manager.register('sink-a', async () => ({}));
    manager.register('sink-b', async () => ({}));

    const statuses = manager.getStatuses();
    expect(statuses).toHaveLength(2);
    expect(statuses[0]).toHaveProperty('name');
    expect(statuses[0]).toHaveProperty('circuitBreaker');
  });
});

// ─── File Sink Tests ─────────────────────────────────────────────────────────

describe('createFileSink', () => {
  const outputDir = path.join(tempDir, 'file-sink');

  beforeEach(() => {
    if (fs.existsSync(outputDir)) {
      fs.rmSync(outputDir, { recursive: true });
    }
  });

  afterAll(() => {
    if (fs.existsSync(outputDir)) {
      fs.rmSync(outputDir, { recursive: true });
    }
  });

  test('should create output directory', async () => {
    const sink = createFileSink(outputDir);
    expect(fs.existsSync(outputDir)).toBe(true);
  });

  test('should write event to file', async () => {
    const sink = createFileSink(outputDir);
    const event = createTestEvent();
    const result = await sink(event);

    expect(result).toHaveProperty('filePath');
    expect(result).toHaveProperty('bytes');
    expect(fs.existsSync(result.filePath)).toBe(true);
  });

  test('should append multiple events', async () => {
    const sink = createFileSink(outputDir);
    const events = [
      createTestEvent({ id: 'evt-1' }),
      createTestEvent({ id: 'evt-2' }),
      createTestEvent({ id: 'evt-3' })
    ];

    for (const event of events) {
      await sink(event);
    }

    const files = fs.readdirSync(outputDir);
    expect(files.length).toBe(1);

    const content = fs.readFileSync(path.join(outputDir, files[0]), 'utf-8');
    const lines = content.trim().split('\n');
    expect(lines.length).toBe(3);

    // Each line should be valid JSON
    for (const line of lines) {
      const parsed = JSON.parse(line);
      expect(parsed).toHaveProperty('_sink');
    }
  });
});

// ─── Console Sink Tests ──────────────────────────────────────────────────────

describe('createConsoleSink', () => {
  test('should return success info', async () => {
    const sink = createConsoleSink();
    const event = createTestEvent();
    const result = await sink(event);

    expect(result).toEqual({ sink: 'console', logged: true });
  });
});

// ─── Webhook Sink Tests ──────────────────────────────────────────────────────

describe('createWebhookSink', () => {
  test('should simulate webhook delivery', async () => {
    const targets = [
      { url: 'https://hooks.example.com/events', secret: 'secret1' },
      { url: 'https://hooks.example.com/backup', secret: 'secret2' }
    ];
    const sink = createWebhookSink(targets);
    const event = createTestEvent();
    const result = await sink(event);

    expect(result).toHaveProperty('sink', 'webhook');
    expect(result).toHaveProperty('deliveries');
    expect(result.deliveries).toHaveLength(2);
    expect(result.deliveries[0].status).toBe('simulated-success');
  });

  test('should skip targets without URL', async () => {
    const targets = [
      { url: 'https://hooks.example.com/events', secret: 'secret1' },
      { url: '', secret: 'secret2' }
    ];
    const sink = createWebhookSink(targets);
    const event = createTestEvent();
    const result = await sink(event);

    expect(result.deliveries).toHaveLength(1);
  });
});