/**
 * Unit tests for the Event Router.
 */

const { Router, createRouter } = require('../src/pipeline/router');

describe('Router', () => {
  let router;

  beforeEach(() => {
    const config = {
      routes: [
        { pattern: 'user.*', processors: ['processorA'] },
        { pattern: 'order.*', processors: ['processorA', 'processorB'] },
        { pattern: 'payment.*', processors: ['processorB', 'processorC'] },
        { pattern: '*', processors: ['processorA'] }
      ]
    };
    router = createRouter(config);
  });

  test('should create router from config', () => {
    expect(router).toBeInstanceOf(Router);
    expect(router.getRoutes().length).toBe(4);
  });

  test('should register a processor', () => {
    const fn = jest.fn(async (event) => event);
    router.register('testProcessor', fn);
    expect(router.getProcessors()).toContain('testProcessor');
  });

  test('should unregister a processor', () => {
    const fn = jest.fn(async (event) => event);
    router.register('tempProcessor', fn);
    expect(router.getProcessors()).toContain('tempProcessor');

    router.unregister('tempProcessor');
    expect(router.getProcessors()).not.toContain('tempProcessor');
  });

  test('should route user events to user processor', async () => {
    const mockFn = jest.fn(async (event) => ({ ...event, processed: true }));
    router.register('processorA', mockFn);

    const event = {
      id: '1',
      type: 'user.login',
      timestamp: new Date().toISOString(),
      source: 'test',
      payload: {}
    };

    const result = await router.route(event);
    expect(mockFn).toHaveBeenCalled();
    expect(result._routing).toBeDefined();
    expect(result._routing.results.length).toBeGreaterThan(0);
  });

  test('should route with multiple matching processors', async () => {
    const fnA = jest.fn(async (event) => event);
    const fnB = jest.fn(async (event) => event);

    router.register('processorA', fnA);
    router.register('processorB', fnB);

    const event = {
      id: '1',
      type: 'order.created',
      timestamp: new Date().toISOString(),
      source: 'test',
      payload: { amount: 100 }
    };

    const result = await router.route(event);
    expect(fnA).toHaveBeenCalled();
    expect(fnB).toHaveBeenCalled();
  });

  test('should use default route for unmatched types', async () => {
    const defaultFn = jest.fn(async (event) => event);
    router.register('processorA', defaultFn);

    const event = {
      id: '1',
      type: 'unknown.event.type',
      timestamp: new Date().toISOString(),
      source: 'test',
      payload: {}
    };

    const result = await router.route(event);
    expect(defaultFn).toHaveBeenCalled();
  });

  test('should handle processor errors gracefully', async () => {
    const errorFn = jest.fn(async () => {
      throw new Error('Processor failed');
    });
    router.register('failingProcessor', errorFn);

    router.routes.set('test.*', ['failingProcessor']);

    const event = {
      id: '1',
      type: 'test.event',
      timestamp: new Date().toISOString(),
      source: 'test',
      payload: {}
    };

    const result = await router.route(event);
    expect(result._routing).toBeDefined();
    expect(result._routing.results[0].status).toBe('failed');
  });

  test('should skip processor if not found', async () => {
    router.routes.set('test.*', ['nonExistentProcessor']);

    const event = {
      id: '1',
      type: 'test.event',
      timestamp: new Date().toISOString(),
      source: 'test',
      payload: {}
    };

    const result = await router.route(event);
    expect(result._routing.results[0].status).toBe('skipped');
    expect(result._routing.results[0].reason).toBe('not_found');
  });

  test('should get list of routes', () => {
    const routes = router.getRoutes();
    expect(routes).toBeInstanceOf(Array);
    expect(routes.length).toBe(5); // 4 from config + test.* from earlier test
  });
});

describe('Router Pattern Matching', () => {
  test('should match user.* pattern', () => {
    const router = createRouter({ routes: [] });
    expect(router._matchPattern('user.login', 'user.*')).toBe(true);
    expect(router._matchPattern('user.logout', 'user.*')).toBe(true);
    expect(router._matchPattern('user.profile.update', 'user.*')).toBe(false);
    expect(router._matchPattern('order.created', 'user.*')).toBe(false);
  });

  test('should match order.* pattern', () => {
    const router = createRouter({ routes: [] });
    expect(router._matchPattern('order.created', 'order.*')).toBe(true);
    expect(router._matchPattern('order.updated', 'order.*')).toBe(true);
    expect(router._matchPattern('order.payment.success', 'order.*')).toBe(false);
  });

  test('should handle exact type patterns', () => {
    const router = createRouter({ routes: [] });
    expect(router._matchPattern('system.error', 'system.error')).toBe(true);
    expect(router._matchPattern('system.warn', 'system.error')).toBe(false);
  });
});