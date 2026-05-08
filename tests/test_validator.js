/**
 * Unit tests for the Pipeline Validator.
 */

const { PipelineValidator } = require('../src/pipeline/validator');
const path = require('path');

// ─── Test Helpers ────────────────────────────────────────────────────────────

function createValidEvent(overrides = {}) {
  return {
    id: '550e8400-e29b-41d4-a716-446655440000',
    type: 'user.login',
    timestamp: new Date().toISOString(),
    source: 'test-suite',
    version: '1.0.0',
    payload: { userId: 'u-123', email: 'test@example.com' },
    metadata: {},
    ...overrides
  };
}

// ─── Tests ───────────────────────────────────────────────────────────────────

describe('PipelineValidator', () => {
  let validator;
  const schemaDir = path.join(__dirname, '..', 'schemas');

  beforeEach(() => {
    validator = new PipelineValidator(schemaDir);
  });

  test('should load base schema on initialization', () => {
    expect(validator.loadedSchemas()).toContain('base');
  });

  test('should validate a correct event', () => {
    const event = createValidEvent();
    const result = validator.validate(event);
    expect(result.valid).toBe(true);
    expect(result.errors).toHaveLength(0);
  });

  test('should reject event missing required id', () => {
    const event = createValidEvent({ id: undefined });
    const result = validator.validate(event);
    expect(result.valid).toBe(false);
    expect(result.errors.some(e => e.field.includes('id'))).toBe(true);
  });

  test('should reject event with invalid uuid', () => {
    const event = createValidEvent({ id: 'not-a-uuid' });
    const result = validator.validate(event);
    expect(result.valid).toBe(false);
    expect(result.errors.some(e => e.field.includes('id'))).toBe(true);
  });

  test('should reject event with missing type', () => {
    const event = createValidEvent({ type: undefined });
    const result = validator.validate(event);
    expect(result.valid).toBe(false);
    expect(result.errors.some(e => e.field.includes('type'))).toBe(true);
  });

  test('should reject event with empty type', () => {
    const event = createValidEvent({ type: '' });
    const result = validator.validate(event);
    expect(result.valid).toBe(false);
  });

  test('should reject event with missing timestamp', () => {
    const event = createValidEvent({ timestamp: undefined });
    const result = validator.validate(event);
    expect(result.valid).toBe(false);
    expect(result.errors.some(e => e.message.includes('timestamp'))).toBe(true);
  });

  test('should reject event with future timestamp', () => {
    const future = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();
    const event = createValidEvent({ timestamp: future });
    const result = validator.validate(event);
    expect(result.valid).toBe(false);
    expect(result.errors.some(e => e.message.includes('future'))).toBe(true);
  });

  test('should reject event with empty payload in strict mode', () => {
    const strictValidator = new PipelineValidator(schemaDir, true);
    const event = createValidEvent({ payload: {} });
    const result = strictValidator.validate(event);
    expect(result.valid).toBe(false);
    expect(result.errors.some(e => e.message.includes('empty'))).toBe(true);
  });

  test('should record validation latency', () => {
    const event = createValidEvent();
    const result = validator.validate(event);
    expect(result.durationMs).toBeDefined();
    expect(result.durationMs).toBeGreaterThanOrEqual(0);
  });

  test('should add custom validator', () => {
    const customFn = (event) => ({
      valid: event.type.startsWith('user.'),
      errors: event.type.startsWith('user.') ? [] : ['Type must start with user.']
    });

    validator.addValidator('typePrefixCheck', customFn);
    const validEvent = createValidEvent({ type: 'user.login' });
    const invalidEvent = createValidEvent({ type: 'order.created' });

    expect(validator.validate(validEvent).valid).toBe(true);
    // Custom validator triggers but other errors may still exist
    expect(validator.validate(invalidEvent).valid).toBe(false);
  });

  test('should validate user-event type-specific schema', () => {
    const event = createValidEvent({
      type: 'user.login',
      payload: {
        userId: 'u-123',
        email: 'valid@example.com',
        username: 'testuser',
        role: 'user'
      }
    });
    const result = validator.validate(event);
    // Base validation should pass for well-formed user event
    expect(result.errors.filter(e => e.type === 'type-specific')).toHaveLength(0);
  });

  test('quickValidate should check required fields only', () => {
    const result = validator.quickValidate(createValidEvent());
    expect(result.valid).toBe(true);

    const missing = validator.quickValidate({ type: 'test' });
    expect(missing.valid).toBe(false);
    expect(missing.errors).toHaveLength(3); // missing id, timestamp, source
  });
});

module.exports = { createValidEvent };