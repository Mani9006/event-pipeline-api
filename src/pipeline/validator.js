/**
 * Pipeline validation stage.
 * Validates events against loaded JSON schemas with support for
 * strict mode, custom validators, and partial validation.
 */

const Ajv = require('ajv');
const addFormats = require('ajv-formats');
const fs = require('fs');
const path = require('path');
const logger = require('../utils/logger');
const { metrics } = require('../utils/metrics');

class PipelineValidator {
  constructor(schemaDir, strictMode = false) {
    this.schemaDir = schemaDir;
    this.strictMode = strictMode;
    this.customValidators = new Map();
    this.ajv = new Ajv({ allErrors: true, strict: false });
    addFormats(this.ajv);
    this.compiledSchemas = new Map();
    this._loadBaseSchema();
  }

  _loadBaseSchema() {
    const basePath = path.join(this.schemaDir, 'event.schema.json');
    if (fs.existsSync(basePath)) {
      const schema = JSON.parse(fs.readFileSync(basePath, 'utf-8'));
      this.compiledSchemas.set('base', this.ajv.compile(schema));
      logger.info('Base event schema loaded');
    }
  }

  /**
   * Register a custom validator function.
   * @param {string} name
   * @param {Function} validator - (event) => { valid, errors }
   */
  addValidator(name, validator) {
    this.customValidators.set(name, validator);
  }

  /**
   * Validate an event against its schema.
   * @param {Object} event
   * @returns {Object} - { valid, errors, event }
   */
  validate(event) {
    const startTime = Date.now();
    const baseValidator = this.compiledSchemas.get('base');

    if (!baseValidator) {
      return {
        valid: true,
        errors: [],
        warnings: ['No base schema loaded'],
        event
      };
    }

    // 1. Base schema validation
    const baseValid = baseValidator(event);
    const allErrors = [];
    const warnings = [];

    if (!baseValid) {
      allErrors.push(...baseValidator.errors.map(err => ({
        type: 'schema',
        field: err.instancePath || 'root',
        message: err.message,
        schemaPath: err.schemaPath
      })));
    }

    // 2. Type-specific validation
    const typeErrors = this._validateTypeSpecific(event);
    allErrors.push(...typeErrors);

    // 3. Custom validators
    for (const [name, validator] of this.customValidators) {
      try {
        const result = validator(event);
        if (!result.valid) {
          allErrors.push(...result.errors.map(e => ({
            type: 'custom',
            validator: name,
            message: e
          })));
        }
      } catch (err) {
        logger.error(`Custom validator '${name}' threw`, { error: err.message });
        warnings.push(`Validator '${name}' failed: ${err.message}`);
      }
    }

    // 4. Semantic validation
    const semanticErrors = this._semanticValidation(event);
    allErrors.push(...semanticErrors);

    const valid = allErrors.length === 0;
    const durationMs = Date.now() - startTime;
    metrics.recordValidationLatency(durationMs);

    if (!valid) {
      logger.warn('Event validation failed', {
        eventId: event.id,
        type: event.type,
        errorCount: allErrors.length
      });
    }

    return {
      valid,
      errors: allErrors,
      warnings: warnings.length > 0 ? warnings : undefined,
      event,
      durationMs
    };
  }

  /**
   * Validate event type against its specific schema if one exists.
   */
  _validateTypeSpecific(event) {
    const typeMap = {
      'user': 'user-event.schema.json'
    };

    const prefix = event.type?.split('.')[0];
    const schemaFile = typeMap[prefix];
    if (!schemaFile) return [];

    const schemaKey = prefix;
    if (!this.compiledSchemas.has(schemaKey)) {
      const schemaPath = path.join(this.schemaDir, schemaFile);
      if (fs.existsSync(schemaPath)) {
        const schema = JSON.parse(fs.readFileSync(schemaPath, 'utf-8'));
        this.compiledSchemas.set(schemaKey, this.ajv.compile(schema));
      } else {
        return [];
      }
    }

    const validate = this.compiledSchemas.get(schemaKey);
    const valid = validate(event);

    if (!valid) {
      return validate.errors.map(err => ({
        type: 'type-specific',
        field: err.instancePath || 'root',
        message: err.message,
        schemaPath: err.schemaPath
      }));
    }

    return [];
  }

  /**
   * Perform semantic validation rules.
   */
  _semanticValidation(event) {
    const errors = [];

    // Timestamp must not be in the future
    if (event.timestamp) {
      const eventTime = new Date(event.timestamp).getTime();
      if (eventTime > Date.now() + 60000) { // 1 minute grace period
        errors.push({
          type: 'semantic',
          field: 'timestamp',
          message: 'Timestamp is in the future'
        });
      }
    }

    // Payload must not be empty object
    if (event.payload && Object.keys(event.payload).length === 0 && this.strictMode) {
      errors.push({
        type: 'semantic',
        field: 'payload',
        message: 'Payload is empty in strict mode'
      });
    }

    return errors;
  }

  /**
   * Quick validation for known fields only.
   */
  quickValidate(event) {
    const required = ['id', 'type', 'timestamp', 'source'];
    const errors = [];
    for (const field of required) {
      if (!event[field]) {
        errors.push({ type: 'required', field, message: `Missing required field: ${field}` });
      }
    }
    return { valid: errors.length === 0, errors, event };
  }

  /**
   * Get loaded schema names.
   */
  loadedSchemas() {
    return [...this.compiledSchemas.keys()];
  }
}

module.exports = { PipelineValidator };