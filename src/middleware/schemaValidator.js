/**
 * Schema validation middleware using AJV.
 * Loads JSON schemas and validates incoming event payloads.
 */

const Ajv = require('ajv');
const addFormats = require('ajv-formats');
const fs = require('fs');
const path = require('path');
const logger = require('../utils/logger');

const ajv = new Ajv({
  allErrors: true,
  strict: false,
  schemas: {} // Cache for compiled schemas
});
addFormats(ajv);

// ─── Schema Loader ───────────────────────────────────────────────────────────

const loadedSchemas = new Map();

function loadSchema(schemaName) {
  if (loadedSchemas.has(schemaName)) return loadedSchemas.get(schemaName);

  const schemaPath = path.join(__dirname, '..', '..', 'schemas', schemaName);
  if (!fs.existsSync(schemaPath)) {
    throw new Error(`Schema not found: ${schemaPath}`);
  }

  const schema = JSON.parse(fs.readFileSync(schemaPath, 'utf-8'));
  loadedSchemas.set(schemaName, schema);
  return schema;
}

// ─── Validate Middleware ─────────────────────────────────────────────────────

function validateEvent(schemaName = 'event.schema.json') {
  return (req, res, next) => {
    try {
      const schema = loadSchema(schemaName);
      const validate = ajv.compile(schema);
      const valid = validate(req.body);

      if (!valid) {
        const errors = validate.errors.map(err => ({
          field: err.instancePath || 'root',
          message: err.message,
          params: err.params
        }));

        logger.warn('Event validation failed', {
          errors,
          eventType: req.body.type,
          source: req.body.source
        });

        return res.status(400).json({
          success: false,
          error: 'Validation failed',
          details: errors
        });
      }

      next();
    } catch (err) {
      logger.error('Schema validation error', { error: err.message });
      res.status(500).json({
        success: false,
        error: 'Internal validation error'
      });
    }
  };
}

/**
 * Validate event type-specific schema if available.
 */
function validateByType(req, res, next) {
  const eventType = req.body.type;
  if (!eventType) return next();

  // Map event types to schemas
  const typeToSchema = {
    'user.registered': 'user-event.schema.json',
    'user.login': 'user-event.schema.json',
    'user.logout': 'user-event.schema.json',
    'user.profile_updated': 'user-event.schema.json',
    'user.password_changed': 'user-event.schema.json',
    'user.deleted': 'user-event.schema.json'
  };

  const schemaName = typeToSchema[eventType];
  if (!schemaName) return next(); // No specific schema, base validation is enough

  try {
    const schema = loadSchema(schemaName);
    const validate = ajv.compile(schema);
    const valid = validate(req.body);

    if (!valid) {
      const errors = validate.errors.map(err => ({
        field: err.instancePath || 'root',
        message: err.message
      }));

      return res.status(400).json({
        success: false,
        error: `Validation failed for type '${eventType}'`,
        details: errors
      });
    }
  } catch (err) {
    logger.error('Type-specific validation error', { error: err.message, eventType });
  }

  next();
}

/**
 * Sanitize request body to prevent injection.
 */
function sanitizeBody(req, res, next) {
  if (!req.body || typeof req.body !== 'object') return next();

  const sanitize = (obj) => {
    if (typeof obj === 'string') {
      return obj.replace(/[<>]/g, ''); // Basic XSS sanitization
    }
    if (Array.isArray(obj)) {
      return obj.map(sanitize);
    }
    if (obj && typeof obj === 'object') {
      const result = {};
      for (const [key, value] of Object.entries(obj)) {
        // Prevent prototype pollution
        if (key === '__proto__' || key === 'constructor') continue;
        result[key] = sanitize(value);
      }
      return result;
    }
    return obj;
  };

  req.body = sanitize(req.body);
  next();
}

module.exports = { validateEvent, validateByType, sanitizeBody, loadSchema };