/**
 * Alert Processor.
 * Detects conditions that should trigger alerts based on configurable rules.
 */

const logger = require('../utils/logger');

class AlertProcessor {
  constructor(options = {}) {
    this.rules = options.rules || [];
    this.alertLog = [];
    this.maxAlertHistory = options.maxAlertHistory || 100;
  }

  async process(event) {
    const alerts = [];

    for (const rule of this.rules) {
      try {
        const triggered = await this._evaluateRule(event, rule);
        if (triggered) {
          const alert = {
            id: `alert-${Date.now()}-${Math.random().toString(36).substring(2, 8)}`,
            rule: rule.name,
            severity: rule.severity || 'medium',
            message: triggered.message || `Alert: ${rule.name}`,
            eventId: event.id,
            eventType: event.type,
            triggeredAt: new Date().toISOString(),
            details: triggered.details || {}
          };

          alerts.push(alert);
          this._recordAlert(alert);

          logger.warn('Alert triggered', {
            alertId: alert.id,
            rule: rule.name,
            severity: alert.severity,
            eventId: event.id
          });
        }
      } catch (err) {
        logger.error(`Alert rule '${rule.name}' failed`, {
          error: err.message,
          eventId: event.id
        });
      }
    }

    return {
      ...event,
      _alerts: {
        triggered: alerts.length > 0,
        alerts,
        alertCount: alerts.length
      }
    };
  }

  async _evaluateRule(event, rule) {
    switch (rule.type) {
      case 'threshold':
        return this._checkThreshold(event, rule.config);
      case 'field-match':
        return this._checkFieldMatch(event, rule.config);
      case 'rate':
        return this._checkRate(event, rule.config);
      case 'pattern':
        return this._checkPattern(event, rule.config);
      case 'composite':
        return this._checkComposite(event, rule.config);
      default:
        return false;
    }
  }

  _checkThreshold(event, config) {
    const { field, operator, value, source } = config;
    if (source && event.source !== source) return false;

    const fieldValue = this._resolveField(event, field);
    if (fieldValue === undefined) return false;

    let triggered = false;
    switch (operator) {
      case 'gt': triggered = fieldValue > value; break;
      case 'gte': triggered = fieldValue >= value; break;
      case 'lt': triggered = fieldValue < value; break;
      case 'lte': triggered = fieldValue <= value; break;
      case 'eq': triggered = fieldValue === value; break;
      default: triggered = false;
    }

    if (triggered) {
      return {
        message: `Value ${fieldValue} ${operator} ${value}`,
        details: { field, value: fieldValue, threshold: value }
      };
    }
    return false;
  }

  _checkFieldMatch(event, config) {
    const { field, pattern, negate = false } = config;
    const fieldValue = this._resolveField(event, field);
    if (fieldValue === undefined) return false;

    const regex = new RegExp(pattern);
    const matched = regex.test(String(fieldValue));
    const result = negate ? !matched : matched;

    if (result) {
      return {
        message: `Field '${field}' ${negate ? 'not ' : ''}matched pattern '${pattern}'`,
        details: { field, value: fieldValue, pattern }
      };
    }
    return false;
  }

  _checkRate(event, config) {
    // Simplified rate check - would use a sliding window in production
    const { eventType, maxPerMinute } = config;
    if (eventType && !event.type?.includes(eventType)) return false;

    // For demo purposes, always pass unless explicitly configured
    if (!maxPerMinute) return false;

    // Simulate rate tracking
    const now = Date.now();
    const key = `rate:${event.type}:${Math.floor(now / 60000)}`;
    return false;
  }

  _checkPattern(event, config) {
    const { field, patterns, matchMode = 'any' } = config;
    const fieldValue = this._resolveField(event, field);
    if (fieldValue === undefined) return false;

    const str = String(fieldValue);
    const matched = matchMode === 'all'
      ? patterns.every(p => str.includes(p))
      : patterns.some(p => str.includes(p));

    if (matched) {
      return {
        message: `Pattern match in '${field}'`,
        details: { field, patterns }
      };
    }
    return false;
  }

  _checkComposite(event, config) {
    const { conditions, joinMode = 'all' } = config;
    const results = conditions.map(c => {
      const rule = { type: c.type, config: c.config };
      return this._evaluateRule(event, rule);
    });

    const met = joinMode === 'all'
      ? results.every(r => r !== false)
      : results.some(r => r !== false);

    if (met) {
      return {
        message: 'Composite conditions met',
        details: { conditionsMet: results.filter(r => r !== false).length }
      };
    }
    return false;
  }

  _resolveField(event, fieldPath) {
    return fieldPath.split('.').reduce((obj, key) => obj?.[key], event);
  }

  _recordAlert(alert) {
    this.alertLog.push(alert);
    if (this.alertLog.length > this.maxAlertHistory) {
      this.alertLog = this.alertLog.slice(-this.maxAlertHistory);
    }
  }

  getAlerts() {
    return [...this.alertLog].reverse();
  }

  clearAlerts() {
    this.alertLog = [];
  }
}

const processorInstance = new AlertProcessor({
  rules: [
    {
      name: 'high-payload-alert',
      type: 'threshold',
      severity: 'high',
      config: {
        field: '_computed.payloadSize',
        operator: 'gt',
        value: 5000
      }
    },
    {
      name: 'suspicious-source',
      type: 'field-match',
      severity: 'medium',
      config: {
        field: 'source',
        pattern: 'test-|mock-|fake-'
      }
    },
    {
      name: 'system-error-alert',
      type: 'field-match',
      severity: 'critical',
      config: {
        field: 'type',
        pattern: 'system\\.error|system\\.alert'
      }
    }
  ]
});

async function alertProcessor(event) {
  return processorInstance.process(event);
}

module.exports = { AlertProcessor, alertProcessor, processorInstance };