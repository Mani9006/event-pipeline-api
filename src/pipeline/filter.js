/**
 * Event filtering module.
 * Supports rule-based filtering with expressions, field matching,
 * and rate-based sampling.
 */

const logger = require('../utils/logger');
const { metrics } = require('../utils/metrics');

class Filter {
  constructor() {
    this.rules = [];
    this.defaultAction = 'pass'; // pass | drop
  }

  /**
   * Add a filter rule.
   * @param {Object} rule
   * @param {string} rule.name - Rule identifier
   * @param {string} rule.type - Rule type: field-match, expression, sample, time-window
   * @param {Object} rule.config - Rule-specific configuration
   * @param {string} rule.action - 'pass' or 'drop'
   * @param {number} rule.priority - Evaluation priority (lower = first)
   */
  addRule(rule) {
    this.rules.push({
      ...rule,
      active: true,
      matchCount: 0,
      createdAt: new Date().toISOString()
    });
    this.rules.sort((a, b) => (a.priority || 0) - (b.priority || 0));
    logger.info(`Filter rule added: ${rule.name}`);
  }

  /**
   * Remove a rule by name.
   */
  removeRule(name) {
    this.rules = this.rules.filter(r => r.name !== name);
  }

  /**
   * Toggle rule active state.
   */
  toggleRule(name) {
    const rule = this.rules.find(r => r.name === name);
    if (rule) {
      rule.active = !rule.active;
      return rule.active;
    }
    return null;
  }

  /**
   * Evaluate an event against all active filter rules.
   * @param {Object} event
   * @returns {Object} - { passed, reason, matchedRules }
   */
  evaluate(event) {
    const matchedRules = [];

    for (const rule of this.rules) {
      if (!rule.active) continue;

      const matched = this._evaluateRule(event, rule);
      if (matched) {
        rule.matchCount++;
        matchedRules.push({ name: rule.name, action: rule.action });

        if (rule.action === 'drop') {
          metrics.increment('eventsFiltered');
          logger.debug('Event filtered', {
            eventId: event.id,
            rule: rule.name
          });
          return { passed: false, reason: `dropped by rule '${rule.name}'`, matchedRules };
        }

        if (rule.action === 'pass') {
          // Explicit pass rules shortcut remaining evaluation
          return { passed: true, reason: `passed by rule '${rule.name}'`, matchedRules };
        }
      }
    }

    return {
      passed: this.defaultAction === 'pass',
      reason: 'default action',
      matchedRules
    };
  }

  _evaluateRule(event, rule) {
    switch (rule.type) {
      case 'field-match':
        return this._fieldMatch(event, rule.config);
      case 'expression':
        return this._expression(event, rule.config);
      case 'sample':
        return this._sample(rule.config);
      case 'time-window':
        return this._timeWindow(rule.config);
      case 'type-match':
        return this._typeMatch(event, rule.config);
      default:
        logger.warn(`Unknown filter rule type: ${rule.type}`);
        return false;
    }
  }

  _fieldMatch(event, config) {
    const { field, operator, value } = config;
    const fieldValue = this._resolveField(event, field);

    switch (operator) {
      case 'eq':   return fieldValue === value;
      case 'neq':  return fieldValue !== value;
      case 'gt':   return fieldValue > value;
      case 'gte':  return fieldValue >= value;
      case 'lt':   return fieldValue < value;
      case 'lte':  return fieldValue <= value;
      case 'in':   return Array.isArray(value) && value.includes(fieldValue);
      case 'nin':  return Array.isArray(value) && !value.includes(fieldValue);
      case 'exists': return fieldValue !== undefined;
      case 'regex':
        try {
          return new RegExp(value).test(String(fieldValue));
        } catch {
          return false;
        }
      default: return false;
    }
  }

  _expression(event, config) {
    try {
      // Safe expression evaluation
      const { expression } = config;
      const sandbox = {
        event,
        payload: event.payload,
        metadata: event.metadata,
        type: event.type,
        timestamp: new Date(event.timestamp)
      };

      const keys = Object.keys(sandbox);
      const values = Object.values(sandbox);
      const fn = new Function(...keys, `return (${expression});`);
      return !!fn(...values);
    } catch (err) {
      logger.warn('Filter expression error', { error: err.message });
      return false;
    }
  }

  _sample(config) {
    const { rate } = config; // 0.0 - 1.0
    return Math.random() < rate;
  }

  _timeWindow(config) {
    const { start, end } = config;
    const now = new Date();
    const startTime = new Date(start);
    const endTime = new Date(end);
    return now >= startTime && now <= endTime;
  }

  _typeMatch(event, config) {
    const { patterns } = config;
    return patterns.some(pattern => {
      const regex = new RegExp(
        '^' + pattern.replace(/\./g, '\\.').replace(/\*/g, '[^.]+') + '$'
      );
      return regex.test(event.type);
    });
  }

  _resolveField(event, fieldPath) {
    return fieldPath.split('.').reduce((obj, key) => obj?.[key], event);
  }

  /**
   * Get all rules with their statistics.
   */
  getRules() {
    return this.rules.map(r => ({
      name: r.name,
      type: r.type,
      active: r.active,
      action: r.action,
      matchCount: r.matchCount,
      priority: r.priority
    }));
  }

  /**
   * Reset all match counts.
   */
  resetStats() {
    for (const rule of this.rules) {
      rule.matchCount = 0;
    }
  }
}

module.exports = { Filter };