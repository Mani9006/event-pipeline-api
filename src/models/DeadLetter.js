/**
 * Dead Letter Queue (DLQ) model.
 * Stores failed events with error context for later inspection and replay.
 */

const fs = require('fs');
const path = require('path');
const logger = require('../utils/logger');

class DeadLetterQueue {
  constructor(outputDir, alertThreshold = 10) {
    this.outputDir = outputDir;
    this.alertThreshold = alertThreshold;
    this._entries = [];
    this._alertTriggered = false;
    this._ensureDir();
  }

  _ensureDir() {
    if (!fs.existsSync(this.outputDir)) {
      fs.mkdirSync(this.outputDir, { recursive: true });
    }
  }

  _filePath() {
    const date = new Date().toISOString().split('T')[0];
    return path.join(this.outputDir, `dlq-${date}.jsonl`);
  }

  /**
   * Add a failed event to the DLQ.
   */
  async add(event, error, stage) {
    const entry = {
      id: event.id,
      originalEvent: event,
      error: {
        message: error.message,
        stack: error.stack,
        stage
      },
      retryCount: event._internal?.retryCount || 0,
      enqueuedAt: new Date().toISOString(),
      status: 'pending' // pending | replayed | discarded
    };

    this._entries.push(entry);

    await this._persist(entry);

    if (this._entries.length >= this.alertThreshold && !this._alertTriggered) {
      this._alertTriggered = true;
      logger.warn('DLQ alert threshold reached', {
        count: this._entries.length,
        threshold: this.alertThreshold
      });
    }

    return entry;
  }

  async _persist(entry) {
    const filePath = this._filePath();
    const line = JSON.stringify(entry) + '\n';
    return new Promise((resolve, reject) => {
      fs.appendFile(filePath, line, (err) => {
        if (err) {
          logger.error('DLQ persist failed', { error: err.message, eventId: entry.id });
          reject(err);
        } else {
          resolve();
        }
      });
    });
  }

  /**
   * List all DLQ entries.
   */
  list({ status = null, limit = 100, offset = 0 } = {}) {
    let entries = [...this._entries];
    if (status) {
      entries = entries.filter(e => e.status === status);
    }
    const total = entries.length;
    return {
      entries: entries.reverse().slice(offset, offset + limit),
      total,
      limit,
      offset
    };
  }

  /**
   * Mark an entry as replayed.
   */
  markReplayed(id) {
    const entry = this._entries.find(e => e.id === id);
    if (entry) {
      entry.status = 'replayed';
      entry.replayedAt = new Date().toISOString();
    }
    return entry;
  }

  /**
   * Discard an entry.
   */
  discard(id) {
    const entry = this._entries.find(e => e.id === id);
    if (entry) {
      entry.status = 'discarded';
      entry.discardedAt = new Date().toISOString();
    }
    return entry;
  }

  /**
   * Get DLQ statistics.
   */
  stats() {
    return {
      totalEntries: this._entries.length,
      byStatus: this._entries.reduce((acc, e) => {
        acc[e.status] = (acc[e.status] || 0) + 1;
        return acc;
      }, {}),
      alertTriggered: this._alertTriggered,
      alertThreshold: this.alertThreshold
    };
  }

  /**
   * Clear all entries (use with caution).
   */
  clear() {
    this._entries = [];
    this._alertTriggered = false;
  }

  /**
   * Get entries eligible for replay.
   */
  getReplayable() {
    return this._entries.filter(e => e.status === 'pending');
  }
}

module.exports = { DeadLetterQueue };