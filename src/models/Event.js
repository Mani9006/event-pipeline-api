/**
 * Event model for normalized event storage and retrieval.
 * Persists events to JSON files with optional rotation support.
 */

const fs = require('fs');
const path = require('path');
const { v4: uuidv4 } = require('uuid');
const logger = require('../utils/logger');

class EventStore {
  constructor(outputDir) {
    this.outputDir = outputDir;
    this._ensureDir();
  }

  _ensureDir() {
    if (!fs.existsSync(this.outputDir)) {
      fs.mkdirSync(this.outputDir, { recursive: true });
    }
  }

  _currentFile() {
    const date = new Date().toISOString().split('T')[0];
    return path.join(this.outputDir, `events-${date}.jsonl`);
  }

  /**
   * Persist a processed event to file storage.
   */
  async persist(event) {
    const filePath = this._currentFile();
    const line = JSON.stringify({
      ...event,
      _storedAt: new Date().toISOString()
    }) + '\n';

    return new Promise((resolve, reject) => {
      fs.appendFile(filePath, line, (err) => {
        if (err) {
          logger.error('EventStore.persist failed', { error: err.message, eventId: event.id });
          reject(err);
        } else {
          resolve({ filePath, eventId: event.id });
        }
      });
    });
  }

  /**
   * Read events from a file with optional pagination.
   */
  async readEvents(fileName, { page = 1, limit = 100, eventType = null } = {}) {
    const filePath = path.join(this.outputDir, fileName);
    if (!fs.existsSync(filePath)) return { events: [], total: 0 };

    const lines = fs.readFileSync(filePath, 'utf-8')
      .split('\n')
      .filter(Boolean)
      .map(l => JSON.parse(l));

    const filtered = eventType
      ? lines.filter(e => e.type === eventType)
      : lines;

    const total = filtered.length;
    const start = (page - 1) * limit;
    const events = filtered.slice(start, start + limit);

    return { events, total, page, limit };
  }

  /**
   * List available event files.
   */
  listFiles() {
    if (!fs.existsSync(this.outputDir)) return [];
    return fs.readdirSync(this.outputDir)
      .filter(f => f.endsWith('.jsonl'))
      .map(f => {
        const stats = fs.statSync(path.join(this.outputDir, f));
        return {
          fileName: f,
          size: stats.size,
          created: stats.birthtime.toISOString()
        };
      });
  }

  /**
   * Find events by type across all files.
   */
  async findByType(eventType, limit = 100) {
    const files = this.listFiles().map(f => f.fileName);
    const results = [];
    for (const file of files.reverse()) {
      const { events } = await this.readEvents(file, { limit: limit - results.length, eventType });
      results.push(...events);
      if (results.length >= limit) break;
    }
    return results;
  }

  /**
   * Find events in a time range.
   */
  async findByTimeRange(startTime, endTime) {
    const files = this.listFiles().map(f => f.fileName);
    const results = [];
    for (const file of files) {
      const { events } = await this.readEvents(file);
      const inRange = events.filter(e => {
        const ts = new Date(e.timestamp).getTime();
        return ts >= startTime && ts <= endTime;
      });
      results.push(...inRange);
    }
    return results;
  }
}

/**
 * Normalize a raw event into the canonical form.
 */
function normalizeEvent(raw) {
  return {
    id: raw.id || uuidv4(),
    type: raw.type,
    timestamp: raw.timestamp || new Date().toISOString(),
    source: raw.source || 'unknown',
    version: raw.version || '1.0.0',
    payload: raw.payload || {},
    metadata: {
      correlationId: raw.metadata?.correlationId || uuidv4(),
      traceId: raw.metadata?.traceId,
      userAgent: raw.metadata?.userAgent,
      ipAddress: raw.metadata?.ipAddress,
      ...raw.metadata
    },
    _internal: {
      receivedAt: new Date().toISOString(),
      processingStage: 'received'
    }
  };
}

module.exports = { EventStore, normalizeEvent };