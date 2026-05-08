/**
 * Pipeline execution state model.
 * Manages pipeline runs, stages, checkpoints, and audit history.
 */

const { v4: uuidv4 } = require('uuid');

class PipelineExecution {
  constructor(eventId, pipelineConfig) {
    this.id = uuidv4();
    this.eventId = eventId;
    this.config = pipelineConfig;
    this.status = 'pending'; // pending | running | completed | failed
    this.stages = [];
    this.createdAt = new Date().toISOString();
    this.completedAt = null;
    this.error = null;
  }

  addStage(name, status, details = {}) {
    this.stages.push({
      name,
      status, // pending | running | completed | failed | skipped
      startedAt: new Date().toISOString(),
      completedAt: null,
      details
    });
  }

  completeStage(name, status, details = {}) {
    const stage = this.stages.find(s => s.name === name);
    if (stage) {
      stage.status = status;
      stage.completedAt = new Date().toISOString();
      stage.details = { ...stage.details, ...details };
    }
  }

  finalize(status, error = null) {
    this.status = status;
    this.completedAt = new Date().toISOString();
    if (error) this.error = { message: error.message, stack: error.stack };
  }

  toJSON() {
    return {
      id: this.id,
      eventId: this.eventId,
      status: this.status,
      stages: this.stages,
      createdAt: this.createdAt,
      completedAt: this.completedAt,
      error: this.error,
      durationMs: this.completedAt
        ? new Date(this.completedAt).getTime() - new Date(this.createdAt).getTime()
        : null
    };
  }
}

class PipelineRegistry {
  constructor() {
    this.executions = new Map();
    this.maxHistory = 1000;
  }

  register(execution) {
    this.executions.set(execution.id, execution);
    if (this.executions.size > this.maxHistory) {
      const oldest = this.executions.keys().next().value;
      this.executions.delete(oldest);
    }
    return execution;
  }

  get(executionId) {
    return this.executions.get(executionId);
  }

  getByEventId(eventId) {
    return [...this.executions.values()].filter(e => e.eventId === eventId);
  }

  recent(limit = 20) {
    return [...this.executions.values()]
      .sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt))
      .slice(0, limit)
      .map(e => e.toJSON());
  }

  stats() {
    const all = [...this.executions.values()];
    return {
      total: all.length,
      byStatus: all.reduce((acc, e) => {
        acc[e.status] = (acc[e.status] || 0) + 1;
        return acc;
      }, {}),
      averageDurationMs: all.filter(e => e.completedAt).reduce((sum, e) => {
        return sum + (new Date(e.completedAt).getTime() - new Date(e.createdAt).getTime());
      }, 0) / Math.max(all.filter(e => e.completedAt).length, 1)
    };
  }
}

module.exports = { PipelineExecution, PipelineRegistry };