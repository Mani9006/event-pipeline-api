/**
 * In-memory event queue with backpressure awareness.
 * Supports priority ordering, batch draining, and overflow protection.
 */

const { EventEmitter } = require('events');

class EventQueue extends EventEmitter {
  constructor(options = {}) {
    super();
    this.maxSize = options.maxSize || 10000;
    this.highWatermark = options.highWatermark || 0.8;
    this.lowWatermark  = options.lowWatermark  || 0.5;
    this._items = [];
    this._draining = false;
    this._dropped = 0;
  }

  /**
   * Enqueue an event. Returns false if backpressure is active.
   * @param {Object} event
   * @param {number} priority - Lower number = higher priority
   * @returns {boolean} - Whether the event was accepted
   */
  enqueue(event, priority = 5) {
    if (this.isBackpressured()) {
      this._dropped++;
      this.emit('dropped', event);
      return false;
    }

    const entry = { event, priority, enqueuedAt: Date.now() };
    // Insert sorted by priority
    const idx = this._items.findIndex(i => i.priority > priority);
    if (idx === -1) {
      this._items.push(entry);
    } else {
      this._items.splice(idx, 0, entry);
    }

    this.emit('enqueued', event);
    return true;
  }

  /**
   * Dequeue a single event.
   */
  dequeue() {
    if (this._items.length === 0) return null;
    return this._items.shift().event;
  }

  /**
   * Dequeue a batch of events.
   */
  drainBatch(batchSize = 100) {
    if (this._items.length === 0) return [];
    const count = Math.min(batchSize, this._items.length);
    const batch = this._items.splice(0, count).map(e => e.event);
    return batch;
  }

  /**
   * Peek at the next event without removing.
   */
  peek() {
    return this._items.length > 0 ? this._items[0].event : null;
  }

  /**
   * Get current queue depth.
   */
  size() {
    return this._items.length;
  }

  /**
   * Check if queue is empty.
   */
  isEmpty() {
    return this._items.length === 0;
  }

  /**
   * Check if queue is at capacity (backpressure).
   */
  isBackpressured() {
    return this._items.length >= this.maxSize * this.highWatermark;
  }

  /**
   * Check if backpressure has eased.
   */
  isResumed() {
    return this._items.length <= this.maxSize * this.lowWatermark;
  }

  /**
   * Get queue fill ratio.
   */
  fillRatio() {
    return this._items.length / this.maxSize;
  }

  /**
   * Get oldest event age in ms.
   */
  oldestAgeMs() {
    if (this._items.length === 0) return 0;
    return Date.now() - this._items[0].enqueuedAt;
  }

  /**
   * Get queue statistics.
   */
  stats() {
    return {
      size: this._items.length,
      maxSize: this.maxSize,
      fillRatio: this.fillRatio(),
      isBackpressured: this.isBackpressured(),
      oldestAgeMs: this.oldestAgeMs(),
      dropped: this._dropped,
      priorities: this._items.reduce((acc, e) => {
        acc[e.priority] = (acc[e.priority] || 0) + 1;
        return acc;
      }, {})
    };
  }

  /**
   * Clear all items.
   */
  clear() {
    this._items = [];
  }

  /**
   * Get items sorted by enqueue time for replay.
   */
  getReplaySlice(startTime, endTime) {
    return this._items
      .filter(e => e.enqueuedAt >= startTime && e.enqueuedAt <= endTime)
      .map(e => e.event);
  }
}

module.exports = { EventQueue };