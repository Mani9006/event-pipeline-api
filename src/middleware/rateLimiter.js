/**
 * Token-bucket rate limiter with per-client IP tracking.
 * Configurable window, burst capacity, and response headers.
 */

const logger = require('../utils/logger');

class TokenBucket {
  constructor(capacity, refillRate) {
    this.capacity = capacity;      // max tokens
    this.refillRate = refillRate;  // tokens per second
    this.tokens = capacity;
    this.lastRefill = Date.now();
  }

  consume(tokens = 1) {
    this._refill();
    if (this.tokens >= tokens) {
      this.tokens -= tokens;
      return true;
    }
    return false;
  }

  _refill() {
    const now = Date.now();
    const elapsed = (now - this.lastRefill) / 1000;
    const added = elapsed * this.refillRate;
    this.tokens = Math.min(this.capacity, this.tokens + added);
    this.lastRefill = now;
  }

  state() {
    this._refill();
    return {
      tokens: Math.floor(this.tokens),
      capacity: this.capacity,
      remaining: Math.floor(this.tokens)
    };
  }
}

// ─── Rate Limiter Factory ────────────────────────────────────────────────────

function createRateLimiter({
  windowMs = 60 * 1000,        // 1 minute
  maxRequests = 100,            // requests per window
  burstAllowance = 10,          // burst over max
  keyGenerator = (req) => req.ip || req.connection.remoteAddress,
  skipSuccessful = false
} = {}) {
  const clients = new Map();
  const capacity = maxRequests + burstAllowance;
  const refillRate = capacity / (windowMs / 1000);

  // Periodic cleanup of stale entries
  setInterval(() => {
    const now = Date.now();
    for (const [ip, bucket] of clients.entries()) {
      if (now - bucket.lastRefill > windowMs * 2) {
        clients.delete(ip);
      }
    }
  }, windowMs * 2);

  return (req, res, next) => {
    const key = keyGenerator(req);
    if (!clients.has(key)) {
      clients.set(key, new TokenBucket(capacity, refillRate));
    }

    const bucket = clients.get(key);

    // Set rate limit headers
    const state = bucket.state();
    res.setHeader('X-RateLimit-Limit', maxRequests);
    res.setHeader('X-RateLimit-Remaining', Math.max(0, state.remaining));
    res.setHeader('X-RateLimit-Reset', new Date(Date.now() + windowMs).toISOString());

    if (bucket.consume(1)) {
      res.setHeader('X-RateLimit-Remaining', Math.max(0, bucket.state().remaining));
      next();
    } else {
      logger.warn('Rate limit exceeded', { client: key, path: req.path });
      res.setHeader('Retry-After', Math.ceil(windowMs / 1000));
      res.status(429).json({
        success: false,
        error: 'Too many requests',
        retryAfterMs: windowMs
      });
    }
  };
}

/**
 * Per-endpoint rate limiter with different limits per route.
 */
function createEndpointLimiter(limits) {
  const limiters = {};
  for (const [route, config] of Object.entries(limits)) {
    limiters[route] = createRateLimiter(config);
  }

  return (req, res, next) => {
    const route = req.route?.path || req.path;
    const limiter = limiters[route];
    if (limiter) {
      return limiter(req, res, next);
    }
    next();
  };
}

module.exports = { createRateLimiter, createEndpointLimiter, TokenBucket };