# Architecture: Event-Driven Data Pipeline API

## Overview

The Event-Driven Data Pipeline API is a production-ready Node.js service for ingesting, processing, transforming, and routing events through a configurable multi-stage pipeline. It supports backpressure handling, dead-letter queues, circuit breaker patterns, event replay, and pluggable processors.

## System Architecture

```
                    +-------------------+         +------------------+
                    |   API Clients     |         |   Admin Clients  |
                    +---------+---------+         +--------+---------+
                              |                            |
                    +---------v---------+         +--------v---------+
                    |   Express Server  |         |   REST API       |
                    |   (Port 3000)     |         |   (Pipeline      |
                    |                   |         |    Management)   |
                    +---------+---------+         +------------------+
                              |
                    +---------v---------+
                    |  Middleware Stack |
                    |  - Rate Limiter   |
                    |  - Schema Val.    |
                    |  - Body Sanitize  |
                    |  - Request Log    |
                    +---------+---------+
                              |
                    +---------v---------+
                    |   Ingest Router   |
                    |  /api/v1/ingest   |
                    +---------+---------+
                              |
                +-------------v-------------+
                |    Event Queue (Memory)   |
                |  - Backpressure aware     |
                |  - Priority support       |
                +-------------+-------------+
                              |
                    +---------v---------+
                    |   Pipeline Engine |
                    +----+----+----+----+
                         |    |    |
            +------------+    |    +------------+
            |                 |                 |
    +-------v-------+ +-------v-------+ +-------v-------+
    |  Validate     | |   Enrich      | |    Filter     |
    |  (AJV Schemas)| | (Pluggable)   | |  (Rules)      |
    +-------+-------+ +-------+-------+ +-------+-------+
            |                 |                 |
    +-------v-------+ +-------v-------+ +-------v-------+
    |  Transform    | |    Route      | |     Sink      |
    |  (Sequential) | |  (Pattern     | |  (Multiple)   |
    |               | |   Matching)   | |               |
    +---------------+ +---------------+ +---------------+
                              |
                    +---------v---------+
                    |   DLQ (on fail)   |
                    |   Registry        |
                    +-------------------+
```

## Data Flow

1. **Ingestion**: Events are received via REST API (`POST /api/v1/ingest` or `/batch`)
2. **Validation**: AJV-based JSON Schema validation against base and type-specific schemas
3. **Enrichment**: Contextual data is added (geo, time, source info, dedup hash)
4. **Filtering**: Rules-based filtering with expressions, field matching, and sampling
5. **Transformation**: Sequential transforms (normalize, compute fields, anonymize, flatten)
6. **Routing**: Pattern-matched routing to registered processors (log, analytics, alert)
7. **Sink Output**: Events written to configured sinks (file, webhook, console)
8. **Dead Letter**: Failed events after max retries are stored for later inspection

## Component Design

### PipelineEngine
Orchestrates all stages. Maintains configuration, manages the event queue, and coordinates processing through each pipeline stage. Supports toggling individual stages at runtime.

### EventQueue
Priority-aware in-memory queue with backpressure support. Uses high/low watermarks to control event acceptance. Emits `dropped` events when backpressure is active.

### PipelineValidator
Two-tier validation: base schema (all events) and type-specific schema (matched by type prefix). Supports custom validators and semantic validation.

### Enricher
Pluggable enrichment system with caching support. Enrichers are executed in priority order and results are cached by cache key with configurable TTL.

### Filter
Rule-based filtering engine supporting:
- **field-match**: Compare field values with operators (eq, gt, regex, etc.)
- **expression**: JavaScript expression evaluation in a safe sandbox
- **sample**: Random sampling with configurable rate
- **time-window**: Time-based window filtering
- **type-match**: Glob pattern matching on event type

### Transformer
Sequential transform pipeline. Each transform receives the output of the previous. Supports conditional transforms and failure recovery.

### Router
Pattern-based event routing with glob matching. Routes events to one or more processors. Supports default fallback routing.

### SinkManager
Multi-sink output manager with per-sink circuit breakers and retry logic. Built-in sinks: file (JSON Lines), webhook (simulated), console.

### DeadLetterQueue
Failed event storage with retry tracking. Supports replaying events from the DLQ.

## Backpressure Strategy

```
Queue Fill Ratio
  100% | ############### REJECT
  80%  | #############  HIGH WATERMARK (start shedding)
       | ###########
       | #########
  50%  | #######      LOW WATERMARK (resume normal)
       | #####
       | ###
    0% | # ACCEPT
```

- Events are accepted while fill ratio < high watermark (80%)
- Events are rejected when fill ratio >= high watermark
- Acceptance resumes when fill ratio drops to low watermark (50%)
- Queue metrics are exposed for monitoring

## Circuit Breaker Pattern

Each sink has an independent circuit breaker:
- **Closed**: Normal operation, requests pass through
- **Open**: Failure threshold reached, requests fail fast
- **Half-Open**: After timeout, limited test requests allowed
- Success in half-open transitions back to closed
- Failure in half-open reopens the circuit

## Event Processing Lifecycle

```
Client Request
    |
    v
Normalize Event (add metadata, correlation ID)
    |
    v
Validate (base schema + type-specific)
    |
    v
Check Backpressure
    |
    +-- Full --> Return 503 / Enqueue with backoff
    |
    +-- OK -----> Process or Queue for async
    |                |
    |                v
    |         Enrich (cached lookups)
    |                |
    |                v
    |         Filter (rule evaluation)
    |                |
    |         +-- Drop --> Return 200 (filtered)
    |         |
    |         +-- Pass -> Transform
    |                        |
    |                        v
    |                   Route to Processors
    |                        |
    |                        v
    |                   Write to Sinks
    |                        |
    |              +-- Success --> Complete
    |              |
    |              +-- Fail -----> Retry (up to N)
    |                                 |
    |                       Max retries reached
    |                                 |
    |                                 v
    |                            Dead Letter Queue
    |
```

## Data Storage

- **Events**: Stored as JSON Lines files, rotated daily
- **Dead Letter**: Separate JSON Lines files with error context
- **Metrics**: In-memory with configurable retention
- **Pipeline Runs**: In-memory registry with configurable max history

## Security Considerations

- Request body sanitization prevents prototype pollution and XSS
- Rate limiting per IP with token bucket algorithm
- Schema validation prevents malformed data injection
- Helmet.js headers for security hardening
- CORS configuration for cross-origin requests

## Scalability

The current implementation is designed for single-node deployment. For horizontal scaling:
- Replace in-memory queue with Redis/RabbitMQ
- Add distributed circuit breaker (Redis-based)
- Use shared storage for event persistence
- Implement worker pool for parallel processing
- Add Kafka for event ingestion buffer