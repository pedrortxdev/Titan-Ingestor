# 📘 Titan Ingestor - API Reference & Operational Manual

> **Version:** 1.0.0 (Zero-Alloc / Deep-Copy Edition)
> **Protocol:** HTTP/1.1 (JSON)

This document details the communication interface for **Titan Ingestor**. The system is engineered for **maximum throughput** and **low ingestion latency**.

⚠️ **Architectural Note:** This service operates in **Asynchronous** mode. A `202 Accepted` response indicates that the message has been validated, deep-copied to safe memory, and enqueued. Database persistence occurs in background *batches* processed by concurrent workers.

---

## ⚙️ Configuration (Environment Variables)

Titan's behavior is controlled exclusively via environment variables. There are no configuration files.

| Variable | Type | Default | Technical Description |
| :--- | :--- | :--- | :--- |
| `ADDR` | `string` | `:8080` | Interface and port for the HTTP server listener. |
| `DATABASE_URL` | `string` | *Required* | PostgreSQL connection string (`postgres://u:p@host/db...`). |
| `WORKER_COUNT` | `int` | `CPU * 2` | Number of concurrent goroutines draining the queue to the DB. |
| `BUFFER_SIZE` | `int` | `16384` | Ring Buffer capacity. If full, the system returns `429`. |
| `LOG_LEVEL` | `int` | `1` | `0`=Silent, `1`=Error/Panic Only. |

---

## 🚀 Endpoints

### 1. Ingest Log Message
The primary high-performance endpoint. Optimized for *Zero-Allocation* parsing.

- **URL:** `/log`
- **Method:** `POST`
- **Content-Type:** `application/json`

#### Request Body
The payload must be strict JSON. The custom state-machine parser will reject malformed JSONs instantly without allocation.
* **Limit:** 128KB (Hard limit).

```json
{
  "message": "Critical failure in cooling system module 4"
}

Responses
| Code | Status | Description | Client Action |
|---|---|---|---|
| 202 | Accepted | Message successfully enqueued. | None. Success. |
| 400 | Bad Request | Invalid JSON, missing key, or corrupt UTF-8. | Fix payload. Do not retry. |
| 413 | Payload Too Large | Body > 128KB. | Truncate or discard message. |
| 429 | Too Many Requests | Active Backpressure. Queue is full. | Wait and Retry (Exponential Backoff). |
| 503 | Service Unavailable | System shutting down or DB unavailable. | Wait for recovery. |
Example (cURL)
curl -i -X POST http://localhost:8080/log \
  -H "Content-Type: application/json" \
  -d '{"message": "System overload warning"}'

2. Real-time Metrics (Observability)
Returns the internal system state without allocating memory on the Heap (direct snapshot of atomic counters). Ideal for Prometheus/Grafana (via adapter) or live monitoring.
 * URL: /metrics
 * Method: GET
Response Example
{
  "processed": 540230,           // Total messages committed to DB
  "rejected": 0,                 // Messages rejected (Queue Full)
  "dropped": 0,                  // Messages lost due to DB fatal errors
  "queued": 150,                 // Messages currently waiting in RAM
  "capacity": 16384,             // Total buffer size
  "utilization_pct": 0.91,       // Queue usage % (< 80% is healthy)
  "client_errors": 12,           // 4xx Errors (Client fault)
  "server_errors": 0,            // 5xx Errors (Server fault)
  "db_errors": 0,                // Connection/Insert failures
  "goroutines": 34,              // Active lightweight threads
  "uptime_seconds": 3600
}

3. Health & Readiness (Kubernetes Probes)
Liveness Probe (/health)
Checks if the process is running and can communicate with the Database.
 * Returns 200 OK: Service is alive and DB is responsive (or health cache is valid).
 * Returns 503 Service Unavailable: DB is down or timed out.
<!-- end list -->
{"status": "healthy", "db_healthy": true, "db_errors": 0}

Readiness Probe (/ready)
Checks if the service is ready to accept traffic (not initializing or shutting down).
 * Returns 200 OK: Ready to accept POST requests.
 * Returns 503 Service Unavailable: During Startup or Graceful Shutdown.
🛡️ Error Handling Protocol
Titan uses a structured and standardized error format. Every non-200 error returns this JSON body and the X-Error-Code header.
Error Schema
{
  "error": "queue at capacity",   // Human-readable message
  "code": 2000,                   // Internal tracing code (See Table)
  "retryable": true,              // If 'true', client MUST retry
  "retry_after": "1s",            // Backoff suggestion (optional)
  "context": {                    // Debug metadata
    "queued": 16384,
    "capacity": 16384
  }
}

Error Codes Reference
| Range | Type | Example |
|---|---|---|
| 1000-1999 | Client Errors | 1000 (Invalid JSON), 1002 (Empty Msg) |
| 2000-2999 | Server/Transient | 2000 (Queue Full), 2005 (DB Timeout) |
| 3000+ | Infrastructure | 3000 (Bind Failed), 3006 (Config Error) |
🏗️ Operational Mechanics (How it Works)
1. The Ingestion Pipeline
 * Request: HTTP arrives. Payload is read into a pre-allocated memory buffer (sync.Pool).
 * Parsing: The "State Machine Parser" validates JSON and extracts the message (Zero-Alloc).
 * Safety Copy: The message undergoes a Deep Copy to an internal structure. The HTTP buffer is returned to the pool immediately.
 * Queueing: The copied message enters the msgChan (Buffered Channel). HTTP returns 202 Accepted.
2. The Worker Pool (Async Batching)
 * Multiple Workers (default: CPU * 2) listen to the channel.
 * They do not save 1-by-1. They accumulate a Batch in memory.
 * Flush Trigger: The batch is committed to Postgres when:
   * The batch reaches 256 messages.
   * OR 25ms have passed since the first message (Latency Timeout).
 * Transaction: The entire batch is saved in a single SQL transaction (BEGIN -> INSERT... -> COMMIT).
3. Graceful Shutdown
Upon receiving SIGTERM or SIGINT (Ctrl+C):
 * The /ready endpoint immediately returns 503 (Cuts off Load Balancer traffic).
 * The HTTP server stops accepting new requests.
 * The system waits for Workers to drain all remaining messages in the queue to the Database.
 * Only then does the process exit. Zero data loss.
Titan Ingestor - Built for the storm.


