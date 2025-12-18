# ⚡ Titan Ingestor (Single-File / No-Framework)

> **"Frameworks are for prototyping. Architecture is for production."**

**Titan Ingestor** is a high-performance log ingestion service written in **pure Go**, contained within a **single file** (`main.go`). It is engineered for scenarios where every microsecond and every byte of allocation matters.

Unlike solutions based on Gin, Echo, or Fiber, Titan strips away all unnecessary abstractions, implementing its own custom State-Machine JSON parser and manual memory management to ensure **absolute stability under extreme pressure**.

## 🚀 Engineering Highlights

* **Single File Architecture:** Zero dependency on structural complexity. 72KB of pure logic.
* **Custom State-Machine JSON Parser:** Forget `encoding/json`. Titan uses a custom lexical parser that reads raw bytes, validates UTF-8, and extracts payloads without *reflection*.
* **Memory Safety Guarantee:** Rigorous implementation of two-stage **Deep Copy** (HTTP -> Pool -> Batch) to eliminate *Race Conditions* and *Segfaults*, while maintaining high throughput.
* **Zero-Allocation Metrics:** The `/metrics` endpoint constructs the JSON response manually (byte buffer) to prevent Heap allocations during monitoring.
* **Smart Postgres Batching:** Groups insertions into atomic transactions with strict timeouts and automatic *backpressure* (returns 429 if the DB chokes).
* **Atomic Health Checks:** Lock-free health monitoring using `sync/atomic`.

## 📊 Performance Targets

Designed to run on server-grade hardware (e.g., Xeon E5 / EPYC):

* **Throughput:** ~500k+ req/s (Safety Mode enabled)
* **Latency (p99):** < 1ms (In-memory queueing)
* **Memory Footprint:** ~200MB RSS under load
* **Strategy:** Trades CPU cycles (memory copying) for absolute stability (zero crashes).

## 🛠️ Installation & Usage

### Prerequisites
* Go 1.21+
* PostgreSQL (with the table created or permission to `CREATE TABLE`)

### 1. Build
For production (strips debug symbols and optimizes the binary):
```bash
go build -tags=production -ldflags="-s -w" -o titan main.go
```
2. Configuration (Environment Variables)
| Variable | Default | Description |
|---|---|---|
| ADDR | :8080 | HTTP Server Port |
| DATABASE_URL | postgres://... | Postgres Connection String |
| WORKER_COUNT | NumCPU * 2 | Number of ingestion goroutines |
| BUFFER_SIZE | 16384 | Main channel buffer size |

3. Run
export DATABASE_URL="postgres://user:pass@localhost/logdb?sslmode=disable"
./titan

🧪 Testing the API
Log Ingestion (High Performance)
```json
curl -X POST http://localhost:8080/log \
  -H "Content-Type: application/json" \
  -d '{"message": "Critical system failure in sector 7G"}'

## Response

Expected Response: {"status":"queued"} (202 Accepted)
Real-time Metrics
curl http://localhost:8080/metrics
```
Returns detailed stats on GC, Goroutines, TPS, and DB errors.
Health Check (K8s Ready)
curl http://localhost:8080/health
curl http://localhost:8080/ready

🧠 Deep Dive: Why not json.Unmarshal?
Go's standard library uses Reflection to map JSON to Structs. This generates memory allocations and consumes significant CPU.
Titan implements a Finite State Machine (FSM) that scans the request byte slice only once:
 * stateSeekKey -> Finds the key.
 * stateInKey -> Checks if it is "message".
 * stateSeekValue -> Skips whitespace.
 * stateInString -> Extracts the value.
This results in linear O(n) parsing without allocating extra memory for maps or empty interfaces.

