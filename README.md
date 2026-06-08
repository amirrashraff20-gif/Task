# Event Processing Engine

A production-grade, high-throughput, and concurrency-bounded event processing engine for Node.js.

## Compliance with Core Requirements (R1-R6)

1. **R1: Bounded Global Concurrency**: Strictly enforces a global maximum number of active executing handlers, ensuring CPU/Network stability under extreme load.
2. **R2: FIFO Per Partition**: Guarantees strict ordering of events bound to the same partition key. No two events from the same partition execute concurrently.
3. **R3: Backpressure**: Enforces strict `O(1)` memory bounds. Safely suspends asynchronous submissions up to a `maxWaiters` limit, and forcefully rejects ingestion with `WaitQueueFullException` when capacity is exceeded.
4. **R4: Graceful Shutdown**: Halts ingestion, cleanly drains pending workloads, enforces hard timeouts via `AbortController`, and guarantees mathematically precise final state snapshots.
5. **R5: Poison Event Isolation**: Individual failing events retry using an exponential backoff simulator. Partition-level failures never starve healthy partitions. Repeated failures are explicitly dead-lettered.
6. **R6: Runtime Reconfiguration**: Supports real-time dynamic adjustment of concurrency limits and configuration parameters without downtime or state loss.

## Production-Grade Enhancements

In addition to the core requirements, this engine incorporates the following enterprise-grade capabilities:
- **AbortController Cancellation**
- **Handler Timeout Support**
- **Dynamic Runtime Reconfiguration**
- **Structured Logging**
- **Health Monitoring**
- **P50/P95/P99 Latency Tracking (Smart Caching)**
- **Throughput Metrics**
- **Top-K Hot Partitions Detection**
- **Memory Leak Prevention**
- **Observability Hooks**
- **Graceful Forced Shutdown with Cancellation**

> **Note:** All production-grade enhancements are strictly additive. They are designed to harden the system's reliability and observability without compromising or modifying the strict guarantees required by R1-R6.

---

## Architecture Q&A

### 1. How do R1 (Global Concurrency) and R2 (FIFO) coexist?

A naive global semaphore fails here because it would blindly pull events out of order or allow a single slow partition to consume all execution slots. Instead, the engine strictly separates **Queueing** from **Scheduling**:
- **Queueing:** Events are grouped into a `Map<PartitionKey, PartitionState>`. Each partition maintains its own isolated, array-based FIFO queue.
- **Scheduling:** A global `ReadyQueue` (Linked List) tracks partition keys that possess pending work but are not currently executing. 
- **Coexistence:** The internal `schedule()` loop polls the `ReadyQueue`. If `activeConcurrency < maxConcurrency`, it activates a partition. That partition sequentially executes events (preserving FIFO) and yields periodically to prevent event-loop starvation. This architecture guarantees that no two events from the *same* partition execute concurrently (R2), while the total number of actively processing partitions never exceeds `maxConcurrency` (R1).

### 2. Where exactly is the concurrency slot released?

The concurrency slot (`activeConcurrency`) is decremented within a strict `try/finally` block inside the `processPartition(partitionKey)` method. Regardless of whether the underlying asynchronous handler succeeds, throws an error, or is aborted via timeout, the slot is guaranteed to be released.

```typescript
try {
  // Handler execution, timeout races, and event resolution...
} finally {
  this.activeConcurrency--;
  if (yieldCounter > 0 && yieldCounter % threshold === 0) {
    setTimeout(() => this.schedule(), 0);
  } else {
    this.schedule();
  }
}
```

### 3. How is backpressure implemented?

The engine utilizes two deterministic numerical boundaries: `maxCapacity` (total admitted events including in-flight) and `maxWaiters` (events suspended in Promises awaiting admission).
- **Bounded Memory:** When `maxCapacity` is reached, `submit()` suspends execution by storing a Promise resolver inside a `waitingSubmitters` Linked List. If this list reaches `maxWaiters`, `submit()` synchronously rejects with `WaitQueueFullException`. Because data structures are capped at explicit numerical values, the Node.js V8 memory footprint remains bounded `O(1)` regardless of infinite producer pressure.
- **Why this matters:** Without `maxWaiters`, unhandled Promises would accumulate indefinitely in the heap, causing an Out-Of-Memory (OOM) crash. Without `maxCapacity`, the engine would ingest millions of concurrent events, starving the event loop and degrading latency.

### 4. How does graceful shutdown work?

Upon invoking `shutdown(opts)`:
1. **State Transition:** The engine immediately transitions to `SHUTTING_DOWN`, instantly rejecting all pending and future `submit()` requests.
2. **Backoff Clearance:** Any partitions suspended in retry backoff timers are forcefully awakened and re-queued.
3. **Draining:** The engine waits on a `drainPromise` that resolves only when `admittedEventsCount === 0`.
4. **Timeout & Abort:** If `opts.timeoutMs` is provided, a `Promise.race` is utilized. If the timeout fires before draining completes, the engine triggers an internal `AbortController`. This propagates cancellation signals to all currently executing handlers. The engine then transitions to `TERMINATED`, halting the scheduler permanently.
5. **Snapshot Generation:** A precise `ShutdownReport` is generated. It mathematically accounts for every event, categorizing them as `processed`, `failed`, `deadLettered`, `unfinishedQueuedEvents`, or `inFlightEvents`.

### 5. Distributed Systems Considerations

If this engine operates behind a Load Balancer (e.g., pulling from Kafka or SQS):
- **Guaranteed Locally:** Each Node.js process will flawlessly enforce its local CPU/Concurrency limits (R1), safely reject overflow traffic back to the broker (R3), and drain gracefully upon SIGTERM (R4).
- **Impossible without external coordination:** 
  - **Global FIFO (R2):** If Partition A is routed to Instance 1, and the next event for Partition A is routed to Instance 2, they may execute concurrently and finish out of order. Strict distributed FIFO requires a sticky-routing or consistent hashing algorithm at the Load Balancer/Broker level to ensure a specific `PartitionKey` always hashes to the *same* engine instance.
  - **Global Poison Isolation (R5):** If Instance 1 dead-letters an event, Instance 2 remains unaware. State must be externalized to a shared datastore (e.g., Redis) to prevent Instance 2 from processing subsequent events in a corrupted partition.

---

## Test Results

```text
// Paste your `npm test` output here
```
