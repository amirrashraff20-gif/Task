# Event Processing Engine

A production-grade, high-throughput, concurrency-bounded event processing engine for Node.js.

## Features (R1-R6 Compliance)
1. **Bounded Global Concurrency**: Strictly enforces max active executing handlers, ensuring system stability.
2. **FIFO Per Partition**: Guarantees strict ordering of events bound to the same partition key.
3. **Backpressure**: Enforces strict `O(1)` memory bounds. Rejects submissions when wait queues overflow.
4. **Graceful Shutdown**: Blocks new requests, drains pending workloads, and enforces hard timeouts.
5. **Poison Event Isolation**: Individual failing events retry using an exponential backoff simulator, without starving healthy partitions. Repeated failures are dead-lettered.
6. **Runtime Reconfiguration**: Real-time adjustment of concurrency limits without downtime or state loss.

## Production Grade Enhancements

In addition to the core requirements, this engine incorporates the following enterprise-grade capabilities:
- **AbortController cancellation**
- **Handler timeout support**
- **Dynamic runtime reconfiguration**
- **Structured logging**
- **Health monitoring**
- **P50/P95/P99 latency tracking**
- **Throughput metrics**
- **Top-K hot partitions detection**
- **Memory leak prevention**
- **Observability hooks**
- **Graceful forced shutdown with cancellation**

## Architecture Q&A

### Question 1: How do R1 (Global Concurrency) and R2 (FIFO) coexist?
**Explanation:** 
A naive global semaphore fails here because it would blindly pull events out of order or allow a single slow partition to consume all slots.
Instead, the engine separates **Queueing** from **Scheduling**. 
- **Data Structures**: Events are grouped into a `Map<PartitionKey, PartitionState>`. Each partition maintains its own array-based FIFO queue.
- **Scheduler**: The engine maintains a `ReadyQueue` (Linked List) of partition keys that have pending work but are not currently executing. 
- **Coexistence**: The scheduler loop (`schedule()`) polls the `ReadyQueue`. If `activeConcurrency < maxConcurrency`, it wakes up a partition. That partition executes exactly one event (preserving FIFO) and then yields. This guarantees no two events from the *same* partition execute concurrently, preserving R2, while the total number of actively processing partitions is strictly capped by `maxConcurrency`, preserving R1.

### Question 2: Where exactly is the concurrency slot released?
**Explanation:**
The concurrency slot (`this.activeConcurrency`) is released within a strict `try/finally` block inside the `processPartition(partitionKey)` method.
- **Success**: The handler resolves, the event is removed, and the `finally` block executes `this.activeConcurrency--`.
- **Failure**: The handler rejects. The event is either marked for retry or dead-lettered, and the `finally` block executes `this.activeConcurrency--`.
- **Timeout (Shutdown)**: If the engine shuts down, in-flight promises are awaited. Once the underlying user handler resolves/rejects, the `finally` block still safely decrements the counter.
*Code Location*: `EventProcessingEngine.ts`, inside `processPartition`, at the bottom `finally { this.activeConcurrency--; ... }`.

### Question 3: How is backpressure implemented?
**Explanation:**
- **Strategy**: The engine utilizes two hard numerical boundaries: `maxCapacity` (total admitted events including in-flight) and `maxWaiters` (events suspended in Promises awaiting admission).
- **Bounded Memory**: When `maxCapacity` is reached, `submit()` suspends execution by storing a Promise `resolve/reject` pair in a `waitingSubmitters` Linked List. If this list reaches `maxWaiters`, `submit()` synchronously rejects with `WaitQueueFullException`. Because arrays and lists are capped at explicit numbers, the memory footprint remains absolutely bounded regardless of infinite producer pressure.
- **Failure modes of incorrect implementations**: Without `maxWaiters`, unhandled Promises would pile up in the V8 heap until an Out-Of-Memory (OOM) crash. Without `maxCapacity`, the engine would ingest millions of events simultaneously, starving the event loop and destroying latency.

### Question 4: How does graceful shutdown work?
**Explanation:**
- **Drain Behavior**: Upon calling `shutdown()`, the engine flips its state to `SHUTTING_DOWN`. This immediately rejects all pending and future `submit()` calls. It clears any pending backoff timers to forcefully retry them immediately. It then waits on a `drainPromise` which resolves only when `admittedEventsCount === 0`.
- **Timeout Behavior**: If `timeoutMs` is provided, a `Promise.race` is utilized. If the timeout fires first, the engine transitions to `TERMINATED` (which short-circuits the scheduler) and generates a snapshot of exactly what was left unfinished.
- **Consistency**: The snapshot `ShutdownReport` is mathematically precise. Events are either counted as `processed`, `failed`, `deadLettered`, `unfinishedQueuedEvents`, or `inFlightEvents`. No event is lost to the ether.

### Question 5: Distributed Systems Considerations
**Scenario**: Multiple engine instances behind a load balancer sharing a central queue (e.g., Kafka, SQS).
- **Guaranteed Locally**: 
  - R1 (Global Concurrency): Each Node.js process will still respect its local CPU/Concurrency limits.
  - R3 (Backpressure): Local memory will not crash; instances will simply reject load back to the LB or message broker.
  - R4 (Shutdown): Local process will drain gracefully.
- **Impossible without external coordination**:
  - R2 (FIFO per partition): If Partition A is routed to Instance 1, and the next event for Partition A is routed to Instance 2, they may execute concurrently and finish out of order. Strict FIFO requires a consistent hashing or sticky-routing algorithm at the Load Balancer/Broker level to ensure a specific PartitionKey always hashes to the *same* engine instance.
  - R5 (Poison Isolation): If Instance 1 dead-letters an event, Instance 2 does not know about it. State must be externalized to a shared Redis/Database to prevent Instance 2 from processing the next event in a corrupted partition.
