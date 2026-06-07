import type { EngineConfig } from '../types/public/EngineConfig';
import type { Event } from '../types/public/Event';
import type { ShutdownReport } from '../types/public/ShutdownReport';
import type { EngineMetrics } from '../types/public/Metrics';
import type { EventHandler } from '../types/public/EventHandler';
import type { HealthSnapshot } from '../types/public/HealthSnapshot';
import type { PartitionMetrics } from '../types/public/PartitionMetrics';
import { EngineState } from '../types/internal/EngineState';
import type { PartitionState } from '../types/internal/PartitionState';
import { PartitionStatus } from '../types/internal/PartitionStatus';
import type { WaitingSubmitter } from '../types/internal/WaitingSubmitter';
import type { InternalEvent } from '../types/internal/InternalEvent';
import { LinkedList } from '../utils/LinkedList';
import { ReadyQueue } from '../utils/ReadyQueue';
import { PercentileTracker } from '../utils/PercentileTracker';
import { RateTracker } from '../utils/RateTracker';
import { TopKHeap } from '../utils/TopKHeap';
import { EngineShutdownException } from '../errors/EngineShutdownException';
import { WaitQueueFullException } from '../errors/WaitQueueFullException';

export class EventProcessingEngine {
  private admittedEventsCount: number = 0;
  private globalQueueDepth: number = 0;
  private engineState: EngineState = EngineState.RUNNING;
  private activeConcurrency: number = 0;
  
  private readonly partitions = new Map<string, PartitionState>();
  private readonly readyPartitions = new ReadyQueue();
  private readonly waitingSubmitters = new LinkedList<WaitingSubmitter>();
  private readonly inFlightEventsTracker = new Set<InternalEvent>();
  private readonly shutdownController = new AbortController();
  
  private drainResolver: (() => void) | null = null;
  private metricsState = { processed: 0, failed: 0, deadLettered: 0 };

  private config: EngineConfig;
  private metricsTimer: ReturnType<typeof setInterval> | null = null;

  // Telemetry trackers
  private readonly queueWaitTracker = new PercentileTracker(1024);
  private readonly processingLatencyTracker = new PercentileTracker(1024);
  private readonly endToEndLatencyTracker = new PercentileTracker(1024);

  private readonly admittedRate = new RateTracker(5);
  private readonly processedRate = new RateTracker(5);
  private readonly failedRate = new RateTracker(5);
  private readonly retriedRate = new RateTracker(5);
  private readonly deadLetteredRate = new RateTracker(5);

  constructor(
    config: EngineConfig,
    private readonly handler: EventHandler
  ) {
    this.config = { ...config };
    if (config.maxConcurrency <= 0) throw new Error("maxConcurrency must be > 0");
    if (config.maxCapacity <= 0) throw new Error("maxCapacity must be > 0");
    if (config.maxWaiters < 0) throw new Error("maxWaiters cannot be negative");
    if (config.maxRetries < 0) throw new Error("maxRetries cannot be negative");
    if (config.backoffMs < 0) throw new Error("backoffMs cannot be negative");
    if (config.handlerTimeoutMs !== undefined && config.handlerTimeoutMs <= 0) {
      throw new Error("handlerTimeoutMs must be > 0");
    }
    if (config.yieldThreshold !== undefined && (config.yieldThreshold <= 0 || config.yieldThreshold > 10000)) {
      throw new Error("yieldThreshold must be between 1 and 10000");
    }

    this.safeLog('info', 'ENGINE_STARTED', { config: this.config });
    this.setupTelemetry();
  }

  private safeLog(level: 'debug' | 'info' | 'warn' | 'error', message: string, metadata?: Record<string, unknown>) {
    if (!this.config.logger) return;
    try {
      this.config.logger[level](message, metadata);
    } catch (e) {
      process.stderr.write(`[EventProcessingEngine] Logger failure: ${e instanceof Error ? e.message : String(e)}\n`);
    }
  }

  private invokeHook<T extends any[]>(hook: ((...args: T) => void) | undefined, ...args: T) {
    if (!hook) return;
    try {
      hook(...args);
    } catch (e) {
      this.safeLog('error', 'OBSERVABILITY_CALLBACK_FAILED', { error: String(e) });
    }
  }

  public metrics(): EngineMetrics {
    const queueWait = this.queueWaitTracker.getMetrics();
    const processing = this.processingLatencyTracker.getMetrics();
    const endToEnd = this.endToEndLatencyTracker.getMetrics();

    return {
      processed: this.metricsState.processed,
      failed: this.metricsState.failed,
      deadLettered: this.metricsState.deadLettered,
      inFlight: this.inFlightEventsTracker.size,
      queueDepth: this.globalQueueDepth,
      
      eventsProcessedPerSecond: this.processedRate.ratePerSecond,
      eventsFailedPerSecond: this.failedRate.ratePerSecond,
      eventsRetriedPerSecond: this.retriedRate.ratePerSecond,
      eventsAdmittedPerSecond: this.admittedRate.ratePerSecond,
      deadLetteredPerSecond: this.deadLetteredRate.ratePerSecond,
      
      avgLatencyMs: processing.avg,
      p50LatencyMs: processing.p50,
      p95LatencyMs: processing.p95,
      p99LatencyMs: processing.p99,
      
      avgQueueWaitMs: queueWait.avg,
      p95QueueWaitMs: queueWait.p95,
      
      avgEndToEndLatencyMs: endToEnd.avg
    };
  }

  public getHealthSnapshot(): HealthSnapshot {
    const m = this.metrics();
    const total = m.eventsProcessedPerSecond + m.eventsFailedPerSecond;
    const failedRate = total > 0 ? (m.eventsFailedPerSecond / total) : 0;
    const retryRate = m.eventsProcessedPerSecond > 0 ? m.eventsRetriedPerSecond / m.eventsProcessedPerSecond : 0;
    const queueRatio = m.queueDepth / this.config.maxCapacity;
    
    let status: "healthy" | "degraded" | "overloaded" = "healthy";

    if (queueRatio >= 0.90 || m.p95LatencyMs > 5000 || failedRate >= 0.20) {
      status = "overloaded";
    } else if (queueRatio >= 0.50 || m.p95LatencyMs > 1000 || failedRate >= 0.05 || retryRate >= 0.10) {
      status = "degraded";
    }

    return {
      status,
      queueDepth: m.queueDepth,
      inFlight: m.inFlight,
      activePartitions: this.partitions.size,
      deadLettered: m.deadLettered,
      retryRate: m.eventsProcessedPerSecond > 0 ? m.eventsRetriedPerSecond / m.eventsProcessedPerSecond : 0,
      throughput: m.eventsProcessedPerSecond,
      avgLatencyMs: m.avgLatencyMs,
      p95LatencyMs: m.p95LatencyMs
    };
  }

  public getPartitionMetrics(partitionKey: string): PartitionMetrics | undefined {
    const partition = this.partitions.get(partitionKey);
    if (!partition) return undefined;
    return { ...partition.metrics };
  }

  public getTopPartitions(limit: number): Array<{ partitionKey: string; metrics: PartitionMetrics }> {
    const heap = new TopKHeap<{ partitionKey: string; metrics: PartitionMetrics }>(
      limit,
      (a, b) => a.metrics.queueDepth - b.metrics.queueDepth
    );
    for (const [k, v] of this.partitions.entries()) {
      heap.push({ partitionKey: k, metrics: v.metrics });
    }
    return heap.getElements().map(r => ({ partitionKey: r.partitionKey, metrics: { ...r.metrics } }));
  }

  public setMaxConcurrency(n: number): void {
    if (n <= 0) throw new Error("maxConcurrency must be > 0");
    this.config = { ...this.config, maxConcurrency: n };
    this.schedule();
  }

  public updateConfig(newConfig: Partial<EngineConfig>): void {
    const merged = { ...this.config, ...newConfig };
    if (merged.maxConcurrency <= 0) throw new Error("maxConcurrency must be > 0");
    if (merged.maxCapacity <= 0) throw new Error("maxCapacity must be > 0");
    if (merged.maxWaiters < 0) throw new Error("maxWaiters cannot be negative");
    if (merged.maxRetries < 0) throw new Error("maxRetries cannot be negative");
    if (merged.backoffMs < 0) throw new Error("backoffMs cannot be negative");
    if (merged.handlerTimeoutMs !== undefined && merged.handlerTimeoutMs <= 0) {
      throw new Error("handlerTimeoutMs must be > 0");
    }
    if (merged.yieldThreshold !== undefined && (merged.yieldThreshold <= 0 || merged.yieldThreshold > 10000)) {
      throw new Error("yieldThreshold must be between 1 and 10000");
    }

    this.config = merged;
    this.setupTelemetry();
    this.admitWaitingSubmitters();
    this.schedule();
  }

  private setupTelemetry(): void {
    if (this.metricsTimer) {
      clearInterval(this.metricsTimer);
      this.metricsTimer = null;
    }
    if (this.config.metricsIntervalMs && this.config.metricsIntervalMs > 0 && this.config.onMetricsReport) {
      this.metricsTimer = setInterval(() => {
        this.invokeHook(this.config.onMetricsReport, this.metrics());
      }, this.config.metricsIntervalMs);
    }
  }

  public submit(event: Event, signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) return Promise.reject(new Error("Submit aborted"));
    if (!event.correlationId) {
      return Promise.reject(new Error("Event must include a valid correlationId"));
    }

    if (this.engineState === EngineState.SHUTTING_DOWN || this.engineState === EngineState.TERMINATED) {
      this.safeLog('warn', 'EVENT_REJECTED', { reason: 'Engine shutting down', eventId: event.id, partitionKey: event.partitionKey });
      return Promise.reject(new EngineShutdownException());
    }

    if (this.admittedEventsCount >= this.config.maxCapacity) {
      if (this.waitingSubmitters.size >= this.config.maxWaiters) {
        this.safeLog('warn', 'EVENT_REJECTED', { reason: 'Queue full', eventId: event.id, correlationId: event.correlationId });
        return Promise.reject(new WaitQueueFullException());
      }

      return new Promise<void>((resolve, reject) => {
        const submitter: WaitingSubmitter = { resolve, reject, event, signal };
        
        if (signal) {
          const onAbort = () => {
            submitter.isAborted = true;
            reject(new Error("Submit aborted"));
          };
          signal.addEventListener('abort', onAbort, { once: true });
          (submitter as any).onAbort = onAbort; // assigned via cast because readonly in type
        }

        this.waitingSubmitters.push(submitter);
      });
    }

    this.admitEvent(event);
    return Promise.resolve();
  }

  private admitEvent(event: Event): void {
    this.admittedEventsCount++;
    this.globalQueueDepth++;
    this.admittedRate.increment();

    this.safeLog('debug', 'EVENT_ADMITTED', { eventId: event.id, partitionKey: event.partitionKey, correlationId: event.correlationId });

    let partition = this.partitions.get(event.partitionKey);
    if (!partition) {
      partition = {
        queue: new LinkedList<InternalEvent>(),
        status: PartitionStatus.QUEUED,
        backoffTimer: null,
        metrics: {
          queueDepth: 0,
          processed: 0,
          failed: 0,
          retried: 0,
          deadLettered: 0,
          avgLatencyMs: 0,
          lastActivityTimestamp: Date.now()
        }
      };
      this.partitions.set(event.partitionKey, partition);
    }

    partition.metrics.queueDepth++;
    partition.metrics.lastActivityTimestamp = Date.now();

    partition.queue.push({ event, retryCount: 0, enqueuedAt: performance.now() });

    if (partition.status === PartitionStatus.QUEUED) {
      this.readyPartitions.push(event.partitionKey);
    }

    this.schedule();
  }

  private admitWaitingSubmitters(): void {
    while (this.admittedEventsCount < this.config.maxCapacity && !this.waitingSubmitters.isEmpty()) {
      const submitter = this.waitingSubmitters.shift()!;
      
      if (submitter.signal && submitter.onAbort) {
        submitter.signal.removeEventListener('abort', submitter.onAbort);
      }
      if (submitter.isAborted) continue;

      if (this.engineState === EngineState.SHUTTING_DOWN || this.engineState === EngineState.TERMINATED) {
        submitter.reject(new EngineShutdownException());
        continue;
      }

      this.admitEvent(submitter.event);
      submitter.resolve();
    }
  }

  private checkDrain(): void {
    if (this.admittedEventsCount === 0 && this.inFlightEventsTracker.size === 0 && this.drainResolver) {
      this.drainResolver();
      this.drainResolver = null;
    }
  }

  private captureSnapshot(timedOut: boolean, shutdownStartedAt: number, shutdownCompletedAt: number): ShutdownReport {
    const unfinishedQueuedEvents: Event[] = [];
    const inFlightEvents: Event[] = [];

    for (const internalEvent of this.inFlightEventsTracker.values()) {
      inFlightEvents.push(internalEvent.event);
    }

    for (const partition of this.partitions.values()) {
      for (const internalEvent of partition.queue) {
        if (!this.inFlightEventsTracker.has(internalEvent)) {
          unfinishedQueuedEvents.push(internalEvent.event);
        }
      }
    }

    return Object.freeze({
      processed: this.metricsState.processed,
      failed: this.metricsState.failed,
      deadLettered: this.metricsState.deadLettered,
      timedOut,
      queuedAtShutdown: unfinishedQueuedEvents.length,
      inFlightAtShutdown: inFlightEvents.length,
      unfinishedQueuedEvents: Object.freeze(unfinishedQueuedEvents),
      inFlightEvents: Object.freeze(inFlightEvents),
      shutdownStartedAt,
      shutdownCompletedAt,
      shutdownDurationMs: shutdownCompletedAt - shutdownStartedAt,
      finalMetricsSnapshot: Object.freeze(this.metrics())
    });
  }

  private shutdownPromise: Promise<ShutdownReport> | null = null;

  public shutdown(opts?: { timeoutMs?: number }): Promise<ShutdownReport> {
    if (this.shutdownPromise) return this.shutdownPromise;
    const promise = this._executeShutdown(opts);
    this.shutdownPromise = promise;
    return promise;
  }

  private async _executeShutdown(opts?: { timeoutMs?: number }): Promise<ShutdownReport> {
    this.engineState = EngineState.SHUTTING_DOWN;
    const shutdownStartedAt = Date.now();
    this.safeLog('info', 'ENGINE_SHUTDOWN_STARTED', { timeoutMs: opts?.timeoutMs });
    this.invokeHook(this.config.onShutdownStarted);

    while (!this.waitingSubmitters.isEmpty()) {
      const submitter = this.waitingSubmitters.shift()!;
      if (submitter.signal && submitter.onAbort) {
        submitter.signal.removeEventListener('abort', submitter.onAbort);
      }
      if (!submitter.isAborted) {
        submitter.reject(new EngineShutdownException());
      }
    }

    for (const [partitionKey, partition] of this.partitions.entries()) {
      if (partition.backoffTimer) {
        clearTimeout(partition.backoffTimer);
        partition.backoffTimer = null;
        partition.status = PartitionStatus.QUEUED;
        this.readyPartitions.push(partitionKey);
      }
    }
    this.schedule();

    let finalReport: ShutdownReport | null = null;

    if (this.admittedEventsCount > 0) {
      const drainPromise = new Promise<void>(resolve => {
        if (this.admittedEventsCount === 0 && this.inFlightEventsTracker.size === 0) {
          if (!finalReport) finalReport = this.captureSnapshot(false, shutdownStartedAt, Date.now());
          resolve();
        } else {
          this.drainResolver = () => {
            if (!finalReport) finalReport = this.captureSnapshot(false, shutdownStartedAt, Date.now());
            resolve();
          };
        }
      });

      if (opts?.timeoutMs !== undefined) {
        let timeoutHandle: ReturnType<typeof setTimeout> | undefined;
        const timeoutPromise = new Promise<void>(resolve => {
          timeoutHandle = setTimeout(() => {
            if (!finalReport) finalReport = this.captureSnapshot(true, shutdownStartedAt, Date.now());
            this.shutdownController.abort(new Error("Engine shutdown timeout exceeded"));
            resolve();
          }, opts.timeoutMs);
        });

        await Promise.race([drainPromise, timeoutPromise]);
        if (timeoutHandle !== undefined) clearTimeout(timeoutHandle);
      } else {
        await drainPromise;
      }
    }

    this.engineState = EngineState.TERMINATED;
    if (this.metricsTimer) {
      clearInterval(this.metricsTimer);
      this.metricsTimer = null;
    }

    if (!finalReport) {
      finalReport = this.captureSnapshot(false, shutdownStartedAt, Date.now());
    }

    this.safeLog('info', 'ENGINE_SHUTDOWN_COMPLETED', {
      durationMs: finalReport.shutdownDurationMs,
      processed: finalReport.processed,
      timedOut: finalReport.timedOut
    });
    this.invokeHook(this.config.onShutdownCompleted, finalReport);

    return finalReport;
  }

  private schedule(): void {
    if (this.engineState === EngineState.TERMINATED) return;

    while (this.activeConcurrency < this.config.maxConcurrency && this.readyPartitions.size > 0) {
      const partitionKey = this.readyPartitions.shift()!;
      const partition = this.partitions.get(partitionKey);

      if (!partition || partition.status !== PartitionStatus.QUEUED) {
        continue;
      }

      partition.status = PartitionStatus.PROCESSING;
      this.activeConcurrency++;

      this.processPartition(partitionKey).catch(err => {
        this.safeLog('error', 'CATASTROPHIC_PARTITION_FAILURE', { partitionKey, error: String(err) });
      });
    }
  }

  private async processPartition(partitionKey: string): Promise<void> {
    let yieldCounter = 0;
    const threshold = this.config.yieldThreshold ?? 100;
    try {
      const partition = this.partitions.get(partitionKey);
      if (!partition) return;

      while (partition.queue.size > 0) {
        if (this.engineState === EngineState.TERMINATED) break;

        const internalEvent = partition.queue.peek()!;
        internalEvent.startedAt = performance.now();
        const queueWaitTimeMs = internalEvent.startedAt - internalEvent.enqueuedAt;
        this.queueWaitTracker.add(queueWaitTimeMs);

        this.globalQueueDepth--;
        this.inFlightEventsTracker.add(internalEvent);

        this.safeLog('debug', 'EVENT_STARTED', {
          eventId: internalEvent.event.id,
          partitionKey,
          correlationId: internalEvent.event.correlationId,
          queueWaitTimeMs,
          retryCount: internalEvent.retryCount
        });
        this.invokeHook(this.config.onEventStarted, internalEvent.event);

        const eventController = new AbortController();
        const onEngineShutdown = () => {
          eventController.abort(new Error("Engine shutdown timeout exceeded"));
        };
        
        if (this.shutdownController.signal.aborted) onEngineShutdown();
        else this.shutdownController.signal.addEventListener('abort', onEngineShutdown);

        try {
          if (this.config.handlerTimeoutMs && this.config.handlerTimeoutMs > 0) {
            let timeoutHandle: ReturnType<typeof setTimeout>;
            let completed = false;
            const timeoutPromise = new Promise<void>((_, reject) => {
              timeoutHandle = setTimeout(() => {
                if (completed) return;
                const error = new Error(`Handler execution exceeded timeout of ${this.config.handlerTimeoutMs}ms`);
                eventController.abort(error);
                reject(error);
              }, this.config.handlerTimeoutMs);
            });

            try {
              await Promise.race([
                this.handler(internalEvent.event, eventController.signal).finally(() => {
                  completed = true;
                }),
                timeoutPromise
              ]);
            } finally {
              clearTimeout(timeoutHandle!);
            }
          } else {
            await this.handler(internalEvent.event, eventController.signal);
          }
          
          const completedAtMono = performance.now();
          const processingLatencyMs = completedAtMono - internalEvent.startedAt;
          this.processingLatencyTracker.add(processingLatencyMs);
          this.endToEndLatencyTracker.add(completedAtMono - internalEvent.enqueuedAt);

          partition.queue.shift();
          this.admittedEventsCount--;
          this.metricsState.processed++;
          this.processedRate.increment();
          
          partition.metrics.queueDepth--;
          partition.metrics.processed++;
          partition.metrics.lastActivityTimestamp = Date.now();
          
          partition.metrics.avgLatencyMs = partition.metrics.processed === 1 ? processingLatencyMs : ((partition.metrics.avgLatencyMs * (partition.metrics.processed - 1)) + processingLatencyMs) / partition.metrics.processed;

          this.safeLog('info', 'EVENT_SUCCEEDED', {
            eventId: internalEvent.event.id,
            correlationId: internalEvent.event.correlationId,
            processingLatencyMs
          });
          this.invokeHook(this.config.onEventSucceeded, internalEvent.event);

          if (this.engineState === EngineState.RUNNING) {
             this.admitWaitingSubmitters();
          }
        } catch (error) {
          const completedAtMono = performance.now();
          const processingLatencyMs = completedAtMono - internalEvent.startedAt!;
          
          this.metricsState.failed++;
          this.failedRate.increment();
          partition.metrics.failed++;
          partition.metrics.lastActivityTimestamp = Date.now();

          this.safeLog('warn', 'EVENT_FAILED', {
            eventId: internalEvent.event.id,
            correlationId: internalEvent.event.correlationId,
            error: String(error),
            processingLatencyMs
          });
          this.invokeHook(this.config.onEventFailed, internalEvent.event, error instanceof Error ? error : new Error(String(error)));

          if (internalEvent.retryCount >= this.config.maxRetries || this.engineState === EngineState.SHUTTING_DOWN) {
            partition.queue.shift();
            this.admittedEventsCount--;
            this.metricsState.deadLettered++;
            this.deadLetteredRate.increment();
            partition.metrics.queueDepth--;
            partition.metrics.deadLettered++;

            this.safeLog('error', 'EVENT_DEAD_LETTERED', {
              eventId: internalEvent.event.id,
              correlationId: internalEvent.event.correlationId,
              retryCount: internalEvent.retryCount
            });
            this.invokeHook(this.config.onDeadLetter, internalEvent.event);

            if (this.engineState === EngineState.RUNNING) {
               this.admitWaitingSubmitters();
            }
          } else {
            internalEvent.retryCount++;
            this.retriedRate.increment();
            partition.metrics.retried++;
            
            partition.status = PartitionStatus.BACKOFF;
            this.globalQueueDepth++;
            
            this.safeLog('info', 'EVENT_RETRIED', {
              eventId: internalEvent.event.id,
              correlationId: internalEvent.event.correlationId,
              retryCount: internalEvent.retryCount,
              backoffMs: this.config.backoffMs
            });
            this.invokeHook(this.config.onEventRetried, internalEvent.event, error instanceof Error ? error : new Error(String(error)));

            partition.backoffTimer = setTimeout(() => {
              const currentPartition = this.partitions.get(partitionKey);
              if (!currentPartition) return;
              currentPartition.backoffTimer = null;
              currentPartition.status = PartitionStatus.QUEUED;
              this.readyPartitions.push(partitionKey);
              this.schedule();
            }, this.config.backoffMs);
            
            break; 
          }
        } finally {
          this.shutdownController.signal.removeEventListener('abort', onEngineShutdown);
          this.inFlightEventsTracker.delete(internalEvent);
          this.checkDrain();
        }

        if (++yieldCounter % threshold === 0) {
          partition.status = PartitionStatus.QUEUED;
          this.readyPartitions.push(partitionKey);
          break; 
        }
      }

      if (partition.queue.size === 0) {
        this.partitions.delete(partitionKey);
      }
    } finally {
      this.activeConcurrency--;
      if (yieldCounter > 0 && yieldCounter % threshold === 0) setImmediate(() => this.schedule());
      else this.schedule();
    }
  }
}
