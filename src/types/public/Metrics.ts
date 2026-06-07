export interface EngineMetrics {
  readonly processed: number;
  readonly failed: number;
  readonly deadLettered: number;
  readonly inFlight: number;
  readonly queueDepth: number;
  
  readonly eventsProcessedPerSecond: number;
  readonly eventsFailedPerSecond: number;
  readonly eventsRetriedPerSecond: number;
  readonly eventsAdmittedPerSecond: number;
  readonly deadLetteredPerSecond: number;
  
  readonly avgLatencyMs: number;
  readonly p50LatencyMs: number;
  readonly p95LatencyMs: number;
  readonly p99LatencyMs: number;
  
  readonly avgQueueWaitMs: number;
  readonly p95QueueWaitMs: number;
  
  readonly avgEndToEndLatencyMs: number;
}
