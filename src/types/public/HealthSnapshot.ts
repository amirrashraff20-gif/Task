export interface HealthSnapshot {
  readonly status: "healthy" | "degraded" | "overloaded";
  readonly queueDepth: number;
  readonly inFlight: number;
  readonly activePartitions: number;
  readonly deadLettered: number;
  readonly retryRate: number;
  readonly throughput: number;
  readonly avgLatencyMs: number;
  readonly p95LatencyMs: number;
}
