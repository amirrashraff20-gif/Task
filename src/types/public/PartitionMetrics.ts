export interface PartitionMetrics {
  readonly queueDepth: number;
  readonly processed: number;
  readonly failed: number;
  readonly retried: number;
  readonly deadLettered: number;
  readonly avgLatencyMs: number;
  readonly lastActivityTimestamp: number;
}
