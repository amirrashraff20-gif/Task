export interface WritablePartitionMetrics {
  queueDepth: number;
  processed: number;
  failed: number;
  retried: number;
  deadLettered: number;
  avgLatencyMs: number;
  lastActivityTimestamp: number;
}
