import type { Event } from './Event';
import type { EngineMetrics } from './Metrics';
import type { Logger } from './Logger';
import type { ShutdownReport } from './ShutdownReport';

export interface EngineConfig {
  readonly maxConcurrency: number;
  readonly maxCapacity: number;
  readonly maxWaiters: number;
  readonly maxRetries: number;
  readonly backoffMs: number;
  readonly yieldThreshold?: number;
  readonly handlerTimeoutMs?: number;
  
  readonly logger?: Logger;
  
  readonly onEventStarted?: (event: Readonly<Event>) => void;
  readonly onEventSucceeded?: (event: Readonly<Event>) => void;
  readonly onEventFailed?: (event: Readonly<Event>, error: Error) => void;
  readonly onEventRetried?: (event: Readonly<Event>, error: Error) => void;
  readonly onDeadLetter?: (event: Readonly<Event>) => void;
  readonly onShutdownStarted?: () => void;
  readonly onShutdownCompleted?: (report: ShutdownReport) => void;

  readonly onMetricsReport?: (metrics: EngineMetrics) => void;
  readonly metricsIntervalMs?: number;
}
