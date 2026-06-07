import type { Event } from './Event';
import type { EngineMetrics } from './Metrics';

export interface ShutdownReport {
  readonly processed: number;
  readonly failed: number;
  readonly deadLettered: number;
  readonly timedOut: boolean;
  readonly queuedAtShutdown: number;
  readonly inFlightAtShutdown: number;
  readonly unfinishedQueuedEvents: ReadonlyArray<Event>;
  readonly inFlightEvents: ReadonlyArray<Event>;
  readonly shutdownStartedAt: number;
  readonly shutdownCompletedAt: number;
  readonly shutdownDurationMs: number;
  readonly finalMetricsSnapshot: EngineMetrics;
}
