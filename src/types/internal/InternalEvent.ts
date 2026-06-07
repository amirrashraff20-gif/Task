import type { Event } from '../public/Event';

export interface InternalEvent {
  readonly event: Event;
  retryCount: number;
  /** Monotonic time (performance.now()) when the event entered the partition queue */
  readonly enqueuedAt: number;
  /** Monotonic time (performance.now()) when processing started */
  startedAt?: number;
}
