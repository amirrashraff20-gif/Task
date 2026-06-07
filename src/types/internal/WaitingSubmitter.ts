import type { Event } from '../public/Event';

export interface WaitingSubmitter {
  readonly resolve: () => void;
  readonly reject: (err: Error) => void;
  readonly event: Event;
  readonly signal?: AbortSignal;
  readonly onAbort?: () => void;
  isAborted?: boolean;
}
