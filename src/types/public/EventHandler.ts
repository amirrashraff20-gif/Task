import type { Event } from './Event';

export type EventHandler<T = unknown> = (event: Readonly<Event<T>>, signal?: AbortSignal) => Promise<void>;
