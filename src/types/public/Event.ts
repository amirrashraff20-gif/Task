export interface Event<T = unknown> {
  readonly id: string;
  readonly partitionKey: string;
  readonly payload: T;
  readonly correlationId: string;
}
