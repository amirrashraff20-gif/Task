export class EngineShutdownException extends Error {
  constructor(message: string = 'Engine is shutting down, event not admitted.') {
    super(message);
    this.name = 'EngineShutdownException';
  }
}
