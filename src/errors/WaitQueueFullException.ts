export class WaitQueueFullException extends Error {
  constructor(message: string = 'Waiting queue is at maximum capacity.') {
    super(message);
    this.name = 'WaitQueueFullException';
  }
}
