import { LinkedList } from './LinkedList';

export class ReadyQueue {
  private readonly queue = new LinkedList<string>();
  private readonly set = new Set<string>();

  public get size(): number {
    return this.queue.size;
  }

  public has(key: string): boolean {
    return this.set.has(key);
  }

  public push(key: string): void {
    if (!this.set.has(key)) {
      this.queue.push(key);
      this.set.add(key);
    }
  }

  public shift(): string | undefined {
    const key = this.queue.shift();
    if (key !== undefined) {
      this.set.delete(key);
    }
    return key;
  }
}
