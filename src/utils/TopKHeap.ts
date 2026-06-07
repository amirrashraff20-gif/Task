export class TopKHeap<T> {
  private heap: T[] = [];
  constructor(private readonly k: number, private readonly compare: (a: T, b: T) => number) {}

  public push(val: T): void {
    if (this.heap.length < this.k) {
      this.heap.push(val);
      this.bubbleUp(this.heap.length - 1);
    } else if (this.compare(val, this.heap[0]!) > 0) {
      this.heap[0] = val;
      this.bubbleDown(0);
    }
  }

  public getElements(): T[] {
    return this.heap.slice().sort((a, b) => this.compare(b, a));
  }

  private bubbleUp(idx: number): void {
    while (idx > 0) {
      const parentIdx = Math.floor((idx - 1) / 2);
      if (this.compare(this.heap[idx]!, this.heap[parentIdx]!) >= 0) break;
      [this.heap[idx], this.heap[parentIdx]] = [this.heap[parentIdx]!, this.heap[idx]!];
      idx = parentIdx;
    }
  }

  private bubbleDown(idx: number): void {
    const len = this.heap.length;
    while (true) {
      const left = 2 * idx + 1;
      const right = 2 * idx + 2;
      let smallest = idx;

      if (left < len && this.compare(this.heap[left]!, this.heap[smallest]!) < 0) {
        smallest = left;
      }
      if (right < len && this.compare(this.heap[right]!, this.heap[smallest]!) < 0) {
        smallest = right;
      }
      if (smallest === idx) break;
      
      [this.heap[idx], this.heap[smallest]] = [this.heap[smallest]!, this.heap[idx]!];
      idx = smallest;
    }
  }
}
