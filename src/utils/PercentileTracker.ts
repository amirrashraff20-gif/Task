export class PercentileTracker {
  private readonly buffer: number[];
  private index: number = 0;
  private count: number = 0;
  
  private cachedMetrics: { avg: number; p50: number; p95: number; p99: number } | null = null;

  constructor(private readonly capacity: number = 1024) {
    this.buffer = new Array(capacity);
  }

  public add(value: number): void {
    this.cachedMetrics = null;
    if (this.count < this.capacity) {
      this.buffer[this.index] = value;
      this.count++;
    } else {
      this.buffer[this.index] = value;
    }
    this.index = (this.index + 1) % this.capacity;
  }

  public getMetrics() {
    if (this.cachedMetrics) return this.cachedMetrics;
    if (this.count === 0) return { avg: 0, p50: 0, p95: 0, p99: 0 };
    const snapshot = this.buffer.slice(0, this.count).sort((a, b) => a - b);
    
    let exactSum = 0;
    for (let i = 0; i < this.count; i++) {
      exactSum += snapshot[i]!;
    }

    const getP = (p: number) => {
      const rank = (p / 100) * (this.count - 1);
      const lower = Math.floor(rank);
      const upper = Math.ceil(rank);
      if (lower === upper) return snapshot[lower]!;
      const weight = rank - lower;
      return snapshot[lower]! * (1 - weight) + snapshot[upper]! * weight;
    };
    this.cachedMetrics = {
      avg: exactSum / this.count,
      p50: getP(50),
      p95: getP(95),
      p99: getP(99)
    };
    
    return this.cachedMetrics;
  }
}
