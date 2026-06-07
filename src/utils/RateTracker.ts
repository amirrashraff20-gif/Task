export class RateTracker {
  private readonly buckets: number[];
  private lastTick: number;

  constructor(private readonly windowSizeSeconds: number = 5) {
    if (windowSizeSeconds <= 0) throw new Error('windowSizeSeconds must be > 0');
    this.buckets = new Array(windowSizeSeconds).fill(0);
    this.lastTick = Math.floor(performance.now() / 1000);
  }

  private advance(): void {
    const currentTick = Math.floor(performance.now() / 1000);
    if (currentTick !== this.lastTick) {
      const diff = currentTick - this.lastTick;
      if (diff >= this.windowSizeSeconds) {
        this.buckets.fill(0);
      } else {
        for (let i = 1; i <= diff; i++) {
          this.buckets[(this.lastTick + i) % this.windowSizeSeconds] = 0;
        }
      }
      this.lastTick = currentTick;
    }
  }

  public increment(count: number = 1): void {
    this.advance();
    const idx = this.lastTick % this.windowSizeSeconds;
    this.buckets[idx] = (this.buckets[idx] ?? 0) + count;
  }

  public get ratePerSecond(): number {
    this.advance();
    let sum = 0;
    for (let i = 0; i < this.windowSizeSeconds; i++) {
      sum += this.buckets[i]!;
    }
    return sum / this.windowSizeSeconds;
  }
}
