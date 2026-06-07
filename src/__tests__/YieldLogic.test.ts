import { EventProcessingEngine } from '../core/EventProcessingEngine';

describe('Yield Logic Bug', () => {
  it('should completely process all events when yieldThreshold is 1', async () => {
    let processed = 0;
    const engine = new EventProcessingEngine(
      {
        maxConcurrency: 1,
        maxCapacity: 20000,
        maxWaiters: 1000,
        maxRetries: 3,
        backoffMs: 10,
        yieldThreshold: 1,
      },
      async () => {
        processed++;
      }
    );

    const promises: Promise<void>[] = [];
    for (let i = 0; i < 10000; i++) {
      promises.push(engine.submit({
        id: `evt-${i}`,
        partitionKey: 'partition-1',
        correlationId: 'corr-1',
        payload: { index: i }
      }));
    }

    await Promise.all(promises);

    // Wait for engine to finish processing by triggering a shutdown which will wait for drain
    const finalReport = await engine.shutdown();
    
    const metrics = engine.metrics();
    expect(metrics.processed).toBe(10000);
    expect(metrics.queueDepth).toBe(0);
    expect(finalReport.processed).toBe(10000);
  }, 120000); // 120s timeout because setTimeout(0) is slow
});
