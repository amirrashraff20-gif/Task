import { EventProcessingEngine } from '../core/EventProcessingEngine';
import type { EngineConfig } from '../types/public/EngineConfig';

describe('Phase C: Stress Validation', () => {
  let config: EngineConfig;

  beforeEach(() => {
    config = {
      maxConcurrency: 10,
      maxCapacity: 15000,
      maxWaiters: 5000,
      maxRetries: 3,
      backoffMs: 1,
    };
  });

  it('Scenario 1: High throughput (10000 events, 100 partitions)', async () => {
    const engine = new EventProcessingEngine(config, async () => {
      // simulate microscopic work
      await new Promise(r => setImmediate(r));
    });

    const start = Date.now();
    const promises: Promise<void>[] = [];
    const numPartitions = 100;

    for (let i = 0; i < 10000; i++) {
      promises.push(
        engine.submit({
          id: `e-${i}`,
          partitionKey: `P${(i % numPartitions) + 1}`,
          correlationId: `corr-${i}`,
          payload: null
        })
      );
    }

    await Promise.all(promises);
    await engine.shutdown();

    const duration = Date.now() - start;
    const m = engine.metrics();

    expect(m.processed).toBe(10000);
    expect(m.failed).toBe(0);
    expect(duration).toBeGreaterThan(0);
  });

  it('Scenario 2: Heavy Backpressure (Small queue, producer flood)', async () => {
    config = { ...config, maxCapacity: 100, maxWaiters: 50 };
    let activeHandlers = 0;
    
    const engine = new EventProcessingEngine(config, async () => {
      activeHandlers++;
      expect(activeHandlers).toBeLessThanOrEqual(config.maxConcurrency);
      await new Promise(r => setTimeout(r, 5));
      activeHandlers--;
    });

    let rejectedCount = 0;
    let acceptedCount = 0;

    const promises: Promise<void>[] = [];
    for (let i = 0; i < 500; i++) {
      promises.push(
        engine.submit({
          id: `e-${i}`,
          partitionKey: 'P1',
          correlationId: `corr-${i}`,
          payload: null
        }).then(() => {
          acceptedCount++;
        }).catch(() => {
          rejectedCount++;
        })
      );
    }

    await Promise.all(promises);
    await engine.shutdown();

    expect(acceptedCount + rejectedCount).toBe(500);
    expect(rejectedCount).toBeGreaterThan(0); // Should have rejected excess events
  });

  it('Scenario 3: Retry Storm', async () => {
    const engine = new EventProcessingEngine(config, async (e) => {
      if (e.partitionKey.startsWith('fail')) {
        throw new Error('Poison');
      }
    });

    const promises: Promise<void>[] = [];
    for (let i = 0; i < 100; i++) {
      promises.push(
        engine.submit({
          id: `ok-${i}`,
          partitionKey: 'P1',
          correlationId: `ok-corr-${i}`,
          payload: null
        })
      );
      promises.push(
        engine.submit({
          id: `fail-${i}`,
          partitionKey: `fail-part-${i}`,
          correlationId: `fail-corr-${i}`,
          payload: null
        })
      );
    }

    await Promise.all(promises.map(p => p.catch(() => {})));
    
    // Wait for retries to complete before shutting down
    while (engine.metrics().deadLettered < 100) {
      await new Promise(r => setTimeout(r, 10));
    }

    await engine.shutdown();

    const m = engine.metrics();
    expect(m.processed).toBe(100);
    // Each failing event retries 3 times, plus the initial attempt = 4 attempts total.
    // So 100 events * 4 = 400 failed executions recorded in metrics.
    expect(m.failed).toBe(400);
    expect(m.deadLettered).toBe(100);
  });
});
