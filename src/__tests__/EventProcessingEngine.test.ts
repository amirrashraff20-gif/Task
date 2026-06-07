import { EventProcessingEngine } from '../core/EventProcessingEngine';
import type { Event } from '../types/public/Event';
import type { EngineConfig } from '../types/public/EngineConfig';
import { WaitQueueFullException } from '../errors/WaitQueueFullException';
import { EngineShutdownException } from '../errors/EngineShutdownException';

describe('EventProcessingEngine Phase 4 Tests', () => {
  const createEvent = (id: string, partitionKey: string, payload?: any): Event => ({
    id,
    partitionKey,
    correlationId: `corr-${id}`,
    payload: payload || {}
  });

  const baseConfig: EngineConfig = {
    maxConcurrency: 2,
    maxCapacity: 1000,
    maxWaiters: 1000,
    maxRetries: 2,
    backoffMs: 10
  };

  test('1. Concurrency Cap: Peak concurrency never exceeds maxConcurrency', async () => {
    let currentConcurrency = 0;
    let peakConcurrency = 0;
    const engine = new EventProcessingEngine({ ...baseConfig, maxConcurrency: 2 }, async () => {
      currentConcurrency++;
      if (currentConcurrency > peakConcurrency) peakConcurrency = currentConcurrency;
      await new Promise(r => setTimeout(r, 20));
      currentConcurrency--;
    });

    engine.submit(createEvent('1', 'P1'));
    engine.submit(createEvent('2', 'P2'));
    engine.submit(createEvent('3', 'P3'));
    engine.submit(createEvent('4', 'P4'));

    await engine.shutdown();

    expect(peakConcurrency).toBeLessThanOrEqual(2);
  });

  test('2. FIFO Ordering: Per-key completion order equals submission order', async () => {
    const executed: string[] = [];
    const engine = new EventProcessingEngine(baseConfig, async (event) => {
      executed.push(event.id);
      await new Promise(r => setTimeout(r, 5)); 
    });

    engine.submit(createEvent('1', 'P1'));
    engine.submit(createEvent('2', 'P1'));
    engine.submit(createEvent('3', 'P1'));

    await engine.shutdown();

    expect(executed).toEqual(['1', '2', '3']);
  });

  test('3. Backpressure: Queue remains bounded, submit blocks/rejects correctly', async () => {
    const engine = new EventProcessingEngine({
      ...baseConfig,
      maxConcurrency: 1,
      maxCapacity: 1,
      maxWaiters: 1
    }, async () => {
      await new Promise(r => setTimeout(r, 50));
    });

    engine.submit(createEvent('1', 'P1')).catch(() => {});
    let p2Resolved = false;
    engine.submit(createEvent('2', 'P1')).then(() => { p2Resolved = true; }).catch(() => {});

    expect(p2Resolved).toBe(false);

    await expect(engine.submit(createEvent('3', 'P1'))).rejects.toThrow(WaitQueueFullException);

    await new Promise(r => setTimeout(r, 100)); // Let Event 1 finish and admit Event 2
    await engine.shutdown();
    expect(p2Resolved).toBe(true);
  });

  test('4. Graceful Shutdown: Accepted events drain correctly, new submits reject', async () => {
    let handlerInvoked = 0;
    const engine = new EventProcessingEngine(baseConfig, async () => {
      handlerInvoked++;
      await new Promise(r => setTimeout(r, 20));
    });

    engine.submit(createEvent('1', 'P1'));
    engine.submit(createEvent('2', 'P1'));
    
    const shutdownPromise = engine.shutdown();
    
    await expect(engine.submit(createEvent('3', 'P1'))).rejects.toThrow(EngineShutdownException);
    
    const report = await shutdownPromise;

    expect(handlerInvoked).toBe(2);
    expect(report.processed).toBe(2);
    expect(report.unfinishedQueuedEvents.length).toBe(0);
    expect(report.inFlightEvents.length).toBe(0);
  });

  test('5. Shutdown Timeout: Returns unfinished vs in-flight perfectly, no silent loss', async () => {
    let manualResolve!: () => void;
    const engine = new EventProcessingEngine(baseConfig, async () => {
      await new Promise<void>(r => { manualResolve = r; }); 
    });

    engine.submit(createEvent('1', 'P1'));
    engine.submit(createEvent('2', 'P1')); // Queued behind 1
    engine.submit(createEvent('3', 'P2')); 

    // Wait slightly to ensure scheduling loop has grabbed P1 and P2
    await new Promise(r => setTimeout(r, 10));

    // Shutdown with timeout 0 -> forces immediate snapshot
    const report = await engine.shutdown({ timeoutMs: 0 }); 

    expect(report.timedOut).toBe(true);
    // 1 and 3 are in-flight (maxConcurrency = 2). 2 is queued.
    expect(report.inFlightAtShutdown).toBe(2);
    expect(report.inFlightEvents.map(e => e.id).sort()).toEqual(['1', '3']);
    
    expect(report.queuedAtShutdown).toBe(1);
    expect(report.unfinishedQueuedEvents.map(e => e.id)).toEqual(['2']);

    // Now let handlers finish to prove they don't break logic or ghost
    manualResolve!();
  });

  test('6. Poison Event Isolation: Retry works, onDeadLetter works', async () => {
    const executed: string[] = [];
    let throwCount = 0;
    const dlqEvents: Event[] = [];

    const engine = new EventProcessingEngine({ 
      ...baseConfig, 
      maxRetries: 2, 
      backoffMs: 10,
      onDeadLetter: (e) => dlqEvents.push(e)
    }, async (event) => {
      executed.push(event.id);
      if (event.id === 'POISON') {
        throwCount++;
        throw new Error('Poison');
      }
    });

    engine.submit(createEvent('POISON', 'P1'));
    engine.submit(createEvent('SAFE', 'P1'));

    await new Promise(r => setTimeout(r, 50)); // Wait for retries to happen before shutting down
    const report = await engine.shutdown();
    
    expect(throwCount).toBe(3); 
    expect(executed).toEqual(['POISON', 'POISON', 'POISON', 'SAFE']); 
    expect(report.deadLettered).toBe(1);
    expect(report.processed).toBe(1);
    expect(dlqEvents.length).toBe(1);
    expect(dlqEvents[0]?.id).toBe('POISON');
  });

  test('7. Slot Leak Protection: Concurrency slots always return', async () => {
    const executed: string[] = [];
    const engine = new EventProcessingEngine({ ...baseConfig, maxConcurrency: 1, maxRetries: 0 }, async (event) => {
      executed.push(event.id);
      if (event.id === 'FAIL') throw new Error('Fail');
    });

    engine.submit(createEvent('FAIL', 'P1')).catch(() => {});
    engine.submit(createEvent('SAFE', 'P2')); 

    await engine.shutdown();
    expect(executed).toEqual(['FAIL', 'SAFE']);
  });

  test('8. Fair Scheduling: Hot partition cannot starve cold partition', async () => {
    const executed: string[] = [];
    const engine = new EventProcessingEngine({ ...baseConfig, maxConcurrency: 1 }, async (event) => {
      executed.push(event.partitionKey);
    });

    for (let i = 0; i < 150; i++) {
      engine.submit(createEvent(`P1-${i}`, 'P1'));
    }
    
    engine.submit(createEvent('P2-1', 'P2'));

    await engine.shutdown();

    const indexOfP2 = executed.indexOf('P2');
    const indexOfP1Last = executed.lastIndexOf('P1');
    
    expect(indexOfP2).toBeGreaterThan(-1); 
    expect(indexOfP2).toBeLessThan(indexOfP1Last); 
  });

  test('9. Duplicate Partition Protection: ReadyQueue never schedules same partition twice', async () => {
    let concurrencyCount = 0;
    let maxObserved = 0;

    const engine = new EventProcessingEngine({ ...baseConfig, maxConcurrency: 2, maxCapacity: 50, maxWaiters: 50 }, async () => {
      concurrencyCount++;
      if (concurrencyCount > maxObserved) maxObserved = concurrencyCount;
      await new Promise(r => setTimeout(r, 10));
      concurrencyCount--;
    });

    for (let i = 0; i < 50; i++) {
      engine.submit(createEvent(`${i}`, 'P1')).catch(() => {});
    }

    await engine.shutdown();

    expect(maxObserved).toBe(1); 
  });

  test('10. Multi-partition retry isolation: P1 backoff does not block P2', async () => {
    const executed: string[] = [];
    const engine = new EventProcessingEngine({ ...baseConfig, maxConcurrency: 1, backoffMs: 50 }, async (event) => {
      executed.push(event.id);
      if (event.partitionKey === 'P1') {
        throw new Error('P1 Error');
      }
    });

    engine.submit(createEvent('1', 'P1')).catch(() => {});
    
    await new Promise(r => setTimeout(r, 10));

    engine.submit(createEvent('2', 'P2'));
    
    await new Promise(r => setTimeout(r, 10));

    expect(executed).toEqual(['1', '2']);
    
    await engine.shutdown();
  });

  test('11. Metrics Accuracy: Live queueDepth and inFlight track accurately', async () => {
    let resolveP1!: () => void;
    let resolveP2!: () => void;
    
    const engine = new EventProcessingEngine({ ...baseConfig, maxConcurrency: 2 }, async (e) => {
      if (e.id === '1') await new Promise<void>(r => resolveP1 = r);
      if (e.id === '2') await new Promise<void>(r => resolveP2 = r);
      if (e.id === '3') await new Promise<void>(r => setTimeout(r, 1000));
      if (e.id === '4') await new Promise<void>(r => setTimeout(r, 1000));
    });

    engine.submit(createEvent('1', 'P1')); // starts executing
    engine.submit(createEvent('2', 'P2')); // starts executing
    engine.submit(createEvent('3', 'P1')); // queued
    engine.submit(createEvent('4', 'P2')); // queued

    await new Promise(r => setTimeout(r, 10));

    let m = engine.metrics();
    expect(m.inFlight).toBe(2);
    expect(m.queueDepth).toBe(2);

    resolveP1!();
    await new Promise(r => setTimeout(r, 10));
    
    m = engine.metrics();
    expect(m.processed).toBe(1);
    // P1 now processed 1. Event 3 starts executing.
    // So inFlight should still be 2 (event 2 and 3). queueDepth should be 1.
    expect(m.inFlight).toBe(2);
    expect(m.queueDepth).toBe(1);

    resolveP2!(); // resolves 2. event 4 starts.
    await new Promise(r => setTimeout(r, 10));

    m = engine.metrics();
    expect(m.processed).toBe(2);
    expect(m.inFlight).toBe(2); // 3 and 4
    expect(m.queueDepth).toBe(0);

    engine.shutdown(); // cleanly shutdown to allow remaining to finish without hanging test
  });

  test('12. R6 Runtime Reconfiguration: Changing maxConcurrency and Telemetry works', async () => {
    const metricsReports: any[] = [];
    let resolveP1!: () => void;
    let resolveP2!: () => void;
    let resolveP3!: () => void;

    const engine = new EventProcessingEngine({
      ...baseConfig,
      maxConcurrency: 1,
      metricsIntervalMs: 10,
      onMetricsReport: (m) => metricsReports.push(m)
    }, async (e) => {
      if (e.id === '1') await new Promise<void>(r => resolveP1 = r);
      if (e.id === '2') await new Promise<void>(r => resolveP2 = r);
      if (e.id === '3') await new Promise<void>(r => resolveP3 = r);
    });

    // Start with maxConcurrency 1
    engine.submit(createEvent('1', 'P1')); 
    engine.submit(createEvent('2', 'P2')); 
    engine.submit(createEvent('3', 'P3')); 

    await new Promise(r => setTimeout(r, 20));

    let m = engine.metrics();
    expect(m.inFlight).toBe(1); // Only P1 is processing

    // Dynamically increase concurrency
    engine.updateConfig({ maxConcurrency: 3 });
    
    await new Promise(r => setTimeout(r, 20));
    
    m = engine.metrics();
    expect(m.inFlight).toBe(3); // P2 and P3 should immediately start

    expect(metricsReports.length).toBeGreaterThan(0); // Telemetry fired
    
    resolveP1!();
    resolveP2!();
    resolveP3!();

    engine.updateConfig({ metricsIntervalMs: 0 }); // disable telemetry

    await engine.shutdown();
  });
});
