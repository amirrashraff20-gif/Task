import { EventProcessingEngine } from '../core/EventProcessingEngine';
import type { EngineConfig } from '../types/public/EngineConfig';
import type { Event } from '../types/public/Event';

describe('Phase 5 Observability & Operations', () => {
  let config: EngineConfig;

  beforeEach(() => {
    config = {
      maxConcurrency: 2,
      maxCapacity: 10,
      maxWaiters: 10,
      maxRetries: 2,
      backoffMs: 10,
    };
  });

  it('preserves correlation ID', async () => {
    let capturedEvent: Event | null = null;
    config = { ...config, onEventStarted: (e) => { capturedEvent = e; } };
    const engine = new EventProcessingEngine(config, async () => {});

    await engine.submit({ id: '1', partitionKey: 'p1', payload: 'data', correlationId: 'my-corr-1' });
    
    // allow microtasks to flush
    await new Promise(r => setImmediate(r));
    
    expect(capturedEvent).toBeDefined();
    expect(capturedEvent!.correlationId).toBe('my-corr-1');
  });

  it('safe execution of callbacks and logger', async () => {
    let logCalled = false;
    const logger = {
      debug: () => { logCalled = true; throw new Error("Logger crash"); },
      info: () => {},
      warn: () => {},
      error: () => {}
    };

    let callbackCalled = false;
    config = { 
      ...config, 
      logger, 
      onEventSucceeded: () => { callbackCalled = true; throw new Error("Callback crash"); }
    };
    
    const engine = new EventProcessingEngine(config, async () => {});
    await engine.submit({ id: '1', partitionKey: 'p1', payload: 'data', correlationId: 'corr-1' });
    await new Promise(r => setImmediate(r));

    // The engine should not have crashed
    expect(logCalled).toBe(true);
    expect(callbackCalled).toBe(true);
    
    const m = engine.metrics();
    expect(m.processed).toBe(1);
    expect(m.failed).toBe(0);
  });

  it('tracks health snapshots correctly', async () => {
    const engine = new EventProcessingEngine(config, async () => { throw new Error('fail'); });
    for (let i = 0; i < 5; i++) {
       await engine.submit({ id: `e${i}`, partitionKey: 'p1', payload: 'data', correlationId: `corr-${i}` });
    }
    
    await new Promise(r => setTimeout(r, 100));

    const health = engine.getHealthSnapshot();
    // It should be overloaded because there are 100% failures
    expect(health.status).toBe('overloaded');
    expect(health.throughput).toBeGreaterThanOrEqual(0);
  });

  it('gets partition metrics and top partitions', async () => {
    let resolveHandler: () => void;
    const p = new Promise<void>(r => { resolveHandler = r; });
    const engine = new EventProcessingEngine(config, async () => { await p; });
    await engine.submit({ id: '1', partitionKey: 'p1', payload: 'data', correlationId: 'corr-1' });
    await engine.submit({ id: '2', partitionKey: 'p2', payload: 'data', correlationId: 'corr-2' });
    await engine.submit({ id: '3', partitionKey: 'p2', payload: 'data', correlationId: 'corr-3' });
    
    await new Promise(r => setImmediate(r));

    const top = engine.getTopPartitions(2);
    expect(top.length).toBe(2);
    
    const p2Metrics = engine.getPartitionMetrics('p2');
    expect(p2Metrics).toBeDefined();
    expect(p2Metrics!.queueDepth).toBe(2);
    
    resolveHandler!();
  });

  it('provides comprehensive shutdown report', async () => {
    const engine = new EventProcessingEngine(config, async () => {});
    await engine.submit({ id: '1', partitionKey: 'p1', payload: 'data', correlationId: 'corr-1' });
    await new Promise(r => setImmediate(r));

    const report = await engine.shutdown();
    expect(report.shutdownStartedAt).toBeDefined();
    expect(report.shutdownCompletedAt).toBeDefined();
    expect(report.shutdownDurationMs).toBeDefined();
    expect(report.finalMetricsSnapshot).toBeDefined();
    expect(report.processed).toBe(1);
  });
});
