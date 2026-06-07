import type { InternalEvent } from './InternalEvent';
import { PartitionStatus } from './PartitionStatus';
import type { WritablePartitionMetrics } from './WritablePartitionMetrics';
import type { LinkedList } from '../../utils/LinkedList';

export interface PartitionState {
  queue: LinkedList<InternalEvent>;
  status: PartitionStatus;
  backoffTimer: ReturnType<typeof setTimeout> | null;
  metrics: WritablePartitionMetrics;
}
