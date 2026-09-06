"""Deterministic tests for load-test utility modules:
- Percentile calculation (p50, p95, p99)
- Rate multiplier and scheduling calculation
- Lag calculation logic
- Produced / consumed reconciliation
- Topic manager defaults
"""

import pytest

from unittest.mock import MagicMock
from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition

from src.loadtest.benchmark_runner import calculate_percentile, reconcile_sequences
from src.loadtest.lag_monitor import LagMonitor
from src.loadtest.topic_manager import DEFAULT_LOADTEST_TOPIC, DEFAULT_PARTITIONS, TopicManager


def test_percentile_calculation():
    """Verify linear interpolation percentile calculation across edge cases."""
    # Simple 1..100 array
    data = [float(i) for i in range(1, 101)]
    assert calculate_percentile(data, 50) == 50.5
    assert calculate_percentile(data, 95) == 95.05
    assert calculate_percentile(data, 99) == 99.01

    # Empty list edge case
    assert calculate_percentile([], 50) == 0.0

    # Single element edge case
    assert calculate_percentile([42.0], 99) == 42.0

    # Identical values
    assert calculate_percentile([10.0, 10.0, 10.0], 95) == 10.0


def test_rate_multiplier_and_schedule():
    """Verify 40x burst ratio and rate slicing calculations."""
    baseline_eps = 50.0
    burst_eps = 2000.0

    # Ratio requirement
    ratio = burst_eps / baseline_eps
    assert ratio == 40.0, "Burst must be precisely 40x of baseline"

    # Scheduling simulation: 50ms slices
    slice_sec = 0.05
    target_cumulative_1s = int((1.0 + slice_sec) * baseline_eps)
    assert 50 <= target_cumulative_1s <= 55

    target_cumulative_burst_1s = int((1.0 + slice_sec) * burst_eps)
    assert 2000 <= target_cumulative_burst_1s <= 2150


def test_lag_calculation_logic():
    """Verify distinct calculation of true processing backlog vs commit lag."""
    high_watermarks = {0: 1000, 1: 1500, 2: 2000, 3: 500, 4: 800, 5: 1200}
    consumer_positions = {0: 990, 1: 1480, 2: 2000, 3: 495, 4: 780, 5: 1190}
    committed_offsets = {0: 950, 1: 1400, 2: 2000, 3: 400, 4: -1001, 5: 1100}

    # 1. Processing backlog = broker log end - consumer current position
    # (True live unconsumed queue depth)
    processing_backlog = sum(
        max(0, high_watermarks[p] - consumer_positions.get(p, 0))
        for p in high_watermarks
    )
    # (10 + 20 + 0 + 5 + 20 + 10 = 65)
    assert processing_backlog == 65

    # 2. Commit lag = broker log end - committed offset
    # (Reflects asynchronous batch commit persistence interval)
    commit_lag = sum(
        max(0, high_watermarks[p] - (committed_offsets.get(p, 0) if committed_offsets.get(p, 0) >= 0 else 0))
        for p in high_watermarks
    )
    # (50 + 100 + 0 + 100 + 800 + 100 = 1150)
    assert commit_lag == 1150


def test_reconciliation_logic():
    """Verify sequence-based message integrity reconciliation across edge cases."""
    # 1. Perfect delivery -> 0 missing, 0 duplicate, 0 unexpected
    produced = 1000
    consumed_perfect = list(range(1, 1001))
    res_perfect = reconcile_sequences(produced, consumed_perfect)
    assert res_perfect.missing_count == 0
    assert res_perfect.duplicate_count == 0
    assert res_perfect.unexpected_count == 0
    assert res_perfect.total_observed == 1000

    # 2. One missing message is detected
    consumed_with_drop = list(range(1, 500)) + list(range(501, 1001))  # 500 missing
    res_drop = reconcile_sequences(produced, consumed_with_drop)
    assert res_drop.missing_count == 1
    assert 500 in res_drop.missing_seqs_sample
    assert res_drop.duplicate_count == 0
    assert res_drop.total_observed == 999

    # 3. Duplicate delivery is detected even if total consumed count equals total produced
    # Case: seq 500 missing, but seq 42 delivered twice -> total count is 1000!
    # Naive produced - consumed would claim 0 missing; sequence reconciliation detects both!
    consumed_cancelling = list(range(1, 500)) + list(range(501, 1001)) + [42]
    assert len(consumed_cancelling) == produced  # 1000 consumed == 1000 produced
    res_cancelling = reconcile_sequences(produced, consumed_cancelling)
    assert res_cancelling.missing_count == 1, "Must detect the 1 missing sequence number"
    assert res_cancelling.missing_seqs_sample == [500]
    assert res_cancelling.duplicate_count == 1, "Must detect the 1 duplicate delivery"
    assert res_cancelling.unexpected_count == 0

    # 4. Multiple duplicates without drops
    consumed_dups = list(range(1, 1001)) + [10, 20, 20]
    res_dups = reconcile_sequences(produced, consumed_dups)
    assert res_dups.missing_count == 0
    assert res_dups.duplicate_count == 3
    assert res_dups.total_observed == 1003

    # 5. Unexpected / out-of-range sequence numbers
    consumed_unexpected = list(range(1, 1001)) + [9999]
    res_unexp = reconcile_sequences(produced, consumed_unexpected)
    assert res_unexp.missing_count == 0
    assert res_unexp.unexpected_count == 1
    assert res_unexp.unexpected_seqs_sample == [9999]


def test_lag_monitor_queries_real_group_and_handles_uncommitted():
    """Verify LagMonitor queries committed offsets for the real worker group and handles -1001 safely."""
    group_id = "real-benchmark-worker-group"
    topic = "fleet.telemetry.loadtest"
    num_partitions = 3

    # Mock Worker
    mock_worker = MagicMock()
    mock_worker.get_positions.return_value = {0: 100, 1: 190, 2: 300}

    # Mock Consumer for watermarks
    mock_consumer = MagicMock()
    # High watermarks for partitions 0, 1, 2
    mock_consumer.get_watermark_offsets.side_effect = lambda tp, timeout: (0, {0: 100, 1: 200, 2: 350}[tp.partition])

    # Mock AdminClient for committed offsets
    mock_admin_client = MagicMock()
    # Partition 0: 100 committed, Partition 1: 180 committed, Partition 2: -1001 (no commit yet)
    tp0 = TopicPartition(topic, 0, 100)
    tp1 = TopicPartition(topic, 1, 180)
    tp2 = TopicPartition(topic, 2, -1001)

    mock_cgtp = MagicMock()
    mock_cgtp.topic_partitions = [tp0, tp1, tp2]
    mock_future = MagicMock()
    mock_future.result.return_value = mock_cgtp
    mock_admin_client.list_consumer_group_offsets.return_value = {group_id: mock_future}

    monitor = LagMonitor(
        group_id=group_id,
        workers=[mock_worker],
        topic=topic,
        num_partitions=num_partitions,
        admin_client=mock_admin_client,
        consumer=mock_consumer,
    )

    snapshot = monitor.sample_current_lag()

    # 1. Verify AdminClient was queried with the REAL group_id
    mock_admin_client.list_consumer_group_offsets.assert_called_once()
    req_list = mock_admin_client.list_consumer_group_offsets.call_args[0][0]
    assert req_list[0].group_id == group_id

    # 2. Broker log ends: 100 + 200 + 350 = 650
    assert snapshot.broker_log_end == 650

    # 3. Consumer positions: 100 + 190 + 300 = 590
    assert snapshot.consumer_position == 590

    # 4. Processing backlog: (100-100) + (200-190) + (350-300) = 0 + 10 + 50 = 60
    assert snapshot.processing_backlog == 60

    # 5. Committed offsets: tp0=100, tp1=180, tp2=-1001 -> safely treated as 0! Total = 280
    assert snapshot.committed_offset == 280

    # 6. Commit lag: 650 - 280 = 370
    assert snapshot.commit_lag == 370


def test_topic_manager_defaults():
    """Verify topic manager defaults match requirements (dedicated topic & 6 partitions)."""
    tm = TopicManager()
    assert tm.topic_name == DEFAULT_LOADTEST_TOPIC
    assert tm.topic_name == "fleet.telemetry.loadtest"
    assert tm.num_partitions == DEFAULT_PARTITIONS
    assert tm.num_partitions == 6
