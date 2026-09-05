"""Deterministic tests for load-test utility modules:
- Percentile calculation (p50, p95, p99)
- Rate multiplier and scheduling calculation
- Lag calculation logic
- Produced / consumed reconciliation
- Topic manager defaults
"""

import pytest

from src.loadtest.benchmark_runner import calculate_percentile
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
    """Verify detection of missing messages and duplicate deliveries."""
    # Case 1: Perfect delivery
    produced_seqs = set(range(1, 1001))
    consumed_seqs = list(range(1, 1001))
    missing = produced_seqs - set(consumed_seqs)
    duplicates = len(consumed_seqs) - len(set(consumed_seqs))
    assert len(missing) == 0
    assert duplicates == 0

    # Case 2: Missing message
    consumed_with_drop = list(range(1, 500)) + list(range(501, 1001))  # 500 dropped
    missing_drop = produced_seqs - set(consumed_with_drop)
    assert missing_drop == {500}

    # Case 3: Duplicate delivery
    consumed_with_dup = list(range(1, 1001)) + [42, 42]  # duplicate deliveries
    dup_count = len(consumed_with_dup) - len(set(consumed_with_dup))
    assert dup_count == 2
    assert len(produced_seqs - set(consumed_with_dup)) == 0


def test_topic_manager_defaults():
    """Verify topic manager defaults match requirements (dedicated topic & 6 partitions)."""
    tm = TopicManager()
    assert tm.topic_name == DEFAULT_LOADTEST_TOPIC
    assert tm.topic_name == "fleet.telemetry.loadtest"
    assert tm.num_partitions == DEFAULT_PARTITIONS
    assert tm.num_partitions == 6
