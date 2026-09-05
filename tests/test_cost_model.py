"""Deterministic unit tests for Milestone 5: Workload Sizing & Parameterized Cost Model."""

import math
import os
import pytest

from src.cost.cost_model import (
    CostModelResult,
    WorkloadSizing,
    build_cost_model,
    calculate_burst_capacity,
    calculate_data_volumes,
    calculate_scenario,
    calculate_workload_sizing,
    load_assumptions,
)
from src.cost.main import export_csv, export_markdown


@pytest.fixture
def sample_config():
    return load_assumptions("config/cost_assumptions.yaml")


@pytest.fixture
def model_result(sample_config):
    return build_cost_model("config/cost_assumptions.yaml")


def test_workload_sizing_annual_volume(sample_config):
    """Verify that annual event volume strictly matches (304 * 18M) + (61 * 140M) = 14.012B."""
    workload = calculate_workload_sizing(sample_config)
    expected_annual = (304 * 18_000_000) + (61 * 140_000_000)
    assert expected_annual == 14_012_000_000
    assert workload.peak_adjusted_annual_events == 14_012_000_000
    assert workload.baseline_annual_events == 18_000_000 * 365
    assert workload.total_days == 365


def test_throughput_rates(sample_config):
    """Verify baseline (208.33 eps), peak (1620.37 eps), and 40x burst (8333.33 eps)."""
    workload = calculate_workload_sizing(sample_config)
    assert math.isclose(workload.baseline_eps, 208.33333333333334, rel_tol=1e-5)
    assert math.isclose(workload.peak_eps, 1620.3703703703704, rel_tol=1e-5)
    assert math.isclose(workload.burst_eps, 8333.333333333334, rel_tol=1e-5)
    assert math.isclose(workload.annual_average_eps, 444.3176052765094, rel_tol=1e-5)


def test_event_size_data_volume_tiers(model_result):
    """Verify data volume sizing for Small (0.5KB), Reference (1.0KB), and Large (2.0KB)."""
    tiers = {t.name: t for t in model_result.event_size_tiers}
    assert "Small" in tiers
    assert "Reference" in tiers
    assert "Large" in tiers
    assert "Prototype Evidence (Measured Avg)" in tiers

    ref_tier = tiers["Reference"]
    assert ref_tier.size_kb == 1.0
    # 18M * 1.0 KB / 1M = 18.00 GB/day baseline
    assert ref_tier.baseline_gb_day == 18.00
    # 140M * 1.0 KB / 1M = 140.00 GB/day peak
    assert ref_tier.peak_gb_day == 140.00
    # 14.012B * 1.0 KB / 1B = 14.01 TB/year
    assert ref_tier.annual_raw_tb == 14.01


def test_burst_worker_sizing_formula(sample_config):
    """Verify ceil(8333.33 / assumed_eps_per_worker) and incremental worker math."""
    workload = calculate_workload_sizing(sample_config)
    burst_cfg = {
        "assumed_eps_per_worker": 400.0,
        "burst_duration_hours": 2.0,
        "bursts_per_month": 4,
        "broker_burst_bandwidth_per_burst": 40.0,
    }
    calc = calculate_burst_capacity(
        workload=workload,
        burst_cfg=burst_cfg,
        worker_rate=0.24,
        baseline_workers=4,
    )

    # ceil(8333.33 / 400) = 21
    assert calc.required_burst_workers == 21
    # 21 - 4 = 17 incremental
    assert calc.incremental_burst_workers == 17
    # 17 * 0.24 * 2.0 = 8.16 compute cost
    assert math.isclose(calc.worker_compute_per_burst, 8.16, rel_tol=1e-4)
    # 8.16 + 40.00 = 48.16 per burst
    assert math.isclose(calc.cost_per_burst, 48.16, rel_tol=1e-4)
    # 48.16 * 4 * 12 = 2311.68
    assert math.isclose(calc.annual_burst_cost, 2311.68, rel_tol=1e-4)


def test_query_compute_exact_arithmetic(model_result):
    """Verify exact formula values for Query Compute: Lean=$7,290, Ref=$22,320, High=$70,080."""
    lean = model_result.scenarios["Lean"]
    ref = model_result.scenarios["Reference"]
    high = model_result.scenarios["High"]

    assert lean.query_compute_annual == 7290.00
    assert ref.query_compute_annual == 22320.00
    assert high.query_compute_annual == 70080.00


def test_stream_processing_exact_arithmetic(model_result):
    """Verify High stream processing = $64,688.64/year."""
    high = model_result.scenarios["High"]
    # (6 * 0.48 * 7296) + (28 * 0.48 * 1464) + 24000 = 20988.48 + 19700.16 + 24000 = 64688.64
    assert high.stream_processing_annual == 64688.64


def test_cost_per_1m_events(model_result):
    """Verify cost per 1M events formula: (annual_cost / 14.012B) * 1M."""
    ref = model_result.reference_scenario
    expected_cost_per_1m = (ref.total_annual_cost / 14_012_000_000) * 1_000_000
    assert math.isclose(ref.cost_per_1m_events, round(expected_cost_per_1m, 2), abs_tol=0.01)
    assert ref.cost_per_1m_events == 13.16


def test_budget_headroom_against_480k(model_result):
    """Verify budget headroom and consumption percentages."""
    ref = model_result.reference_scenario
    assert model_result.budget_cap == 480000.0
    assert math.isclose(ref.budget_headroom, 480000.0 - ref.total_annual_cost, abs_tol=0.01)
    assert ref.budget_percent_consumed < 100.0

    high = model_result.scenarios["High"]
    assert high.total_annual_cost < 480000.0  # High scenario is $470,769.51, within 98.1% of budget
    assert math.isclose(high.budget_headroom, 480000.0 - high.total_annual_cost, abs_tol=0.01)
    assert high.budget_percent_consumed == 98.1


def test_scenario_monotonicity(model_result):
    """Verify that Lean < Reference < High across all aggregated totals."""
    lean = model_result.scenarios["Lean"]
    ref = model_result.scenarios["Reference"]
    high = model_result.scenarios["High"]

    assert lean.subtotal_annual < ref.subtotal_annual < high.subtotal_annual
    assert lean.total_annual_cost < ref.total_annual_cost < high.total_annual_cost
    assert lean.cost_per_1m_events < ref.cost_per_1m_events < high.cost_per_1m_events


def test_dynamic_top_cost_risks(model_result):
    """Verify dynamic cost driver ranking: Kafka is #1, Stream Proc is #2 for Ref."""
    ref = model_result.reference_scenario
    top_driver_1, amt_1, pct_1 = ref.top_cost_drivers[0]
    top_driver_2, amt_2, pct_2 = ref.top_cost_drivers[1]

    assert top_driver_1 == "Streaming / Kafka"
    assert amt_1 == 52000.00
    assert top_driver_2 == "Stream Processing"
    assert amt_2 == 23631.36

    high = model_result.scenarios["High"]
    high_driver_1 = high.top_cost_drivers[0][0]
    high_driver_2 = high.top_cost_drivers[1][0]
    assert high_driver_1 == "Streaming / Kafka"
    assert high_driver_2 == "Monitoring / Logging"


def test_export_artifacts(model_result, tmp_path):
    """Verify CSV and Markdown export functions run cleanly."""
    csv_file = os.path.join(tmp_path, "test_cost_model.csv")
    md_file = os.path.join(tmp_path, "test_cost_model.md")

    export_csv(model_result, csv_file)
    export_markdown(model_result, md_file)

    assert os.path.exists(csv_file)
    assert os.path.exists(md_file)

    with open(csv_file, "r") as f:
        content = f.read()
        assert "Category / Metric" in content
        assert "Streaming / Kafka" in content
        assert "Total Annual Cost" in content

    with open(md_file, "r") as f:
        md_content = f.read()
        assert "Milestone 5 Cost Model & Workload Sizing Report" in md_content
        assert "Executive Summary" in md_content
        assert "14,012,000,000" in md_content
