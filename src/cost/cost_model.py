"""WBL Telemetry Platform: Parameterized Cost Model and Workload Sizing Engine.

All calculations are derived programmatically from configuration inputs without hardcoded totals.
"""

from dataclasses import dataclass, field
import math
import os
from typing import Dict, List, Tuple
import yaml


@dataclass
class WorkloadSizing:
    baseline_events_per_day: int
    baseline_days: int
    peak_season_events_per_day: int
    peak_season_days: int
    instantaneous_burst_multiplier: float

    @property
    def total_days(self) -> int:
        return self.baseline_days + self.peak_season_days

    @property
    def baseline_annual_events(self) -> int:
        """Annual events if 365 days were at baseline rate."""
        return self.baseline_events_per_day * self.total_days

    @property
    def peak_adjusted_annual_events(self) -> int:
        """Annual events adjusted for 61 days of peak season (Nov-Dec)."""
        return (self.baseline_days * self.baseline_events_per_day) + (
            self.peak_season_days * self.peak_season_events_per_day
        )

    @property
    def baseline_eps(self) -> float:
        """Sustained baseline throughput in events per second."""
        return self.baseline_events_per_day / 86400.0

    @property
    def peak_eps(self) -> float:
        """Sustained peak season throughput in events per second."""
        return self.peak_season_events_per_day / 86400.0

    @property
    def burst_eps(self) -> float:
        """Instantaneous 40x burst throughput relative to baseline rate."""
        return self.baseline_eps * self.instantaneous_burst_multiplier

    @property
    def annual_average_eps(self) -> float:
        """Average throughput across the entire 365-day year."""
        return self.peak_adjusted_annual_events / (self.total_days * 86400.0)


@dataclass
class EventSizeTier:
    name: str
    size_kb: float
    baseline_gb_day: float
    peak_gb_day: float
    annual_raw_tb: float
    is_prototype_evidence: bool = False


@dataclass
class BurstCalculation:
    assumed_eps_per_worker: float
    required_burst_workers: int
    baseline_workers: int
    incremental_burst_workers: int
    worker_hourly_rate: float
    burst_duration_hours: float
    worker_compute_per_burst: float
    broker_burst_bandwidth: float
    cost_per_burst: float
    bursts_per_month: int
    annual_burst_cost: float


@dataclass
class ScenarioBreakdown:
    scenario_name: str
    description: str
    # Line items (annual)
    kafka_annual: float
    stream_processing_annual: float
    storage_annual: float
    logging_annual: float
    network_annual: float
    query_compute_annual: float
    orchestration_annual: float
    catalog_annual: float
    backup_dr_annual: float
    burst_calc: BurstCalculation
    # Aggregates
    subtotal_annual: float
    contingency_percentage: float
    contingency_dollar: float
    total_annual_cost: float
    monthly_run_rate: float
    cost_per_1m_events: float
    budget_cap: float
    budget_headroom: float
    budget_percent_consumed: float
    top_cost_drivers: List[Tuple[str, float, float]] = field(default_factory=list)


@dataclass
class CostModelResult:
    budget_cap: float
    workload: WorkloadSizing
    event_size_tiers: List[EventSizeTier]
    scenarios: Dict[str, ScenarioBreakdown]

    @property
    def reference_scenario(self) -> ScenarioBreakdown:
        return self.scenarios["Reference"]


def load_assumptions(config_path: str = "config/cost_assumptions.yaml") -> dict:
    """Load cost assumptions YAML configuration file."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Cost assumptions config not found: {config_path}")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def calculate_workload_sizing(config: dict) -> WorkloadSizing:
    """Derive workload sizing parameters from configuration."""
    wl = config["workload"]
    return WorkloadSizing(
        baseline_events_per_day=wl["baseline_events_per_day"],
        baseline_days=wl["baseline_days"],
        peak_season_events_per_day=wl["peak_season_events_per_day"],
        peak_season_days=wl["peak_season_days"],
        instantaneous_burst_multiplier=wl["instantaneous_burst_multiplier"],
    )


def calculate_data_volumes(
    workload: WorkloadSizing, config: dict
) -> List[EventSizeTier]:
    """Calculate data volume across event size sensitivity tiers."""
    sizes = config.get("event_sizes_kb", {})
    tiers = [
        ("Small", sizes.get("small", 0.5), False),
        ("Reference", sizes.get("reference", 1.0), False),
        ("Large", sizes.get("large", 2.0), False),
        (
            "Prototype Evidence (Measured Avg)",
            sizes.get("prototype_measured_avg", 0.218),
            True,
        ),
    ]
    results = []
    for name, size_kb, is_proto in tiers:
        base_gb = (workload.baseline_events_per_day * size_kb) / 1_000_000.0
        peak_gb = (workload.peak_season_events_per_day * size_kb) / 1_000_000.0
        annual_tb = (
            (workload.peak_adjusted_annual_events * size_kb) / 1_000_000_000.0
        )
        results.append(
            EventSizeTier(
                name=name,
                size_kb=size_kb,
                baseline_gb_day=round(base_gb, 2),
                peak_gb_day=round(peak_gb, 2),
                annual_raw_tb=round(annual_tb, 2),
                is_prototype_evidence=is_proto,
            )
        )
    return results


def calculate_burst_capacity(
    workload: WorkloadSizing,
    burst_cfg: dict,
    worker_rate: float,
    baseline_workers: int,
) -> BurstCalculation:
    """Calculate burst worker sizing and annual elastic flash-sale costs."""
    assumed_eps = burst_cfg["assumed_eps_per_worker"]
    duration_hrs = burst_cfg["burst_duration_hours"]
    bursts_mo = burst_cfg["bursts_per_month"]
    bandwidth_fee = burst_cfg["broker_burst_bandwidth_per_burst"]

    # Explicit formula: ceil(burst_eps / assumed_eps_per_worker)
    req_workers = math.ceil(workload.burst_eps / assumed_eps)
    incr_workers = max(0, req_workers - baseline_workers)

    compute_per_burst = incr_workers * worker_rate * duration_hrs
    total_per_burst = compute_per_burst + bandwidth_fee
    annual_cost = total_per_burst * bursts_mo * 12.0

    return BurstCalculation(
        assumed_eps_per_worker=assumed_eps,
        required_burst_workers=req_workers,
        baseline_workers=baseline_workers,
        incremental_burst_workers=incr_workers,
        worker_hourly_rate=worker_rate,
        burst_duration_hours=duration_hrs,
        worker_compute_per_burst=round(compute_per_burst, 2),
        broker_burst_bandwidth=bandwidth_fee,
        cost_per_burst=round(total_per_burst, 2),
        bursts_per_month=bursts_mo,
        annual_burst_cost=round(annual_cost, 2),
    )


def calculate_scenario(
    name: str,
    sc_cfg: dict,
    workload: WorkloadSizing,
    budget_cap: float,
) -> ScenarioBreakdown:
    """Derive all line items, subtotals, contingency, and headroom for a scenario."""
    desc = sc_cfg.get("description", "")

    # 1. Streaming / Kafka (10 baseline months + 2 peak months with uplift)
    k_cfg = sc_cfg["kafka"]
    kafka_annual = (k_cfg["base_monthly_platform"] * 10.0) + (
        (k_cfg["base_monthly_platform"] + k_cfg["seasonal_monthly_uplift"]) * 2.0
    )

    # 2. Stream Processing (baseline hours + peak hours + control plane)
    sp_cfg = sc_cfg["stream_processing"]
    w_rate = sp_cfg["worker_hourly_rate"]
    base_w = sp_cfg["baseline_workers"]
    peak_w = sp_cfg["peak_workers"]
    base_hours = workload.baseline_days * 24.0  # 304 * 24 = 7296
    peak_hours = workload.peak_season_days * 24.0  # 61 * 24 = 1464
    sp_annual = (
        (base_w * w_rate * base_hours)
        + (peak_w * w_rate * peak_hours)
        + (sp_cfg["control_plane_monthly"] * 12.0)
    )

    # 3. Object Storage / Lakehouse
    st_cfg = sc_cfg["storage"]
    storage_annual = (
        (st_cfg["average_stored_gb"] * st_cfg["rate_per_gb_month"] * st_cfg["dr_replication_multiplier"])
        + st_cfg["api_monthly_fees"]
    ) * 12.0

    # 4. Monitoring / Logging
    log_cfg = sc_cfg["logging"]
    logging_annual = (
        (log_cfg["ingested_log_gb_month"] * log_cfg["rate_per_gb_ingested"])
        + log_cfg["platform_monthly_fee"]
    ) * 12.0

    # 5. Network / Egress
    net_cfg = sc_cfg["network"]
    network_annual = (
        (net_cfg["egress_gb_month"] * net_cfg["rate_per_gb_egress"])
        + net_cfg["nat_gateway_base_monthly"]
    ) * 12.0

    # 6. Batch / Query Compute
    qc_cfg = sc_cfg["query_compute"]
    query_annual = (
        qc_cfg["active_hours_per_day"] * qc_cfg["rate_per_compute_hour"] * workload.total_days
    ) + (qc_cfg["warehouse_storage_monthly"] * 12.0)

    # 7. Fixed Services (Orchestration, Catalog, Backup/DR)
    fx_cfg = sc_cfg["fixed_services"]
    orch_annual = fx_cfg["orchestration_monthly"] * 12.0
    cat_annual = fx_cfg["catalog_monthly"] * 12.0
    bdr_annual = fx_cfg["backup_dr_monthly"] * 12.0

    # 8. Flash-Sale Elastic Burst
    burst_calc = calculate_burst_capacity(
        workload=workload,
        burst_cfg=sc_cfg["flash_sale_burst"],
        worker_rate=w_rate,
        baseline_workers=base_w,
    )

    # Subtotal
    subtotal = (
        kafka_annual
        + sp_annual
        + storage_annual
        + logging_annual
        + network_annual
        + query_annual
        + orch_annual
        + cat_annual
        + bdr_annual
        + burst_calc.annual_burst_cost
    )

    # Contingency (percentage of subtotal)
    contingency_pct = sc_cfg["contingency_percentage"]
    contingency_dollar = subtotal * contingency_pct
    total_annual = subtotal + contingency_dollar
    monthly_run_rate = total_annual / 12.0

    # Metrics
    cost_per_1m = (total_annual / workload.peak_adjusted_annual_events) * 1_000_000.0
    headroom = budget_cap - total_annual
    pct_consumed = (total_annual / budget_cap) * 100.0

    # Rank cost drivers dynamically
    line_items = [
        ("Streaming / Kafka", kafka_annual),
        ("Stream Processing", sp_annual),
        ("Object Storage / Lakehouse", storage_annual),
        ("Monitoring / Logging", logging_annual),
        ("Network / Egress", network_annual),
        ("Batch / Query Compute", query_annual),
        ("Orchestration", orch_annual),
        ("Catalog / Governance", cat_annual),
        ("Backup / Disaster Recovery", bdr_annual),
        ("Flash-Sale Elastic Burst", burst_calc.annual_burst_cost),
    ]
    ranked = sorted(line_items, key=lambda x: x[1], reverse=True)
    top_drivers = [
        (name, amount, (amount / total_annual) * 100.0)
        for name, amount in ranked[:3]
    ]

    return ScenarioBreakdown(
        scenario_name=name,
        description=desc,
        kafka_annual=round(kafka_annual, 2),
        stream_processing_annual=round(sp_annual, 2),
        storage_annual=round(storage_annual, 2),
        logging_annual=round(logging_annual, 2),
        network_annual=round(network_annual, 2),
        query_compute_annual=round(query_annual, 2),
        orchestration_annual=round(orch_annual, 2),
        catalog_annual=round(cat_annual, 2),
        backup_dr_annual=round(bdr_annual, 2),
        burst_calc=burst_calc,
        subtotal_annual=round(subtotal, 2),
        contingency_percentage=contingency_pct,
        contingency_dollar=round(contingency_dollar, 2),
        total_annual_cost=round(total_annual, 2),
        monthly_run_rate=round(monthly_run_rate, 2),
        cost_per_1m_events=round(cost_per_1m, 2),
        budget_cap=round(budget_cap, 2),
        budget_headroom=round(headroom, 2),
        budget_percent_consumed=round(pct_consumed, 1),
        top_cost_drivers=top_drivers,
    )


def build_cost_model(config_path: str = "config/cost_assumptions.yaml") -> CostModelResult:
    """Execute complete cost model calculations across all configured scenarios."""
    cfg = load_assumptions(config_path)
    budget_cap = cfg.get("year_1_budget_cap", 480000.0)
    workload = calculate_workload_sizing(cfg)
    volumes = calculate_data_volumes(workload, cfg)

    scenarios = {}
    for sc_name, sc_cfg in cfg["scenarios"].items():
        scenarios[sc_name] = calculate_scenario(
            name=sc_name,
            sc_cfg=sc_cfg,
            workload=workload,
            budget_cap=budget_cap,
        )

    return CostModelResult(
        budget_cap=budget_cap,
        workload=workload,
        event_size_tiers=volumes,
        scenarios=scenarios,
    )
