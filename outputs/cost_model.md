# WBL Telemetry Platform: Milestone 5 Cost Model & Workload Sizing Report

## 1. Executive Summary

- **Annual Workload Volume**: **14,012,000,000 events/year (14.012B)**.
- **Primary Planning Scenario (Reference)**: **$184,446.94 / year** ($15,370.58 / month).
- **Reference Cost per 1M Events**: **$13.16**.
- **Year-1 Budget Headroom ($480,000 Cap)**: **+$295,553.06** (38.4% consumed).
- **Sensitivity Range**: **$78,364.20 / yr** (Lean, 16.3% budget) to **$470,769.51 / yr** (High, 98.1% budget).

> [!NOTE]
> All infrastructure prices, worker capacities, storage tiers, and burst durations are explicit **planning assumptions** configured in `config/cost_assumptions.yaml`. They are not vendor quotes or empirical production guarantees.

---

## 2. Workload Sizing & Throughput Rates

| Sizing Dimension | Case Study Input / Formula | Derived Value |
|---|---|---|
| **Baseline Daily Volume** | Case Input (Jan–Oct, 304 days) | **18,000,000 events/day** |
| **Baseline Throughput Rate** | 18M events / 86,400s | **208.33 events/sec** |
| **Peak Season Daily Volume** | Case Input (Nov–Dec, 61 days) | **140,000,000 events/day** |
| **Peak Season Throughput Rate** | 140M events / 86,400s | **1620.37 events/sec** |
| **40x Instantaneous Burst Rate** | 40 × Baseline EPS (flash-sale surge) | **8333.33 events/sec** |
| **Peak-Adjusted Annual Events** | (304 × 18M) + (61 × 140M) | **14,012,000,000 events/year (14.012B)** |
| **Annual Average Throughput** | 14.012B events / (365 × 86,400s) | **444.32 events/sec** |

*Key Insight: Peak season spans only 16.7% of days (61 days) but generates **60.9%** of annual event volume.*

---

## 3. Event Size Sensitivity & Data Volume Sizing

Production event payload size is not specified in the case study. Sizing is evaluated across 3 explicit sensitivity tiers, alongside empirical prototype measurements:

| Sensitivity Tier | Event Size | Baseline Data / Day | Peak Season Data / Day | Annual Raw Volume |
|---|---|---|---|---|
| **Small** | 0.500 KB | 9.00 GB/day | 70.00 GB/day | 7.01 TB/year |
| **Reference** | 1.000 KB | 18.00 GB/day | 140.00 GB/day | 14.01 TB/year |
| **Large** | 2.000 KB | 36.00 GB/day | 280.00 GB/day | 28.02 TB/year |
| *Prototype Evidence (Measured Avg)* | 0.218 KB | 3.92 GB/day | 30.52 GB/day | 3.05 TB/year |

*Note: The empirical prototype measurement (~0.218 KB / event) reflects local simulated JSON telemetry with minimal fields. Production vehicles with rich CAN/sensor payloads typically align closer to Reference (1.0 KB) or Large (2.0 KB).*

---

## 4. Flash-Sale Elastic Capacity Modeling

Flash-sale surges (up to 40x baseline = 8,333.33 eps) are modeled strictly as **temporary on-demand burst capacity**, rather than permanent 24/7 overprovisioning.

$$\text{Required Burst Workers} = \left\lceil \frac{8{,}333.33\text{ eps}}{\text{Assumed EPS / Worker Core}} \right\rceil$$
$$\text{Incremental Workers} = \max(0, \text{Required} - \text{Baseline Active})$$

| Parameter | Lean | Reference (Primary) | High | Description |
|---|---|---|---|---|
| **Assumed EPS / Worker Core** | 500 eps | **400 eps** | 300 eps | Planning assumption (not production claim) |
| **Required Total Burst Workers** | 17 workers | **21 workers** | 28 workers | Fleet sized for 8,333.33 eps |
| **Baseline Workers Already Active** | 3 workers | **4 workers** | 6 workers | Active baseline consumer pods |
| **Incremental Burst Workers Spun Up** | 14 workers | **17 workers** | 22 workers | Autoscaled on-demand pods |
| **Worker Hourly Rate** | $0.12 / hr | **$0.24 / hr** | $0.48 / hr | Compute tier rate |
| **Burst Duration per Event** | 2.0 hrs | **2.0 hrs** | 3.0 hrs | Temporary surge duration |
| **Cost per Flash-Sale Burst** | $28.36 | **$48.16** | $106.68 | Elastic compute + broker bandwidth |
| **Campaign Frequency** | 2 / mo (24/yr) | **4 / mo (48/yr)** | 8 / mo (96/yr) | Planned marketing campaigns |
| **Annual Elastic Burst Cost** | **$680.64** | **$2,311.68** | **$10,241.28** | Modeled strictly as temporary hours |

---

## 5. Annual Infrastructure Cost Comparison Table

| Cost Category | Lean | Reference (Primary) | High | Planning Rationale |
|---|---|---|---|---|
| **1. Streaming / Kafka** | $30,000.00 | **$52,000.00** | $82,000.00 | 10 baseline months + 2 peak uplift months |
| **2. Stream Processing** | $9,388.80 | **$23,631.36** | $64,688.64 | Worker hours (7,296h base + 1,464h peak) + control plane |
| **3. Object Storage / Lakehouse** | $4,350.00 | **$11,736.00** | $33,720.00 | Avg stored GB × rate × DR multiplier + API requests |
| **4. Monitoring / Logging** | $5,550.00 | **$16,680.00** | $71,400.00 | Ingested log GB × rate + platform base fee |
| **5. Network / Egress** | $4,500.00 | **$12,000.00** | $30,600.00 | Egress GB × rate + NAT Gateway base infrastructure |
| **6. Batch / Query Compute** | $7,290.00 | **$22,320.00** | $70,080.00 | Daily query runtime hours × rate + warehouse staging |
| **7. Orchestration** | $4,200.00 | **$10,200.00** | $19,200.00 | Workflow scheduler (e.g. lightweight vs Managed Airflow) |
| **8. Catalog / Governance** | $3,000.00 | **$7,800.00** | $16,800.00 | Schema registry & table metadata catalog |
| **9. Backup / Disaster Recovery** | $3,600.00 | **$9,000.00** | $21,600.00 | Automated snapshots and cross-region replication |
| **10. Flash-Sale Elastic Burst** | $680.64 | **$2,311.68** | $10,241.28 | Temporary on-demand compute & broker surge |
| **Modeled Subtotal** | $72,559.44 | **$167,679.04** | $420,329.92 | Core infrastructure run-rate |
| **Contingency / Operational Reserve** | $5,804.76 (8%) | **$16,767.90 (10%)** | $50,439.59 (12%) | Configurable buffer for re-indexing & surges |
| **TOTAL ANNUAL COST** | **$78,364.20** | **$184,446.94** | **$470,769.51** | **Fully loaded annual infrastructure** |
| **Monthly Run Rate** | $6,530.35 / mo | **$15,370.58 / mo** | $39,230.79 / mo | Annual / 12 |
| **Cost per 1 Million Events** | **$5.59** | **$13.16** | **$33.60** | Annual Cost / 14.012B × 1M |
| **Year-1 Budget Headroom ($480k)** | **+$401,635.80** | **+$295,553.06** | **+$9,230.49** | Headroom against $480,000 cap |
| **Percent of Budget Consumed** | **16.3%** | **38.4%** | **98.1%** | Budget boundary status |

---

## 6. Two Largest Modeled Cost Risks

Cost driver rankings are derived programmatically from the model calculations rather than hardcoded:

### In the Primary Planning Scenario (Reference):
1. **Streaming / Kafka ($52,000.00 / 28.2% of annual cost)**:
   - Sizing brokers to absorb both steady-state 208 eps and peak-season 1,620 eps with multi-AZ replication makes Kafka the single largest infrastructure line item.
2. **Stream Processing ($23,631.36 / 12.8% of annual cost)**:
   - Sustained consumer worker capacity during the 61-day seasonal peak accounts for nearly a quarter of streaming spend, followed closely by **Batch/Query Compute** ($22,320.00 / 12.1%).

### In the High Sensitivity Scenario:
1. **Streaming / Kafka ($82,000.00 / 17.4% of annual cost)**.
2. **Monitoring / Logging ($71,400.00 / 15.2% of annual cost)**:
   - When high-frequency telemetry events are indexed with verbose APM tracing without sampling filters, **observability costs escalate dramatically** ($71,400/yr), threatening to rival the entire messaging layer.

