# Fleet Telemetry Processing Pipeline (Milestones 1–4)

A lightweight, local data engineering prototype for ingesting fleet telemetry events into raw storage, processing them into a validated and watermark-aware Conformed Parquet layer, building an analytics-ready Curated Parquet layer with DuckDB metrics, and stress-testing the Kafka ingestion layer with a live 40x traffic burst.

---

## End-to-End Pipeline Architecture

```
[Sensor Simulator]
        │
        ▼ (Kafka Topic: fleet.telemetry.raw)
[Raw Consumer] ────► data/raw/telemetry_raw_YYYY-MM-DD.jsonl
        │
        ▼ (Milestone 2 Processing Pipeline)
[Validation] ──────────────► Invalid ───► data/quarantine/telemetry_quarantine_YYYY-MM-DD.jsonl
        │ (Valid)                         (raw payload + rejection reason + error details)
        ▼
[Deduplication] ───────────► Duplicate ──► Filtered from Conformed (Raw preserves all duplicates)
        │ (Unique event_id)
        ▼
[Watermark & Skew Guard] ──► Late ──────► Flagged (is_late=true, audit preserved, never dropped)
        │
        ▼
[Conformed Storage Writer]
        │ (DuckDB Engine)
data/conformed/processing_date=YYYY-MM-DD/*.parquet
        │
        ▼ (Milestone 3 Curated Builder - Idempotent DuckDB)
data/curated/event_date=YYYY-MM-DD/*.parquet
        │
        ├──► BI Analytics (Speed, Temp, Lateness by Vehicle)
        └──► Pipeline Metrics Reporter (Reprocessing Delay, Arrival Offset, Quality Rates)
```

---

## Milestone 4: Live Load & 40x Burst Test Architecture

A completely isolated subsystem designed to evaluate Kafka ingestion behavior under a sudden 40x traffic surge without contaminating the verified production-like layers.

```
[Traffic Generator]
   Baseline: 50 msg/s (15s) ──► Instantaneous 40x Burst: 2,000 msg/s (15s) ──► Drain
           │
           ▼
[Dedicated Topic: fleet.telemetry.loadtest (6 Partitions)]
           │
     ┌─────┴────────────────────────┐
     │                              │
[Run A: 1 Consumer Worker]    [Run B: 4 Consumer Workers]
(Single consumer group)       (Same consumer group, shared 6 partitions)
     │                              │
     ▼                              ▼
data/loadtest/run_a/*.jsonl    data/loadtest/run_b/*.jsonl
```

---

## Storage & Partitioning Layers

| Layer | Format | Partition Scheme | Purpose |
|---|---|---|---|
| **Raw** | JSON Lines | Ingestion Date (`YYYY-MM-DD`) | Immutable landing zone preserving raw sensor payloads verbatim, including duplicates and malformed records. |
| **Quarantine** | JSON Lines | Quarantine Date (`YYYY-MM-DD`) | Isolated rejection zone preserving verbatim raw records with detailed schema rejection reasons. |
| **Conformed** | Parquet (DuckDB) | `processing_date=YYYY-MM-DD` | Clean, deduplicated layer with late-event flags (`is_late`). Partitioned by processing date to avoid file fragmentation. |
| **Curated** | Parquet (DuckDB) | `event_date=YYYY-MM-DD` | Analytics-ready BI layer partitioned by event date for optimal time-range query performance. |
| **Load Test** | JSON Lines | Worker-isolated (`worker_{id}.jsonl`) | Isolated benchmark zone under `data/loadtest/` preventing concurrent write collisions. |

---

## Key Metrics & Domain Distinctions

1. **Device Clock Skew**: Physical discrepancy between the onboard hardware clock and true UTC reference time (observed drift: -10.90 to +10.55 minutes).
2. **Observed Arrival Offset (`ingest_time - event_time`)**: Conflates device clock skew with network transmission delay. True network latency cannot be isolated from the event schema alone without synchronized hardware clocks (e.g. GPS PPS).
3. **Batch Reprocessing Delay / Age at Processing (`processed_at - ingest_time`)**: Measures historical elapsed duration between raw file landing and batch execution of the Conformed/Curated jobs (~49,000s). Represents dataset age at time of processing, **not** live streaming pipeline latency.
4. **Kafka Publish-to-Consume Latency**: Real live latency measured in Milestone 4 within the same machine clock domain (`consumer_receive_time - kafka_message_timestamp`). Measures true queueing delay during burst backpressure (p50: ~11ms, p95: ~16ms, p99: ~18ms).
5. **Consumer Lag & Backpressure**: Real-time difference between log end offset and committed offset across all 6 partitions (`sum(high_watermark - committed)`). Serves as prototype evidence of ingestion backpressure during traffic spikes.

---

## Quickstart & Operations

### 1. Set Up Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Run All Automated Unit Tests

```bash
pytest -v tests/
```

### 3. Run Pipeline Stages

```bash
# Milestone 2: Process Raw to Conformed Parquet
python -m src.processor.main

# Milestone 3: Build Curated Parquet & Generate Metrics / BI Reports
python -m src.curated.main

# Milestone 4: Run Live Kafka 40x Load & Burst Benchmark (Run A vs Run B)
python -m src.loadtest.main

# Milestone 5: Workload Sizing & Parameterized Cost Model
python -m src.cost.main
```

---

## Milestone 4 Benchmark Results

| Metric | Run A (1 Consumer) | Run B (4 Consumers) |
|---|---|---|
| **Topic Partitions** | 6 | 6 |
| **Active Consumer Workers** | 1 | 4 |
| **Requested Baseline Rate** | 50 msg/s (15s) | 50 msg/s (15s) |
| **Achieved Baseline Rate** | 50.0 msg/s | 50.0 msg/s |
| **Requested Burst Rate (40x)** | 2,000 msg/s (15s) | 2,000 msg/s (15s) |
| **Achieved Burst Rate** | 1,999.9 msg/s | 1,999.4 msg/s |
| **Total Events Produced** | 30,752 | 30,744 |
| **Total Events Consumed** | 30,752 | 30,744 |
| **Missing Events After Drain** | **0 (0.0% loss)** | **0 (0.0% loss)** |
| **Consumer Throughput** | 1,025.9 msg/s | 1,026.5 msg/s |
| **Peak Processing Backlog (Unconsumed)** | **0 msgs** | **0 msgs** |
| **Peak Commit Lag (Offset Batching)** | 30,752 msgs | 30,744 msgs |
| **Backlog Recovery Time (to 0 queue)** | **0.04s** | **0.09s** |
| **Publish-to-Consume Latency (p50)** | 11.0 ms | 10.7 ms |
| **Publish-to-Consume Latency (p95)** | 15.6 ms | 15.6 ms |
| **Publish-to-Consume Latency (p99)** | 17.7 ms | 18.7 ms |
| **Publish-to-Consume Latency (Max)** | 24.6 ms | 28.2 ms |

---

## Architectural Implications for Production

1. **Processing Backlog vs. Commit Lag**:
   - 'Commit lag' (log end offset - committed offset) reflects asynchronous batch commit persistence intervals, whereas 'processing backlog' (log end offset - consumer position) reflects true unconsumed queue depth. Real-time consumption keeps processing backlog at ~0 while commit lag progresses in background batches.
2. **Scale-Out Observations on Local Prototype**:
   - On this local single-node environment, 1 consumer worker was already capable of consuming ~2,000 msg/s directly from Kafka with negligible backlog.
   - Run B demonstrates that 4 workers in a consumer group cleanly divide the 6 partitions without contention, achieving zero message loss.
   - Because the single consumer was not bottlenecked at 2,000 msg/s, overall throughput was producer-rate-bound. True scale-out throughput gains emerge when downstream processing (complex validation, analytical transforms, database writes) creates a CPU or I/O bottleneck exceeding single-worker capacity.
3. **Live Latency vs. Simulated Clock Skew**:
   - Observed local benchmark maximum latency was 24.6 ms for Run A and 28.2 ms for Run B; no business SLA was provided for comparison.
   - Live Kafka publish-to-consume latency is small (~11-18ms), showing that broker transport adds minimal queueing delay under prototype burst conditions.
   - While this transport latency is orders of magnitude smaller than the ±11-minute timestamp differences observed in Milestones 1–3, that clock drift was synthetically injected by the prototype simulator and does NOT prove physical hardware behavior on real vehicles.
4. **Backpressure and Zero Data Loss**:
   - Kafka's partitioned append-only log smoothly absorbs instantaneous 40x surges without dropping records, ensuring 0 missing messages after drain in both runs.

---

## Milestone 5: Workload Sizing & Parameterized Cost Model

### Sizing & Throughput Rates
- **Baseline Throughput (304 days)**: 208.33 events/sec (18,000,000 events/day)
- **Peak Season Throughput (61 days, Nov–Dec)**: 1,620.37 events/sec (140,000,000 events/day)
- **40x Instantaneous Burst Rate**: 8,333.33 events/sec
- **Peak-Adjusted Annual Volume**: 14,012,000,000 events/year (14.012B)

### Annual Infrastructure Cost Scenarios vs. $480,000 Budget Cap

| Cost Category / Metric | Lean | Reference (Primary) | High |
|---|---|---|---|
| **Streaming / Kafka** | $30,000.00 | **$52,000.00** | $82,000.00 |
| **Stream Processing** | $9,388.80 | **$23,631.36** | $64,688.64 |
| **Object Storage / Lakehouse** | $4,350.00 | **$11,736.00** | $33,720.00 |
| **Monitoring / Logging** | $5,550.00 | **$16,680.00** | $71,400.00 |
| **Network / Egress** | $4,500.00 | **$12,000.00** | $30,600.00 |
| **Batch / Query Compute** | $7,290.00 | **$22,320.00** | $70,080.00 |
| **Orchestration** | $4,200.00 | **$10,200.00** | $19,200.00 |
| **Catalog / Governance** | $3,000.00 | **$7,800.00** | $16,800.00 |
| **Backup / Disaster Recovery** | $3,600.00 | **$9,000.00** | $21,600.00 |
| **Flash-Sale Elastic Burst** | $680.64 | **$2,311.68** | $10,241.28 |
| **Modeled Subtotal** | $72,559.44 | **$167,679.04** | $420,329.92 |
| **Contingency Reserve** | $5,804.76 (8.0%) | **$16,767.90 (10.0%)** | $50,439.59 (12.0%) |
| **TOTAL ANNUAL COST** | **$78,364.20** | **$184,446.94** | **$470,769.51** |
| **Monthly Run Rate** | $6,530.35 / mo | **$15,370.58 / mo** | $39,230.79 / mo |
| **Cost per 1M Events** | **$5.59** | **$13.16** | **$33.60** |
| **Year-1 Budget Headroom ($480k)** | **+$401,635.80** | **+$295,553.06** | **+$9,230.49** |
| **Percent of Budget Consumed** | **16.3%** | **38.4%** | **98.1%** |

*Detailed outputs available in [outputs/cost_model.csv](outputs/cost_model.csv) and [outputs/cost_model.md](outputs/cost_model.md).*

