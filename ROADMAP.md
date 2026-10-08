# 🗺️ Roadmap: Real-Time Transactions Pipeline

> End-to-end streaming pipeline: **Generator → Redpanda (Kafka) → Spark Structured Streaming → Delta Lake (S3) → Snowflake + dbt**, orchestrated with Airflow.
>
> **Pace:** 1 commit per day, with a devlog entry (`docs/devlog/YYYY-MM-DD.md`).
> **Estimated duration:** 8 to 10 weeks, depending on availability.

---

## Phase overview

| # | Phase | Focus | Estimated duration |
|---|-------|-------|--------------------|
| 0 | Foundation | Repo, environment, secure AWS, pinned versions | 3 to 4 days |
| 1 | Event generator | Realistic data with deliberate defects | 4 to 5 days |
| 2 | Ingestion (Redpanda) | Topics, partitioning, DLQ | 4 to 5 days |
| 3 | Bronze | Spark → Delta on S3, checkpointing | 5 to 7 days |
| 4 | Silver | Idempotent dedup, watermark, validation | 7 to 10 days |
| 5 | Gold | Time-window aggregations | 5 to 6 days |
| 6 | Snowflake + dbt | Consumption layer and models | 6 to 8 days |
| 7 | Orchestration | Airflow: Delta maintenance, dbt, quality | 4 to 5 days |
| 8 | Failures and observability | Chaos tests, latency, small files | 6 to 8 days |
| 9 | Wrap-up | Final README, diagram, costs, post | 3 to 4 days |

> ⚠️ **Snowflake free trial:** Phase 6 is the first one that consumes credits. All development before it runs without Snowflake. Plan the start date of Phase 6 so the trial covers Phases 6 to 9.

---

## Phase 0: Foundation

**Goal:** have a reproducible, safe environment before writing the first line of pipeline code.

**Actions**
- [ ] Create the repository with `README.md`, `ROADMAP.md`, `.gitignore` (Python, Terraform, dbt, `.env`) and a license
- [ ] Define the folder structure (see "Final structure" in the README)
- [ ] Set up `pyproject.toml` (or `requirements.txt`), `ruff` and `pre-commit`
- [ ] Create `.env.example` and make sure the real `.env` is never committed
- [ ] **AWS:** create a private S3 bucket, an IAM user/role with minimal permissions for that bucket only, and an **AWS Budget with an alert** (e.g., US$ 5)
- [ ] (Optional, recommended) Codify bucket + IAM + Budget in Terraform (`infra/terraform`)
- [ ] Choose and **pin the versions** of Spark, Delta Lake, `hadoop-aws` and the Kafka connector (they must be compatible with each other) and record them in `docs/adr/001-versions.md`
- [ ] Create the base `docker-compose.yml` (Redpanda + Redpanda Console + Spark) and validate that it starts
- [ ] Smoke test: Spark in Docker writing a test Delta table to S3
- [ ] Basic GitHub Actions CI workflow (lint + unit tests)

**Deliverables:** structured repo, environment starting with `make up`, versions ADR.
**Done when:** `make smoke-test` writes and reads a Delta table on S3.

---

## Phase 1: Event generator

**Goal:** produce a realistic stream of financial transactions **with controlled defects** that feed every test in the following phases.

**Actions**
- [ ] Define the event contract (`docs/data-contract.md`): `event_id`, `transaction_id`, `account_id`, `merchant_id`, `category`, `amount`, `currency`, `status`, `event_ts`, `schema_version`
- [ ] Implement the generator in Python (`src/generator`) with a configurable rate (events/second)
- [ ] Implement anomaly injection, each with a configurable rate:
  - [ ] **duplicate** events (same `event_id` resent)
  - [ ] **late** events (`event_ts` in the past)
  - [ ] **out-of-order** events
  - [ ] **invalid** events (null required field, wrong type, negative amount)
  - [ ] **schema change** (`schema_version` 2 with a new field)
- [ ] Deterministic mode (fixed seed) to reproduce tests
- [ ] Unit tests for the generator and the anomalies
- [ ] Export the generated anomaly counts (to later check whether the pipeline handled everything)

**Deliverables:** `make generate` producing events to stdout/file, documented contract.
**Done when:** with a fixed seed, the generator always produces the same N events with the same anomalies.

---

## Phase 2: Ingestion into Redpanda (Kafka)

**Goal:** publish the events to well-designed topics.

**Actions**
- [ ] Create the topics: `transactions.raw`, `transactions.dlq`
- [ ] Define the **number of partitions** and the **partitioning key** (e.g., `account_id`, to preserve per-account order) and record the decision in an ADR
- [ ] Define topic retention and cleanup policy
- [ ] Implement the producer with `acks=all`, retries and serialization (JSON first; evaluate Avro/Schema Registry as an evolution)
- [ ] Topic setup script (`scripts/create_topics.sh`), idempotent
- [ ] Inspect messages and lag via Redpanda Console (attach a screenshot to the devlog)
- [ ] Integration test: generator → topic → simple consumer checking the count

**Deliverables:** events flowing into `transactions.raw`.
**Done when:** `make produce` sends N events and the test consumer reads exactly N.

---

## Phase 3: Bronze layer (Spark → Delta on S3)

**Goal:** persist the raw data without losing anything, in a recoverable way.

**Actions**
- [ ] Create the Spark session with Delta + S3A (`src/streaming/common/spark_session.py`)
- [ ] Streaming job reading `transactions.raw` and writing to `s3a://<bucket>/bronze/transactions`
- [ ] Store the **raw** payload + Kafka metadata (`topic`, `partition`, `offset`, `kafka_timestamp`) + `ingestion_ts`
- [ ] Configure the **checkpoint** on S3 and choose the `trigger` (e.g., `processingTime='10 seconds'`), documenting the latency × small files trade-off
- [ ] Partition the table by ingestion date
- [ ] Validate the Kafka × Bronze count
- [ ] Test: restart the job and confirm it resumes from the checkpoint

**Deliverables:** Bronze Delta table updating continuously.
**Done when:** `count(bronze) == published messages`, including after restarting the job.

---

## Phase 4: Silver layer (cleaning, dedup, validation)

**Goal:** reliable, unique and typed data. This is the most important phase of the project.

**Actions**
- [ ] Parse the JSON with an explicit schema (`src/streaming/schemas.py`)
- [ ] **Validation:** business and schema rules; invalid events go to `transactions.dlq` (or a `silver_rejected` table) with the **rejection reason**
- [ ] **Idempotent deduplication** with `foreachBatch` + `MERGE INTO` by `event_id` (or `transaction_id` + version)
- [ ] **Watermark** to handle late events; define and justify the tolerance window size
- [ ] Handle late events beyond the tolerance (drop, send to DLQ, or reprocess) and document the choice
- [ ] Handle `schema_version` 2 with **schema evolution** (`mergeSchema`) without breaking the job
- [ ] Tests: generate N events with M duplicates and check `count(silver) == N − M`
- [ ] Unit tests for the validation rules (pytest, no Kafka needed)
- [ ] ADR: why `MERGE` instead of stateful `dropDuplicates`

**Deliverables:** deduplicated Silver table + DLQ populated with reasons.
**Done when:** the generator's anomaly rates match what Silver deduplicated and what the DLQ received.

---

## Phase 5: Gold layer (windowed metrics)

**Goal:** ready-to-consume aggregations.

**Actions**
- [ ] Define the metrics (`docs/gold-metrics.md`): volume and amount per **1-minute window** × category, average ticket, decline rate, top merchants
- [ ] Silver → Gold streaming job with `window()` and watermark
- [ ] Choose the output mode (`append` vs `update`) and justify it
- [ ] Write to Delta (`s3a://<bucket>/gold/...`)
- [ ] Tests: compare the streaming aggregation with the same aggregation run in batch over Silver (they must match)
- [ ] Measure and record event → Gold latency

**Deliverables:** Gold tables updated in near real time.
**Done when:** the streaming × batch consistency test passes.

---

## Phase 6: Snowflake + dbt (consumption layer)

**Goal:** expose Gold for analysis and apply modeling and testing with dbt.

**Actions**
- [ ] Create `snowflake/setup.sql`: database, schemas, **XS warehouse with `AUTO_SUSPEND = 60`**, project role and user
- [ ] **Decide and document (ADR)** the S3/Delta → Snowflake integration path, comparing cost, latency and complexity. Check the current Snowflake documentation for Delta/Iceberg support. Options:
  - Gold Parquet → `COPY INTO` / Snowpipe via *storage integration*
  - External table or Iceberg
- [ ] Implement the chosen load
- [ ] dbt project (`dbt-snowflake`): `staging` → `marts`
- [ ] Models: `fct_transactions_minute`, `dim_merchant`, `dim_category`, daily metrics
- [ ] dbt tests (`unique`, `not_null`, `accepted_values`, `relationships`) + custom tests
- [ ] Documentation and lineage (`dbt docs generate`)
- [ ] Simple dashboard (Streamlit, Metabase or Snowsight) with the metrics

**Deliverables:** marts in Snowflake, dbt docs, dashboard.
**Done when:** `dbt build` passes and the dashboard shows data with a few minutes of delay.

---

## Phase 7: Orchestration with Airflow

**Goal:** automate maintenance and consumption, making clear what runs as continuous streaming and what runs as batch.

**Actions**
- [ ] Start Airflow in Docker Compose
- [ ] `delta_maintenance` DAG: `OPTIMIZE` and `VACUUM` for bronze/silver/gold, with a defined retention
- [ ] `load_snowflake_and_dbt` DAG: load + `dbt build`
- [ ] `data_quality` DAG: Kafka × bronze × silver × gold reconciliation and an alert on divergence
- [ ] Configure retries, SLAs and failure alerts
- [ ] Document why the streaming jobs are **not** orchestrated by Airflow (they run continuously) and what Airflow does around them

**Deliverables:** 3 working DAGs.
**Done when:** the DAGs run on schedule and a forced failure triggers the alert.

---

## Phase 8: Failures and observability (the differentiator)

**Goal:** prove with evidence that the pipeline is reliable.

**Actions (each test becomes a document in `docs/failure-tests/`)**
- [ ] **Spark crash** mid-processing → the checkpoint resumes without losing or duplicating data
- [ ] **Redpanda outage** and recovery → the job recovers
- [ ] **Reprocessing:** delete Silver and rebuild it from Bronze
- [ ] **Backpressure:** increase the generator rate (10×) and observe the behavior (`maxOffsetsPerTrigger`)
- [ ] **Schema evolution:** send v2 events while running
- [ ] **Small files:** measure the number of files before and after `OPTIMIZE`
- [ ] **Time travel:** query an old Delta version and run `RESTORE`
- [ ] Measure **end-to-end latency** (p50/p95) and record it in `docs/benchmarks/`
- [ ] Final count reconciliation across all layers

**Deliverables:** `docs/failure-tests/` folder with hypothesis, procedure, result and evidence for each test.
**Done when:** every test has a documented result, including those that revealed problems and how they were fixed.

---

## Phase 9: Wrap-up and portfolio

**Actions**
- [ ] Final architecture diagram (Mermaid + image)
- [ ] Final README: results, decisions, real AWS/Snowflake cost, lessons learned
- [ ] "What I would do differently in production" section (MSK/Kinesis, Schema Registry, monitoring with Prometheus/Grafana, CDC, etc.)
- [ ] Short GIF or video of the demo
- [ ] `v1.0.0` tag and GitHub release
- [ ] LinkedIn post and CV/GitHub profile update
- [ ] **Shut everything down:** destroy the infra (`terraform destroy`), suspend the warehouse and check billing

---

## Definition of "project done"

- [ ] `make up && make demo` starts everything and shows data flowing
- [ ] Every relevant decision has an ADR
- [ ] Every failure test has evidence
- [ ] Cost is documented
- [ ] The README lets someone else reproduce the project
