## STAR script (2–3 minutes): AWS Fraud Detection Pipeline

Use this as a concise talk track in interviews. Adjust specifics (team size, dataset volume, SLA) to match your experience.

### Situation
Fraud attempts happen continuously and patterns evolve quickly. In prior work, we learned that a model is only as good as the data pipeline feeding it: if ingestion lags, schemas drift, or ETL silently fails, business teams lose trust in the signals and operational response time suffers.

In this project, the goal was to build an end-to-end, near-real-time fraud analytics system on AWS that could:
1. ingest transaction events with low latency,
2. store immutable raw data for replay,
3. run repeatable ETL to produce curated datasets,
4. load analytics-ready tables into a warehouse,
5. visualize KPIs in a refreshable dashboard,
6. fail fast with data quality gates when bad data arrives.

### Task
I owned the design and implementation of a pipeline that connects streaming ingestion to a governed analytics layer:
- build a streaming ingestion path (Kinesis -> S3 raw),
- trigger rule-based ETL (Lambda -> Glue),
- load curated Parquet into Redshift (COPY),
- enforce data quality checks (fail Airflow when invalid),
- support dashboarding and operational monitoring (CloudWatch + SNS).

The success criteria were reliability (no silent corruption), scalability (handle bursty event volume), and cost efficiency (avoid unnecessary scans and over-provisioned compute).

### Action
I structured the architecture in layers and made key trade-offs:
1. **Streaming ingest + durable landing:** Events land in **Kinesis** for buffering and fan-out. **Firehose** delivers to S3 under a partition-friendly prefix (`year=/month=/day/` style).
2. **Event-driven ETL:** **S3 event notifications** trigger a **Lambda** function that starts a **Glue** PySpark job with the correct `--S3_INPUT` and `--S3_OUTPUT`.
3. **Curated transformations:** The Glue job casts/cleans core fields, derives time features (hour, weekend), and applies a rule-based fraud scoring ladder. Outputs are written as **Parquet partitioned by year/month/day**.
4. **Warehouse loading:** An Airflow task loads Parquet into **Redshift** using `S3ToRedshiftOperator` and `COPY ... FORMAT AS PARQUET`.
5. **Data quality gate:** After COPY, a Python-based gate (psycopg2) checks row count, null sentinels, amount ranges, fraud_score bounds, and duplicate transaction IDs. If any check fails, the DAG fails so operators catch issues immediately.
6. **Observability + alerting:** CloudWatch alarms notify an SNS topic on Glue failures, Kinesis iterator lag, and Lambda errors, keeping the pipeline operationally accountable.
7. **BI layer:** QuickSight connects to Redshift and visualizes fraud KPIs with scheduled refresh.

### Result
This architecture delivers a complete fraud analytics capability with:
- **Operational reliability:** Quality gates ensure corrupted or out-of-range data never becomes “dashboard truth.”
- **Low-latency analytics readiness:** Streaming ingestion reduces time-to-insight, while Glue + Redshift provide consistent query performance.
- **Replayability and lineage:** Raw immutable data in S3 supports audits and reprocessing.
- **Performance-aware design:** Parquet partitioning and Redshift `SORTKEY`/`DISTKEY` are aligned with common fraud analytics queries.

If I were to extend it, I’d move from heuristic scoring to an ML-backed scoring model, but keep the same governance pattern (quality gates, monitoring, and partition strategy) so the ML layer remains dependable.

---

## Expected interview questions (with bullet-point answers)

| Interview question | Bullet-point answers you can use |
|---|---|
| **Why Kinesis over Kafka?** | - AWS-native integration reduces ops overhead: IAM, scaling, managed stream retention, easier Firehose integration.<br>- Kinesis provides straightforward Lambda consumers / metrics and integrates cleanly with our Firehose-to-S3 delivery.<br>- Latency and throughput are tunable for the event volume; we use partition keys to keep ordering semantics per entity (e.g., merchant/user).<br>- For this project’s AWS-first goal, Kinesis improves time-to-production and aligns with the rest of the managed services. |
| **Why Parquet over CSV?** | - Parquet is columnar: Redshift and Spark can scan only required columns, cutting IO and speeding queries.<br>- Strong typing (decimal/timestamp/boolean) improves schema stability compared to CSV parsing and reduces downstream casting bugs.<br>- Smaller storage footprint and better compression (e.g., Snappy) reduce S3 cost and load time.<br>- Supports efficient partition discovery and Parquet-friendly analytics pipelines (Glue -> Redshift COPY). |
| **How would you scale this pipeline?** | - Scale by increasing Kinesis shard count and Firehose buffering throughput (plus optional data pre-processing).<br>- Parallelize Glue jobs by increasing capacity, tuning Spark settings, and running multiple partitions concurrently (e.g., per date/hour).<br>- In Redshift, consider Redshift Serverless or scaling node type; keep `SORTKEY` aligned with time predicates and monitor WLM queues.<br>- Use partition pruning via year/month/day so COPY and queries touch the minimum data slice.<br>- Add concurrency controls in Airflow (e.g., max active tasks) and ensure idempotent ETL outputs. |
| **How do you handle late-arriving data?** | - Store immutable raw events in S3 so late events can be reprocessed without data loss.<br>- ETL uses job bookmarks / incremental logic so re-runs can incorporate new arrivals for affected partitions (hour/day).<br>- Ensure idempotency in curated outputs: deduplicate by `transaction_id` (and/or use overwrite strategy for recent partitions).<br>- In Redshift, implement upsert-like patterns (staging + merge) if needed, rather than only append. |
| **How do you ensure data quality?** | - Fail fast with automated checks after each critical stage (e.g., after COPY): row count, null sentinels, range checks, duplicate ID checks.<br>- Add schema validation in Glue: enforce casts and drop/route malformed records.<br>- Use partition-aware checks: validate that the expected `year/month/day` slice arrived and is non-empty.<br>- Track metrics in CloudWatch: ingestion lag, Glue job failures, and alarm thresholds for drift or anomalies. |
| **What would you add next?** | - Replace heuristic scoring with an ML scoring pipeline (SageMaker), keeping the same governance: quality gates + monitoring + model drift checks.<br>- Introduce a dbt layer for curated transformations and standardized metrics definitions.<br>- Add CDC (DMS) for real operational systems so you get richer features beyond synthetic events.<br>- Improve lineage with data catalog conventions and automated lineage tooling. |

---

## Key architectural decisions

### Redshift `DISTKEY` and `SORTKEY` rationale

Our warehouse table is `fraud.fact_transactions` with:

- **`DISTKEY(merchant_id)`**
  - Fraud analysis is commonly grouped by merchant (category, risk by merchant, merchant-specific drilldowns).
  - Co-locating rows for the same `merchant_id` reduces data redistribution during merchant-centric joins and aggregations.
  - It improves performance for typical “merchant cohort” queries versus redistributing wide fact rows across nodes.

- **`SORTKEY(transaction_timestamp)`**
  - Fraud investigation and reporting are time-based (daily trends, hourly heatmaps, “last N hours/days” monitoring).
  - Sorting by time enables zone-map pruning, reducing scanned blocks for time filters.
  - It aligns with partitioning strategy in S3 and the Glue-derived time features.

If the workload shifts toward compound filters (time + merchant), you could consider a compound sort key or adjust dist keys, but the current defaults match the dashboard and monitoring use cases.

### Partitioning strategy (S3 and curated outputs)

- Raw and curated datasets are laid out with a **hive-style date partition**:
  - `year=YYYY/month=MM/day=DD/`
- Benefits:
  - **Partition pruning** in Spark/Glue/Athena/Redshift Spectrum reduces IO.
  - COPY operations ingest only the partitions that correspond to each ETL run.
  - Late-arrival handling becomes manageable: reprocess only the affected date partitions.

In practice, if events are very high volume, you can extend partitions with `hour=` to further narrow reprocessing scope and reduce load times for hourly DAG runs.

---

## Cost optimization tips for this architecture

### 1) Keep partitioning tight
- Ensure curated outputs are written only to the relevant `year/month/day` (and optionally `hour`) partitions.
- Align Airflow hourly scheduling with the partition granularity so COPY loads the smallest possible slice.

### 2) Prefer columnar formats and column pruning
- Parquet + snappy reduces S3 storage and improves Redshift load and query speed.
- Keep the schema stable to maximize predicate effectiveness and avoid rework.

### 3) Tune Glue Spark runs
- Right-size Glue job types and tune workers based on the size of each partition slice.
- Use job bookmarks and incremental processing to avoid reprocessing entire history.

### 4) Control Redshift workload management
- Enable WLM settings appropriate for dashboard concurrency.
- Use time-based filters in the dashboard queries to leverage `SORTKEY` pruning.

### 5) Make data quality checks inexpensive
- DQ checks should operate on metadata and small aggregations (COUNTs, NULL scans, duplicates via GROUP BY), not full-table sorts.
- Consider sampling checks for high-volume tables, while keeping deterministic checks for key constraints (e.g., duplicates by transaction_id).

### 6) Reduce alert noise
- Set CloudWatch alarm thresholds with operational intent (e.g., “iterator age > 60s”).
- Route alerts to SNS; use deduplication/grouping in downstream ticketing systems if possible.

### 7) Avoid “double compute”
- Avoid running the same transformation in multiple places (e.g., do not recompute Glue features again in Redshift for dashboards).
- Keep transformations in Glue; keep Redshift for analytics modeling and aggregations.

