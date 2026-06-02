# Architecture

## Overview

This repository is a local data platform playground built around Apache Airflow, Apache Spark, Kafka, Iceberg, and MinIO.

It currently has four related flows:

1. A realtime Kafka demo that produces and consumes sample events.
2. A realtime Bytewax pipeline that consumes Kafka events, writes metrics to InfluxDB, and optionally posts Grafana annotations.
3. The active edge-device medallion pipeline, which runs every 30 minutes and uses Spark for all processing.
4. An older sample Iceberg DAG that uses PyIceberg with a SQLite catalog.

The edge-device flow is the main path and is used for ingestion, transformation, data quality checks, and SQL-based BI serving.

## Repository Layout

The most important runtime folders are:

- `dags/` - Airflow DAG definitions
- `spark_jobs/` - Spark entrypoints for bronze, silver, gold, and reader jobs
- `test.py` - local wrapper that runs the reader job inside Spark and copies CSV files back to the host
- `docker-compose.yaml` - orchestrates Airflow, Spark, Kafka, PostgreSQL, and Kafka UI
- `airflow/` - Airflow image build context and dependencies
- `spark/` - Spark image build context and dependencies
- `utils/` - shared helper modules for non-Spark code paths
- `README.md` - user-facing setup and run instructions

## Runtime Topology

```text
PostgreSQL 16  <-- Airflow metadata DB
Airflow API / Scheduler / DAG Processor
Spark Master + Spark Worker
Kafka broker + Kafka UI
MinIO S3 API + console

Airflow DAGs submit Spark jobs to the Spark master.
Spark reads and writes Iceberg tables in MinIO through the Hadoop S3A filesystem.
Kafka provides the source stream for the edge-device bronze job.
```

## Realtime Kafka Flow

The repository also includes a simple realtime Kafka DAG that exercises the event stream directly:

- DAG: `dags/kafka_producer_dag.py`
- Topic: `iceberg_events`
- Schedule: manual trigger only
- Tags: `kafka`, `streaming`

```text
Sample events
        ↓
Kafka producer task
        ↓
Kafka topic: iceberg_events
        ↓
Kafka consumer task
        ↓
Console output
```

This DAG is a lightweight realtime demonstration. It does not write Iceberg tables, but it shows the Kafka event path that the broader platform is built around.

In the edge-device flow, the bronze Spark job is also streaming-based: it reads `edge_computer_stats` with Spark structured streaming, processes available Kafka data in micro-batches, and writes to Iceberg incrementally.

## Realtime Bytewax Monitoring Flow

The repository includes a dedicated realtime observability pipeline built with Bytewax:

- Flow file: `bytewax/dataflow.py`
- Sink implementation: `bytewax/connector/influxdb.py`
- Container image: `bytewax/Dockerfile`
- Runtime stack: `storage.docker-compose.yaml`

```text
Kafka topic: edge_computer_stats
        ↓
Bytewax flow (decode + enrich)
        ↓
Optional Grafana annotations API
        ↓
InfluxDB bucket: kafka_events
        ↓
Grafana dashboards / monitoring
```

Flow behavior:

- Bytewax reads from Kafka brokers configured by `KAFKA_BOOTSTRAP_SERVERS`.
- The flow decodes JSON payloads and enriches them with Kafka metadata (topic, partition, offset, processed timestamp).
- Sensor readings are flattened into field-level metrics in the Influx sink.
- Data points are written to InfluxDB measurement `edge_computer_stats`.
- If `GRAFANA_API_KEY` is provided, each event is also posted to Grafana annotations as best effort.

Storage/monitoring stack behavior from `storage.docker-compose.yaml`:

- `influxdb` runs on port 8086 and initializes org `iceberg` with bucket `kafka_events`.
- `grafana` runs on port 3000 and is provisioned from `grafana-provisioning/`.
- `bytewax` consumes Kafka continuously and writes to InfluxDB.

### Compose Services

The main services defined in `docker-compose.yaml` are:

- `postgres`
- `airflow-apiserver`
- `airflow-scheduler`
- `airflow-dag-processor`
- `airflow-init`
- `spark-master`
- `spark-worker`
- `kafka`
- `kafka-ui`

The Airflow containers mount the DAGs, Spark jobs, logs, config, plugins, and great-expectation folders from the repo. The Spark containers mount the DAGs and `spark_jobs/` folders so job files are available inside the cluster.

## Active Edge-Device Flow

The edge-device pipeline is scheduled by Airflow every 30 minutes:

- DAG: `dags/edge_device.py`
- Schedule: `*/30 * * * *`
- Catchup: disabled
- DAG id: `edge_device_medallion_30m`

```text
Kafka topic: edge_computer_stats
        ↓
Airflow DAG: edge_device_medallion_30m
        ↓
Spark bronze job
        ↓
Iceberg table: local.edge_device.edge_device_bronze
        ↓
Great Expectations checkpoint (Bronze quality gate)
        ↓
Spark silver job
        ↓
Iceberg table: local.edge_device.edge_device_silver
        ↓
Great Expectations checkpoint (Silver quality gate)
        ↓
Spark gold job
        ↓
Iceberg table: local.edge_device.edge_device_gold
        ↓
Great Expectations checkpoint (Gold quality gate)
        ↓
Spark SQL engine over Iceberg catalog
        ↓
Metabase dashboards and ad-hoc analytics
```

### Orchestration Details

The target Airflow orchestration for this flow is:

- `edge_device_bronze`
- `ge_validate_bronze`
- `edge_device_silver`
- `ge_validate_silver`
- `edge_device_gold`
- `ge_validate_gold`
- `serve_gold_via_spark_sql`

The first three tasks are Spark transformation jobs. The `ge_validate_*` tasks run Great Expectations checkpoints as quality gates between layers. The final serving task publishes Gold data through Spark SQL so Metabase can query the curated Iceberg tables.

The DAG resolves MinIO settings from the Airflow connection configured in the UI. It falls back to `aws_default` and then environment variables if the named connection is missing.

The DAG passes the same Spark configuration to each task:

- Iceberg Spark extensions enabled
- Spark `local` Hadoop catalog
- S3A warehouse path in MinIO
- S3A endpoint, access key, and secret key
- UTC session timezone

It also passes the runtime environment for Kafka and MinIO so the Spark jobs can run in the containerized Spark environment without hardcoding credentials.

## Data Quality Layer

Great Expectations is used as a quality gate between medallion layers:

- Bronze checks validate schema shape and ingestion completeness before Silver consumes Bronze outputs.
- Silver checks validate metric normalization and null handling before Gold aggregation runs.
- Gold checks validate aggregation correctness and business-facing constraints before serving.

These checkpoints are designed to fail fast in Airflow, preventing downstream propagation of bad data.

## SQL Serving for BI

After Gold is materialized and validated, Spark SQL is used as the serving engine on top of the Iceberg catalog.

- Spark SQL exposes the curated Gold tables as queryable datasets.
- Metabase connects to the Spark SQL endpoint for dashboards and exploration.
- This keeps BI reads on curated, quality-checked Gold data instead of raw layers.

## Spark Job Behavior

### Shared Helpers

File: `spark_jobs/edge_device_common.py`

This file defines the shared runtime setup for all edge-device Spark jobs:

- Normalize MinIO endpoints for in-container and host usage
- Resolve MinIO and Kafka configuration from environment variables
- Create the MinIO bucket if it does not already exist
- Build a Spark session with Iceberg and S3A settings
- Create the `local.edge_device` namespace and the three Iceberg tables if needed

### Bronze Job

File: `spark_jobs/edge_device_bronze_job.py`

Responsibilities:

- Read Kafka messages from `edge_computer_stats`
- Parse the JSON envelope emitted by `kafka/producer.py`
- Extract the first reading and device metadata
- Convert the Kafka message into one Bronze row per event
- Write to `local.edge_device.edge_device_bronze`

Incremental behavior:

- Uses Kafka structured streaming
- Starts from `latest`
- Uses a checkpoint directory under `/opt/spark_jobs/checkpoints/edge_device_medallion/bronze`
- Uses `availableNow=True` so each Airflow run processes the currently available batch and stops

### Silver Job

File: `spark_jobs/edge_device_silver_job.py`

Responsibilities:

- Read the Bronze Iceberg table
- Skip Bronze `batch_id` values that already exist in Silver
- Parse the nested `raw_value` payload into metric rows
- Explode each metric map into one row per metric
- Filter out null metric keys
- Write to `local.edge_device.edge_device_silver`

### Gold Job

File: `spark_jobs/edge_device_gold_job.py`

Responsibilities:

- Read the Silver Iceberg table
- Skip Silver `batch_id` values that already exist in Gold
- Filter to numeric metrics
- Bucket event times into half-hour windows
- Aggregate count, average, min, and max by time window, source, and metric key
- Write to `local.edge_device.edge_device_gold`

## Reader and CSV Export (Utility Path)

### Reader Job

File: `spark_jobs/edge_device_reader_job.py`

This job is an auxiliary utility path. It reads the three Iceberg tables and does two things:

- Prints row counts and sample rows to the console
- Exports one CSV snapshot per table

The job uses the same Spark catalog and MinIO settings as the pipeline jobs.

### Wrapper Script

File: `test.py`

This script is the host-side entrypoint for the reader flow. It:

- Runs `spark-submit` inside the `spark-master` container
- Forwards MinIO and edge-device environment variables into the container
- Forces Ivy cache to `/tmp/.ivy2` so dependency resolution works in the container
- Copies the generated CSV files back to `output/edge_device/` on the host

Expected exported files:

- `output/edge_device/edge_device_bronze.csv`
- `output/edge_device/edge_device_silver.csv`
- `output/edge_device/edge_device_gold.csv`

## Storage Model

### Iceberg Catalog

The active edge-device pipeline uses Spark’s `local` Hadoop catalog.

- Catalog name: `local`
- Catalog type: `hadoop`
- Warehouse path pattern: `s3a://<bucket>/warehouse`

Tables used by the edge-device flow:

- `local.edge_device.edge_device_bronze`
- `local.edge_device.edge_device_silver`
- `local.edge_device.edge_device_gold`

### MinIO Layout

MinIO stores the warehouse data and metadata. The current edge-device tables end up under a path like:

```text
<bucket>/warehouse/edge_device/
  edge_device_bronze/
  edge_device_silver/
  edge_device_gold/
```

The CSV export is written to a writable container-local temp directory first and then copied back to the host workspace.

## Incremental Semantics

The pipeline is incremental rather than full reload:

- Bronze advances through Kafka offsets via checkpoints.
- Silver processes only Bronze `batch_id` values that have not yet been materialized in Silver.
- Gold processes only Silver `batch_id` values that have not yet been materialized in Gold.

This keeps every Airflow run scoped to new data and avoids reprocessing the full history on each schedule.

## Legacy Sample Flow

The repository still includes an older Iceberg example in `dags/iceberg.py` and `spark_jobs/iceberg_write_job.py`.

That flow is separate from the edge-device pipeline:

- It uses PyIceberg instead of Spark for table writes.
- It defaults to a SQLite catalog stored at `iceberg_catalog.db`.
- It writes a demo table under `demo.sample_iceberg_table`.

Because of that, `iceberg_catalog.db` is not part of the active edge-device runtime. It only exists for the legacy sample DAG.

## Supporting Files

- `dags/edge_device.py` - 30-minute Spark medallion DAG
- `spark_jobs/edge_device_common.py` - shared Spark/MinIO helpers
- `spark_jobs/edge_device_bronze_job.py` - Kafka to Bronze
- `spark_jobs/edge_device_silver_job.py` - Bronze to Silver
- `spark_jobs/edge_device_gold_job.py` - Silver to Gold
- `great_expectation/` - Great Expectations suites and checkpoints for layer quality gates
- `spark_jobs/edge_device_reader_job.py` - read and export tables
- `test.py` - host wrapper for the reader job
- `bytewax/dataflow.py` - realtime Kafka to Influx/Grafana flow
- `bytewax/connector/influxdb.py` - Bytewax Influx sink with metric flattening
- `storage.docker-compose.yaml` - Bytewax + InfluxDB + Grafana runtime stack
- `dags/kafka_producer_dag.py` - manual Kafka realtime demo
- `dags/iceberg.py` - legacy PyIceberg sample DAG
- `spark_jobs/iceberg_write_job.py` - legacy Spark sample writer

## Operational Notes

- The Spark submit path includes Iceberg, Hadoop AWS, Kafka, and AWS SDK dependencies so Spark can talk to MinIO and Kafka in the same container network.
- `output/` and `iceberg_catalog.db` are ignored in Git because they are runtime artifacts, not source inputs.
- The Docker stack is development-oriented and is intended to run locally, not as a production deployment.
