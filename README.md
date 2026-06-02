# End-to-End Data Pipeline with Iceberg, Spark, Kafka & Airflow

A complete data engineering stack demonstrating streaming and batch processing with Apache Airflow, Spark, Iceberg, Kafka, and MinIO.

The current edge-device pipeline is a 30-minute Spark medallion flow:
- Bronze ingests Kafka events into Iceberg.
- Silver normalizes the nested payload into metric rows.
- Gold aggregates numeric metrics into summary rows.

The repository also includes a reader script that prints the Iceberg tables and exports CSV snapshots for quick inspection.

## 🏗 Architecture

### Two-Part Data Pipeline

```
                                ╔═════════════════════════════════════════════════════════════════════╗
                                ║                     ORCHESTRATION LAYER                             ║
                                ║                      Apache Airflow 3.1.6                           ║
                                ╚═════════════════════════════════════════════════════════════════════╝
                                                                 │
                                                ┌────────────────┼────────────────┐
                                                │                │                │
                                                ▼                ▼                ▼


                            ┌─────────────────────────────────┐  ┌───────────────────────────────┐
                            │     STREAM PROCESSING           │  │    BATCH PROCESSING           │
                            ├─────────────────────────────────┤  ├───────────────────────────────┤
                            │                                 │  │                               │
                            │  Apache Kafka 3.8.0 (KRaft)     │  │  Apache Airflow               │
                            │  ├─ Topic: iceberg_events       │  │  ├─ Scheduler triggers jobs   │
                            │  ├─ Partitions: 1               │  │  ├─ DAG: spark_test_dag.py    │
                            │  ├─ Replication: 1              │  │  └─ Cron: On schedule         │
                            │  ├─ Python: kafka-python 2.3.0  │  │                               │
                            │  └─ UI: Kafka UI (8088)         │  │  Apache Spark 3.4.1 (Batch)   │
                            │                                 │  │  ├─ Master: spark://7077      │
                            │  Events (JSON):                 │  │  ├─ Workers: 2 cores/1GB      │
                            │  ├─ user_signup                 │  │  └─ Executor: LocalExecutor   │
                            │  ├─ purchase                    │  │                               │
                            │  ├─ page_view                   │  │  Batch Job:                   │
                            │  └─ user_logout                 │  │  └─ spark_jobs/               │
                            │                                 │  │     iceberg_write_job.py      │
                            │  ↓                              │  │                               │
                            │  Spark Streaming                │  │  Processing:                  │
                            │  ├─ Source: Kafka               │  │  ├─ Scheduled by Airflow      │
                            │  ├─ Format: JSON                │  │  ├─ Transforms batch data     │
                            │  ├─ Processing: Real-time       │  │  └─ Writes to Iceberg         │
                            │  └─ Output: Iceberg Tables      │  │                               │
                            │                                 │  │  Iceberg Lakehouse:           │
                            │  Streaming Job:                 │  │  ├─ Version: 1.4.3            │
                            │  └─ (To be implemented)         │  │  ├─ Format: Parquet + Avro    │
                            │                                 │  │  ├─ Catalog: Hadoop (SQL)     │
                            │                                 │  │  └─ Storage: S3A compatible   │
                            │                                 │  │                               │
                            └─────────────────────────────────┘  └───────────────────────────────┘
                                                │                                        │
                                (Real-time)     │                                        │ (Periodic)
                                                │                                        │
                                                └────────────────┬───────────────────────┘
                                                                 │
                                                ┌────────────────┴────────────────┐
                                                │                                 │
                                                ▼                                 ▼
                                        ┌─────────────────┐          ┌──────────────────┐
                                        │  PostgreSQL 16  │          │  MinIO (S3 API)  │
                                        │  ├─ Airflow DB  │          │  ├─ Endpoint:    │
                                        │  └─ Metadata    │          │  │   9000/9001   │
                                        └─────────────────┘          │  ├─ Buckets:     │
                                                                     │  │   - warehouse │
                                                                     │  │   - raw-data  │
                                                                     │  │   - delta-lake│
                                                                     │  └─ Data: Persist| 
                                                                     └──────────────────┘
```
### Data Flow

**Stream Path (Real-time - Continuous):**
```
Kafka Topic (iceberg_events)
        ↓
Spark Structured Streaming Job
        ├─ Reads events in real-time
        ├─ Transforms/validates
        └─ Writes to Iceberg tables (micro-batches)
        
        Output → Iceberg Tables in MinIO (continuous)
```

**Batch Path (Scheduled by Airflow):**
```
Airflow DAG (spark_test_dag)
        ↓
Trigger: On schedule (or manual)
        ↓
Spark Batch Job (iceberg_write_job.py)
        ├─ Reads batch data
        ├─ Transforms/aggregates
        └─ Writes to Iceberg tables
        
        Output → Iceberg Tables in MinIO (periodic)
```

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- 4GB+ RAM
- 10GB+ Disk space

### Services

| Service | Port | URL |
|---------|------|-----|
| Airflow Web UI | 8082 | `http://localhost:8082` |
| Airflow API | 8082 | `http://localhost:8082/api/v1` |
| MinIO Console | 9001 | `http://localhost:9001` |
| MinIO S3 API | 9000 | `http://localhost:9000` |
| Kafka Broker | 9092 | `localhost:9092` (internal) / `localhost:9094` (host) |
| Kafka UI | 8088 | `http://localhost:8088` |
| Spark Master | 9090 | `http://localhost:9090` |
| Trino Engine | 8080 | `jdbc:trino://localhost:8080` |

### Start All Services

```bash
docker compose up -d
```

Verify services:
```bash
docker compose ps
```

### Stop All Services

```bash
docker compose down
```

## 📊 Included DAGs & Jobs

### 1. **Kafka Producer/Consumer DAG**
**File:** `dags/kafka_producer_dag.py`

Demonstrates streaming data with Kafka:
- Produces sample events (user_signup, purchase, page_view, user_logout)
- Consumes and displays events

**Trigger:**
```bash
docker compose exec airflow-scheduler airflow dags trigger kafka_producer_dag
```

**Expected Output:**
```
✓ Event 1 -> topic: iceberg_events, partition: 0, offset: 0
✓ Event 2 -> topic: iceberg_events, partition: 0, offset: 1
...
✓ Successfully produced 4 events to Kafka topic 'iceberg_events'
✓ Consumed 4 events from Kafka
```

### 2. **Spark + Iceberg Write DAG**
**File:** `dags/spark_test.py`

Writes Iceberg tables to MinIO:
- Task 1: `print_spark_info` - Check Spark cluster
- Task 2: `submit_spark_job` - Run test Spark job
- Task 3: `write_iceberg_to_minio` - Write data to Iceberg format

**Spark Job:** `spark_jobs/iceberg_write_job.py`

**Features:**
- KRaft mode (no ZooKeeper)
- Automatic bucket creation
- Schema validation
- Table versioning
- Metadata tracking

**Trigger:**
```bash
docker compose exec airflow-scheduler airflow dags trigger spark_test_dag
```

**Expected Output:**
```
✓ Bucket 'test-bucket' created
✓ Sample DataFrame created
✓ Data successfully written to local.spark_demo.products
✓ Reading back data from Iceberg table
```

### 3. **Edge Device Medallion DAG**
**File:** `dags/edge_device.py`

Runs the edge-device pipeline on a 30-minute cron schedule using three separate Spark tasks:

- `edge_device_bronze`
- `edge_device_silver`
- `edge_device_gold`

**Spark Jobs:**

- `spark_jobs/edge_device_bronze_job.py`
- `spark_jobs/edge_device_silver_job.py`
- `spark_jobs/edge_device_gold_job.py`

**Reader Script:**

- `test.py`

**What it does:**

- Reads simulated events from Kafka topic `edge_computer_stats`
- Writes Bronze, Silver, and Gold tables to MinIO-backed Iceberg
- Prints each table to the console
- Exports CSV snapshots to `output/edge_device/`

**Run the reader locally:**

```bash
uv run test.py
```

**Expected CSV files:**

```text
output/edge_device/edge_device_bronze.csv
output/edge_device/edge_device_silver.csv
output/edge_device/edge_device_gold.csv
```

## 🔧 Configuration

### Environment Variables (`.env`)

```env
AIRFLOW_UID=50000
AIRFLOW_API_BASE_URL=http://localhost:8082
AIRFLOW_WEBSERVER_BASE_URL=http://localhost:8082
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=test-bucket
EDGE_DEVICE_KAFKA_BOOTSTRAP=kafka:9092
EDGE_DEVICE_KAFKA_TOPIC=edge_computer_stats
EDGE_DEVICE_ICEBERG_NAMESPACE=edge_device
EDGE_DEVICE_CSV_OUTPUT_DIR=/tmp/edge_device_csv
```

### Airflow Credentials

- **Username:** airflow
- **Password:** airflow

### MinIO Credentials

- **Access Key:** minioadmin
- **Secret Key:** minioadmin

## 📚 Key Technologies

### Kafka (KRaft Mode)
- **Version:** 3.8.0 (Apache Kafka)
- **Mode:** KRaft (no ZooKeeper required)
- **Partitions:** 1
- **Replication Factor:** 1
- **Python Client:** `kafka-python` 2.3.0

### Spark
- **Version:** 3.4.1
- **Mode:** Local[2] (2 cores)
- **Iceberg Version:** 1.4.3 (compatible with Spark 3.4)
- **Hadoop AWS:** 3.3.4 (S3A support)

### Iceberg
- **Version:** 1.4.3
- **Catalog Type:** Hadoop (file-based with SQL)
- **Storage:** S3A compatible (MinIO)
- **Format:** Parquet (data) + Avro (metadata)

### Edge Device Medallion
- **Catalog Type:** Spark `local` Hadoop catalog
- **Storage:** S3A compatible (MinIO)
- **Tables:** `edge_device_bronze`, `edge_device_silver`, `edge_device_gold`

### MinIO
- **Version:** Latest
- **Mode:** Standalone
- **Data Location:** `/minio_data/` (persisted volume)

### Airflow
- **Version:** 3.1.6
- **Executor:** LocalExecutor
- **Database:** PostgreSQL 16
- **Base Image:** Apache Airflow official

## 📦 Data Storage

### MinIO Bucket Structure

```
test-bucket/
├── warehouse/
│   └── spark_demo/
│       └── products/
│           ├── data/
│           │   ├── 00000-*.parquet
│           │   └── 00001-*.parquet
│           └── metadata/
│               ├── v1.metadata.json
│               ├── *.avro
│               └── version-hint.text
├── raw-data/
├── delta-lake/
└── processed-data/
```

## 🔌 Kafka Topics

**Topic:** `iceberg_events`
- **Partitions:** 1
- **Schema:** JSON
- **Sample Event:**
  ```json
  {
    "event_id": 1,
    "event_type": "user_signup",
    "user_id": 101,
    "timestamp": "2026-02-04T15:07:00",
    "data": {
      "email": "user1@example.com",
      "country": "US"
    }
  }
  ```

## 🧪 Testing

### 1. Check Kafka Health

```bash
docker compose exec kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092
```

### 2. Produce Test Message

```bash
printf "test-message\n" | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic iceberg_events
```

### 3. Consume Messages

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic iceberg_events --from-beginning --max-messages 1
```

### 4. List Iceberg Tables in MinIO

```bash
docker compose exec airflow-scheduler python -c "
import boto3
s3 = boto3.client('s3', endpoint_url='http://minio:9000', aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin')
response = s3.list_objects_v2(Bucket='test-bucket', Prefix='warehouse/', MaxKeys=50)
for obj in response.get('Contents', []):
    print(obj['Key'])
"

```

### 5. Print and Export Edge Device Tables

```bash
uv run test.py
```

This prints the Bronze, Silver, and Gold tables and writes CSV snapshots to `output/edge_device/`.
```

## 🐛 Troubleshooting

### Kafka Connection Issues

```
"advertised.listeners listener names must be equal to or a subset of the ones defined in listeners"
```

**Solution:** Ensure `KAFKA_LISTENERS` includes all listener types in `KAFKA_ADVERTISED_LISTENERS`.

### Spark Iceberg Version Mismatch

```
"ClassNotFoundException: org.apache.spark.sql.catalyst.expressions.AnsiCast"
```

**Solution:** Use Iceberg 1.4.3 for Spark 3.4.1 (not 1.4.2).

### MinIO Bucket Not Found

```
"The specified bucket does not exist (NoSuchBucket)"
```

**Solution:** Iceberg write job auto-creates buckets. If issue persists:
```bash
docker compose exec minio /usr/bin/mc mb minio/test-bucket
```

### Reader Script Cannot Export CSV

The reader writes CSV files to `/tmp/edge_device_csv` inside the Spark container and copies them back to `output/edge_device/` on the host. If export fails, check that the `spark-master` container is running and that the host `output/` folder is writable.

### Airflow DAG Not Found

Ensure DAGs are in `/dags/` directory and restart:
```bash
docker compose restart airflow-scheduler
```

## 📖 Documentation Links

- [Apache Airflow](https://airflow.apache.org/)
- [Apache Kafka](https://kafka.apache.org/)
- [Apache Spark](https://spark.apache.org/)
- [Apache Iceberg](https://iceberg.apache.org/)
- [MinIO](https://min.io/)

## 🤝 Contributing

To extend this pipeline:

1. **Add new DAGs:** Place Python files in `dags/`
2. **Add Spark jobs:** Place scripts in `spark_jobs/`
3. **Install packages:** Update `requirements.txt` and rebuild
4. **Add services:** Update `docker-compose.yaml`
## Current Bug

The api server on http://localhost:8082/api/v2/dags is currently unavailble

## 📝 License

Apache License 2.0
