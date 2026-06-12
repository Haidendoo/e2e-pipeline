# End-to-End HPC Monitoring Data Pipeline

A complete Big Data engineering stack demonstrating streaming and batch processing for High-Performance Computing (HPC) edge nodes using Telegraf, EdgeX Foundry, Apache Kafka, Apache Airflow, Apache Spark, and Apache Iceberg (Medallion Architecture).

## 🏗 Architecture & Data Flow

The system captures host metrics (CPU, Memory, Disk, Network, System Load) and GPU metrics (NVIDIA) and flows through a hybrid real-time / batch pipeline:

```text
[ WSL Host / Edge Node ]
   1. Telegraf
        ├── Collects: cpu, mem, diskio, net, system, nvidia_smi
        └── Outputs: JSON HTTP (localhost:8085)
             ↓
   2. Telegraf-Bridge (Python Flask Docker Container)
        ├── Receives Telegraf JSON payload
        ├── Flattens & Maps to EdgeX resources (cpu_data, gpu_data, etc.)
        └── HTTP POST -> EdgeX device-rest API
             ↓
[ EdgeX Ecosystem ]
   3. EdgeX Foundry
        ├── device-rest (receives data)
        ├── core-data (validates against Telegraf-Full-Node-Profile)
        ├── Redis MessageBus
        └── app-service (MQTT Export)
             ↓
[ Streaming & Storage ]
   4. MQTT to Kafka Bridge
        └── Pushes data to Kafka topic: edgex_system_metrics
             ↓
[ Batch Processing / Lakehouse ]
   5. Apache Airflow (Scheduler)
        └── Triggers Spark Jobs every 5 minutes (edgex_system_monitoring_5m DAG)
             ↓
   6. Apache Spark & Iceberg (Medallion Architecture)
        ├── Bronze Job: Ingests raw JSON from Kafka, explodes EdgeX readings.
        ├── Silver Job: Parses nested metric strings into native Doubles.
        └── Gold Job: Aggregates metrics by 5-minute windows (avg, max, min).
             ↓
[ Presentation ]
   7. Trino & Grafana
        └── Dashboards query the Iceberg Gold tables via Trino.
```

## 📋 Data Schema & Resource Mapping

The pipeline maps Telegraf plugins to EdgeX resources in the `Telegraf-Full-Node-Profile`. The Silver layer explicitly extracts numeric values from the raw JSON payload to allow aggregations in the Gold layer.

| Telegraf Plugin | EdgeX Resource | Silver `metric_value_num` extraction | Description |
|-----------------|----------------|--------------------------------------|-------------|
| `cpu` | `cpu_data` | `$.usage_user` | CPU utilization |
| `mem` | `mem_data` | `$.used` | RAM used (Bytes) |
| `diskio` | `disk_data` | `$.read_bytes` | Disk I/O bytes read |
| `net` | `net_data` | `$.bytes_recv` | Network bytes received |
| `system` | `sys_data` | `$.load1` | System Load 1m |
| `processes` | `proc_data` | `$.running` | Running processes |
| `nvidia_smi` | `gpu_data` | `$.utilization_gpu` | GPU utilization (%) |

### Iceberg Medallion Schemas

1. **Bronze (`local.edgex.edgex_bronze`)**
   - Columns: `event_id`, `device_name`, `profile_name`, `resource_name`, `raw_value` (String JSON), `value_type`, `event_time`, `batch_id`
   - *Purpose*: Immutable raw event ledger.

2. **Silver (`local.edgex.edgex_silver`)**
   - Inherits Bronze columns + `metric_value_num` (Double).
   - *Purpose*: Normalized, typed, clean data ready for analysis.

3. **Gold (`local.edgex.edgex_gold`)**
   - Columns: `window_start`, `window_end`, `device_name`, `resource_name`, `avg_value`, `count_events`
   - *Purpose*: Time-series aggregations for Grafana dashboards.

---

## 🚀 Setup & Execution Guide

### 1. Start Services & Infrastructure (via WSL)
Open a **WSL Terminal** (or Git Bash/Ubuntu on Windows) and run the automated startup script from the root directory:
```bash
./start_all_wsl.sh
```
> *This script automatically orchestrates Airflow, Spark, Kafka, MinIO, Postgres, EdgeX Foundry, and the Bridge components.*

### 2. Start Telegraf Agent (Data Collection)
Open a normal **WSL** terminal, navigate to the `hpc-monitoring-system` folder, and run Telegraf inside WSL:
```powershell
cd hpc-monitoring-system
telegraf --config config/telegraf.conf
```

### 3. View Output Data Locally
To quickly verify that the pipeline is working, the Silver Spark job automatically outputs a formatted JSON file you can inspect locally:
```text
D:\e2e-pipeline-main\e2e-pipeline-main\spark_jobs\logs\edgex_silver_debug_output.json
```
*(Data in this file is sorted dynamically with the newest events appearing at the top).*

---

## 🧹 Reset & Clean Pipeline (Start Fresh)
To delete all historical data, reset the Iceberg tables, and start collecting from scratch, execute these commands inside WSL from the `e2e-pipeline-main` directory:

```bash
# 1. Delete physical warehouse files and Spark checkpoints in MinIO local mount
rm -rf ./minio_data/test-bucket/checkpoints/edgex_system_metrics_v2
rm -rf ./minio_data/test-bucket/warehouse/*

# 2. Clear Iceberg metadata from Airflow's PostgreSQL
docker exec e2e-pipeline-main-postgres-1 psql -U airflow -d airflow -c "DELETE FROM iceberg_tables; DELETE FROM iceberg_namespace_properties;"

# 3. Delete Kafka topic to flush buffered events
docker exec edgex-kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic edgex-events

# 4. Restart EdgeX and Bridge containers to re-initiate streams
docker restart telegraf-bridge edgex-app-kafka-export edgex-device-rest
```
After running this, trigger the Airflow DAG manually to recreate the Iceberg tables with fresh data.

---

## ⚙️ Service Ports Reference

| Service | Port | Local URL |
|---------|------|-----------|
| Airflow UI | 8082 | `http://localhost:8082` |
| MinIO Console | 9001 | `http://localhost:9001` |
| MinIO S3 API | 9000 | `http://localhost:9000` |
| Kafka UI | 8088 | `http://localhost:8088` |
| Kafka Broker | 9092 | `localhost:9092` |
| Spark Master | 9090 | `http://localhost:9090` |
| Grafana | 3000 | `http://localhost:3000` |
| Telegraf Bridge | 8085 | `http://localhost:8085` |
| Trino Engine | 8080 | `jdbc:trino://localhost:8080` |

## 🤝 Contributing & Extending
To extend this pipeline:
1. **New Metrics**: Add plugins to `telegraf.conf`, update the schema in `Telegraf-Full-Node-Profile` via EdgeX metadata, and modify mapping logic in `telegraf-bridge`.
2. **Spark Job Modifications**: Update scripts in the `spark_jobs/` folder.
3. **Airflow DAGs**: Place new workflows in the `dags/` folder.
