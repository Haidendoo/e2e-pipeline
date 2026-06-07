# 🔗 Hướng dẫn kết nối End-to-End: System Monitoring → EdgeX → Pipeline → Dashboard

## Tổng quan luồng dữ liệu

```
┌──────────────────────────────────────────────────────────────────────────┐
│  EDGE LAYER (thiết bị biên — RPi hoặc máy local)                         │
│                                                                          │
│  [system_monitor_agent.py]                                               │
│  psutil → 25 metrics (CPU/Mem/Net/Disk/Temp...)                          │
│       │ HTTP POST mỗi 5 giây                                             │
│       ↓                                                                  │
│  [EdgeX device-rest :59986]                                              │
│       │ (Device Profile + Device tự động đăng ký khi khởi động)          │
│       ↓                                                                  │
│  [EdgeX core-data :59880] → lưu readings                                │
│       │ internal MQTT MessageBus                                          │
│       ↓                                                                  │
│  [EdgeX App Service] → filter → transform JSON                           │
│       │ MQTT publish → topic: edgex_system_metrics_raw                   │
│       ↓                                                                  │
│  [MQTT-Kafka Bridge]                                                     │
│       │ Kafka produce → topic: edgex_system_metrics                      │
└───────┼──────────────────────────────────────────────────────────────────┘
        │
        ↓ (cross-host: qua localhost:9094 hoặc shared network)
┌──────────────────────────────────────────────────────────────────────────┐
│  E2E PIPELINE LAYER                                                      │
│                                                                          │
│  [Kafka :9092/9094]  topic: edgex_system_metrics                         │
│       │                                                                  │
│       ↓ Airflow DAG: edgex_system_monitoring_5m (*/5 * * * *)           │
│  [Spark Bronze]  → local.edgex.edgex_bronze (Iceberg/MinIO)             │
│  [Spark Silver]  → local.edgex.edgex_silver (normalized readings)        │
│  [Spark Gold]    → local.edgex.edgex_gold (5-min aggregates)             │
│       │                                                                  │
│  [Trino :8080]  ← query engine                                           │
│       │                                                                  │
│  [Grafana :3000] ← Dashboard: "EdgeX System Monitoring"                  │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## ⚡ Khởi động nhanh

### Bước 1: Khởi động EdgeX Foundry

```bash
cd EdgeX-Foundry-main/

# Cấu hình Kafka bootstrap servers (IP/hostname của máy chạy E2E pipeline)
# Nếu cùng máy: dùng localhost hoặc host-gateway
export KAFKA_BOOTSTRAP_SERVERS=localhost:9094

# Khởi động EdgeX stack (Device Profile và Device sẽ tự động đăng ký)
docker compose -f docker-compose-no-secty.yml up -d
```

> **Lưu ý**: Service `edgex-init-registration` sẽ tự chờ EdgeX sẵn sàng rồi đăng ký profile + device. Xem log bằng:
> ```bash
> docker logs edgex-init-registration -f
> ```

### Bước 2: Kiểm tra đăng ký Device Profile và Device

```bash
# Kiểm tra profile
curl http://localhost:59881/api/v3/deviceprofile/name/RPi-REST-Profile-v2 | python -m json.tool

# Kiểm tra device
curl http://localhost:59881/api/v3/device/name/RPi4-REST-v2 | python -m json.tool

# Hoặc chạy script thủ công
python scripts/register_profile_and_device.py --host localhost
```

### Bước 3: Chạy System Monitor Agent (trên thiết bị biên)

```bash
cd EdgeX-Foundry-main/

# Cài dependencies
pip install psutil requests

# Chạy agent (gửi dữ liệu lên EdgeX mỗi 5 giây)
python src/system_monitor_agent.py --host localhost --port 59986

# Nếu EdgeX chạy trên máy khác (ví dụ IP 192.168.1.100):
python src/system_monitor_agent.py --host 192.168.1.100

# Test 1 lần rồi thoát (để verify kết nối)
python src/system_monitor_agent.py --test-one-shot
```

### Bước 4: Verify dữ liệu đến EdgeX

```bash
# Xem readings mới nhất của device
curl "http://localhost:59880/api/v3/reading/device/name/RPi4-REST-v2?limit=25" | python -m json.tool

# Xem một resource cụ thể (ví dụ CPUUsage)
curl "http://localhost:59880/api/v3/reading/device/name/RPi4-REST-v2/resourceName/CPUUsage?limit=5"
```

### Bước 5: Verify MQTT và Kafka

```bash
# Kiểm tra MQTT topic (cần mosquitto_sub)
mosquitto_sub -h localhost -p 1883 -t "edgex_system_metrics_raw" -v

# Kiểm tra Kafka topic (qua Kafka UI)
# Mở: http://localhost:8088 → Topic: edgex_system_metrics
```

### Bước 6: Khởi động E2E Pipeline

```bash
cd e2e-pipeline-main/

# Copy và cấu hình .env
copy .env .env.local
# Chỉnh sửa EDGEX_HOST nếu EdgeX chạy trên máy khác

# Khởi động storage stack (MinIO, InfluxDB, Grafana)
docker compose -f storage.docker-compose.yaml up -d

# Khởi động pipeline stack (Airflow, Spark, Kafka)
docker compose up -d

# Chờ Airflow init xong (~2-3 phút), sau đó đăng ký connections:
bash connection.sh
```

### Bước 7: Đăng ký Airflow Connections

```bash
# Chạy script đăng ký tất cả connections
bash connection.sh

# Hoặc dùng Airflow UI: http://localhost:8082
# Admin → Connections → thêm:
#   - edgex_core_data: HTTP, host=localhost, port=59880
#   - edgex_core_metadata: HTTP, host=localhost, port=59881
#   - trino_default: Trino, host=trino, port=8080
```

### Bước 8: Kích hoạt DAG

```bash
# Qua Airflow CLI
docker exec e2e-pipeline-airflow-scheduler-1 airflow dags unpause edgex_system_monitoring_5m

# Trigger thủ công lần đầu
docker exec e2e-pipeline-airflow-scheduler-1 airflow dags trigger edgex_system_monitoring_5m
```

### Bước 9: Xem Dashboard

1. Mở **Grafana**: http://localhost:3000 (admin/adminpassword)
2. Vào **Dashboards → EdgeX → EdgeX System Monitoring**
3. Chọn device `RPi4-REST-v2` từ dropdown
4. Dashboard tự refresh mỗi 30 giây

---

## 🗂 Files mới được tạo

### EdgeX Foundry

| File | Mô tả |
|------|-------|
| `scripts/register_profile_and_device.py` | Auto-register Device Profile + Device qua API |
| `scripts/wait_and_register.sh` | Bash wrapper cho Docker init |
| `scripts/mqtt_to_kafka_bridge.py` | MQTT → Kafka bridge |
| `src/system_monitor_agent.py` | Agent thu thập 25 metrics |
| `config/app-service-kafka.env` | Config cho EdgeX App Service |

### E2E Pipeline

| File | Mô tả |
|------|-------|
| `spark_jobs/edgex_common.py` | Shared Spark utilities + Iceberg schemas |
| `spark_jobs/edgex_bronze_job.py` | Kafka → Iceberg Bronze (explode readings) |
| `spark_jobs/edgex_silver_job.py` | Bronze → Silver (normalize + cast) |
| `spark_jobs/edgex_gold_job.py` | Silver → Gold (5-min aggregates) |
| `dags/edgex_system_monitoring_dag.py` | Airflow DAG (*/5 * * * *) |
| `grafana-provisioning/dashboards/dashboards.yaml` | Grafana dashboard provider |
| `grafana-provisioning/dashboards/edgex_system_monitoring.json` | Dashboard JSON (CPU/Mem/Net/Disk) |
| `.env` | Environment variables đầy đủ |

---

## 🔧 Cấu hình mạng

### Cùng một máy (phổ biến nhất)

```
EdgeX stack ──────────────────────────────────────────
  edgex-mqtt-broker :1883
  edgex-device-rest :59986
  edgex-core-data   :59880
  edgex-app-kafka-export → publish MQTT :1883
  mqtt-kafka-bridge → consume MQTT → produce Kafka localhost:9094

E2E Pipeline stack ───────────────────────────────────
  kafka :9092 (internal) / :9094 (external/host-mapped)
  airflow :8082
  grafana :3000
  trino :8080
```

KAFKA_BOOTSTRAP_SERVERS=localhost:9094 (trong EdgeX .env)

### Hai máy khác nhau

Nếu EdgeX chạy trên máy A (192.168.1.5) và E2E Pipeline trên máy B (192.168.1.10):

```bash
# Trên máy A (EdgeX) — .env
KAFKA_BOOTSTRAP_SERVERS=192.168.1.10:9094

# Trên máy B (E2E Pipeline) — docker-compose.yaml Kafka config
KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:9092,EXTERNAL://192.168.1.10:9094
```

---

## 🧪 Troubleshooting

| Triệu chứng | Kiểm tra |
|---|---|
| Agent báo lỗi kết nối EdgeX | `curl http://localhost:59986/api/v3/ping` → phải trả 200 |
| Không thấy readings trong EdgeX | Kiểm tra device đã đăng ký chưa: `curl localhost:59881/api/v3/device/name/RPi4-REST-v2` |
| MQTT bridge không nhận events | `docker logs edgex-mqtt-kafka-bridge` — kiểm tra MQTT connection |
| Kafka topic rỗng | `docker logs edgex-mqtt-kafka-bridge` — kiểm tra forward count |
| Airflow DAG fails | Kiểm tra connections đã đăng ký chưa: `docker exec ... airflow connections list` |
| Grafana không có data | Kiểm tra Trino: `http://localhost:8080/ui/` → xem catalog `iceberg.edgex.edgex_gold` |

---

## 📊 Iceberg Tables

| Table | Mô tả |
|---|---|
| `local.edgex.edgex_bronze` | Raw readings từ Kafka (1 row / reading) |
| `local.edgex.edgex_silver` | Normalized values với numeric cast |
| `local.edgex.edgex_gold` | 5-min aggregates per device/resource |

Query via Trino:
```sql
-- Xem dữ liệu gần nhất
SELECT * FROM iceberg.edgex.edgex_gold
WHERE device_name = 'RPi4-REST-v2'
ORDER BY window_start DESC
LIMIT 50;

-- CPU Usage trung bình theo giờ
SELECT DATE_TRUNC('hour', window_start) AS hour,
       AVG(avg_value) AS cpu_avg
FROM iceberg.edgex.edgex_gold
WHERE resource_name = 'CPUUsage'
GROUP BY 1 ORDER BY 1 DESC;
```
