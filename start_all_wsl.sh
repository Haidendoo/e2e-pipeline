#!/bin/bash

echo "🚀 BẮT ĐẦU KHỞI ĐỘNG HỆ THỐNG E2E PIPELINE TRÊN WSL"
echo "------------------------------------------------------"

# Lấy thư mục gốc (chứa 3 thư mục con)
ROOT_DIR=$(cd "$(dirname "$0")" && pwd)

# =====================================================================
# BƯỚC 1: Khởi động E2E Pipeline (Kafka, Airflow, Spark, Trino)
# =====================================================================
echo -e "\n[1/5] Đang khởi động E2E Data Pipeline (Kafka, Airflow, Spark, Trino)..."
cd "$ROOT_DIR/e2e-pipeline-main"
docker compose up -d

echo "⏳ Đợi 25 giây để Kafka và Postgres khởi động hoàn tất..."
sleep 25

# =====================================================================
# BƯỚC 2: Khởi động Storage Stack (MinIO, InfluxDB, Grafana, Bytewax)
# =====================================================================
echo -e "\n[2/5] Đang khởi động Storage Stack (MinIO, InfluxDB, Grafana, Bytewax)..."
cd "$ROOT_DIR/e2e-pipeline-main"
docker compose -f storage.docker-compose.yaml up -d

echo "⏳ Đợi 15 giây để MinIO và InfluxDB khởi động..."
sleep 15

# Kết nối Bytewax vào network của Kafka (e2e-pipeline-main_default)
# để Bytewax có thể dùng hostname "kafka:9092" thay vì localhost
echo "🔗 Kết nối Bytewax vào network của Kafka..."
docker network connect e2e-pipeline-main_default bytewax 2>/dev/null || echo "   (Bytewax đã trong network)"
docker restart bytewax
sleep 5

# =====================================================================
# BƯỚC 3: Khởi động EdgeX Foundry & MQTT-Kafka Bridge
# =====================================================================
echo -e "\n[3/5] Đang khởi động EdgeX Foundry & MQTT-Kafka Bridge..."
cd "$ROOT_DIR/EdgeX-Foundry-main"
docker compose -f docker-compose-no-secty.yml up -d

echo "⏳ Đợi 20 giây để EdgeX đăng ký Device Profile..."
sleep 20

# =====================================================================
# BƯỚC 4: Khởi động HPC Monitoring (Telegraf Agent)
# =====================================================================
echo -e "\n[4/5] Đang khởi động HPC Monitoring (Telegraf Agent)..."
cd "$ROOT_DIR/hpc-monitoring-system-old-thesis-main/multidisciplinary"
docker compose up -d --build

# =====================================================================
# BƯỚC 5: Kích hoạt Airflow DAG
# =====================================================================
echo -e "\n[5/5] Đang kích hoạt Airflow DAG..."
cd "$ROOT_DIR/e2e-pipeline-main"
docker compose exec -T e2e-pipeline-main-airflow-scheduler-1 airflow dags unpause edgex_system_monitoring_5m 2>/dev/null
docker exec e2e-pipeline-main-airflow-scheduler-1 airflow dags trigger edgex_system_monitoring_5m 2>/dev/null || true

echo "------------------------------------------------------"
echo "✅ HOÀN TẤT! CÁC DỊCH VỤ ĐÃ ĐƯỢC KHỞI ĐỘNG."
echo ""
echo "📊 Truy cập các giao diện:"
echo "   - Grafana Dashboards : http://localhost:3000  (admin / adminpassword)"
echo "   - Airflow UI         : http://localhost:8082  (airflow / airflow)"
echo "   - Kafka UI           : http://localhost:8088"
echo "   - EdgeX UI           : http://localhost:4000"
echo "   - MinIO Storage      : http://localhost:9002  (minioadmin / minioadmin)"
echo "   - InfluxDB UI        : http://localhost:8086  (admin / adminpassword)"
echo ""
echo "💡 Theo dõi luồng data (mở terminal mới):"
echo "   docker logs -f telegraf-bridge         # Telegraf -> EdgeX"
echo "   docker logs -f edgex-mqtt-kafka-bridge # EdgeX -> Kafka"
echo "   docker logs -f bytewax                 # Kafka -> InfluxDB"
echo ""
echo "🛑 Để dừng toàn bộ: ./stop_all_wsl.sh"
