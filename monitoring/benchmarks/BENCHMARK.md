# Data Platform Benchmarking Guide

Tài liệu này hướng dẫn cách sử dụng bộ công cụ benchmark để đánh giá hiệu năng của hệ thống E2E Data Platform (EdgeX - Kafka - Spark - Iceberg - Bytewax).

## 1. Thành phần Benchmark

Bộ benchmark bao gồm 4 script Python chính nằm trong thư mục `monitoring/benchmarks/`:

| Script | Mục tiêu | Tầng tác động |
| :--- | :--- | :--- |
| `benchmark_producer.py` | Đo Throughput (EPS) | Kafka Ingestion |
| `benchmark_bytewax.py` | Đo Real-time Latency | Bytewax -> InfluxDB |
| `benchmark_latency.py` | Đo End-to-End Latency | Kafka -> Spark -> Iceberg (Gold) |
| `benchmark_query.py` | Đo Query Performance | Trino / Serving Layer |

---

## 2. Quy trình thực hiện Benchmark

### Bước 1: Tạo tải (Load Generation)
Đẩy một lượng lớn dữ liệu giả lập vào Kafka để bắt đầu stress test.
```bash
# Đẩy 5000 records vào Kafka
uv run monitoring/benchmarks/benchmark_producer.py
```

### Bước 2: Đánh giá Luồng Real-time (Bytewax)
Luồng này xử lý streaming liên tục. Bạn có thể kiểm tra ngay lập tức.
```bash
uv run monitoring/benchmarks/benchmark_bytewax.py
```
*   **KPI kỳ vọng:** Latency < 2 giây.

### Bước 3: Đánh giá Luồng Medallion (Spark/Iceberg)
**Lưu ý quan trọng:** Luồng này được cấu hình chạy định kỳ mỗi **4 phút** (DAG `edgex_system_monitoring_4m`).
1. Chờ DAG hoàn thành ít nhất một chu kỳ (hoặc Trigger tay trên Airflow).
2. Chạy script đo độ trễ:
```bash
uv run monitoring/benchmarks/benchmark_latency.py
```
*   **KPI kỳ vọng:** Latency xấp xỉ thời gian chu kỳ (4-5 phút).

### Bước 4: Kiểm tra tốc độ truy vấn (Trino)
Đánh giá khả năng phản hồi của lớp Serving khi dữ liệu đã tích lũy lớn.
```bash
uv run monitoring/benchmarks/benchmark_query.py
```

---

## 3. Các chỉ số Benchmark quan trọng (KPIs)

1.  **Ingestion Throughput (EPS):** Số lượng sự kiện hệ thống có thể nhận mỗi giây mà không gây lag Kafka.
2.  **Processing Latency (E2E):** Tổng thời gian từ Edge đến Gold Layer. Đối với hệ thống này, mục tiêu là khớp với chu kỳ **4 phút** của Spark job.
3.  **Real-time Observability Latency:** Độ trễ từ Kafka đến Grafana (qua InfluxDB). Mục tiêu: **Mili giây**.
4.  **Query Execution Time:** Thời gian Trino thực hiện các lệnh aggregation phức tạp trên Iceberg.

---

## 4. Ghi chú cấu hình
*   **Kafka:** Sử dụng cổng `9094` (External) cho các script benchmark.
*   **Trino:** Hoạt động trên cổng `8060`.
*   **InfluxDB:** Hoạt động trên cổng `8086`.

---
*Tài liệu được cập nhật ngày 12/06/2026 bởi Gemini CLI.*
