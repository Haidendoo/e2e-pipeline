# Realtime Data Pipeline: EdgeX -> Kafka -> Spark -> MinIO (Iceberg)

Hệ thống mô phỏng luồng dữ liệu thời gian thực (Real-time Data Pipeline) với kiến trúc:

1. **Producer**: Mô phỏng thiết bị IoT/EdgeX sinh dữ liệu liên tục. Đóng gói batch **500 messages** và bắn lên Kafka mỗi **2 giây**.
2. **Kafka**: Đóng vai trò là Message Broker đệm cấu hình thời gian thực.
3. **Spark Streaming (Consumer)**: Đọc luồng dữ liệu từ Kafka Topic, parse JSON schema, flatten data và ghi xuống Datalake.
4. **MinIO (Datalake)**: Lưu trữ Object Storage (S3-compatible). Spark ghi dữ liệu dưới dạng **Apache Iceberg** (hỗ trợ ACID transaction và lưu trữ vật lý bằng Parquet file).

## Yêu cầu hệ thống

- Docker & Docker Compose
- Python 3.8+ (Nếu muốn chạy script ở host thay vì Docker)
- Có đủ dung lượng RAM (Khuyến nghị Docker cấp >= 4GB RAM cho Kafka & Spark).

## Hướng dẫn chạy (Run Flow)

Toàn bộ hệ thống hiện đã được cấu hình chặt chẽ để chạy nội bộ qua Docker Compose (không cần cài thêm Python ở host). Các container sẽ tự động kết nối qua network nội bộ.

**Cách 1: Khởi động tất cả cùng lúc (Nhanh nhất - Khuyên dùng)**
Mở terminal và chạy 1 lệnh duy nhất:

```bash
docker-compose up -d
```

_Lưu ý: Bạn chọn cách này thì cả Producer (bắn data) và Spark (hứng data) sẽ chạy tự động ngầm bên trong. Tuy nhiên ở lần đầu tiên, Spark có thể mất vài phút tải thư viện nên luồng dữ liệu sẽ bắt đầu sinh ra sau khoảng 2-3 phút._

**Cách 2: Khởi động từng phần (Dành cho việc Debug/Quan sát rõ luồng)**

1. Dựng nền tảng (Kafka, UI, MinIO):
   `docker-compose up -d zookeeper kafka kafka-ui minio createbuckets`
2. Chạy Spark để hứng và xử lý dữ liệu:
   `docker-compose up -d spark-consumer` _(Đợi khoảng 1 phút cho Spark khởi động xong)_
3. Chạy nguồn phát dữ liệu (nguồn IoT):
   `docker-compose up -d producer`

## Giải thích & Phân tích Giao diện (UI) để kiểm tra luồng

Khi các container đang chạy (Up), hệ thống cung cấp 3 bộ giao diện cực kỳ trực quan để bạn kiểm tra dòng chảy dữ liệu từ lúc sinh ra đến lúc lưu trữ:

### 1. KAFKA UI - Kiểm tra Dữ liệu thô (Input Queue)

Đây là trạm thu phát đầu tiên, bạn sẽ thấy dữ liệu IoT được bơm vào liên tục.

- **URL**: 👉 `http://127.0.0.1:8085` _(Hoặc mở qua tab Ports của docker desktop/VS Code)_
- **Cách xem**:
  - Tại Menu bên trái, chọn cluster **`local`** $\rightarrow$ chọn **`Topics`** $\rightarrow$ bấm vào **`edgex_telegraf_topic`**.
  - Chuyển sang tab **`Messages`**. Ở đây dòng dữ liệu JSON sẽ đổ về liên tục. Bạn cứ tải lại trang (F5) là sẽ thấy lượng message tăng lên vùn vụt (500 tin/2 giây).

### 2. SPARK UI - Theo dõi tốc độ Streaming (Processing Core)

Đây là bộ não (Core Processor), cho bạn giám sát xem Spark có đang "nuốt" và ăn kịp luồng data khổng lồ kia hay không.

- **URL**: 👉 `http://127.0.0.1:4040`
- **Cách xem**:
  - Đợi khoảng vài phút để Spark nạp thư viện ở lần đầu. Khi UI xuất hiện, click chuyển ngay sang tab **`Streaming`**.
  - Các thống kê cần quan tâm:
    - **`Input Rate` (Biểu đồ vạch dọc):** Tốc độ Spark hút data, thường sẽ có nhịp độ rất đều.
    - **`Processing Time` (Đồ thị đường màu xanh):** Thời gian Spark cần để phân giải ra Iceberg. Nếu tốc độ này thấp hơn `< 2 giây` tức là cực kỳ an toàn, hệ thống không bị nghẽn (delay).

### 3. MINIO UI - Kho bãi Dữ liệu lưu trữ (Datalake Storage)

Nơi "yên nghỉ" cuối cùng của Data sau khi qua Spark, dữ liệu tại đây được nén lại sạch sẽ theo format Apache Iceberg để truy vấn.

- **URL**: 👉 `http://127.0.0.1:9001`
- **Đăng nhập**: User: `minioadmin` / Pass: `minioadmin`
- **Cách xem**:
  - Tại giao diện, ấn vào logo **`Object Browser`** ở menu nhỏ bên trái.
  - Sau đó nhấn theo đường dẫn sau: `datalake` $\rightarrow$ `warehouse` $\rightarrow$ `local` $\rightarrow$ `db` $\rightarrow$ `edgex_telegraf` $\rightarrow$ **`data`**
  - Trong này bạn sẽ thấy xuất hiện liên tục các Block dữ liệu mới (các file có phần mở rộng `.parquet`). Đây là những khối dữ liệu siêu nén chứa 500 cái dòng JSON ở trên và có khả năng truy xuất SQL thần tốc.

---

## Dừng & Reset hệ thống

Khi test xong, hãy chạy lệnh sau để dọn sạch sẽ và giải phóng cho máy:

```bash
docker-compose down -v
```

_(Tham số `-v` giúp xóa sạch volume của MinIO & Kafka, để lần sau up lên làm lại từ đầu)._
