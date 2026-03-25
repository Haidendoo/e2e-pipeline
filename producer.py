import json
import time
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer

# Cấu hình Kafka Broker 
# Dùng tên service nội bộ của docker-compose: kafka:29092
KAFKA_BROKER = 'kafka:29092'
TOPIC = 'edgex_telegraf_topic'

def load_data_templates(filepath):
    """
    Hàm đọc file edgeX_recv_data.json
    Nó sẽ nạp toàn bộ các block dữ liệu gốc làm mẫu để tí nữa tái sử dụng.
    """
    templates = []
    buffer = ""
    brace_count = 0
    try:
        with open(filepath, "r", encoding='utf-8') as f:
            for line in f:
                buffer += line
                brace_count += line.count('{') - line.count('}')
                # Parse chừng nào cân bằng dấu ngoặc thì đó là 1 đoạn JSON Event
                if brace_count == 0 and buffer.strip():
                    try:
                        templates.append(json.loads(buffer))
                    except Exception as e:
                        print(f"Cảnh báo: Lỗi lúc đang phân tích file JSON mẫu: {e}")
                    buffer = ""
    except FileNotFoundError:
        print(f"Không tìm thấy file {filepath}! Vui lòng để đúng tên file trong thư mục này.")
        
    return templates

# Khởi tạo Kafka Producer với cơ chế tự động thử lại khi chưa có Broker
producer = None
while not producer:
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print(f" Kết nối thành công tới Kafka tại {KAFKA_BROKER}")
    except Exception as e:
        print(f" Đang chờ Kafka ({KAFKA_BROKER}) khởi động... Lỗi: {e}")
        time.sleep(5)

def build_realtime_message(templates):
    """
    Nhặt 1 template ngẫu nhiên đã đọc từ file và CẬP NHẬT LẠI TIMESTAMP + ID 
    để giả lập có tin đồ thị mới nhưng vẫn giữ nguyên cấu trúc gốc
    """
    # Lấy 1 bản ghi bất kì làm template (gpu, cpu, mem, etc..)
    template = random.choice(templates)
    
    # Copy sâu để không làm dơ template list ngoài kia
    msg = json.loads(json.dumps(template))
    
    # Sinh ID mới và Gán thời gian hiện tại
    msg["id"] = str(uuid.uuid4())
    current_time_ns = int(time.time() * 1e9)
    msg["origin"] = current_time_ns
    
    # Cập nhật thời gian origin thực của Telegraf Reading
    if "readings" in msg:
        for r in msg["readings"]:
            # Dịch đi xíu cho ngẫu nhiên hợp lý với time lúc nhận
            r["origin"] = current_time_ns - random.randint(1000, 50000)
            
    return msg

if __name__ == "__main__":
    FILE_PATH = "edgeX_recv_data.json"
    print(f"Đang đọc data mẫu gốc từ file: {FILE_PATH}...")
    templates = load_data_templates(FILE_PATH)
    
    if not templates:
        print("Lỗi: Không nạp được dữ liệu. Hãy kiểm tra lại file data!")
        exit(1)
        
    print(f"✅ Đã nạp thành công {len(templates)} mẫu cấu trúc thực từ file.")
    print(f"🚀 Bắt đầu gửi BATCH 500 messages lên Topic Kafka '{TOPIC}' mỗi 2s...")
    
    try:
        while True:
            # Vòng lặp bắn gom đúng 500 bản ghi thay đổi timestamp
            for _ in range(500):
                new_data = build_realtime_message(templates)
                producer.send(TOPIC, value=new_data)
            
            # Gửi lên 1 đợt sau vòng lặp
            producer.flush()
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Đã xuất xưởng thành công 500 records.")
            
            # Ngủ đúng 2s cho chu kỳ tiếp
            time.sleep(2)
    except KeyboardInterrupt:
        print("\n⏹ Đã dừng tiến trình giả lập Real-time.")
    finally:
        producer.close()
