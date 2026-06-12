import json
import time
import os
import uuid
from kafka import KafkaProducer
from faker import Faker

# Config - Lấy từ environment hoặc mặc định
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC = os.getenv("KAFKA_TOPIC_NAME", "edge_computer_stats")
TOTAL_RECORDS = int(os.getenv("BENCHMARK_RECORDS", "5000"))

fake = Faker()

def get_producer():
    try:
        return KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks=1, # Đợi ack từ leader để đảm bảo throughput vs reliability cân bằng
            batch_size=65536, # Tăng batch size để tối ưu throughput
            linger_ms=10      # Gom message lại gửi một lượt
        )
    except Exception as e:
        print(f"❌ Error connecting to Kafka: {e}")
        return None

def generate_benchmark_event():
    """Tạo event giả lập cấu hình EdgeX/Telegraf"""
    device_name = "Benchmark-Node-01"
    profile_name = "Benchmark-Profile"
    source_name = "cpu_data"
    now_ns = time.time_ns()
    
    metrics = {
        "host": "benchmark-host",
        "usage_user": round(fake.pyfloat(min_value=10.0, max_value=80.0), 4),
        "usage_system": round(fake.pyfloat(min_value=1.0, max_value=10.0), 4),
        "usage_idle": round(fake.pyfloat(min_value=10.0, max_value=80.0), 4),
    }

    return {
        "apiVersion": "v3",
        "id": str(uuid.uuid4()),
        "deviceName": device_name,
        "profileName": profile_name,
        "sourceName": source_name,
        "origin": now_ns,
        "readings": [
            {
                "origin": now_ns,
                "deviceName": device_name,
                "resourceName": source_name,
                "profileName": profile_name,
                "valueType": "String",
                "value": json.dumps({source_name: json.dumps(metrics)}),
            }
        ],
    }

def run_benchmark():
    producer = get_producer()
    if not producer:
        return

    print(f"🚀 [Throughput Test] Sending {TOTAL_RECORDS} records to topic '{TOPIC}'...")
    print(f"🔗 Broker: {KAFKA_BROKER}")
    
    start_time = time.time()
    
    for i in range(TOTAL_RECORDS):
        event = generate_benchmark_event()
        producer.send(TOPIC, event)
        if (i + 1) % 1000 == 0:
            print(f"📤 Sent {i + 1} records...")

    producer.flush()
    end_time = time.time()
    
    duration = end_time - start_time
    eps = TOTAL_RECORDS / duration
    
    print("\n" + "="*30)
    print(f"✅ BENCHMARK FINISHED")
    print(f"⏱  Total Duration : {duration:.2f} seconds")
    print(f"📊 Throughput       : {eps:.2f} EPS (Events Per Second)")
    print("="*30)
    
    producer.close()

if __name__ == "__main__":
    run_benchmark()
