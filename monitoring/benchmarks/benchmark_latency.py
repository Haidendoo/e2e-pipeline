import json
import time
import os
import uuid
from kafka import KafkaProducer
from trino.dbapi import connect

# Kafka Config
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC = os.getenv("EDGEX_KAFKA_TOPIC", "edgex_system_metrics")

# Trino Config
TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8060"))

def run_medallion_probe_test():
    probe_device = f"Latency-Probe-{uuid.uuid4().hex[:4]}"
    probe_value = 88.88
    
    print(f"🚀 [Medallion Probe] Sending probe event to Kafka...")
    print(f"   Device: {probe_device} | Value: {probe_value}")
    print(f"   Topic: {TOPIC}")

    # 1. Gửi probe vào Kafka
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    now_ns = time.time_ns()
    # Payload EdgeX chuẩn: value là string chứa JSON của metric
    metrics = {"usage_user": probe_value}
    event = {
        "apiVersion": "v3",
        "id": str(uuid.uuid4()),
        "deviceName": probe_device,
        "profileName": "Benchmark-Profile",
        "sourceName": "cpu_data",
        "origin": now_ns,
        "readings": [
            {
                "origin": now_ns,
                "deviceName": probe_device,
                "resourceName": "cpu_data",
                "valueType": "String",
                "value": json.dumps(metrics)
            }
        ],
    }
    
    start_time = time.time()
    producer.send(TOPIC, event)
    producer.flush()
    producer.close()

    print(f"⌛ Probe sent. Waiting for Medallion Pipeline (Spark) to process...")
    print(f"   (Note: Pipeline runs every 4 minutes. This may take a while...)")

    # 2. Polling Trino
    conn = connect(
        host=TRINO_HOST, 
        port=TRINO_PORT, 
        user="airflow", 
        catalog="iceberg", 
        schema="edgex"
    )
    
    max_wait_minutes = 15
    found = False
    
    try:
        # Kiểm tra mỗi 30 giây
        for i in range(max_wait_minutes * 2):
            cur = conn.cursor()
            # Query rộng hơn: chỉ cần device_name khớp là được
            sql = f"SELECT count(*) FROM edgex_gold WHERE device_name = '{probe_device}'"
            
            cur.execute(sql)
            res = cur.fetchone()
            cur.close()

            if res and res[0] and res[0] > 0:
                end_time = time.time()
                latency = end_time - start_time
                print("\n" + "🏆"*20)
                print(f"✅ PROBE REACHED GOLD LAYER!")
                print(f"⏱  Total E2E Latency: {latency:.2f} seconds ({latency/60:.2f} minutes)")
                print(f"📍 Target Device    : {probe_device}")
                print("🏆"*20)
                found = True
                break
            
            if (i + 1) % 2 == 0:
                elapsed = (i + 1) * 30 / 60
                print(f"   ... still waiting ({elapsed:.1f} min elapsed)")
            
            time.sleep(30)

    except Exception as e:
        print(f"❌ Error polling Trino: {e}")
    finally:
        conn.close()

    if not found:
        print(f"\n❌ Timeout: Probe not found in Gold table after {max_wait_minutes} minutes.")
        print("   Make sure the Airflow DAG 'edgex_system_monitoring_4m' is enabled and running.")

if __name__ == "__main__":
    run_medallion_probe_test()
