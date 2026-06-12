import json
import time
import os
import uuid
import statistics
from kafka import KafkaProducer
from influxdb_client import InfluxDBClient

# Kafka Config
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC = os.getenv("KAFKA_TOPIC_NAME", "edge_computer_stats")

# InfluxDB Config
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "iceberg-token")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "iceberg")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "kafka_events")

NUM_TESTS = 10
RECORDS_PER_BURST = 50000  # Số lượng records mỗi đợt để tạo tải và đo Throughput

def send_burst_and_probe(test_id, producer):
    probe_id = f"test-{test_id}-{uuid.uuid4().hex[:4]}"
    # Dùng giá trị probe duy nhất cho mỗi lượt test
    probe_value = float(f"{test_id}.99")
    
    # 1. Đo Throughput: Gửi một lượng lớn records liên tục
    start_burst = time.time()
    for _ in range(RECORDS_PER_BURST):
        # Gửi dữ liệu nhẹ để đo băng thông (throughput)
        event = {
            "sourceName": "benchmark-load", 
            "deviceName": "load-gen", 
            "readings": [{"value": '{"val": 0}'}]
        }
        producer.send(TOPIC, value=event)
    
    # 2. Gửi Probe để đo Latency ngay sau khi burst
    now_ns = time.time_ns()
    probe_event = {
        "apiVersion": "v3",
        "id": probe_id,
        "deviceName": "Bytewax-Benchmark-Node",
        "profileName": "Benchmark-Profile",
        "sourceName": "cpu_data",
        "origin": now_ns,
        "readings": [
            {
                "origin": now_ns,
                "deviceName": "Bytewax-Benchmark-Node",
                "resourceName": "cpu_data",
                "valueType": "String",
                "value": json.dumps({"cpu_data": json.dumps({"usage_user": probe_value})})
            }
        ],
    }
    
    send_time = time.time()
    producer.send(TOPIC, probe_event)
    producer.flush()
    end_burst = time.time()
    
    # Tính RPS (Records Per Second) của đợt burst
    throughput_rps = RECORDS_PER_BURST / (end_burst - start_burst)
    return throughput_rps, send_time, probe_value

def wait_for_probe(query_api, probe_value, send_time):
    flux_query = f'''
    from(bucket: "{INFLUXDB_BUCKET}")
        |> range(start: -1m)
        |> filter(fn: (r) => r["_measurement"] == "edge_computer_stats")
        |> filter(fn: (r) => r["device_name"] == "Bytewax-Benchmark-Node")
        |> filter(fn: (r) => r["_field"] == "usage_user")
        |> filter(fn: (r) => r["_value"] == {probe_value})
        |> last()
    '''
    
    # Polling InfluxDB
    for _ in range(40): # Chờ tối đa 20 giây
        try:
            result = query_api.query(flux_query)
            if result and len(result) > 0:
                recv_time = time.time()
                return (recv_time - send_time) * 1000  # Trả về ms
        except Exception:
            pass
        time.sleep(0.5)
    return None

def run_full_benchmark():
    print(f"🚀 Starting Bytewax Multi-Run Benchmark ({NUM_TESTS} Tests)")
    print(f"📊 Each test sends {RECORDS_PER_BURST} records + 1 Latency Probe")
    print("-" * 60)

    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
        query_api = client.query_api()
    except Exception as e:
        print(f"❌ Initialization Error: {e}")
        return

    results = []

    for i in range(1, NUM_TESTS + 1):
        print(f"📦 Running Test {i}/{NUM_TESTS}...", end="\r")
        rps, send_t, p_val = send_burst_and_probe(i, producer)
        latency_ms = wait_for_probe(query_api, p_val, send_t)
        
        if latency_ms:
            results.append({"test": i, "rps": rps, "latency": latency_ms})
        else:
            print(f"\n❌ Test {i} failed (Probe timeout)")

    # Hiển thị bảng kết quả tổng hợp
    print("\n\n📈 CHART 1 DATA: THROUGHPUT VS LATENCY")
    print("=" * 55)
    print(f"| {'Run #':<8} | {'Throughput (RPS)':<18} | {'Avg Latency (ms)':<15} |")
    print("-" * 55)
    for r in results:
        print(f"| Test {r['test']:<3} | {r['rps']:>16.2f} | {r['latency']:>13.2f} ms |")
    print("-" * 55)

    if results:
        avg_rps = statistics.mean([r['rps'] for r in results])
        avg_lat = statistics.mean([r['latency'] for r in results])
        print(f"SUMMARY:   Throughput Avg: {avg_rps:.2f} RPS | Latency Avg: {avg_lat:.2f} ms")
        
        # Lưu kết quả ra file JSON để script vẽ biểu đồ sử dụng
        output_data = {
            "test_runs": [f"Test {r['test']}" for r in results],
            "throughput": [r['rps'] for r in results],
            "latency": [r['latency'] for r in results]
        }
        with open("monitoring/benchmarks/bytewax_results.json", "w") as f:
            json.dump(output_data, f, indent=4)
        print(f"\n💾 Results saved to monitoring/benchmarks/bytewax_results.json")
    print("=" * 55)

    producer.close()
    client.close()

if __name__ == "__main__":
    run_full_benchmark()
