import time
import requests
import concurrent.futures
import argparse
import statistics
import random

def send_telemetry(host, port, device_name):
    start = time.time()
    # Randomly pick a resource and generate dummy data matching Telegraf-Node-01 profile
    resources = {
        "cpu_data": '{"usage_idle": 90.0, "usage_user": 5.0, "usage_system": 5.0}',
        "mem_data": '{"used_percent": 65.4, "free": 1024000, "total": 8048000}',
        "disk_data": '{"read_bytes": 4096, "write_bytes": 1024}',
        "sys_data": '{"load1": 1.5, "load5": 1.2, "uptime": 3600}',
        "net_data": '{"bytes_recv": 500, "bytes_sent": 1000}'
    }
    resource_name, value = random.choice(list(resources.items()))
    url = f"http://{host}:{port}/api/v3/resource/{device_name}/{resource_name}"
    
    try:
        r = requests.post(url, data=value, timeout=5)
        status = r.status_code in (200, 201, 202)
    except:
        status = False
        
    duration = time.time() - start
    return status, duration

def run_benchmark(host, port, device, total_requests, concurrency):
    print(f"[*] Bắt đầu bài test CHỊU TẢI NẶNG (Ingestion Benchmark) trên EdgeX Foundry...")
    print(f"[*] Gửi Dữ Liệu Thực Tế tới: http://{host}:{port}/api/v3/resource/{device}/<metrics>")
    print(f"[*] Số luồng đồng thời (Concurrency): {concurrency}")
    print(f"[*] Tổng số request sẽ gửi: {total_requests}\n")

    latencies = []
    success = 0
    failed = 0

    start_time = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(send_telemetry, host, port, device) for _ in range(total_requests)]
        
        # In tien do de bot nham chan neu request qua nhieu
        completed = 0
        for f in concurrent.futures.as_completed(futures):
            status, duration = f.result()
            latencies.append(duration)
            if status:
                success += 1
            else:
                failed += 1
                
            completed += 1
            if completed % (total_requests // 10 if total_requests >= 10 else 1) == 0:
                print(f"   -> Đã xử lý {completed}/{total_requests} requests...")

    total_time = time.time() - start_time
    throughput = total_requests / total_time if total_time > 0 else 0

    print("\n==================================================")
    print("      KẾT QUẢ BENCHMARK INGESTION (HEAVY LOAD)    ")
    print("==================================================")
    print(f"Thời gian hoàn thành (Total Time): {total_time:.2f} seconds")
    print(f"Thành công (Successful Requests):  {success}")
    print(f"Thất bại (Failed Requests):        {failed}")
    print(f"--------------------------------------------------")
    print(f"Thông lượng (Throughput/RPS):      {throughput:.2f} req/s")
    print(f"Độ trễ trung bình (Avg Latency):   {statistics.mean(latencies)*1000:.2f} ms")
    print(f"Độ trễ thấp nhất (Min Latency):    {min(latencies)*1000:.2f} ms")
    print(f"Độ trễ cao nhất (Max Latency):     {max(latencies)*1000:.2f} ms")
    print("==================================================")
    
    if failed > 0:
        print(f"\n[!] Chú ý: Có {failed} request bị rớt (drop).")
        print("[!] Điều này chứng tỏ EdgeX đã đạt tới GIỚI HẠN CHỊU TẢI (Bottleneck) ở mức concurrency hiện tại.")
        print("[!] Bạn nên ghi nhận con số này vào báo cáo để đánh giá điểm gãy (Breaking Point) của hệ thống.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="EdgeX Ingestion Benchmark Tool")
    parser.add_argument('--host', type=str, default="localhost", help="EdgeX Host")
    parser.add_argument('--port', type=int, default=59986, help="EdgeX Port (default: 59986 for device-rest)")
    parser.add_argument('--device', type=str, default="RPi4-REST-v2", help="Target device name")
    parser.add_argument('-n', '--requests', type=int, default=100000, help="Total number of requests")
    parser.add_argument('-c', '--concurrency', type=int, default=200, help="Concurrent threads")
    args = parser.parse_args()
    
    run_benchmark(args.host, args.port, args.device, args.requests, args.concurrency)
    