import time
import requests
import concurrent.futures
import argparse
import statistics

def send_request(url):
    start = time.time()
    try:
        r = requests.get(url, timeout=2)
        status = r.status_code == 200
    except:
        status = False
    duration = time.time() - start
    return status, duration

def run_benchmark(host, port, total_requests, concurrency):
    url = f"http://{host}:{port}/api/v3/ping"
    print(f"[*] Bắt đầu bài test hiệu năng (Performance Benchmark) trên EdgeX Foundry...")
    print(f"[*] Target Endpoint: {url}")
    print(f"[*] Số luồng đồng thời (Concurrency): {concurrency}")
    print(f"[*] Tổng số request sẽ gửi: {total_requests}\n")

    latencies = []
    success = 0
    failed = 0

    start_time = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(send_request, url) for _ in range(total_requests)]
        for f in concurrent.futures.as_completed(futures):
            status, duration = f.result()
            latencies.append(duration)
            if status:
                success += 1
            else:
                failed += 1

    total_time = time.time() - start_time
    throughput = total_requests / total_time if total_time > 0 else 0

    print("==================================================")
    print("      KẾT QUẢ BENCHMARK EDGEX FOUNDRY (CORE-DATA)  ")
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
        print("[!] Lưu ý: Có request thất bại. Hệ thống EdgeX có thể đang quá tải hoặc chưa chạy.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="EdgeX Performance Benchmark Tool")
    parser.add_argument('--host', type=str, default="localhost", help="EdgeX Host")
    parser.add_argument('--port', type=int, default=59986, help="EdgeX Port (default: 59986 for device-rest or 59880 for core-data)")
    parser.add_argument('-n', '--requests', type=int, default=1000, help="Total number of requests")
    parser.add_argument('-c', '--concurrency', type=int, default=50, help="Concurrent threads")
    args = parser.parse_args()
    
    run_benchmark(args.host, args.port, args.requests, args.concurrency)
