import matplotlib.pyplot as plt
import json
import os

# Đường dẫn file dữ liệu
DATA_FILE = "monitoring/benchmarks/bytewax_results.json"
OUTPUT_IMAGE = "monitoring/benchmarks/benchmark_chart_output.png"

def generate_chart():
    if not os.path.exists(DATA_FILE):
        print(f"❌ Không tìm thấy file dữ liệu: {DATA_FILE}")
        print("💡 Hãy chạy 'uv run monitoring/benchmarks/benchmark_bytewax.py' trước.")
        return

    # 1. Đọc dữ liệu từ file JSON
    with open(DATA_FILE, "r") as f:
        data = json.load(f)
    
    test_runs = data["test_runs"]
    throughput = data["throughput"]
    latency = data["latency"]

    # 2. Khởi tạo figure và trục chính (Throughput - Trục Trái)
    fig, ax1 = plt.subplots(figsize=(12, 7))

    # Thiết lập tiêu đề
    plt.title("Chart 1: Benchmark Ingestion - Throughput vs Latency (Bytewax Flow)", 
              fontsize=15, fontweight='bold', pad=20)

    # Trục X
    ax1.set_xlabel("Test Runs", fontsize=12)
    ax1.set_xticks(range(len(test_runs)))
    ax1.set_xticklabels(test_runs)

    # Vẽ Throughput (Màu xanh, marker hình tròn 'o')
    ax1.set_ylabel("Throughput (RPS)", color='#1f77b4', fontsize=12, fontweight='bold')
    line1, = ax1.plot(test_runs, throughput, marker='o', markersize=8, color='#1f77b4', 
                     label='Throughput (RPS)', linewidth=2.5)
    ax1.tick_params(axis='y', labelcolor='#1f77b4')
    
    # Tự động điều chỉnh giới hạn trục Y trái
    ax1.set_ylim(min(throughput) * 0.8, max(throughput) * 1.2)

    # 3. Tạo trục thứ hai (Latency - Trục Phải)
    ax2 = ax1.twinx()
    ax2.set_ylabel("Average Latency (ms)", color='#d62728', fontsize=12, fontweight='bold')
    line2, = ax2.plot(test_runs, latency, marker='s', markersize=8, color='#d62728', 
                     label='Avg Latency (ms)', linewidth=2.5)
    ax2.tick_params(axis='y', labelcolor='#d62728')
    
    # Tự động điều chỉnh giới hạn trục Y phải
    ax2.set_ylim(min(latency) * 0.8, max(latency) * 1.2)

    # 4. Thêm Grid (Đường lưới)
    ax1.grid(True, linestyle='--', alpha=0.6)

    # 5. Thêm Chú thích (Legend) ở góc dưới bên phải
    lines = [line1, line2]
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc='lower right', frameon=True, shadow=True, borderpad=1)

    # Căn chỉnh bố cục
    plt.tight_layout()

    # Lưu biểu đồ ra file
    plt.savefig(OUTPUT_IMAGE, dpi=300)
    print(f"✅ Biểu đồ đã được tạo thành công tại: {OUTPUT_IMAGE}")

if __name__ == "__main__":
    generate_chart()
