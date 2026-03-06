#!/usr/bin/env python3
"""
Bus GPS Streaming Simulator
============================
Đọc dữ liệu GPS xe buýt từ các file JSON lớn (~14GB, 221 files)
và mô phỏng streaming đẩy vào Kafka topic.

Cách dùng:
    # Đảm bảo Kafka đang chạy
    docker-compose up -d

    # Stream tất cả files
    python stream_gps.py

    # Stream giới hạn số file
    python stream_gps.py --files 5

    # Thay đổi tốc độ stream (records/giây)
    python stream_gps.py --rate 500

    # Chỉ đọc và in ra console (không cần Kafka)
    python stream_gps.py --dry-run
"""

import json
import time
import argparse
import sys
from pathlib import Path
from datetime import datetime

# ============================================================
# CONFIG
# ============================================================
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "gps-bus-streaming"
DATA_DIR = Path("data/part1/part1")
DEFAULT_RATE = 200  # records/giây mặc định


def create_kafka_producer(broker):
    """Tạo Kafka producer. Trả về None nếu không kết nối được."""
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=[broker],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            batch_size=32768,       # 32KB batch
            linger_ms=20,           # đợi 20ms gom batch
            buffer_memory=67108864, # 64MB buffer
            retries=3,
            acks=1,
        )
        print(f"[OK] Kafka producer connected -> {broker}")
        return producer
    except Exception as e:
        print(f"[ERROR] Không kết nối được Kafka ({broker}): {e}")
        return None


def get_json_files(data_dir, limit=None):
    """Lấy danh sách file JSON, sorted theo tên."""
    if not data_dir.exists():
        print(f"[ERROR] Thư mục data không tồn tại: {data_dir}")
        sys.exit(1)

    files = sorted(data_dir.glob("*.json"))
    if not files:
        print(f"[ERROR] Không tìm thấy file JSON trong {data_dir}")
        sys.exit(1)

    if limit and limit < len(files):
        files = files[:limit]

    return files


def stream_file(file_path, producer, topic, delay, dry_run=False):
    """
    Đọc 1 file JSON và stream từng record.
    Trả về số record đã gửi thành công.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            records = json.load(f)
    except Exception as e:
        print(f"  [SKIP] Lỗi đọc {file_path.name}: {e}")
        return 0

    if not isinstance(records, list):
        records = [records]

    sent = 0
    for rec in records:
        # Parse cấu trúc msgBusWayPoint
        wp = rec.get("msgBusWayPoint")
        if not wp:
            continue

        lon = wp.get("x", 0)
        lat = wp.get("y", 0)
        if lat == 0 or lon == 0:
            continue

        # Chuẩn hóa thành GPS record
        ts = wp.get("datetime", 0)
        iso_time = (
            datetime.fromtimestamp(ts).isoformat() if ts > 0
            else datetime.now().isoformat()
        )

        gps_record = {
            "timestamp":  iso_time,
            "latitude":   lat,
            "longitude":  lon,
            "speed":      wp.get("speed", 0),
            "heading":    wp.get("heading", 0),
            "vehicle_id": wp.get("vehicle", "unknown")[:16],
            "ignition":   wp.get("ignition", False),
            "aircon":     wp.get("aircon", False),
            "source":     file_path.name,
        }

        if dry_run:
            # Chỉ in ra console, không cần Kafka
            if sent < 3:
                print(f"    {json.dumps(gps_record, ensure_ascii=False)}")
        else:
            producer.send(topic, key=gps_record["vehicle_id"], value=gps_record)

        sent += 1

        # Throttle tốc độ
        if delay > 0:
            time.sleep(delay)

    # Flush sau mỗi file
    if producer and not dry_run:
        producer.flush()

    return sent


def main():
    parser = argparse.ArgumentParser(
        description="Bus GPS Streaming Simulator — đọc JSON, đẩy Kafka"
    )
    parser.add_argument(
        "--files", type=int, default=None,
        help="Giới hạn số file cần stream (mặc định: tất cả 221 files)"
    )
    parser.add_argument(
        "--rate", type=int, default=DEFAULT_RATE,
        help=f"Số records/giây (mặc định: {DEFAULT_RATE})"
    )
    parser.add_argument(
        "--broker", type=str, default=KAFKA_BROKER,
        help=f"Kafka broker address (mặc định: {KAFKA_BROKER})"
    )
    parser.add_argument(
        "--topic", type=str, default=KAFKA_TOPIC,
        help=f"Kafka topic (mặc định: {KAFKA_TOPIC})"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Chỉ đọc và in ra console, không gửi Kafka"
    )
    args = parser.parse_args()

    delay = 1.0 / args.rate if args.rate > 0 else 0

    print("=" * 55)
    print("  BUS GPS STREAMING SIMULATOR")
    print("=" * 55)
    print(f"  Data dir : {DATA_DIR}")
    print(f"  Broker   : {args.broker}")
    print(f"  Topic    : {args.topic}")
    print(f"  Rate     : {args.rate} records/s")
    print(f"  Dry-run  : {args.dry_run}")
    print("=" * 55)

    # Lấy danh sách file
    files = get_json_files(DATA_DIR, limit=args.files)
    print(f"[INFO] {len(files)} file(s) sẽ được stream\n")

    # Kết nối Kafka (trừ dry-run)
    producer = None
    if not args.dry_run:
        producer = create_kafka_producer(args.broker)
        if producer is None:
            print("[HINT] Chạy 'docker-compose up -d' trước, hoặc dùng --dry-run")
            sys.exit(1)

    # ---- STREAM ----
    total_records = 0
    t_start = time.time()

    try:
        for i, fpath in enumerate(files, 1):
            print(f"[{i}/{len(files)}] {fpath.name} ...", end=" ", flush=True)
            t_file = time.time()

            sent = stream_file(fpath, producer, args.topic, delay, args.dry_run)
            total_records += sent

            elapsed_file = time.time() - t_file
            rate = sent / elapsed_file if elapsed_file > 0 else 0
            print(f"{sent:,} records  ({rate:,.0f} rec/s)")

    except KeyboardInterrupt:
        print("\n[STOP] Dừng bởi người dùng.")

    finally:
        if producer:
            producer.close()

    # ---- KẾT QUẢ ----
    total_time = time.time() - t_start
    avg_rate = total_records / total_time if total_time > 0 else 0
    print(f"\n{'=' * 55}")
    print(f"  DONE")
    print(f"  Files   : {min(i, len(files)) if 'i' in dir() else 0}")
    print(f"  Records : {total_records:,}")
    print(f"  Time    : {total_time:.1f}s")
    print(f"  Avg rate: {avg_rate:,.0f} records/s")
    print(f"{'=' * 55}")


if __name__ == "__main__":
    main()