# Bus GPS Streaming Simulator

Đọc dữ liệu GPS xe buýt (~14GB, 221 JSON files) và mô phỏng streaming đẩy vào Kafka.

## Cấu trúc

```
simulate/
├── stream_gps.py         # File chính — đọc JSON, đẩy Kafka
├── docker-compose.yml    # Kafka + Kafka UI
├── requirements.txt      # kafka-python
├── data/
│   └── part1/part1/      # 221 file JSON (~70MB/file, ~211K records/file)
└── README.md
```

## Data format

Mỗi file JSON chứa array các GPS record xe buýt:

```json
{
  "msgType": "MsgType_BusWayPoint",
  "msgBusWayPoint": {
    "vehicle": "488439849e64c06f...",
    "datetime": 1742615276,
    "x": 106.619255,
    "y": 10.739566,
    "speed": 12.0,
    "heading": 14.0,
    "ignition": true,
    "aircon": true
  }
}
```

## Quick Start

```bash
# 1. Vào virtualenv và cài dependency
source .venv/bin/activate
pip install kafka-python

# 2. Chạy Kafka
docker-compose up -d

# 3. Stream dữ liệu
python stream_gps.py
```

## Cách dùng

```bash
# Stream tất cả 221 files
python stream_gps.py

# Stream 5 files đầu (để test)
python stream_gps.py --files 5

# Thay đổi tốc độ (mặc định 200 records/s)
python stream_gps.py --rate 1000

# Dry-run: đọc và in ra console, không cần Kafka
python stream_gps.py --dry-run --files 1

# Đổi topic hoặc broker
python stream_gps.py --broker localhost:9092 --topic my-topic
```

## Monitoring

- **Kafka UI**: http://localhost:8080 — xem topic, messages, consumer groups