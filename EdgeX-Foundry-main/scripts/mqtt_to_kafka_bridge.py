"""
MQTT-to-Kafka Bridge
=====================
Đọc messages từ EdgeX MQTT broker (edgex_system_metrics_raw)
và forward vào Kafka topic (edgex_system_metrics) của E2E pipeline.

Đây là cầu nối giữa EdgeX App Service và Kafka của E2E pipeline.

Architecture:
    EdgeX App Service
        ↓ MQTT publish
    EdgeX MQTT broker (mosquitto) — topic: edgex_system_metrics_raw
        ↓ MQTT subscribe (this script)
    Kafka (E2E pipeline) — topic: edgex_system_metrics

Environment variables:
    MQTT_BROKER_HOST        — MQTT broker host (default: edgex-mqtt-broker / localhost)
    MQTT_BROKER_PORT        — MQTT broker port (default: 1883)
    MQTT_TOPIC              — MQTT topic to subscribe (default: edgex_system_metrics_raw)
    KAFKA_BOOTSTRAP_SERVERS — Kafka bootstrap servers (default: localhost:9094)
    KAFKA_TOPIC             — Kafka topic to produce (default: edgex_system_metrics)
    BRIDGE_CLIENT_ID        — MQTT client ID (default: edgex-mqtt-kafka-bridge)
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from threading import Event

import paho.mqtt.client as mqtt
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# ─── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── Config ─────────────────────────────────────────────────────────────────────
MQTT_HOST = os.getenv("MQTT_BROKER_HOST", "edgex-mqtt-broker")
MQTT_PORT = int(os.getenv("MQTT_BROKER_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "edgex_system_metrics_raw")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "edgex_system_metrics")
CLIENT_ID = os.getenv("BRIDGE_CLIENT_ID", "edgex-mqtt-kafka-bridge")

# ─── Globals ─────────────────────────────────────────────────────────────────────
kafka_producer: KafkaProducer | None = None
stop_event = Event()
stats = {"received": 0, "forwarded": 0, "errors": 0}


# ─── Kafka Setup ─────────────────────────────────────────────────────────────────

def create_kafka_producer(retries: int = 10) -> KafkaProducer:
    """Tạo Kafka producer với retry logic."""
    brokers = [s.strip() for s in KAFKA_BOOTSTRAP.split(",")]
    log.info("🔗 Kết nối tới Kafka: %s", brokers)

    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=brokers,
                value_serializer=lambda v: v if isinstance(v, bytes) else v.encode("utf-8"),
                retries=5,
                request_timeout_ms=15000,
                api_version_auto_timeout_ms=10000,
            )
            log.info("✅ Kafka producer kết nối thành công!")
            return producer
        except Exception as e:
            log.warning("⚠️  Kafka chưa sẵn sàng (attempt %d/%d): %s", attempt, retries, e)
            time.sleep(5)

    raise RuntimeError(f"❌ Không thể kết nối Kafka sau {retries} lần thử")


def ensure_kafka_topic(producer: KafkaProducer) -> None:
    """Đảm bảo Kafka topic tồn tại."""
    brokers = [s.strip() for s in KAFKA_BOOTSTRAP.split(",")]
    try:
        admin = KafkaAdminClient(bootstrap_servers=brokers)
        try:
            admin.create_topics(
                new_topics=[NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)],
                validate_only=False,
            )
            log.info("✅ Kafka topic '%s' đã được tạo.", KAFKA_TOPIC)
        except TopicAlreadyExistsError:
            log.info("ℹ️  Kafka topic '%s' đã tồn tại.", KAFKA_TOPIC)
        finally:
            admin.close()
    except Exception as e:
        log.warning("⚠️  Không thể tạo Kafka topic (có thể đã tồn tại): %s", e)


# ─── MQTT Callbacks ─────────────────────────────────────────────────────────────

def on_connect(client: mqtt.Client, userdata, flags, rc: int) -> None:
    if rc == 0:
        log.info("✅ Đã kết nối MQTT broker tại %s:%d", MQTT_HOST, MQTT_PORT)
        client.subscribe(MQTT_TOPIC, qos=1)
        log.info("📡 Subscribed topic: %s", MQTT_TOPIC)
    else:
        log.error("❌ MQTT kết nối thất bại, rc=%d", rc)


def on_disconnect(client: mqtt.Client, userdata, rc: int) -> None:
    if rc != 0:
        log.warning("⚠️  MQTT ngắt kết nối không mong đợi (rc=%d). Đang reconnect...", rc)


def on_message(client: mqtt.Client, userdata, msg: mqtt.MQTTMessage) -> None:
    """Forward MQTT message tới Kafka."""
    global kafka_producer
    stats["received"] += 1

    try:
        payload = msg.payload

        # Validate JSON (optional, để đảm bảo format đúng)
        try:
            parsed = json.loads(payload)
            # Re-serialize để normalize encoding
            payload = json.dumps(parsed, ensure_ascii=False).encode("utf-8")
        except json.JSONDecodeError:
            # Gửi raw bytes nếu không phải JSON hợp lệ
            log.debug("⚠️  Payload không phải JSON hợp lệ, gửi raw")

        # Publish tới Kafka
        future = kafka_producer.send(KAFKA_TOPIC, value=payload)
        future.get(timeout=10)
        stats["forwarded"] += 1

        if stats["forwarded"] % 10 == 0:
            log.info(
                "📊 Stats: received=%d, forwarded=%d, errors=%d",
                stats["received"], stats["forwarded"], stats["errors"],
            )

    except Exception as e:
        stats["errors"] += 1
        log.error("❌ Lỗi forward message: %s", e)


def on_log(client: mqtt.Client, userdata, level: int, buf: str) -> None:
    if level == mqtt.MQTT_LOG_ERR:
        log.error("MQTT: %s", buf)


# ─── Main ───────────────────────────────────────────────────────────────────────

def main() -> int:
    global kafka_producer

    log.info("═══════════════════════════════════════════════════════")
    log.info("  MQTT-to-Kafka Bridge")
    log.info("  MQTT: %s:%d → topic: %s", MQTT_HOST, MQTT_PORT, MQTT_TOPIC)
    log.info("  Kafka: %s → topic: %s", KAFKA_BOOTSTRAP, KAFKA_TOPIC)
    log.info("═══════════════════════════════════════════════════════")

    # Khởi tạo Kafka producer
    kafka_producer = create_kafka_producer()
    ensure_kafka_topic(kafka_producer)

    # Khởi tạo MQTT client
    mqtt_client = mqtt.Client(client_id=CLIENT_ID, clean_session=True)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.on_message = on_message
    mqtt_client.on_log = on_log

    # Kết nối MQTT với retry
    log.info("🔗 Kết nối MQTT broker tại %s:%d...", MQTT_HOST, MQTT_PORT)
    mqtt_client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    mqtt_client.reconnect_delay_set(min_delay=1, max_delay=30)

    log.info("🚀 Bridge đang chạy. Ctrl+C để dừng.")
    try:
        mqtt_client.loop_forever()
    except KeyboardInterrupt:
        log.info("\n⛔ Đang dừng bridge...")
    finally:
        mqtt_client.disconnect()
        kafka_producer.flush()
        kafka_producer.close()
        log.info(
            "📊 Tổng kết: received=%d, forwarded=%d, errors=%d",
            stats["received"], stats["forwarded"], stats["errors"],
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
