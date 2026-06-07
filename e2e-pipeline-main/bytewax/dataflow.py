"""Bytewax flow: Kafka -> Grafana annotation + InfluxDB."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import requests
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaSource

from connector.influxdb import InfluxSink


logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(name)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("bytewax-flow")


KAFKA_BROKERS = [broker.strip() for broker in os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").split(",") if broker.strip()]
KAFKA_TOPICS = [topic.strip() for topic in os.getenv("KAFKA_TOPICS", "edge_computer_stats").split(",") if topic.strip()]
INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "iceberg-token")
INFLUX_ORG = os.getenv("INFLUX_ORG", "iceberg")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "kafka_events")
GRAFANA_URL = os.getenv("GRAFANA_URL", "http://grafana:3000")
GRAFANA_API_KEY = os.getenv("GRAFANA_API_KEY", "")


def decode_message(msg: Any) -> dict[str, Any] | None:
    """Decode Kafka payload into JSON."""

    raw_value = getattr(msg, "value", None)
    if raw_value is None:
        return None

    if isinstance(raw_value, bytes):
        raw_value = raw_value.decode("utf-8")

    try:
        payload = json.loads(raw_value)
    except Exception:
        return None

    payload["_kafka_topic"] = getattr(msg, "topic", "unknown")
    payload["_kafka_partition"] = getattr(msg, "partition", -1)
    payload["_kafka_offset"] = getattr(msg, "offset", -1)
    payload["_processed_at"] = datetime.now(timezone.utc).isoformat()
    LOGGER.info(
        "Processed Kafka event topic=%s partition=%s offset=%s source=%s",
        payload["_kafka_topic"],
        payload["_kafka_partition"],
        payload["_kafka_offset"],
        payload.get("sourceName", "unknown"),
    )
    return payload


def post_grafana_annotation(event: dict[str, Any]) -> dict[str, Any]:
    """Post each event as a Grafana annotation (best effort)."""

    if not GRAFANA_API_KEY:
        return event

    annotation = {
        "time": int(datetime.now(timezone.utc).timestamp() * 1000),
        "tags": ["kafka", str(event.get("sourceName", "unknown"))],
        "text": f"source={event.get('sourceName', 'unknown')} device={event.get('deviceName', 'unknown')}",
    }

    try:
        requests.post(
            f"{GRAFANA_URL}/api/annotations",
            json=annotation,
            headers={
                "Authorization": f"Bearer {GRAFANA_API_KEY}",
                "Content-Type": "application/json",
            },
            timeout=5,
        )
    except Exception:
        pass

    return event


def build_flow() -> Dataflow:
    """Build Bytewax dataflow graph."""

    LOGGER.info("Starting Bytewax flow: kafka-bytewax-grafana-influxdb")
    LOGGER.info("Kafka brokers=%s topics=%s", KAFKA_BROKERS, KAFKA_TOPICS)
    LOGGER.info("Influx target org=%s bucket=%s", INFLUX_ORG, INFLUX_BUCKET)

    flow = Dataflow("kafka-bytewax-grafana-influxdb")
    influx_sink = InfluxSink(INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET)
    kafka_source = KafkaSource(brokers=KAFKA_BROKERS, topics=KAFKA_TOPICS)
    stream = op.input("kafka-in", flow, kafka_source)
    events = op.map("decode", stream, decode_message)
    valid_events = op.filter("drop-invalid", events, lambda item: item is not None)
    grafana_events = op.map("grafana", valid_events, post_grafana_annotation)
    op.output("influx-out", grafana_events, influx_sink)
    return flow


flow = build_flow()
