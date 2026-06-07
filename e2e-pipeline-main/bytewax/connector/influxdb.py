"""InfluxDB DynamicSink implementation for Bytewax with JSON flattening."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
from typing import Any

from bytewax.outputs import DynamicSink, StatelessSinkPartition
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


LOGGER = logging.getLogger("bytewax-influx-sink")


class InfluxSinkPartition(StatelessSinkPartition):
    """Worker-local sink partition that writes flattened sensor data to InfluxDB."""

    def _parse_sensor_value(self, value_str: str) -> dict[str, Any]:
        """Parse nested JSON sensor data from Kafka payload.
        
        Kafka sends: {"source_name": '{"field": value, ...}'}
        We extract and flatten the inner metrics.
        """
        try:
            if not isinstance(value_str, str):
                LOGGER.error("Value is not a string, type=%s value=%s", type(value_str), value_str)
                return {}
            
            # First parse: extract outer dict {"cpu_data": "..."}
            outer = json.loads(value_str)
            LOGGER.debug("Parsed outer JSON keys: %s", list(outer.keys()) if isinstance(outer, dict) else "NOT A DICT")
            
            if not isinstance(outer, dict):
                LOGGER.error("Outer parse resulted in non-dict type=%s", type(outer))
                return {}
            
            for source_key, metrics_str in outer.items():
                LOGGER.debug("Processing key=%s type=%s", source_key, type(metrics_str))
                if isinstance(metrics_str, str):
                    # Second parse: extract inner metrics
                    inner_metrics = json.loads(metrics_str)
                    LOGGER.debug("Extracted metrics from %s: keys=%s values=%s", 
                                source_key, list(inner_metrics.keys()), inner_metrics)
                    return inner_metrics
                else:
                    LOGGER.error("Metrics for key %s is not a string, type=%s", source_key, type(metrics_str))
            
            LOGGER.error("No string values found in outer dict")
            return {}
            
        except json.JSONDecodeError as e:
            LOGGER.error("JSON decode error at position %d: %s (input: %s)", 
                        e.pos, e.msg, value_str[:500] if isinstance(value_str, str) else str(value_str))
            return {}
        except Exception as e:
            LOGGER.error("Failed to parse sensor value: %s (type=%s)", e, type(e).__name__, exc_info=True)
            return {}

    def __init__(self, url: str, token: str, org: str, bucket: str) -> None:
        self._org = org
        self._bucket = bucket
        self._client = InfluxDBClient(url=url, token=token, org=org)
        self._write_api = self._client.write_api(write_options=SYNCHRONOUS)
        
        # Ensure bucket exists
        self._ensure_bucket()
        LOGGER.info("Influx sink connected org=%s bucket=%s", org, bucket)

    def _ensure_bucket(self) -> None:
        """Create bucket if it doesn't exist."""
        try:
            from influxdb_client.client.buckets_api import BucketsApi
            buckets_api = BucketsApi(self._client)
            buckets = buckets_api.find_bucket_by_name(self._bucket)
            if buckets:
                LOGGER.info("Bucket %s already exists", self._bucket)
                return
        except Exception:
            pass
        
        # Bucket doesn't exist, create it
        try:
            from influxdb_client.client.buckets_api import BucketsApi
            from influxdb_client.client.organizations_api import OrganizationsApi
            
            orgs_api = OrganizationsApi(self._client)
            org = orgs_api.find_organizations_by_org(self._org)
            if not org:
                LOGGER.error("Organization %s not found", self._org)
                return
            
            org_id = org[0].id if org else None
            if not org_id:
                LOGGER.error("Could not find org ID for %s", self._org)
                return
            
            buckets_api = BucketsApi(self._client)
            from influxdb_client.client.bucket import BucketRetentionRules
            buckets_api.create_bucket(
                bucket_name=self._bucket,
                org_id=org_id,
                retention_rules=BucketRetentionRules(type="expire", every_seconds=2592000)
            )
            LOGGER.info("Created bucket %s", self._bucket)
        except Exception as e:
            LOGGER.error("Failed to create bucket %s: %s", self._bucket, e)

    def _build_point(self, event: dict[str, Any]) -> Point:
        """Build a single InfluxDB point with flattened sensor data."""
        point = Point("edge_computer_stats")
        
        # Tags: categorical attributes (indexed, for filtering)
        point = point.tag("source_name", str(event.get("sourceName", "unknown")))
        point = point.tag("device_name", str(event.get("deviceName", "unknown")))
        point = point.tag("profile_name", str(event.get("profileName", "unknown")))
        point = point.tag("topic", str(event.get("_kafka_topic", "unknown")))

        # Extract and flatten sensor metrics from nested readings
        readings = event.get("readings", [])
        LOGGER.debug("Event has %d readings", len(readings) if readings else 0)
        
        sensor_metrics = {}
        if readings and isinstance(readings, list):
            for idx, reading in enumerate(readings):
                LOGGER.debug("Reading %d: type=%s keys=%s", idx, type(reading), 
                           list(reading.keys()) if isinstance(reading, dict) else "NOT-DICT")
                if isinstance(reading, dict) and "value" in reading:
                    value = reading["value"]
                    LOGGER.debug("Reading %d value: type=%s len=%d first_100=%s", 
                               idx, type(value), len(value) if isinstance(value, str) else -1,
                               value[:100] if isinstance(value, str) else str(value))
                    extracted = self._parse_sensor_value(value)
                    LOGGER.debug("Reading %d extracted metrics: %d fields", idx, len(extracted))
                    sensor_metrics.update(extracted)
                else:
                    LOGGER.warning("Reading %d missing 'value' field", idx)

        # Fields: numeric values from flattened sensor data
        fields_added = 0
        for key, value in sensor_metrics.items():
            if isinstance(value, bool):
                point = point.field(key, value)
                fields_added += 1
            elif isinstance(value, int):
                point = point.field(key, value)
                fields_added += 1
            elif isinstance(value, float):
                point = point.field(key, value)
                fields_added += 1
            elif isinstance(value, str):
                point = point.field(f"{key}_str", value)
                fields_added += 1
        
        # Add top-level string metadata as fields
        for key, value in event.items():
            if key in {"sourceName", "deviceName", "profileName", "_kafka_topic", "readings", "_kafka_partition", "_kafka_offset", "_processed_at", "apiVersion", "origin"}:
                continue
            if isinstance(value, str):
                point = point.field(f"meta_{key}", value)
                fields_added += 1

        LOGGER.info(
            "Built point: source=%s device=%s metrics_count=%d fields_added=%d",
            event.get("sourceName", "unknown"),
            event.get("deviceName", "unknown"),
            len(sensor_metrics),
            fields_added
        )
        
        point = point.time(datetime.now(timezone.utc), WritePrecision.NS)
        return point

    def write_batch(self, items: list[dict[str, Any]]) -> None:
        """Write a batch of flattened events to InfluxDB."""
        points = [self._build_point(event) for event in items]
        if points:
            self._write_api.write(bucket=self._bucket, org=self._org, record=points)
            LOGGER.info("Influx sink inserted batch size=%s", len(points))


class InfluxSink(DynamicSink):
    """Dynamic sink that builds one InfluxDB sink partition per worker."""

    def __init__(self, url: str, token: str, org: str, bucket: str) -> None:
        self._url = url
        self._token = token
        self._org = org
        self._bucket = bucket

    def build(self, _step_id: str, _worker_index: int, _worker_count: int) -> StatelessSinkPartition:
        return InfluxSinkPartition(self._url, self._token, self._org, self._bucket)
