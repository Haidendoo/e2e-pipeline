"""Generate EdgeX-style JSON payloads and publish them to Kafka.

The script creates topics automatically if they do not already exist, then
publishes Faker-generated events that match the sample JSON structure used in
this repository.
"""

from __future__ import annotations

import json
import os
import uuid
import time
from random import randint

from faker import Faker
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


DEFAULT_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
DEFAULT_PROFILE_NAME = os.getenv("KAFKA_PROFILE_NAME", "Telegraf-Full-Node-Profile")
DEFAULT_DEVICE_NAME = os.getenv("KAFKA_DEVICE_NAME", "Telegraf-Node-01")
DEFAULT_MESSAGES_PER_TOPIC = int(os.getenv("KAFKA_MESSAGES_PER_TOPIC", "5"))
DEFAULT_TOPIC_NAME = os.getenv("KAFKA_TOPIC_NAME", "edge_computer_stats")


def current_unix_nanos() -> int:
	"""Return a timestamp in nanoseconds."""

	return time.time_ns()


def generate_sensor_value(fake: Faker, source_name: str) -> dict:
	"""Build a fake payload matching the nested JSON string in the sample data."""

	host = fake.hostname()

	if source_name == "cpu_data":
		metrics = {
			"cpu": "cpu-total",
			"host": host,
			"usage_idle": round(fake.pyfloat(min_value=75.0, max_value=95.0), 12),
			"usage_iowait": round(fake.pyfloat(min_value=0.0, max_value=1.0), 12),
			"usage_system": round(fake.pyfloat(min_value=1.0, max_value=4.0), 12),
			"usage_user": round(fake.pyfloat(min_value=4.0, max_value=20.0), 12),
		}
	elif source_name == "gpu_data":
		metrics = {
			"compute_mode": "Default",
			"host": host,
			"index": 0,
			"name": "NVIDIA GeForce RTX 2080",
			"pstate": "P8",
			"uuid": f"GPU-{uuid.uuid4()}",
			"fan_speed": randint(0, 20),
			"memory_free": randint(7000, 7400),
			"memory_reserved": randint(350, 500),
			"memory_total": 8192,
			"memory_used": randint(400, 700),
			"temperature_gpu": randint(40, 65),
			"utilization_gpu": randint(0, 40),
			"utilization_memory": randint(0, 20),
		}
	elif source_name == "gpu_proc_data":
		metrics = {
			"gpu_idx": 0,
			"host": host,
			"mem_util": "-",
			"pid": randint(1000, 9999),
			"sm_util": "-",
		}
	elif source_name == "mem_data":
		total = randint(16, 64) * 1024 * 1024 * 1024
		used = randint(int(total * 0.12), int(total * 0.35))
		metrics = {
			"host": host,
			"available": total - used,
			"mem_total": total,
			"used": used,
		}
	elif source_name == "net_data":
		metrics = {
			"host": host,
			"comm": fake.word(),
			"dport": randint(1024, 65535),
			"laddr": fake.ipv4_private(),
			"lport": randint(1024, 65535),
			"pid": randint(100, 9999),
			"raddr": fake.ipv4_private(),
			"rx_kb": randint(0, 1024),
			"tx_kb": randint(0, 1024),
		}
	elif source_name == "sys_data":
		load1 = round(fake.pyfloat(min_value=0.1, max_value=4.0), 2)
		load5 = round(max(load1 - fake.pyfloat(min_value=0.0, max_value=0.5), 0.01), 2)
		load15 = round(max(load5 - fake.pyfloat(min_value=0.0, max_value=0.4), 0.01), 2)
		metrics = {
			"host": host,
			"load1": load1,
			"load5": load5,
			"load15": load15,
			"n_cpus": randint(2, 32),
			"uptime": randint(100, 200000),
		}
	elif source_name == "disk_data":
		read_bytes = randint(1_000_000_000, 10_000_000_000)
		write_bytes = randint(1_000_000_000, 10_000_000_000)
		metrics = {
			"host": host,
			"name": fake.word(),
			"wwid": f"eui.{uuid.uuid4().hex[:16]}",
			"io_time": randint(1000, 100000),
			"iops_in_progress": 0,
			"read_bytes": read_bytes,
			"reads": randint(1000, 100000),
			"write_bytes": write_bytes,
			"writes": randint(1000, 100000),
		}
	elif source_name == "proc_data":
		running = randint(0, 4)
		sleeping = randint(200, 400)
		stopped = randint(0, 2)
		zombie = randint(0, 2)
		metrics = {
			"host": host,
			"blocked": randint(0, 2),
			"idle": randint(60, 90),
			"proc_total": running + sleeping + stopped + zombie + randint(40, 80),
			"running": running,
			"sleeping": sleeping,
			"stopped": stopped,
			"total_threads": randint(1000, 5000),
			"zombies": zombie,
		}
	else:
		metrics = {
			"host": host,
			"value": fake.word(),
		}

	return metrics


def build_event(fake: Faker, source_name: str, profile_name: str, device_name: str) -> dict:
	"""Build a single event in the EdgeX-style envelope used by the sample file."""

	metrics = generate_sensor_value(fake, source_name)
	return {
		"apiVersion": "v3",
		"id": str(uuid.uuid4()),
		"deviceName": device_name,
		"profileName": profile_name,
		"sourceName": source_name,
		"origin": current_unix_nanos(),
		"readings": [
			{
				"origin": current_unix_nanos(),
				"deviceName": device_name,
				"resourceName": source_name,
				"profileName": profile_name,
				"valueType": "String",
				"value": json.dumps({source_name: json.dumps(metrics)}),
			}
		],
	}


def ensure_topics(admin: KafkaAdminClient, topics: list[str]) -> None:
	"""Create Kafka topics when they are missing."""

	if not topics:
		return

	new_topics = [NewTopic(name=topic, num_partitions=1, replication_factor=1) for topic in topics]
	try:
		admin.create_topics(new_topics=new_topics, validate_only=False)
	except TopicAlreadyExistsError:
		pass


def publish_events(
	bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
	topic_names: list[str] | None = None,
	messages_per_topic: int = DEFAULT_MESSAGES_PER_TOPIC,
	profile_name: str = DEFAULT_PROFILE_NAME,
	device_name: str = DEFAULT_DEVICE_NAME,
	topic_name: str = DEFAULT_TOPIC_NAME,
) -> None:
	"""Generate fake events and publish them to Kafka."""

	fake = Faker()
	source_names = topic_names or [
		"cpu_data",
		"gpu_data",
		"gpu_proc_data",
		"mem_data",
		"net_data",
		"sys_data",
		"disk_data",
		"proc_data",
	]
	brokers = [server.strip() for server in bootstrap_servers.split(",") if server.strip()]

	admin = KafkaAdminClient(bootstrap_servers=brokers)
	try:
		ensure_topics(admin, [topic_name])
	finally:
		admin.close()

	producer = KafkaProducer(
		bootstrap_servers=brokers,
		value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
		retries=3,
	)

	try:
		total_events = 0
		for source_name in source_names:
			print(f"Producing {messages_per_topic} events for source '{source_name}' to topic '{topic_name}'")
			for _ in range(messages_per_topic):
				event = build_event(fake, source_name, profile_name, device_name)
				future = producer.send(topic_name, value=event)
				record_metadata = future.get(timeout=10)
				total_events += 1
				print(
					f"✓ Event {total_events} -> topic: {record_metadata.topic}, "
					f"partition: {record_metadata.partition}, offset: {record_metadata.offset}"
				)

		producer.flush()
		print(f"✓ Successfully produced {total_events} events for {len(source_names)} source schemas to topic '{topic_name}'")
	finally:
		producer.close()


if __name__ == "__main__":
	raw_source_names = os.getenv("KAFKA_SOURCE_NAMES") or os.getenv("KAFKA_TOPICS")
	source_names = [name.strip() for name in raw_source_names.split(",") if name.strip()] if raw_source_names else None
	publish_events(topic_names=source_names)