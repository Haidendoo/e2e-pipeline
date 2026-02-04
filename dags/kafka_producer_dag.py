"""
Kafka Producer DAG
Produces sample events to Kafka topic
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def produce_to_kafka():
    """Produce sample events to Kafka"""
    try:
        from kafka import KafkaProducer
    except ImportError:
        raise ImportError("kafka-python not installed. Install with: pip install kafka-python")
    
    # Kafka broker address (internal Docker network)
    bootstrap_servers = ["kafka:9092"]
    topic = "iceberg_events"
    
    print(f"Connecting to Kafka at {bootstrap_servers}")
    
    # Create producer
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=3,
    )
    
    # Sample events
    events = [
        {
            "event_id": 1,
            "event_type": "user_signup",
            "user_id": 101,
            "timestamp": datetime.now().isoformat(),
            "data": {"email": "user1@example.com", "country": "US"},
        },
        {
            "event_id": 2,
            "event_type": "purchase",
            "user_id": 102,
            "timestamp": datetime.now().isoformat(),
            "data": {"product_id": 5001, "amount": 99.99, "currency": "USD"},
        },
        {
            "event_id": 3,
            "event_type": "page_view",
            "user_id": 101,
            "timestamp": datetime.now().isoformat(),
            "data": {"page_url": "/products", "duration_seconds": 45},
        },
        {
            "event_id": 4,
            "event_type": "user_logout",
            "user_id": 102,
            "timestamp": datetime.now().isoformat(),
            "data": {"session_duration_minutes": 120},
        },
    ]
    
    # Produce events
    print(f"Producing {len(events)} events to topic '{topic}'")
    for event in events:
        future = producer.send(topic, value=event)
        record_metadata = future.get(timeout=10)
        print(
            f"✓ Event {event['event_id']} -> "
            f"topic: {record_metadata.topic}, "
            f"partition: {record_metadata.partition}, "
            f"offset: {record_metadata.offset}"
        )
    
    # Flush and close
    producer.flush()
    producer.close()
    
    print(f"✓ Successfully produced {len(events)} events to Kafka topic '{topic}'")


def consume_from_kafka():
    """Consume and display events from Kafka"""
    try:
        from kafka import KafkaConsumer, TopicPartition
    except ImportError:
        raise ImportError("kafka-python not installed. Install with: pip install kafka-python")
    
    bootstrap_servers = ["kafka:9092"]
    topic = "iceberg_events"
    
    print(f"Consuming from Kafka topic '{topic}'")
    
    # Create consumer without group_id for direct partition assignment
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    
    # Get partitions for the topic
    partitions = consumer.partitions_for_topic(topic)
    if partitions is None:
        print(f"Warning: Topic '{topic}' not found. Creating it...")
        consumer.close()
        return
    
    print(f"Assigning partitions: {partitions}")
    # Assign all partitions
    topic_partitions = [TopicPartition(topic, p) for p in partitions]
    consumer.assign(topic_partitions)
    # Seek to beginning
    for tp in topic_partitions:
        consumer.seek(tp, 0)
    
    # Consume messages
    message_count = 0
    print("-" * 80)
    for message in consumer:
        event = message.value
        message_count += 1
        print(f"Event {message_count}:")
        print(f"  Type: {event.get('event_type')}")
        print(f"  User ID: {event.get('user_id')}")
        print(f"  Data: {event.get('data')}")
        print()
    
    consumer.close()
    
    print("-" * 80)
    print(f"✓ Consumed {message_count} events from Kafka")


with DAG(
    dag_id="kafka_producer_dag",
    default_args=DEFAULT_ARGS,
    description="DAG to produce and consume events from Kafka",
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["kafka", "streaming"],
) as dag:

    produce_task = PythonOperator(
        task_id="produce_events",
        python_callable=produce_to_kafka,
    )

    consume_task = PythonOperator(
        task_id="consume_events",
        python_callable=consume_from_kafka,
    )

    # Dependency: consume after produce
    produce_task >> consume_task
