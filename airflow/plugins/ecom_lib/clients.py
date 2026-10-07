"""Thin clients for Kafka, Kafka Connect, Spark master and ClickHouse. Endpoints come from env vars."""
import os

import clickhouse_connect
import requests
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "ecommerce_events_stream")
CONNECT_URL = os.environ.get("KAFKA_CONNECT_URL", "http://kafka-connect:8083")
SPARK_UI_URL = os.environ.get("SPARK_MASTER_UI_URL", "http://spark-master:8080")
CONNECTOR = "ecommerce-csv-source"
STREAMING_APP = "ecommerce-streaming-clean-clickhouse"


def ch():
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        database=os.environ.get("CLICKHOUSE_DB", "ecommerce"),
    )


def scalar(sql):
    return ch().query(sql).result_rows[0][0]


def kafka_topic_partitions():
    md = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP}).list_topics(topic=TOPIC, timeout=10)
    topic = md.topics.get(TOPIC)
    if topic is None or topic.error is not None:
        return None
    return sorted(topic.partitions.keys())


def kafka_end_offsets():
    parts = kafka_topic_partitions()
    if not parts:
        raise RuntimeError(f"topic {TOPIC} not found")
    consumer = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": "airflow-offset-probe",
                         "enable.auto.commit": False})
    try:
        per = {}
        for p in parts:
            _low, high = consumer.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10)
            per[p] = high
        return per, sum(per.values())
    finally:
        consumer.close()


def connect_status():
    r = requests.get(f"{CONNECT_URL}/connectors/{CONNECTOR}/status", timeout=10)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()


def spark_master_json():
    r = requests.get(f"{SPARK_UI_URL}/json/", timeout=10)
    r.raise_for_status()
    return r.json()


def streaming_job_running():
    try:
        apps = spark_master_json().get("activeapps", [])
    except Exception:
        return False
    return any(str(a.get("name", "")) == STREAMING_APP for a in apps)