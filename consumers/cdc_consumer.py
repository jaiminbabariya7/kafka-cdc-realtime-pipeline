"""
CDC Event Consumer

Consumes Debezium CDC events from Kafka topics (cdc.public.*),
transforms them into BigQuery-compatible records, and streams
inserts/updates/deletes into the BigQuery raw CDC dataset.

Provides at-least-once delivery: Kafka consumer group offsets are
committed only after every pending batch has been written to BigQuery.

Usage:
    python consumers/cdc_consumer.py --tables orders,customers,products
"""
from __future__ import annotations
import argparse, json, logging, os, signal
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any
from confluent_kafka import Consumer, KafkaError, KafkaException
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("cdc.consumer")


def project_id() -> str:
    return os.environ["GCP_PROJECT_ID"]


def bq_dataset() -> str:
    return os.getenv("BQ_CDC_DATASET", "cdc_raw")


def kafka_brokers() -> str:
    return os.environ["KAFKA_BOOTSTRAP_SERVERS"]


def group_id() -> str:
    return os.getenv("KAFKA_GROUP_ID", "cdc-bq-consumer")


@lru_cache(maxsize=1)
def bq_client() -> bigquery.Client:
    return bigquery.Client(project=project_id())


@dataclass
class CDCEvent:
    table:     str
    operation: str   # c=create, u=update, d=delete, r=read(snapshot)
    before:    dict[str, Any] | None
    after:     dict[str, Any] | None
    ts_ms:     int
    source:    dict[str, Any] = field(default_factory=dict)


def parse_debezium_event(raw: dict) -> CDCEvent | None:
    """Parse a Debezium CDC JSON envelope into a CDCEvent."""
    try:
        payload = raw.get("payload", raw)
        op      = payload.get("__op", payload.get("op", "r"))
        table   = payload.get("__source_table", "unknown")
        ts_ms   = payload.get("__source_ts_ms", 0)
        if "after" in payload:
            after = payload["after"]
        else:
            # Payload already flattened by ExtractNewRecordState: the row
            # columns sit at the top level alongside the __-prefixed metadata.
            after = {k: v for k, v in payload.items() if not k.startswith("_")} or None
        return CDCEvent(
            table=table, operation=op,
            before=payload.get("before"),
            after=after,
            ts_ms=ts_ms, source=payload.get("source", {}),
        )
    except Exception as exc:
        logger.warning("Failed to parse CDC event: %s — %s", exc, raw)
        return None


def to_bq_row(event: CDCEvent) -> dict[str, Any]:
    """Convert a CDCEvent into a BigQuery row dict."""
    row = {
        "_cdc_op":        event.operation,
        "_cdc_ts_ms":     event.ts_ms,
        "_cdc_processed_at": datetime.now(timezone.utc).isoformat(),
    }
    if event.after:
        row.update(event.after)
    return row


def stream_to_bigquery(rows: list[dict], table_name: str) -> None:
    """Stream a batch of rows to BigQuery using insert_rows_json."""
    bq_table = f"{project_id()}.{bq_dataset()}.{table_name}"
    errors   = bq_client().insert_rows_json(bq_table, rows)
    if errors:
        logger.error("BigQuery insert errors for %s: %s", table_name, errors)
        raise RuntimeError(f"BigQuery insert failed: {errors}")
    logger.info("Streamed %d rows to %s", len(rows), bq_table)


def run(tables: list[str]) -> None:
    topics = [f"cdc.public.{t}" for t in tables]

    conf = {
        "bootstrap.servers": kafka_brokers(),
        "group.id":          group_id(),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": 300_000,
        "session.timeout.ms":   30_000,
    }
    consumer = Consumer(conf)
    consumer.subscribe(topics)
    logger.info("Subscribed to topics: %s", topics)

    batch: dict[str, list] = {t: [] for t in tables}
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
    running    = True

    def flush_all() -> None:
        """Write every pending batch to BigQuery, then commit offsets."""
        for tname, rows in batch.items():
            if rows:
                stream_to_bigquery(rows, tname)
                rows.clear()
        consumer.commit(asynchronous=False)

    def _shutdown(sig, frame):
        nonlocal running
        logger.info("Shutdown signal received.")
        running = False

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT,  _shutdown)

    try:
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            try:
                raw   = json.loads(msg.value().decode("utf-8"))
                event = parse_debezium_event(raw)
                if not event:
                    continue

                table_batch = batch.setdefault(event.table, [])
                table_batch.append(to_bq_row(event))

                # Flush when batch is full. Offsets cover every topic the
                # consumer group reads, so all pending batches must be written
                # before committing.
                if len(table_batch) >= BATCH_SIZE:
                    flush_all()

            except Exception as exc:
                logger.error("Error processing message: %s", exc, exc_info=True)

    finally:
        try:
            flush_all()
        except Exception as exc:
            logger.error("Final flush failed: %s", exc)
        consumer.close()
        logger.info("Consumer shut down cleanly.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tables", default="orders,customers,products,order_items")
    args   = parser.parse_args()
    run(args.tables.split(","))