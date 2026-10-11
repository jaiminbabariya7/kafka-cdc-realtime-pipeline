"""
CDC Event Consumer

Consumes Debezium CDC events from Kafka topics (cdc.public.*),
transforms them into BigQuery-compatible records, and streams
inserts/updates/deletes into the BigQuery raw CDC dataset as a change log
(every row carries _cdc_op, _cdc_ts_ms and _cdc_processed_at).

Delivery is at-least-once: Kafka offsets are committed only after ALL
buffered rows have been written to BigQuery. A failed BigQuery write stops
the consumer without committing, so the batch is re-read on restart.
Duplicates from redelivery are removed downstream (latest row per key).

Usage:
    python consumers/cdc_consumer.py --tables orders,customers,products
"""
from __future__ import annotations
import argparse, json, logging, os, signal, threading, time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

try:                                    # run as a script: python consumers/cdc_consumer.py
    from cdc_events import CDCEvent, parse_debezium_event
except ImportError:                     # imported as a package (tests)
    from consumers.cdc_events import CDCEvent, parse_debezium_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("cdc.consumer")

BQ_DATASET  = os.getenv("BQ_CDC_DATASET", "cdc_raw")
GROUP_ID    = os.getenv("KAFKA_GROUP_ID", "cdc-bq-consumer")

_bq_client = None


def _bq():
    """BigQuery client, created on first use (keeps imports side-effect free)."""
    global _bq_client
    if _bq_client is None:
        from google.cloud import bigquery
        _bq_client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])
    return _bq_client


def to_bq_row(event: CDCEvent) -> dict[str, Any]:
    """Convert a CDCEvent into a BigQuery change-log row."""
    row = dict(event.row)
    row.update({
        "_cdc_op":           event.operation,
        "_cdc_ts_ms":        event.ts_ms,
        "_cdc_processed_at": datetime.now(timezone.utc).isoformat(),
    })
    return row


def stream_to_bigquery(rows: list[dict], table_name: str) -> None:
    """Stream a batch of rows to BigQuery; raises on any insert error."""
    bq_table = f"{os.environ['GCP_PROJECT_ID']}.{BQ_DATASET}.{table_name}"
    errors   = _bq().insert_rows_json(bq_table, rows)
    if errors:
        logger.error("BigQuery insert errors for %s: %s", table_name, errors)
        raise RuntimeError(f"BigQuery insert failed: {errors}")
    logger.info("Streamed %d rows to %s", len(rows), bq_table)


def flush_all(batch: dict[str, list]) -> int:
    """Write every buffered table batch; returns rows written."""
    written = 0
    for tname, rows in batch.items():
        if rows:
            stream_to_bigquery(rows, tname)
            written += len(rows)
            rows.clear()
    return written


def _start_health_server() -> None:
    """Answer HTTP health checks when PORT is set (Cloud Run requires a listener)."""
    port = os.getenv("PORT")
    if not port:
        return

    class _Health(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200); self.end_headers(); self.wfile.write(b"ok")

        def log_message(self, *args):
            pass

    server = HTTPServer(("0.0.0.0", int(port)), _Health)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    logger.info("Health endpoint listening on :%s", port)


def run(tables: list[str]) -> None:
    from confluent_kafka import Consumer, KafkaError, KafkaException

    topics = [f"cdc.public.{t}" for t in tables]
    conf = {
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "group.id":          GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": 300_000,
        "session.timeout.ms":   30_000,
    }
    consumer = Consumer(conf)
    consumer.subscribe(topics)
    logger.info("Subscribed to topics: %s", topics)
    _start_health_server()

    batch: dict[str, list] = {t: [] for t in tables}
    BATCH_SIZE  = int(os.getenv("BATCH_SIZE", "500"))
    FLUSH_SECS  = int(os.getenv("FLUSH_INTERVAL_SECS", "30"))
    running     = True
    last_flush  = time.monotonic()

    def _shutdown(sig, frame):
        nonlocal running
        logger.info("Shutdown signal received.")
        running = False

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT,  _shutdown)

    def _flush_and_commit() -> None:
        nonlocal last_flush
        if flush_all(batch):                     # raises on failure -> no commit
            consumer.commit(asynchronous=False)
        last_flush = time.monotonic()

    try:
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        raise KafkaException(msg.error())
                elif msg.value() is not None:    # skip tombstones
                    try:
                        event = parse_debezium_event(json.loads(msg.value().decode("utf-8")))
                    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                        logger.warning("Skipping unparseable message: %s", exc)
                        event = None
                    if event:
                        batch.setdefault(event.table, []).append(to_bq_row(event))

            buffered = sum(len(r) for r in batch.values())
            if buffered >= BATCH_SIZE or (buffered and time.monotonic() - last_flush >= FLUSH_SECS):
                _flush_and_commit()
        _flush_and_commit()                      # clean shutdown: flush, then commit
    finally:
        consumer.close()
        logger.info("Consumer shut down.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tables", default="orders,customers,products,order_items")
    args   = parser.parse_args()
    run(args.tables.split(","))