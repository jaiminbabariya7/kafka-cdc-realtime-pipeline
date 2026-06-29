"""
Stream Processor

Consumes CDC order events from Kafka in micro-batches, computes
real-time aggregations (rolling revenue, order counts, top products),
and writes them to BigQuery streaming_metrics table every 30 seconds.

Runs as a long-lived service alongside the CDC consumer.
"""
from __future__ import annotations
import json, logging, os, signal, time
from collections import defaultdict
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("cdc.processor")

PROJECT_ID  = os.environ["GCP_PROJECT_ID"]
BQ_DATASET  = os.getenv("BQ_CDC_DATASET", "cdc_raw")
KAFKA_BROKERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
FLUSH_SECS  = int(os.getenv("FLUSH_INTERVAL_SECS", "30"))

bq = bigquery.Client(project=PROJECT_ID)


class RollingWindow:
    """In-memory accumulator for a tumbling time window."""

    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.total_revenue: float = 0.0
        self.order_count:   int   = 0
        self.product_counts: dict[str, int] = defaultdict(int)
        self.channel_revenue: dict[str, float] = defaultdict(float)
        self.window_start: datetime = datetime.now(timezone.utc)

    def ingest(self, event: dict) -> None:
        op = event.get("_cdc_op", "c")
        if op not in ("c", "u"):
            return
        revenue   = float(event.get("net_revenue", 0) or 0)
        channel   = event.get("channel", "unknown")
        product   = event.get("product_id", "unknown")
        self.total_revenue            += revenue
        self.order_count              += 1
        self.product_counts[product]  += 1
        self.channel_revenue[channel] += revenue

    def to_bq_row(self) -> dict:
        top_products = sorted(self.product_counts.items(), key=lambda x: -x[1])[:5]
        return {
            "window_start":   self.window_start.isoformat(),
            "window_end":     datetime.now(timezone.utc).isoformat(),
            "window_secs":    FLUSH_SECS,
            "total_revenue":  round(self.total_revenue, 2),
            "order_count":    self.order_count,
            "top_products":   json.dumps([{"product_id": p, "count": c} for p, c in top_products]),
            "channel_revenue": json.dumps(dict(self.channel_revenue)),
        }


def flush(window: RollingWindow) -> None:
    if window.order_count == 0:
        return
    row      = window.to_bq_row()
    bq_table = f"{PROJECT_ID}.{BQ_DATASET}.streaming_metrics"
    errors   = bq.insert_rows_json(bq_table, [row])
    if errors:
        logger.error("BQ flush errors: %s", errors)
    else:
        logger.info("Flushed window: %d orders, $%.2f revenue", window.order_count, window.total_revenue)


def run() -> None:
    conf = {
        "bootstrap.servers": KAFKA_BROKERS,
        "group.id":          "cdc-stream-processor",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    }
    consumer = Consumer(conf)
    consumer.subscribe(["cdc.public.orders"])
    window  = RollingWindow()
    running = True
    last_flush = time.monotonic()

    def _shutdown(sig, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT,  _shutdown)

    try:
        while running:
            msg = consumer.poll(timeout=0.5)
            if msg and not msg.error():
                try:
                    event = json.loads(msg.value().decode("utf-8"))
                    window.ingest(event)
                except Exception as exc:
                    logger.warning("Parse error: %s", exc)

            if time.monotonic() - last_flush >= FLUSH_SECS:
                flush(window)
                window.reset()
                last_flush = time.monotonic()
    finally:
        flush(window)
        consumer.close()


if __name__ == "__main__":
    run()