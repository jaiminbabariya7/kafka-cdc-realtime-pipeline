"""
Stream Processor

Consumes CDC events for new orders and order lines from Kafka, computes
real-time aggregations over a tumbling window (revenue, order count,
revenue by channel, top-5 products by units), and writes one row per
window to the BigQuery streaming_metrics table every 30 seconds.

Only newly created rows (op = "c") count: updates, deletes and snapshot
reads of existing rows are ignored so an order is never counted twice.
The window lives in memory, so these are live operational metrics; the
cdc_consumer change log in BigQuery remains the source of truth.

Runs as a long-lived service alongside the CDC consumer.
"""
from __future__ import annotations
import json, logging, os, signal, time
from collections import defaultdict
from datetime import datetime, timezone

try:                                    # run as a script
    from cdc_events import CDCEvent, parse_debezium_event
except ImportError:                     # imported as a package (tests)
    from consumers.cdc_events import CDCEvent, parse_debezium_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("cdc.processor")

BQ_DATASET  = os.getenv("BQ_CDC_DATASET", "cdc_raw")
FLUSH_SECS  = int(os.getenv("FLUSH_INTERVAL_SECS", "30"))
TOPICS      = ["cdc.public.orders", "cdc.public.order_items"]


class RollingWindow:
    """In-memory accumulator for one tumbling time window."""

    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.total_revenue:   float = 0.0
        self.order_count:     int   = 0
        self.product_units:   dict[str, int]   = defaultdict(int)
        self.channel_revenue: dict[str, float] = defaultdict(float)
        self.window_start: datetime = datetime.now(timezone.utc)

    @property
    def is_empty(self) -> bool:
        return self.order_count == 0 and not self.product_units

    def ingest(self, event: CDCEvent) -> None:
        if event.operation != "c":
            return
        row = event.row
        if event.table == "orders":
            revenue = float(row.get("net_revenue") or 0)
            self.total_revenue += revenue
            self.order_count   += 1
            self.channel_revenue[row.get("channel") or "unknown"] += revenue
        elif event.table == "order_items":
            self.product_units[str(row.get("product_id", "unknown"))] += int(row.get("quantity") or 0)

    def to_bq_row(self) -> dict:
        top = sorted(self.product_units.items(), key=lambda x: -x[1])[:5]
        return {
            "window_start":    self.window_start.isoformat(),
            "window_end":      datetime.now(timezone.utc).isoformat(),
            "window_secs":     FLUSH_SECS,
            "total_revenue":   round(self.total_revenue, 2),
            "order_count":     self.order_count,
            "top_products":    json.dumps([{"product_id": p, "units": u} for p, u in top]),
            "channel_revenue": json.dumps({k: round(v, 2) for k, v in self.channel_revenue.items()}),
        }


def flush(window: RollingWindow, bq) -> None:
    if window.is_empty:
        return
    bq_table = f"{os.environ['GCP_PROJECT_ID']}.{BQ_DATASET}.streaming_metrics"
    errors   = bq.insert_rows_json(bq_table, [window.to_bq_row()])
    if errors:
        logger.error("BQ flush errors: %s", errors)
    else:
        logger.info("Flushed window: %d orders, $%.2f revenue",
                    window.order_count, window.total_revenue)


def run() -> None:
    from confluent_kafka import Consumer
    from google.cloud import bigquery

    bq = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])
    consumer = Consumer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "group.id":          "cdc-stream-processor",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,      # live metrics: approximate on restart is fine
    })
    consumer.subscribe(TOPICS)
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
            if msg is not None and not msg.error() and msg.value() is not None:
                try:
                    event = parse_debezium_event(json.loads(msg.value().decode("utf-8")))
                    if event:
                        window.ingest(event)
                except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
                    logger.warning("Parse error: %s", exc)

            if time.monotonic() - last_flush >= FLUSH_SECS:
                flush(window, bq)
                window.reset()
                last_flush = time.monotonic()
    finally:
        flush(window, bq)
        consumer.close()


if __name__ == "__main__":
    run()
