"""Unit tests for the rolling window stream processor."""
import json

from consumers.cdc_events import CDCEvent
from consumers.stream_processor import RollingWindow


def order(op="c", revenue=100.0, channel="online"):
    row = {"order_id": "o1", "net_revenue": revenue, "channel": channel}
    return CDCEvent(table="orders", operation=op,
                    before=row if op == "d" else None,
                    after=None if op == "d" else row, ts_ms=0)


def item(product, qty=1, op="c"):
    return CDCEvent(table="order_items", operation=op, before=None,
                    after={"order_id": "o1", "product_id": product, "quantity": qty}, ts_ms=0)


def test_new_order_adds_revenue_and_count():
    w = RollingWindow()
    w.ingest(order(revenue=150.0))
    assert w.total_revenue == 150.0
    assert w.order_count == 1


def test_updates_deletes_and_snapshots_not_counted():
    w = RollingWindow()
    for op in ("u", "d", "r"):
        w.ingest(order(op=op, revenue=100.0))
    assert w.total_revenue == 0.0
    assert w.order_count == 0
    assert w.is_empty


def test_revenue_by_channel():
    w = RollingWindow()
    w.ingest(order(revenue=100, channel="online"))
    w.ingest(order(revenue=200, channel="store"))
    w.ingest(order(revenue=50,  channel="online"))
    assert w.channel_revenue["online"] == 150.0
    assert w.channel_revenue["store"] == 200.0
    assert w.order_count == 3


def test_top_products_by_units_from_order_items():
    w = RollingWindow()
    w.ingest(item("p_top", qty=3))
    w.ingest(item("p_top", qty=2))
    w.ingest(item("p_other", qty=4))
    top = json.loads(w.to_bq_row()["top_products"])
    assert top[0] == {"product_id": "p_top", "units": 5}
    assert top[1] == {"product_id": "p_other", "units": 4}
    assert w.order_count == 0          # order lines don't count as orders


def test_numeric_revenue_as_string():
    w = RollingWindow()
    w.ingest(order(revenue="99.50"))
    assert w.total_revenue == 99.5


def test_reset_clears_state():
    w = RollingWindow()
    w.ingest(order(revenue=99))
    w.ingest(item("p1"))
    w.reset()
    assert w.is_empty
    assert w.total_revenue == 0.0
