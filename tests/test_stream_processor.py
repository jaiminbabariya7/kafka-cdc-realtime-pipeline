"""Unit tests for the rolling window stream processor."""
from consumers.stream_processor import RollingWindow


def test_ingest_adds_revenue():
    w = RollingWindow()
    w.ingest({"_cdc_op": "c", "net_revenue": "150.0", "channel": "online", "product_id": "p1"})
    assert w.total_revenue == 150.0
    assert w.order_count == 1


def test_delete_op_ignored():
    w = RollingWindow()
    w.ingest({"_cdc_op": "d", "net_revenue": "100.0"})
    assert w.total_revenue == 0.0
    assert w.order_count == 0


def test_multiple_channels():
    w = RollingWindow()
    w.ingest({"_cdc_op": "c", "net_revenue": "100", "channel": "online",  "product_id": "p1"})
    w.ingest({"_cdc_op": "c", "net_revenue": "200", "channel": "store",   "product_id": "p2"})
    w.ingest({"_cdc_op": "u", "net_revenue": "50",  "channel": "online",  "product_id": "p1"})
    assert w.channel_revenue["online"] == 150.0
    assert w.channel_revenue["store"]  == 200.0
    assert w.order_count == 3


def test_top_products():
    w = RollingWindow()
    for _ in range(3):
        w.ingest({"_cdc_op": "c", "net_revenue": "10", "channel": "web", "product_id": "p_top"})
    w.ingest({"_cdc_op": "c", "net_revenue": "10", "channel": "web", "product_id": "p_other"})
    row = w.to_bq_row()
    import json
    top = json.loads(row["top_products"])
    assert top[0]["product_id"] == "p_top"
    assert top[0]["count"] == 3


def test_reset_clears_state():
    w = RollingWindow()
    w.ingest({"_cdc_op": "c", "net_revenue": "99", "channel": "x", "product_id": "p1"})
    w.reset()
    assert w.total_revenue == 0.0
    assert w.order_count == 0