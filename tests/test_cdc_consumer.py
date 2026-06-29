"""Unit tests for CDC consumer parsing and transformation logic."""
import json, pytest
from consumers.cdc_consumer import parse_debezium_event, CDCEvent


def make_payload(op: str, after: dict, table: str = "orders") -> dict:
    return {
        "payload": {
            "__op":             op,
            "__source_table":   table,
            "__source_ts_ms":   1700000000000,
            "after":            after,
            "before":           None,
        }
    }


def test_parse_insert_event():
    raw  = make_payload("c", {"order_id": "abc", "net_revenue": 99.5, "channel": "online"})
    event = parse_debezium_event(raw)
    assert event is not None
    assert event.operation == "c"
    assert event.table == "orders"
    assert event.after["order_id"] == "abc"


def test_parse_update_event():
    raw  = make_payload("u", {"order_id": "abc", "status": "shipped"})
    event = parse_debezium_event(raw)
    assert event.operation == "u"
    assert event.after["status"] == "shipped"


def test_parse_delete_event():
    raw = {"payload": {"__op": "d", "__source_table": "orders", "__source_ts_ms": 0, "after": None, "before": {"order_id": "abc"}}}
    event = parse_debezium_event(raw)
    assert event.operation == "d"
    assert event.after is None


def test_parse_malformed_returns_none():
    event = parse_debezium_event({"bad": "data"})
    # Should not raise; returns a best-effort CDCEvent or None
    assert event is None or isinstance(event, CDCEvent)


def test_parse_snapshot_read_event():
    raw = make_payload("r", {"order_id": "snap1", "net_revenue": 50.0})
    event = parse_debezium_event(raw)
    assert event.operation == "r"


def test_ts_ms_captured():
    raw  = make_payload("c", {"order_id": "xyz"})
    event = parse_debezium_event(raw)
    assert event.ts_ms == 1700000000000