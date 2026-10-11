"""Unit tests for CDC event parsing and BigQuery row conversion."""
from consumers.cdc_consumer import flush_all, to_bq_row
from consumers.cdc_events import CDCEvent, parse_debezium_event


def flat(op, row, table="orders", **extra):
    """Message shaped like this project's connector output (ExtractNewRecordState)."""
    payload = dict(row, __op=op, __source_table=table, __source_ts_ms=1700000000000, **extra)
    return {"payload": payload}


def test_parse_flattened_insert():
    event = parse_debezium_event(flat("c", {"order_id": "abc", "net_revenue": 99.5}))
    assert event.operation == "c"
    assert event.table == "orders"
    assert event.after == {"order_id": "abc", "net_revenue": 99.5}
    assert event.ts_ms == 1700000000000


def test_parse_flattened_update():
    event = parse_debezium_event(flat("u", {"order_id": "abc", "status": "shipped"}))
    assert event.operation == "u"
    assert event.after["status"] == "shipped"


def test_parse_flattened_delete_keeps_last_values():
    event = parse_debezium_event(flat("d", {"order_id": "abc"}, __deleted="true"))
    assert event.operation == "d"
    assert event.after is None
    assert event.before == {"order_id": "abc"}       # __deleted metadata stripped


def test_parse_raw_envelope_delete():
    raw = {"payload": {"op": "d", "before": {"order_id": "abc"}, "after": None,
                       "source": {"table": "orders", "ts_ms": 5}}}
    event = parse_debezium_event(raw)
    assert event.operation == "d"
    assert event.after is None
    assert event.row == {"order_id": "abc"}
    assert event.table == "orders"


def test_parse_snapshot_read():
    assert parse_debezium_event(flat("r", {"order_id": "snap1"})).operation == "r"


def test_parse_malformed_returns_none():
    assert parse_debezium_event({"bad": "data"}) is None
    assert parse_debezium_event(None) is None


def test_epoch_day_dates_converted_using_schema():
    raw = flat("c", {"order_id": "a", "order_date": 19723})
    raw["schema"] = {"type": "struct", "fields": [
        {"field": "order_id", "type": "string"},
        {"field": "order_date", "type": "int32", "name": "io.debezium.time.Date"},
    ]}
    assert parse_debezium_event(raw).after["order_date"] == "2024-01-01"


def test_to_bq_row_adds_audit_columns():
    event = CDCEvent(table="orders", operation="d", before={"order_id": "abc"},
                     after=None, ts_ms=42)
    row = to_bq_row(event)
    assert row["order_id"] == "abc"
    assert row["_cdc_op"] == "d"
    assert row["_cdc_ts_ms"] == 42
    assert "T" in row["_cdc_processed_at"]          # ISO timestamp, not a type name


def test_flush_all_writes_every_table_before_commit(monkeypatch):
    written = []
    monkeypatch.setattr("consumers.cdc_consumer.stream_to_bigquery",
                        lambda rows, t: written.append((t, len(rows))))
    batch = {"orders": [{"a": 1}, {"a": 2}], "customers": [{"b": 1}], "products": []}
    assert flush_all(batch) == 3
    assert sorted(written) == [("customers", 1), ("orders", 2)]
    assert all(not rows for rows in batch.values())
