"""
Shared parsing for Debezium CDC messages.

Handles both message shapes:
  * flattened by the ExtractNewRecordState SMT (this project's connector):
    row columns plus __op / __source_table / __source_ts_ms / __deleted
  * the raw Debezium envelope: before / after / op / source

Pure Python (no Kafka or BigQuery imports) so it can be unit-tested directly.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

# Debezium / Kafka Connect logical types that arrive as "days since epoch"
_DATE_TYPES = {"io.debezium.time.Date", "org.apache.kafka.connect.data.Date"}
_EPOCH = date(1970, 1, 1)


@dataclass
class CDCEvent:
    table:     str
    operation: str                      # c=create, u=update, d=delete, r=snapshot read
    before:    dict[str, Any] | None
    after:     dict[str, Any] | None
    ts_ms:     int
    source:    dict[str, Any] = field(default_factory=dict)

    @property
    def row(self) -> dict[str, Any]:
        """Current row values; for deletes, the last known values."""
        return self.after if self.after is not None else (self.before or {})


def _date_fields(schema: dict | None) -> set[str]:
    """Names of fields the JSON converter encoded as epoch-day integers."""
    if not schema:
        return set()
    names: set[str] = set()
    for f in schema.get("fields", []):
        if f.get("name") in _DATE_TYPES:
            names.add(f.get("field"))
        for inner in f.get("fields", []) or []:          # envelope: before/after structs
            if inner.get("name") in _DATE_TYPES:
                names.add(inner.get("field"))
    return names


def _convert_dates(row: dict | None, date_fields: set[str]) -> dict | None:
    if row is None:
        return None
    out = dict(row)
    for name in date_fields:
        if isinstance(out.get(name), int):
            out[name] = (_EPOCH + timedelta(days=out[name])).isoformat()
    return out


def parse_debezium_event(raw: dict | None) -> CDCEvent | None:
    """Parse a Debezium JSON message into a CDCEvent, or None if it isn't one."""
    if not isinstance(raw, dict):
        return None
    payload = raw.get("payload", raw)
    if not isinstance(payload, dict):
        return None
    dates = _date_fields(raw.get("schema"))

    # Raw envelope (no SMT)
    if "op" in payload and ("before" in payload or "after" in payload):
        source = payload.get("source") or {}
        return CDCEvent(
            table=source.get("table", "unknown"),
            operation=payload["op"],
            before=_convert_dates(payload.get("before"), dates),
            after=_convert_dates(payload.get("after"), dates),
            ts_ms=int(source.get("ts_ms", payload.get("ts_ms", 0)) or 0),
            source=source,
        )

    # Flattened by ExtractNewRecordState
    if "__op" in payload:
        op  = payload["__op"]
        row = _convert_dates({k: v for k, v in payload.items() if not k.startswith("__")}, dates)
        return CDCEvent(
            table=payload.get("__source_table", "unknown"),
            operation=op,
            before=row if op == "d" else None,
            after=None if op == "d" else row,
            ts_ms=int(payload.get("__source_ts_ms", 0) or 0),
        )
    return None
