"""Import NSE bhav CSV into PostgreSQL `NSE_BHAV.stock_market_data`.

Features:
- Reads a bhav CSV (header expected like the sample in `sec_bhavdata_full_13022026.csv`).
- Filters rows where `SERIES` == 'EQ' only.
- Creates schema/table if missing (schema: `NSE_BHAV`).
- Upserts rows on (symbol, series, date1) primary key.

Public API:
- import_bhav_csv(fileobj, *, schema='NSE_BHAV', table='stock_market_data') -> int

This module is imported by the Flask endpoint `POST /import/stock_market_data`.
"""
from __future__ import annotations

import csv
import io
import logging
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from typing import BinaryIO, Iterable, List, Optional, TextIO, Tuple

from db import connection as db_connection

logger = logging.getLogger(__name__)

# Target columns in DB (order used for INSERT)
_DB_COLS = (
    "symbol",
    "series",
    "date1",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "ttl_trd_qnty",
    "deliv_qty",
    "deliv_per",
)


def _clean_number(s: Optional[str]) -> Optional[Decimal]:
    if s is None:
        return None
    s = s.strip()
    if s in ("", "-", "NA", "N/A"):
        return None
    s = s.replace(",", "")
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def _clean_int(s: Optional[str]) -> Optional[int]:
    dec = _clean_number(s)
    if dec is None:
        return None
    try:
        return int(dec)
    except (ValueError, InvalidOperation):
        return None


def _parse_date(s: str) -> date:
    s = s.strip()
    # CSV uses '13-Feb-2026' style format
    for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            continue
    raise ValueError(f"Unrecognized date format: {s!r}")


def _row_to_tuple(row: dict) -> Optional[Tuple]:
    # Normalize keys from CSV (strip spaces)
    row = {k.strip(): (v if v is not None else "") for k, v in row.items()}

    if row.get("SERIES", "").strip().upper() != "EQ":
        return None

    try:
        dt = _parse_date(row.get("DATE1", ""))
    except Exception:
        logger.warning("Skipping row with unparseable date: %r", row.get("DATE1"))
        return None

    return (
        row.get("SYMBOL", "").strip(),
        row.get("SERIES", "").strip(),
        dt,
        _clean_number(row.get("OPEN_PRICE")),
        _clean_number(row.get("HIGH_PRICE")),
        _clean_number(row.get("LOW_PRICE")),
        _clean_number(row.get("CLOSE_PRICE")),
        _clean_int(row.get("TTL_TRD_QNTY")),
        _clean_int(row.get("DELIV_QTY")),
        _clean_number(row.get("DELIV_PER")),
    )


def _rows_from_csv(f: TextIO) -> Iterable[Tuple]:
    reader = csv.DictReader(f)
    for raw in reader:
        tup = _row_to_tuple(raw)
        if tup is not None:
            yield tup


def import_bhav_csv(
    fileobj: TextIO | BinaryIO | str,
    *,
    schema: str = "NSE_BHAV",
    table: str = "stock_market_data",
    batch_size: int = 500,
) -> int:
    """Import the provided CSV into the database.

    - fileobj: path or a text/binary file-like object (text mode preferred).
    - Returns number of rows processed (inserted/upserted).
    """
    # Accept file path or file-like (binary/text)
    if isinstance(fileobj, str):
        fh = open(fileobj, "r", encoding="utf-8")
        close_after = True
    else:
        # if binary, wrap in TextIO
        if isinstance(fileobj, io.BytesIO) or hasattr(fileobj, "read") and isinstance(
            getattr(fileobj, "read"), type(open(__file__, "rb").read)
        ):
            try:
                # try to detect binary vs text: read(0) returns bytes for binary streams
                peek = fileobj.read(0)
                is_binary = isinstance(peek, (bytes, bytearray))
            except Exception:
                is_binary = False

            if is_binary:
                fh = io.TextIOWrapper(fileobj, encoding="utf-8")
            else:
                fh = fileobj  # type: ignore
        else:
            fh = fileobj  # type: ignore
        close_after = False

    rows: List[Tuple] = list(_rows_from_csv(fh))
    if close_after:
        fh.close()

    if not rows:
        return 0

    insert_sql = f"""
    INSERT INTO {schema}.{table} ({', '.join(_DB_COLS)})
    VALUES ({', '.join(['%s'] * len(_DB_COLS))})
    ON CONFLICT (symbol, series, date1) DO UPDATE SET
      open_price = EXCLUDED.open_price,
      high_price = EXCLUDED.high_price,
      low_price = EXCLUDED.low_price,
      close_price = EXCLUDED.close_price,
      ttl_trd_qnty = EXCLUDED.ttl_trd_qnty,
      deliv_qty = EXCLUDED.deliv_qty,
      deliv_per = EXCLUDED.deliv_per
    """

    total = 0
    with db_connection.get_cursor(commit=True) as cur:
        # ensure schema exists (separate statement)
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        
        # ensure table exists (separate statement)
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            symbol           VARCHAR(20)        NOT NULL,
            series           VARCHAR(10)        NOT NULL,
            date1            DATE               NOT NULL,
            open_price       NUMERIC(12,4),
            high_price       NUMERIC(12,4),
            low_price        NUMERIC(12,4),
            close_price      NUMERIC(12,4),
            ttl_trd_qnty     BIGINT,
            deliv_qty        BIGINT,
            deliv_per        NUMERIC(6,2),
            PRIMARY KEY (symbol, series, date1)
        );
        """)

        # batch insert/upsert
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            cur.executemany(insert_sql, batch)
            total += len(batch)

    return total


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Import bhav CSV into Postgres table")
    parser.add_argument("path", nargs="?", default="sec_bhavdata_full_13022026.csv")
    args = parser.parse_args()

    count = import_bhav_csv(args.path)
    print(f"Inserted/updated {count} rows")
