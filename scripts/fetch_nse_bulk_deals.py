"""Fetch NSE Bulk Deals (ad-hoc scraper).

Usage:
  python scripts/fetch_nse_bulk_deals.py --format json
  python scripts/fetch_nse_bulk_deals.py --output bulk.json

Notes:
- This script uses the public NSE Bulk Deals page and may break if NSE changes the page.
- For production, prefer an official/licensed data feed.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from datetime import date, datetime, timedelta
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}

BASE_URL = "https://www.nseindia.com"
BULK_DEALS_PATH = "/products/content/equities/equities/eq_bulkd_deals.htm"


def _safe_get(session: requests.Session, url: str, **kw) -> requests.Response:
    """GET with simple retry/backoff and raise_for_status."""
    attempts = 3
    backoff = 0.5
    for attempt in range(1, attempts + 1):
        resp = session.get(url, timeout=10, **kw)
        if resp.status_code == 200:
            return resp
        if attempt < attempts:
            time.sleep(backoff)
            backoff *= 2
    resp.raise_for_status()
    return resp


def fetch_bulk_deals(session: Optional[requests.Session] = None) -> List[Dict[str, str]]:
    """Return list of bulk-deal records scraped from NSE public page.

    Each record is a dict with keys such as: date, client, script, quantity, price, buy_sell.
    """
    close_session = False
    if session is None:
        session = requests.Session()
        close_session = True

    session.headers.update(HEADERS)

    # Seed cookies / anti-bot requirements by visiting the home page first
    _safe_get(session, BASE_URL)

    resp = _safe_get(session, BASE_URL + BULK_DEALS_PATH)
    soup = BeautifulSoup(resp.text, "html.parser")

    # Find the table which contains the bulk deals by matching header text.
    table = None
    for t in soup.find_all("table"):
        heads = " ".join(th.get_text(strip=True).lower() for th in t.find_all("th"))
        if "client" in heads and "script" in heads:
            table = t
            break

    if table is None:
        # Page might be JS-rendered or structure changed.
        raise RuntimeError(
            "Bulk deals table not found on page. NSE may have changed the page or it is JS-rendered."
        )

    rows: List[Dict[str, str]] = []
    trs = table.find_all("tr")
    for tr in trs[1:]:  # skip header row
        cols = [td.get_text(strip=True) for td in tr.find_all("td")]
        if not cols:
            continue
        # Defensive mapping — adapt if NSE changes columns
        record = {}
        if len(cols) >= 6:
            record = {
                "date": cols[0],
                "client": cols[1],
                "script": cols[2],
                "quantity": cols[3].replace(",", ""),
                "price": cols[4].replace(",", ""),
                "buy_sell": cols[5],
            }
        else:
            # fallback: store all columns generically
            for i, v in enumerate(cols):
                record[f"col_{i}"] = v
        rows.append(record)

    if close_session:
        session.close()
    return rows


def _write_json(path: str, data: List[Dict[str, str]]):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def _write_csv(path: str, data: List[Dict[str, str]]):
    if not data:
        open(path, "w", encoding="utf-8").close()
        return
    # use union of keys to allow missing fields
    keys = sorted({k for row in data for k in row.keys()})
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)


def _write_xlsx(path: str, data: List[Dict[str, str]]):
    """Write list-of-dicts to an .xlsx file using openpyxl.

    This avoids adding pandas as a dependency.
    """
    from openpyxl import Workbook

    wb = Workbook()
    ws = wb.active

    if not data:
        wb.save(path)
        return

    keys = sorted({k for row in data for k in row.keys()})
    ws.append(keys)
    for row in data:
        ws.append([row.get(k, "") for k in keys])
    wb.save(path)


def _parse_date_str(s: str) -> date:
    s = s.strip()
    # Try common formats seen on NSE pages, be permissive.
    fmts = ("%d-%b-%Y", "%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d %b %Y", "%d %B %Y")
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            continue
    raise ValueError(f"Unrecognized date format: {s!r}")


def cli(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch NSE bulk deals (ad-hoc)")
    parser.add_argument("--format", choices=("json", "csv", "xlsx"), default="json")
    parser.add_argument("--output", "-o", help="Write output to file (otherwise prints to stdout)")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of rows returned (0 = all)")
    parser.add_argument("--days", type=int, default=0, help="Return rows from the last N days (0 = all)")
    args = parser.parse_args(argv)

    try:
        data = fetch_bulk_deals()
    except Exception as exc:
        print("Error fetching bulk deals:", exc, file=sys.stderr)
        return 2

    # filter by days (if requested)
    if args.days and args.days > 0:
        threshold = date.today() - timedelta(days=args.days)
        filtered: List[Dict[str, str]] = []
        for r in data:
            dstr = r.get("date") or next((v for k, v in r.items() if k.startswith("col_")), None)
            if not dstr:
                continue
            try:
                rd = _parse_date_str(dstr)
            except ValueError:
                # skip rows with unparseable dates
                continue
            if rd >= threshold:
                filtered.append(r)
        data = filtered

    if args.limit > 0:
        data = data[: args.limit]

    # output handling
    if args.output:
        if args.format == "json":
            _write_json(args.output, data)
        elif args.format == "csv":
            _write_csv(args.output, data)
        else:
            _write_xlsx(args.output, data)
        print(f"Wrote {len(data)} rows to {args.output}")
    else:
        if args.format == "json":
            print(json.dumps(data, indent=2, ensure_ascii=False))
        elif args.format == "csv":
            # print CSV to stdout
            if not data:
                return 0
            keys = sorted({k for row in data for k in row.keys()})
            writer = csv.DictWriter(sys.stdout, fieldnames=keys)
            writer.writeheader()
            writer.writerows(data)
        else:
            print("xlsx output requires --output <file.xlsx>", file=sys.stderr)
            return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(cli())
