from flask_healthcheck import create_app
import contextlib

from db import connection as db_connection


def test_healthcheck():
    app = create_app()
    client = app.test_client()

    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.get_json() == {"status": "ok"}


def test_db_health_ok(monkeypatch):
    app = create_app()
    client = app.test_client()

    @contextlib.contextmanager
    def fake_get_cursor(commit: bool = False):
        class DummyCursor:
            def execute(self, q, *a, **kw):
                return None
            def close(self):
                return None

        yield DummyCursor()

    monkeypatch.setattr(db_connection, "get_cursor", fake_get_cursor)

    resp = client.get("/health/db")
    assert resp.status_code == 200
    assert resp.get_json() == {"status": "ok", "db": "ok"}


def test_db_health_fail(monkeypatch):
    app = create_app()
    client = app.test_client()

    @contextlib.contextmanager
    def bad_get_cursor(commit: bool = False):
        raise RuntimeError("db down")
        yield  # pragma: no cover - unreachable

    monkeypatch.setattr(db_connection, "get_cursor", bad_get_cursor)

    resp = client.get("/health/db")
    assert resp.status_code == 500
    data = resp.get_json()
    assert data["status"] == "error"
    assert data["db"] == "unavailable"
    assert "db down" in data["error"]


def test_import_bhav_endpoint(monkeypatch):
    """POST the repository BHAV CSV and assert rows with SERIES == 'EQ' are upserted."""
    app = create_app()
    client = app.test_client()

    inserted = {"count": 0}

    @contextlib.contextmanager
    def fake_get_cursor(commit: bool = False):
        class DummyCursor:
            def execute(self, sql, *a, **kw):
                # allow schema/table creation SQL
                return None

            def executemany(self, sql, seq_of_params):
                # count number of parameter tuples inserted
                inserted["count"] += len(seq_of_params)

            def close(self):
                return None

        yield DummyCursor()

    monkeypatch.setattr(db_connection, "get_cursor", fake_get_cursor)

    csv_path = "sec_bhavdata_full_13022026.csv"

    # compute expected number of rows with SERIES == 'EQ'
    import csv as _csv
    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = _csv.reader(fh)
        next(reader)  # header
        expected = sum(1 for r in reader if r[1].strip() == "EQ")

    with open(csv_path, "rb") as f:
        data = {"file": (f, "sec_bhavdata_full_13022026.csv")}
        resp = client.post("/import/stock_market_data", data=data)

    assert resp.status_code == 200
    assert resp.get_json()["inserted"] == expected
    assert inserted["count"] == expected


def test_trend_filter_endpoint(monkeypatch):
    """Ensure /api/filter/trend returns matching symbols (monotonic increase/decrease)."""
    app = create_app()
    client = app.test_client()

    @contextlib.contextmanager
    def fake_get_cursor(commit: bool = False):
        class DummyCursor:
            def execute(self, sql, params=None):
                # respond with a single matching symbol for the trend query
                self.description = (("symbol",), ("latest_value",))
                self._rows = [("ABB", 5780.5)]

            def fetchall(self):
                return self._rows

            def close(self):
                return None

        yield DummyCursor()

    monkeypatch.setattr(db_connection, "get_cursor", fake_get_cursor)

    resp = client.get('/api/filter/trend?days=3&trend=increase&parameter=close_price')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["count"] == 1
    assert data["results"][0]["symbol"] == "ABB"
    assert float(data["results"][0]["latest_value"]) == 5780.5
