"""flask_healthcheck — minimal Flask app with a /health endpoint."""
from flask import Flask, jsonify, request, send_from_directory, make_response
from flask_cors import CORS
from db import connection as db_connection
from scripts.import_bhav import import_bhav_csv
from scripts import analysis
import os
import csv
from io import StringIO
from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash


def create_app():
    """Application factory returning a Flask app with a /health route."""
    app = Flask(__name__)
    CORS(app)  # Enable CORS for all routes

    def ensure_auth_schema():
        try:
            with db_connection.get_cursor(commit=True) as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS NSE_BHAV")
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS NSE_BHAV.app_users (
                        id SERIAL PRIMARY KEY,
                        username TEXT UNIQUE NOT NULL,
                        password_hash TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
        except Exception as exc:
            app.logger.error("Auth schema init failed: %s", exc)

    ensure_auth_schema()

    @app.route("/health", methods=["GET"])
    def health():
        return jsonify(status="ok")

    @app.route("/health/db", methods=["GET"])
    def health_db():
        """Health endpoint that verifies DB connectivity by running a simple query.

        Returns 200 with {status: 'ok', db: 'ok'} when the DB is reachable, or
        500 with an error payload when not.
        """
        try:
            # Use the centralized DB helper; monkeypatchable in tests
            with db_connection.get_cursor() as cur:
                cur.execute("SELECT 1")
            return jsonify(status="ok", db="ok")
        except Exception as exc:
            return jsonify(status="error", db="unavailable", error=str(exc)), 500

    @app.route("/api/auth/register", methods=["POST"])
    def register_user():
        """Create a user with username and password (stored as a hash)."""
        data = request.get_json(silent=True) or {}
        username = (data.get("username") or "").strip()
        password = data.get("password") or ""

        if not username or not password:
            return jsonify(error="username and password are required"), 400
        if len(username) < 3:
            return jsonify(error="username must be at least 3 characters"), 400
        if len(password) < 6:
            return jsonify(error="password must be at least 6 characters"), 400

        password_hash = generate_password_hash(password)

        try:
            with db_connection.get_cursor(commit=True) as cur:
                cur.execute(
                    "SELECT 1 FROM NSE_BHAV.app_users WHERE username = %s",
                    (username,)
                )
                if cur.fetchone():
                    return jsonify(error="username already exists"), 409

                cur.execute(
                    """
                    INSERT INTO NSE_BHAV.app_users (username, password_hash)
                    VALUES (%s, %s)
                    RETURNING id, created_at
                    """,
                    (username, password_hash),
                )
                user_id, created_at = cur.fetchone()

            return jsonify(
                message="registered",
                user_id=user_id,
                created_at=created_at.isoformat(),
            ), 201
        except Exception as exc:
            return jsonify(error=str(exc)), 500

    @app.route("/api/auth/login", methods=["POST"])
    def login_user():
        """Authenticate a user by username and password."""
        data = request.get_json(silent=True) or {}
        username = (data.get("username") or "").strip()
        password = data.get("password") or ""

        if not username or not password:
            return jsonify(error="username and password are required"), 400

        try:
            with db_connection.get_cursor() as cur:
                cur.execute(
                    "SELECT id, password_hash FROM NSE_BHAV.app_users WHERE username = %s",
                    (username,),
                )
                row = cur.fetchone()
                if not row:
                    return jsonify(error="invalid username or password"), 401

                user_id, password_hash = row
                if not check_password_hash(password_hash, password):
                    return jsonify(error="invalid username or password"), 401

            return jsonify(message="authenticated", user_id=user_id), 200
        except Exception as exc:
            return jsonify(error=str(exc)), 500

    @app.route("/api/auth/reset-password", methods=["POST"])
    def reset_password():
        """Reset a user's password by username."""
        data = request.get_json(silent=True) or {}
        username = (data.get("username") or "").strip()
        new_password = data.get("new_password") or ""

        if not username or not new_password:
            return jsonify(error="username and new_password are required"), 400
        if len(new_password) < 6:
            return jsonify(error="new_password must be at least 6 characters"), 400

        new_hash = generate_password_hash(new_password)

        try:
            with db_connection.get_cursor(commit=True) as cur:
                cur.execute(
                    "SELECT id FROM NSE_BHAV.app_users WHERE username = %s",
                    (username,),
                )
                row = cur.fetchone()
                if not row:
                    return jsonify(error="user not found"), 404

                cur.execute(
                    "UPDATE NSE_BHAV.app_users SET password_hash = %s WHERE username = %s",
                    (new_hash, username),
                )

            return jsonify(message="password_reset"), 200
        except Exception as exc:
            return jsonify(error=str(exc)), 500

    @app.route("/import/stock_market_data", methods=["POST"])
    def import_stock_market_data():
        """Accept a BHAV CSV file (multipart/form-data, field name `file`) and
        upsert rows with SERIES == 'EQ' into `NSE_BHAV.stock_market_data`.

        Returns JSON: {"inserted": <count>}.
        """
        if "file" not in request.files:
            return jsonify(error="missing file"), 400

        file = request.files["file"]
        import io
        try:
            # FileStorage.stream is binary — wrap as text for csv reader
            stream = io.TextIOWrapper(file.stream, encoding="utf-8")
            inserted = import_bhav_csv(stream)
            return jsonify(inserted=inserted)
        except Exception as exc:
            return jsonify(error=str(exc)), 500

    @app.route("/api/analysis/generate", methods=["POST"])
    def generate_analysis():
        """Generate analysis metrics (5DMA, 10DMA, etc.) for all symbols.

        Uses last 10 days of data from NSE_BHAV.stock_market_data.
        Returns JSON: {\"analyzed\": <count>}.
        """
        try:
            count = analysis.generate_analysis()
            return jsonify(analyzed=count)
        except Exception as exc:
            return jsonify(error=str(exc)), 500

    @app.route("/api/analysis/results", methods=["GET"])
    def get_analysis_results():
        """Retrieve pre-calculated analysis metrics.

        Query parameters:
        - limit: max rows to return (default 100)
        - order_by: column to sort (default 'symbol')
        
        Returns JSON array with symbol, five_dma, ten_dma, delivery_avg_5d, etc.
        """
        try:
            limit = request.args.get("limit", default=100, type=int)
            order_by = request.args.get("order_by", default="symbol", type=str)
            
            results = analysis.get_analysis(limit=limit, order_by=order_by)
            return jsonify(results=results, count=len(results))
        except Exception as exc:
            return jsonify(error=str(exc)), 500

    @app.route("/api/analysis/data", methods=["GET"])
    def get_analysis_data():
        """Retrieve technical analysis data.

        Query parameters:
        - limit: number of records (default 100, max 1000)
        - order_by: column to sort by (default 'symbol')
        
        Returns JSON array of analysis records with all columns:
        symbol, five_dma, ten_dma, delivery_avg_5d, avg_volume_10d, price_range_10d,
        close_price_latest, latest_delivery_percentage, current_low_price,
        current_high_price, current_volume, date_analyzed
        """
        try:
            limit = request.args.get("limit", 100, type=int)
            limit = min(limit, 1000)  # Cap at 1000
            order_by = request.args.get("order_by", "symbol", type=str)
            
            data = analysis.get_analysis(limit=limit, order_by=order_by)
            return jsonify(data=data, count=len(data)), 200
        except Exception as exc:
            return jsonify(error=str(exc)), 500

    @app.route("/api/historical/<symbol>", methods=["GET"])
    def get_historical_data(symbol):
        """Query all historical data for a specific symbol from nse_bhav.stock_market_data.

        Parameters:
        - symbol: stock symbol (required, path parameter)
        - limit: max number of records (default 500)
        - sort_by: 'date' (default, DESC) or 'date_asc'

        Returns JSON array with columns:
        symbol, series, date1, open_price, high_price, low_price, close_price,
        avg_price, ttl_trd_qnty, turnover_lacs, no_of_trades, deliv_qty, deliv_per
        """
        try:
            symbol = symbol.upper().strip()
            if not symbol:
                return jsonify(error="symbol is required"), 400

            limit = request.args.get("limit", 500, type=int)
            limit = min(limit, 5000)  # Cap at 5000
            sort_by = request.args.get("sort_by", "date", type=str)

            order_clause = "ORDER BY date1 DESC" if sort_by != "date_asc" else "ORDER BY date1 ASC"

            query = f"""
            SELECT 
                symbol, series, date1, open_price, high_price, low_price, close_price,
                ttl_trd_qnty, deliv_qty, deliv_per
            FROM NSE_BHAV.stock_market_data
            WHERE UPPER(symbol) = %s
            {order_clause}
            LIMIT %s
            """

            with db_connection.get_cursor() as cur:
                cur.execute(query, (symbol, limit))
                cols = [desc[0] for desc in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]
                return jsonify(symbol=symbol, count=len(rows), data=rows), 200

        except Exception as exc:
            return jsonify(error=str(exc)), 500

    @app.route('/api/filter/trend', methods=['GET'])
    def filter_trend():
        """Filter symbols by monotonic trend over the last N days (runs across ALL symbols).

        Query parameters:
        - days (int, required) : number of most recent days to check (min 2)
        - trend (str, required) : 'increase' or 'decrease'
        - parameter (str, required) : 'close_price' or 'deliv_qty'

        Returns JSON: {count: <n>, results: [{symbol, latest_value}, ...]}
        """
        try:
            days = request.args.get('days', type=int)
            trend = request.args.get('trend', type=str)
            parameter = request.args.get('parameter', type=str)

            # Validate inputs
            if not days or days < 2:
                return jsonify(error='days must be >= 2'), 400
            if days > 60:
                return jsonify(error='days must be <= 60'), 400
            if trend not in ('increase', 'decrease'):
                return jsonify(error="trend must be 'increase' or 'decrease'"), 400
            if parameter not in ('close_price', 'deliv_qty'):
                return jsonify(error="parameter must be 'close_price' or 'deliv_qty'"), 400

            param_col = parameter  # safe column name from validated set

            # Use row_number to pick last `days` rows per symbol, then check monotonicity
            comparison = '>' if trend == 'increase' else '<'

            query = f"""
            WITH last_n AS (
                SELECT symbol, date1, {param_col} AS value,
                       row_number() OVER (PARTITION BY symbol ORDER BY date1 DESC) rn_desc
                FROM NSE_BHAV.stock_market_data
                WHERE series = 'EQ' AND {param_col} IS NOT NULL
            ),
            picked AS (
                SELECT symbol, date1, value
                FROM last_n
                WHERE rn_desc <= %s
            ),
            qualified AS (
                SELECT symbol
                FROM (
                    SELECT symbol, value, lag(value) OVER (PARTITION BY symbol ORDER BY date1 ASC) AS prev_val
                    FROM picked
                ) t
                GROUP BY symbol
                HAVING count(*) = %s AND bool_and(prev_val IS NULL OR value {comparison} prev_val)
            )
            SELECT q.symbol, ln.value AS latest_value
            FROM qualified q
            JOIN last_n ln ON ln.symbol = q.symbol AND ln.rn_desc = 1
            ORDER BY q.symbol
            """

            params = (days, days)

            with db_connection.get_cursor() as cur:
                cur.execute(query, params)
                cols = [desc[0] for desc in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]

            return jsonify(count=len(rows), results=rows), 200

        except Exception as exc:
            return jsonify(error=str(exc)), 500

    @app.route("/api/analysis/<symbol>", methods=["GET"])
    def get_analysis_by_symbol(symbol):
        """Query analysis data for a specific symbol from AnalysisDataforCurrentDate.

        Parameters:
        - symbol: stock symbol (required, path parameter)

        Returns JSON with columns:
        symbol, five_dma, ten_dma, delivery_avg_5d, avg_volume_10d, price_range_10d,
        close_price_latest, latest_delivery_percentage, current_low_price,
        current_high_price, current_volume, date_analyzed
        """
        try:
            symbol = symbol.upper().strip()
            if not symbol:
                return jsonify(error="symbol is required"), 400

            query = """
            SELECT 
                symbol, five_dma, ten_dma, delivery_avg_5d, avg_volume_10d, 
                price_range_10d, close_price_latest, latest_delivery_percentage,
                current_low_price, current_high_price, current_volume, date_analyzed
            FROM NSE_BHAV.AnalysisDataforCurrentDate
            WHERE UPPER(symbol) = %s
            """

            with db_connection.get_cursor() as cur:
                cur.execute(query, (symbol,))
                cols = [desc[0] for desc in cur.description]
                row = cur.fetchone()
                if row:
                    data = dict(zip(cols, row))
                    return jsonify(symbol=symbol, found=True, data=data), 200
                else:
                    return jsonify(symbol=symbol, found=False, error="Symbol not found in analysis"), 404

        except Exception as exc:
            return jsonify(error=str(exc)), 500

    @app.route("/api/download/analysis-all", methods=["GET"])
    def download_analysis_all():
        """Download all analysis data as CSV."""
        try:
            query = """
            SELECT 
                symbol, five_dma, ten_dma, delivery_avg_5d, avg_volume_10d, 
                price_range_10d, close_price_latest, latest_delivery_percentage,
                current_low_price, current_high_price, current_volume, date_analyzed
            FROM NSE_BHAV.AnalysisDataforCurrentDate
            ORDER BY symbol
            """
            
            with db_connection.get_cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                cols = [desc[0] for desc in cur.description]
            
            # Create CSV
            output = StringIO()
            writer = csv.writer(output)
            writer.writerow(cols)  # Header
            writer.writerows(rows)
            
            # Prepare response
            response = make_response(output.getvalue())
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            response.headers["Content-Disposition"] = f"attachment; filename=NSE_Analysis_{timestamp}.csv"
            response.headers["Content-Type"] = "text/csv; charset=utf-8"
            
            return response
        except Exception as exc:
            return jsonify(error=str(exc)), 500

    @app.route("/api/download/historical-all", methods=["GET"])
    def download_historical_all():
        """Download last 10 days historical data for all symbols as CSV."""
        try:
            query = """
            SELECT 
                symbol, series, date1, open_price, high_price, low_price, close_price,
                ttl_trd_qnty, deliv_qty, deliv_per
            FROM NSE_BHAV.stock_market_data
            WHERE date1 >= CURRENT_DATE - INTERVAL '10 days'
                AND series = 'EQ'
            ORDER BY symbol, date1 DESC
            """
            
            with db_connection.get_cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                cols = [desc[0] for desc in cur.description]
            
            # Create CSV
            output = StringIO()
            writer = csv.writer(output)
            writer.writerow(cols)  # Header
            writer.writerows(rows)
            
            # Prepare response
            response = make_response(output.getvalue())
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            response.headers["Content-Disposition"] = f"attachment; filename=NSE_Historical_10Days_{timestamp}.csv"
            response.headers["Content-Type"] = "text/csv; charset=utf-8"
            
            return response
        except Exception as exc:
            return jsonify(error=str(exc)), 500

    @app.route("/api/download/historical/<symbol>", methods=["GET"])
    def download_historical_symbol(symbol):
        """Download all historical data for a specific symbol as CSV."""
        try:
            symbol = symbol.upper().strip()
            
            query = """
            SELECT 
                symbol, series, date1, open_price, high_price, low_price, close_price,
                ttl_trd_qnty, deliv_qty, deliv_per
            FROM NSE_BHAV.stock_market_data
            WHERE UPPER(symbol) = %s
            ORDER BY date1 DESC
            """
            
            with db_connection.get_cursor() as cur:
                cur.execute(query, (symbol,))
                rows = cur.fetchall()
                cols = [desc[0] for desc in cur.description]
            
            if not rows:
                return jsonify(error=f"No data found for symbol: {symbol}"), 404
            
            # Create CSV
            output = StringIO()
            writer = csv.writer(output)
            writer.writerow(cols)  # Header
            writer.writerows(rows)
            
            # Prepare response
            response = make_response(output.getvalue())
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            response.headers["Content-Disposition"] = f"attachment; filename=NSE_{symbol}_Historical_{timestamp}.csv"
            response.headers["Content-Type"] = "text/csv; charset=utf-8"
            
            return response
        except Exception as exc:
            return jsonify(error=str(exc)), 500

    @app.route("/", methods=["GET"])
    def index():
        """Serve the UI dashboard."""
        ui_path = os.path.join(os.path.dirname(__file__), "..", "ui")
        if os.path.exists(os.path.join(ui_path, "index.html")):
            return send_from_directory(ui_path, "index.html")
        return jsonify(error="UI not found"), 404

    @app.route("/ui/<path:filename>", methods=["GET"])
    def serve_ui(filename):
        """Serve static UI files."""
        ui_path = os.path.join(os.path.dirname(__file__), "..", "ui")
        return send_from_directory(ui_path, filename)

    return app


# WSGI entrypoint for servers (e.g. gunicorn)
app = create_app()
__all__ = ["create_app", "app"]
