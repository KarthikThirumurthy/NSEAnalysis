"""Calculate technical analysis metrics from stock market data.

This module generates analysis metrics based on last 10 days of price and volume data:
- 5DMA: 5-day moving average of closing price
- 10DMA: 10-day moving average of closing price
- DeliveryAverage5Days: Average delivery percentage (last 5 days)
- avg_volu_10d: Average trading quantity (last 10 days)
- pricerange10d: (max high - min low) / 10 for last 10 days
- closePriceLatest: Latest closing price for the symbol
- latestDeliveryPercentage: Latest delivery percentage
- CURRENT_LOW_PRICE: Latest low price
- current_HIGH_PRICE: Latest high price
- current_volume: Latest trading volume (TTL_TRD_QNTY)
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path
from decimal import Decimal
from typing import Optional

# Add parent directory to path so we can import db
sys.path.insert(0, str(Path(__file__).parent.parent))

from db import connection as db_connection
from datetime import datetime

logger = logging.getLogger(__name__)


def generate_analysis(
    schema: str = "NSE_BHAV",
    source_table: str = "stock_market_data",
    target_table: str = "AnalysisDataforCurrentDate",
) -> int:
    """Generate analysis metrics for all symbols based on last 10 days of data.

    - Creates target_table if missing
    - Calculates 5DMA, 10DMA, DeliveryAverage5Days, avg_volu_10d, pricerange10d
    - Upserts results (replaces existing analysis for the current date)
    - Returns number of symbols analyzed
    """

    # SQL to create the analysis table
    # Use a date-suffixed table name for today's analysis (e.g., AnalysisDataforCurrentDate_YYYYMMDD)
    date_suffix = datetime.now().strftime("%Y%m%d")
    dynamic_target_table = f"{target_table}_{date_suffix}"
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{target_table} (
        symbol                      VARCHAR(20)     NOT NULL PRIMARY KEY,
        date_analyzed               DATE            NOT NULL DEFAULT CURRENT_DATE,
        five_dma                    NUMERIC(12,4),
        ten_dma                     NUMERIC(12,4),
        delivery_avg_5d             NUMERIC(6,2),
        avg_volume_10d              NUMERIC(18,2),
        price_range_10d             NUMERIC(12,4),
        close_price_latest          NUMERIC(12,4),
        latest_delivery_percentage  NUMERIC(6,2),
        current_low_price           NUMERIC(12,4),
        current_high_price          NUMERIC(12,4),
        current_volume              BIGINT
    );
    """

    # Create a date-suffixed copy of the create-table SQL for today's analysis table
    create_table_sql_dynamic = create_table_sql.replace(f"{schema}.{target_table}", f"{schema}.{dynamic_target_table}")

    # SQL to calculate metrics for all symbols
    analysis_sql = f"""
    INSERT INTO {schema}.{dynamic_target_table} 
      (symbol, date_analyzed, five_dma, ten_dma, delivery_avg_5d, avg_volume_10d, 
       price_range_10d, close_price_latest, latest_delivery_percentage, 
       current_low_price, current_high_price, current_volume)
    WITH last_10_days AS (
        SELECT 
            symbol,
            date1,
            close_price,
            high_price,
            low_price,
            ttl_trd_qnty,
            deliv_per,
            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date1 DESC) as rn
        FROM {schema}.{source_table}
        WHERE date1 >= CURRENT_DATE - INTERVAL '10 days'
            AND series = 'EQ'
    ),
    latest_data AS (
        SELECT 
            symbol,
            close_price as close_price_latest,
            deliv_per as latest_delivery_percentage,
            low_price as current_low_price,
            high_price as current_high_price,
            ttl_trd_qnty as current_volume
        FROM (
            SELECT 
                symbol,
                close_price,
                deliv_per,
                low_price,
                high_price,
                ttl_trd_qnty,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date1 DESC) as rn
            FROM {schema}.{source_table}
            WHERE series = 'EQ'
        ) t
        WHERE rn = 1
    )
    SELECT
        l10.symbol,
        CURRENT_DATE as date_analyzed,
        AVG(CASE WHEN l10.rn <= 5 THEN l10.close_price ELSE NULL END) as five_dma,
        AVG(l10.close_price) as ten_dma,
        AVG(CASE WHEN l10.rn <= 5 THEN l10.deliv_per ELSE NULL END) as delivery_avg_5d,
        AVG(l10.ttl_trd_qnty) as avg_volume_10d,
        (MAX(l10.high_price) - MIN(l10.low_price)) / 10.0 as price_range_10d,
        ld.close_price_latest,
        ld.latest_delivery_percentage,
        ld.current_low_price,
        ld.current_high_price,
        ld.current_volume
    FROM last_10_days l10
    LEFT JOIN latest_data ld ON l10.symbol = ld.symbol
    GROUP BY l10.symbol, ld.close_price_latest, ld.latest_delivery_percentage, 
             ld.current_low_price, ld.current_high_price, ld.current_volume
    ON CONFLICT (symbol) DO UPDATE SET
        date_analyzed = CURRENT_DATE,
        five_dma = EXCLUDED.five_dma,
        ten_dma = EXCLUDED.ten_dma,
        delivery_avg_5d = EXCLUDED.delivery_avg_5d,
        avg_volume_10d = EXCLUDED.avg_volume_10d,
        price_range_10d = EXCLUDED.price_range_10d,
        close_price_latest = EXCLUDED.close_price_latest,
        latest_delivery_percentage = EXCLUDED.latest_delivery_percentage,
        current_low_price = EXCLUDED.current_low_price,
        current_high_price = EXCLUDED.current_high_price,
        current_volume = EXCLUDED.current_volume
    """

    try:
        with db_connection.get_cursor(commit=True) as cur:
            # Ensure both the base table and today's date-suffixed table exist
            cur.execute(create_table_sql)                # base table (backward compatibility)
            cur.execute(create_table_sql_dynamic)        # dated table for today's snapshot
            logger.info(f"Ensured {schema}.{target_table} and {schema}.{dynamic_target_table} exist")

            # Run analysis into the date-suffixed table
            cur.execute(analysis_sql)
            # For INSERT ... ON CONFLICT, rowcount tells us how many rows affected
            count = cur.rowcount
            logger.info(f"Generated analysis for {count} symbols into {schema}.{dynamic_target_table}")

            # Synchronize results into the base table so existing APIs keep working
            upsert_sql = f"""
            INSERT INTO {schema}.{target_table}
              (symbol, date_analyzed, five_dma, ten_dma, delivery_avg_5d, avg_volume_10d,
               price_range_10d, close_price_latest, latest_delivery_percentage,
               current_low_price, current_high_price, current_volume)
            SELECT symbol, date_analyzed, five_dma, ten_dma, delivery_avg_5d, avg_volume_10d,
               price_range_10d, close_price_latest, latest_delivery_percentage,
               current_low_price, current_high_price, current_volume
            FROM {schema}.{dynamic_target_table}
            ON CONFLICT (symbol) DO UPDATE SET
                date_analyzed = EXCLUDED.date_analyzed,
                five_dma = EXCLUDED.five_dma,
                ten_dma = EXCLUDED.ten_dma,
                delivery_avg_5d = EXCLUDED.delivery_avg_5d,
                avg_volume_10d = EXCLUDED.avg_volume_10d,
                price_range_10d = EXCLUDED.price_range_10d,
                close_price_latest = EXCLUDED.close_price_latest,
                latest_delivery_percentage = EXCLUDED.latest_delivery_percentage,
                current_low_price = EXCLUDED.current_low_price,
                current_high_price = EXCLUDED.current_high_price,
                current_volume = EXCLUDED.current_volume;
            """
            cur.execute(upsert_sql)
            logger.info(f"Synchronized {schema}.{target_table} with {schema}.{dynamic_target_table}")

        return count

    except Exception as e:
        logger.error(f"Error generating analysis: {e}")
        raise


def get_analysis(
    limit: int = 100,
    order_by: str = "symbol",
    schema: str = "NSE_BHAV",
    target_table: str = "AnalysisDataforCurrentDate",
) -> list[dict]:
    """Retrieve analysis data, optionally filtered/sorted.

    Returns list of dicts with keys: symbol, five_dma, ten_dma, delivery_avg_5d, etc.
    """
    valid_orders = {"symbol", "five_dma", "ten_dma", "delivery_avg_5d", "avg_volume_10d", "price_range_10d",
                    "close_price_latest", "latest_delivery_percentage", "current_low_price", "current_high_price", "current_volume"}
    if order_by not in valid_orders:
        order_by = "symbol"

    query = f"""
    SELECT 
        symbol, 
        five_dma, 
        ten_dma, 
        delivery_avg_5d, 
        avg_volume_10d, 
        price_range_10d,
        close_price_latest,
        latest_delivery_percentage,
        current_low_price,
        current_high_price,
        current_volume,
        date_analyzed
    FROM {schema}.{target_table}
    ORDER BY {order_by}
    LIMIT %s
    """

    try:
        with db_connection.get_cursor() as cur:
            cur.execute(query, (limit,))
            cols = [desc[0] for desc in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as e:
        logger.error(f"Error retrieving analysis: {e}")
        return []


if __name__ == "__main__":
    count = generate_analysis()
    print(f"Generated analysis for {count} symbols")

    results = get_analysis(limit=10)
    print(f"\nTop 10 results:")
    for row in results:
        print(row)
