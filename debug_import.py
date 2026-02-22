"""Debug script to test the import_bhav importer directly."""
import sys
from scripts.import_bhav import import_bhav_csv

csv_path = "sec_bhavdata_full_13022026.csv"

try:
    print(f"Importing from {csv_path}...")
    count = import_bhav_csv(csv_path, schema="NSE_BHAV", table="stock_market_data")
    print(f"✓ Success: {count} rows inserted/upserted")
except Exception as e:
    print(f"✗ Error: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
