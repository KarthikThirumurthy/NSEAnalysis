"""Diagnostic script to check DB state and Flask configuration."""
import os
import sys

print("=" * 60)
print("DATABASE DIAGNOSTIC CHECK")
print("=" * 60)

# Check environment
print("\n1. Environment Variables:")
print(f"   DB_NAME: {os.getenv('DB_NAME', '[NOT SET - defaults to postgres]')}")
print(f"   DB_HOST: {os.getenv('DB_HOST', '[NOT SET - defaults to localhost]')}")
print(f"   DB_PORT: {os.getenv('DB_PORT', '[NOT SET - defaults to 5432]')}")
print(f"   DB_USER: {os.getenv('DB_USER', '[NOT SET - defaults to postgres]')}")

# Check connection
from db.connection import get_connection, _DEFAULT_CONFIG

print("\n2. DB Connection Config:")
print(f"   Host: {_DEFAULT_CONFIG['host']}")
print(f"   Port: {_DEFAULT_CONFIG['port']}")
print(f"   User: {_DEFAULT_CONFIG['user']}")
print(f"   DB: {_DEFAULT_CONFIG['dbname']}")

print("\n3. Testing Connection...")
try:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT current_database(), current_user;")
        db, user = cur.fetchone()
        print(f"   ✓ Connected to database: {db}")
        print(f"   ✓ Connected as user: {user}")
        
        # Check if schema exists
        cur.execute("""
            SELECT schema_name FROM information_schema.schemata 
            WHERE schema_name = 'NSE_BHAV'
        """)
        schema_exists = cur.fetchone() is not None
        print(f"\n4. Schema NSE_BHAV: {'✓ EXISTS' if schema_exists else '✗ DOES NOT EXIST'}")
        
        if schema_exists:
            # Check if table exists
            cur.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'NSE_BHAV' AND table_name = 'stock_market_data'
            """)
            table_exists = cur.fetchone() is not None
            print(f"5. Table stock_market_data: {'✓ EXISTS' if table_exists else '✗ DOES NOT EXIST'}")
            
            if table_exists:
                # Get row count
                cur.execute("SELECT COUNT(*) FROM NSE_BHAV.stock_market_data")
                count = cur.fetchone()[0]
                print(f"6. Row count: {count} rows")
                
                if count > 0:
                    # Show sample
                    cur.execute("""
                        SELECT symbol, series, date1, open_price, close_price 
                        FROM NSE_BHAV.stock_market_data LIMIT 3
                    """)
                    print("\n7. Sample rows:")
                    for row in cur.fetchall():
                        print(f"   {row}")
                else:
                    print("   ⚠ Table exists but is empty!")
        
        cur.close()
except Exception as e:
    print(f"   ✗ Connection failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n" + "=" * 60)
