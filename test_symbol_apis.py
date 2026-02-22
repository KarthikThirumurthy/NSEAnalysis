"""Test the new symbol-based APIs."""
import requests
import json

print("Testing new symbol-based APIs...\n")

# Test 1: Historical data for ABB
print("1. GET /api/historical/ABB")
resp = requests.get("http://127.0.0.1:5000/api/historical/ABB")
print(f"   Status: {resp.status_code}")
data = resp.json()
print(f"   Found {data.get('count', 0)} records for symbol: {data.get('symbol')}")
if data.get('data'):
    row = data['data'][0]
    print(f"   Latest record: {row['symbol']} on {row['date1']}, Close: {row['close_price']}, Volume: {row['ttl_trd_qnty']}")

# Test 2: Historical data with limit
print("\n2. GET /api/historical/AARTIIND?limit=5&sort_by=date_asc")
resp = requests.get("http://127.0.0.1:5000/api/historical/AARTIIND?limit=5&sort_by=date_asc")
print(f"   Status: {resp.status_code}")
data = resp.json()
print(f"   Found {data.get('count', 0)} records (sorted ascending)")
if data.get('data'):
    for row in data['data'][:3]:
        print(f"     {row['date1']}: Close {row['close_price']}, Deliv% {row['deliv_per']}")

# Test 3: Analysis data for a symbol
print("\n3. GET /api/analysis/ABB")
resp = requests.get("http://127.0.0.1:5000/api/analysis/ABB")
print(f"   Status: {resp.status_code}")
data = resp.json()
if data.get('found'):
    a = data['data']
    print(f"   Symbol: {a['symbol']}")
    print(f"   5DMA: {a['five_dma']}, 10DMA: {a['ten_dma']}")
    print(f"   Current Close: {a['close_price_latest']}, Volume: {a['current_volume']}")
    print(f"   Latest Delivery %: {a['latest_delivery_percentage']}")
else:
    print(f"   Error: {data.get('error')}")

# Test 4: Non-existent symbol
print("\n4. GET /api/historical/NOTEXIST")
resp = requests.get("http://127.0.0.1:5000/api/historical/NOTEXIST")
print(f"   Status: {resp.status_code}")
data = resp.json()
print(f"   Found {data.get('count', 0)} records")

print("\n✓ All tests completed!")
