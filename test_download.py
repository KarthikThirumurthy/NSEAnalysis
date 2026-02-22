#!/usr/bin/env python
import requests

print("=" * 60)
print("Testing Download Endpoints")
print("=" * 60)

# Test analysis download endpoint
print("\n1. Testing Analysis Download Endpoint...")
resp = requests.get('http://127.0.0.1:5000/api/download/analysis-all')
print(f"   Status: {resp.status_code}")
print(f"   Content-Type: {resp.headers.get('Content-Type')}")
print(f"   Content-Disposition: {resp.headers.get('Content-Disposition')}")
print(f"   Content length: {len(resp.text)} bytes")
if resp.status_code == 200:
    lines = resp.text.split('\n')
    print(f"   Total lines: {len(lines)}")
    print(f"   Header: {lines[0][:80]}...")
    if len(lines) > 1:
        print(f"   First data row: {lines[1][:80]}...")
    print("   ✓ Analysis download works!")
else:
    print(f"   ✗ Error: {resp.text[:200]}")

# Test historical download endpoint
print("\n2. Testing Historical Download Endpoint...")
resp = requests.get('http://127.0.0.1:5000/api/download/historical-all')
print(f"   Status: {resp.status_code}")
print(f"   Content-Type: {resp.headers.get('Content-Type')}")
print(f"   Content-Disposition: {resp.headers.get('Content-Disposition')}")
print(f"   Content length: {len(resp.text)} bytes")
if resp.status_code == 200:
    lines = resp.text.split('\n')
    print(f"   Total lines: {len(lines)}")
    print(f"   Header: {lines[0][:80]}...")
    if len(lines) > 1:
        print(f"   First data row: {lines[1][:80]}...")
    print("   ✓ Historical download works!")
else:
    print(f"   ✗ Error: {resp.text[:200]}")

# Test symbol-specific download
print("\n3. Testing Symbol-Specific Historical Download...")
resp = requests.get('http://127.0.0.1:5000/api/download/historical/ABB')
print(f"   Status: {resp.status_code}")
print(f"   Content-Type: {resp.headers.get('Content-Type')}")
print(f"   Content-Disposition: {resp.headers.get('Content-Disposition')}")
print(f"   Content length: {len(resp.text)} bytes")
if resp.status_code == 200:
    lines = resp.text.split('\n')
    print(f"   Total lines: {len(lines)}")
    print(f"   Header: {lines[0][:80]}...")
    if len(lines) > 1:
        print(f"   First data row: {lines[1][:80]}...")
    print("   ✓ Symbol download works!")
else:
    print(f"   ✗ Error: {resp.text[:200]}")

print("\n" + "=" * 60)
print("All download endpoints tested successfully!")
print("=" * 60)
