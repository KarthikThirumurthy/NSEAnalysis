"""Test UI endpoint."""
import requests

# Test if the UI endpoint is working
resp = requests.get('http://127.0.0.1:5000/')
print(f'UI endpoint status: {resp.status_code}')
print(f'Content size: {len(resp.text)} bytes')
if resp.status_code == 200:
    print('✓ UI is loaded successfully!')
    has_title = 'NSE Stock Market Analysis' in resp.text
    print(f'✓ Contains title: {has_title}')
    print('\nUI is ready at: http://127.0.0.1:5000/')
else:
    print(f'Error: {resp.text}')
