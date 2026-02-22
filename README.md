# Flask Healthcheck — sample project

Minimal Flask service exposing a single `/health` endpoint that returns JSON `{ "status": "ok" }`.

Quick start (Windows PowerShell):

1. Create & activate a venv:
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\activate
   ```
2. Install dependencies and run:
   ```powershell
   pip install -r requirements.txt
   python -m flask_healthcheck
   ```

Run tests:

```powershell
pytest -q
```
