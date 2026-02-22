# NSEAnalysis

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

Production run (same command used on Render):

```powershell
gunicorn flask_healthcheck:app --bind 0.0.0.0:5000 --workers 2 --threads 4 --timeout 120
```

Run tests:

```powershell
pytest -q
```

## Deploy on Render

This repository includes [render.yaml](render.yaml) and [Procfile](Procfile).

1. Push this repo to GitHub.
2. In Render, create a new **Web Service** from the repo.
3. Render auto-detects `render.yaml`; if prompted, use:
   - Build command: `pip install -r requirements.txt`
   - Start command: `gunicorn flask_healthcheck:app --bind 0.0.0.0:$PORT --workers 2 --threads 4 --timeout 120`
4. Add environment variable `DATABASE_URL` (from your Render Postgres instance).
5. Deploy and verify:
   - `GET /health`
   - `GET /health/db`
