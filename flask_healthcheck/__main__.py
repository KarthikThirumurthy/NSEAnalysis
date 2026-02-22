from . import app
import os

if __name__ == "__main__":
    # Run development server when executed as a module: python -m flask_healthcheck
    port = int(os.getenv("PORT", "5000"))
    debug = os.getenv("FLASK_DEBUG", "false").lower() == "true"
    app.run(host="0.0.0.0", port=port, debug=debug)
