from . import app

if __name__ == "__main__":
    # Run development server when executed as a module: python -m flask_healthcheck
    app.run(host="0.0.0.0", port=5000, debug=True)
