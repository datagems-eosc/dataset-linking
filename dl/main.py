"""Launch the Flask interface when ``python -m dl.main`` is used.

FastAPI is started separately with Uvicorn; see the README for both commands.
"""

from dl.flask_app import app as flask_app

if __name__ == "__main__":
    flask_app.run(debug=True)
