# dl/main.py
"""
Main entry point.
--------------------------------
Launch the Flask web interface (or optionally use the FastAPI backend separately).
"""

from dl.flask_app import app

if __name__ == "__main__":
    app.run(debug=True)
