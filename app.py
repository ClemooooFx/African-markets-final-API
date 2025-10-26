from flask import Flask, jsonify
import os
import json
from flask_cors import CORS

app = Flask(__name__)

CORS(app)

DATA_DIR = os.path.join(os.path.dirname(__file__), 'market_data')

def load_json(filename):
    """Safely load JSON file from the market_data directory."""
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        return None
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

@app.route('/')
def home():
    return jsonify({
        "message": "🌍 African Markets Data API",
        "routes": [
            "/api/nse/index",
            "/api/zse/losers",
            "/api/zse/gainers"
        ]
    })

@app.route('/api/<exchange>/<dataset>')
def get_data(exchange, dataset):
    """Fetch market JSON data dynamically."""
    filename = f"{exchange.lower()}_{dataset.lower()}.json"
    data = load_json(filename)
    if not data:
        return jsonify({"error": f"No data found for {exchange}/{dataset}"}), 404
    return jsonify({"data": data, "success": True})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
