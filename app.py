from flask import Flask, jsonify
import os
import json
from flask_cors import CORS
import math

app = Flask(__name__)
CORS(app)

DATA_DIR = os.path.join(os.path.dirname(__file__), 'market_data')

def clean_nan_values(obj):
    """Recursively replace NaN values with null in nested structures."""
    if isinstance(obj, dict):
        return {key: clean_nan_values(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [clean_nan_values(item) for item in obj]
    elif isinstance(obj, float) and math.isnan(obj):
        return None
    else:
        return obj

def load_json(filename):
    """Safely load JSON file from the market_data directory and clean NaN values."""
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        return None
    
    try:
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()
            # Replace NaN with null in the JSON string before parsing
            content = content.replace('NaN', 'null')
            data = json.loads(content)
            # Additional cleaning in case NaN slipped through
            return clean_nan_values(data)
    except Exception as e:
        print(f"Error loading {filename}: {str(e)}")
        return None

@app.route('/')
def home():
    return jsonify({
        "message": "🌍 African Markets Data API",
        "status": "healthy",
        "routes": [
            "/api/<exchange>/index - Get index data",
            "/api/<exchange>/gainers - Get top gainers",
            "/api/<exchange>/losers - Get top losers",
            "/api/<exchange>/companies - Get all listed companies"
        ],
        "available_exchanges": ["nse", "bse", "gse", "zse"],
        "example": "/api/nse/index"
    })

@app.route('/api/<exchange>/<dataset>')
def get_data(exchange, dataset):
    """Fetch market JSON data dynamically."""
    # Validate exchange and dataset
    valid_exchanges = ['nse', 'bse', 'gse', 'zse']
    valid_datasets = ['index', 'gainers', 'losers', 'companies']
    
    exchange = exchange.lower()
    dataset = dataset.lower()
    
    if exchange not in valid_exchanges:
        return jsonify({
            "error": f"Invalid exchange: {exchange}",
            "valid_exchanges": valid_exchanges,
            "success": False
        }), 400
    
    if dataset not in valid_datasets:
        return jsonify({
            "error": f"Invalid dataset: {dataset}",
            "valid_datasets": valid_datasets,
            "success": False
        }), 400
    
    filename = f"{exchange}_{dataset}.json"
    data = load_json(filename)
    
    if data is None:
        return jsonify({
            "error": f"No data found for {exchange}/{dataset}",
            "success": False
        }), 404
    
    return jsonify({
        "data": data,
        "exchange": exchange,
        "dataset": dataset,
        "success": True
    })

@app.route('/health')
def health_check():
    """Health check endpoint."""
    return jsonify({
        "status": "healthy",
        "message": "API is running"
    })

@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors."""
    return jsonify({
        "error": "Endpoint not found",
        "success": False
    }), 404

@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors."""
    return jsonify({
        "error": "Internal server error",
        "success": False
    }), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
