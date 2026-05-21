from flask import Flask

# Import blueprints
from api.config_api import config_bp
from api.alpha_api import alpha_api_blueprint as alpha_bp
from api.optimization_api import optimization_bp

# Create Flask app
app = Flask(__name__)

# Register blueprints
app.register_blueprint(config_bp)
app.register_blueprint(alpha_bp)
app.register_blueprint(optimization_bp)

def run_server(port=5001):
    """Start the Flask API service."""
    app.run(host='0.0.0.0', port=port)

if __name__ == '__main__':
    run_server()
