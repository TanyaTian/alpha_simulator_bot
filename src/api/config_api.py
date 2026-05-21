from flask import Blueprint, request, jsonify
from config_manager import config_manager  # Import the singleton instance

# Create a blueprint for configuration management.
config_bp = Blueprint('config_api', __name__, url_prefix='/config')

def mask_sensitive_data(config):
    """Mask sensitive fields in the configuration for display.

    Fields containing 'key', 'password', 'token', 'secret' will be masked.
    'username' is also masked for security.
    """
    sensitive_keywords = ['key', 'password', 'token', 'secret', 'username']
    masked_config = config.copy()

    for key, value in masked_config.items():
        if any(keyword in key.lower() for keyword in sensitive_keywords):
            if value and isinstance(value, str):
                if len(value) <= 8:
                    masked_config[key] = "********"
                else:
                    # Show first 4 and last 4 characters, mask the rest
                    masked_config[key] = f"{value[:4]}****{value[-4:]}"
            else:
                masked_config[key] = "********"

    return masked_config

@config_bp.route('', methods=['GET'])
def get_config():
    """Get the current configuration from memory (masked).

    curl -X GET http://47.111.116.132:5001/config
    """
    config_manager.logger.debug("GET /config endpoint accessed")
    full_config = config_manager.get_all()
    masked_config = mask_sensitive_data(full_config)
    config_manager.logger.debug(f"Returning masked config: {masked_config}")
    return jsonify(masked_config)

@config_bp.route('/update', methods=['POST'])
def update_config():
    """Update the configuration and notify observers.

    curl -X POST http://47.111.116.132:5001/config/update -H "Content-Type: application/json" -d '{"batch_number_for_every_queue": 8}'
    """
    config_manager.logger.info("POST /config/update endpoint accessed")
    data = request.json
    if not data:
        config_manager.logger.warning("No JSON data provided in request")
        return jsonify({'status': 'error', 'message': 'No JSON data provided'}), 400

    # Mask received data for logging
    masked_received = mask_sensitive_data(data)
    config_manager.logger.debug(f"Received config update data: {masked_received}")

    updated_keys = []
    for key, value in data.items():
        config_manager.set(key, value)
        updated_keys.append(key)

    config_manager.notify_observers()

    full_config = config_manager.get_all()
    masked_config = mask_sensitive_data(full_config)

    return jsonify({
        'status': 'success',
        'updated': updated_keys,
        'current_config': masked_config
    })