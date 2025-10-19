from flask import Blueprint, request, jsonify
from config_manager import config_manager  # 导入单例

# 创建配置蓝图
config_bp = Blueprint('config_api', __name__, url_prefix='/config')

@config_bp.route('', methods=['GET'])
def get_config():
    """获取当前内存中的配置

    curl -X GET http://localhost:5001/config
    """
    config_manager.logger.debug("GET /config endpoint accessed")
    result = jsonify(config_manager.get_all())
    config_manager.logger.debug(f"Returning config: {result.json}")
    return result

@config_bp.route('/update', methods=['POST'])
def update_config():
    """更新配置并通知观察者

    curl -X POST http://localhost:5001/config/update -H "Content-Type: application/json" -d '{"max_concurrent": 8}'
    """
    config_manager.logger.info("POST /config/update endpoint accessed")
    data = request.json
    if not data:
        config_manager.logger.warning("No JSON data provided in request")
        return jsonify({'status': 'error', 'message': 'No JSON data provided'}), 400

    config_manager.logger.debug(f"Received config update data: {data}")
    updated_keys = []
    for key, value in data.items():
        config_manager.set(key, value)
        updated_keys.append(key)
    
    config_manager.notify_observers()
    
    return jsonify({
        'status': 'success',
        'updated': updated_keys,
        'current_config': config_manager.get_all()
    })