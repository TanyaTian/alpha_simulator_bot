from flask import Blueprint, request, jsonify
from optimization_manager import optimization_manager

# 定义优化服务蓝图
optimization_bp = Blueprint('optimization', __name__)

@optimization_bp.route('/api/optimization/submit', methods=['POST'])
def submit_optimization():
    """提交一组 Alpha ID 开始优化。

    curl -X POST http://47.111.116.132:5001/api/optimization/submit -H "Content-Type: application/json" -d '{"alpha_ids": ["ID1", "ID2"]}'
    """
    data = request.json
    alpha_ids = data.get('alpha_ids', [])
    if not alpha_ids:
        return jsonify({"error": "No alpha_ids provided"}), 400

    optimization_manager.add_tasks(alpha_ids)
    return jsonify({"message": f"Successfully submitted {len(alpha_ids)} alpha(s) for optimization"}), 200

@optimization_bp.route('/api/optimization/append', methods=['POST'])
def append_optimization():
    """追加 Alpha ID 到当前优化队列。

    curl -X POST http://47.111.116.132:5001/api/optimization/append -H "Content-Type: application/json" -d '{"alpha_ids": ["ID3", "ID4"]}'
    """
    data = request.json
    alpha_ids = data.get('alpha_ids', [])
    if not alpha_ids:
        return jsonify({"error": "No alpha_ids provided"}), 400

    optimization_manager.add_tasks(alpha_ids)
    return jsonify({"message": f"Successfully appended {len(alpha_ids)} alpha(s) to the queue"}), 200

@optimization_bp.route('/api/optimization/status', methods=['GET'])
def get_status():
    """查询当前优化服务的运行状态。

    curl -X GET http://47.111.116.132:5001/api/optimization/status
    """
    status = optimization_manager.get_status()
    return jsonify(status), 200

@optimization_bp.route('/api/optimization/clear', methods=['POST'])
def clear_queue():
    """清空当前排队等待中的所有优化任务。

    curl -X POST http://47.111.116.132:5001/api/optimization/clear
    """
    removed_count = optimization_manager.clear_tasks()
    return jsonify({
        "message": "Queue cleared",
        "removed_count": removed_count
    }), 200
