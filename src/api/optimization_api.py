from flask import Blueprint, request, jsonify
from optimization_manager import optimization_manager

# 定义优化服务蓝图
optimization_bp = Blueprint('optimization', __name__)

@optimization_bp.route('/api/optimization/submit', methods=['POST'])
def submit_optimization():
    """
    提交一组 Alpha ID 开始优化。
    接收 JSON: {"alpha_ids": ["ID1", "ID2", ...]}
    """
    data = request.json
    alpha_ids = data.get('alpha_ids', [])
    if not alpha_ids:
        return jsonify({"error": "未提供 alpha_ids"}), 400
    
    optimization_manager.add_tasks(alpha_ids)
    return jsonify({"message": f"成功提交 {len(alpha_ids)} 个 Alpha 进行优化"}), 200

@optimization_bp.route('/api/optimization/append', methods=['POST'])
def append_optimization():
    """
    追加 Alpha ID 到当前优化队列。
    """
    data = request.json
    alpha_ids = data.get('alpha_ids', [])
    if not alpha_ids:
        return jsonify({"error": "未提供 alpha_ids"}), 400
    
    optimization_manager.add_tasks(alpha_ids)
    return jsonify({"message": f"成功向队列追加 {len(alpha_ids)} 个 Alpha"}), 200

@optimization_bp.route('/api/optimization/status', methods=['GET'])
def get_status():
    """
    查询当前优化服务的运行状态。
    返回: 状态（空闲/运行）、当前 Alpha、队列长度等。
    """
    status = optimization_manager.get_status()
    return jsonify(status), 200

@optimization_bp.route('/api/optimization/clear', methods=['POST'])
def clear_queue():
    """
    清空当前排队等待中的所有优化任务。
    """
    removed_count = optimization_manager.clear_tasks()
    return jsonify({
        "message": "队列已清空",
        "removed_count": removed_count
    }), 200
