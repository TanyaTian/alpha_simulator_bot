# api/alpha_api.py

from flask import Blueprint, request, jsonify
from data_exploration_service import DataExplorationService
# 直接从 dao 包导入实例化好的对象
from dao import DataExplorationDAO
from dao import AlphaSignalDAO
from logger import Logger

alpha_api_blueprint = Blueprint('alpha_api', __name__)
logger = Logger()

# 实例化Service
data_exploration_service = DataExplorationService()

@alpha_api_blueprint.route('/data-exploration/process', methods=['POST'])
def process_data_exploration_trigger():
    """
    Trigger the data exploration process for a specific date.

    Example:
    curl -X POST http://localhost:5001/data-exploration/process -H "Content-Type: application/json" -d '{"datetime": "20251018"}'
    curl -X POST http://localhost:5001/data-exploration/process -H "Content-Type: application/json" -d '{"datetime": "20251018", "force_process": True}'
    """
    data = request.get_json()
    datetime = data.get('datetime')
    force_process = data.get('force_process', False)
    logger.info(f"Received data exploration process request for datetime: {datetime}, force_process: {force_process}") # 日志：记录收到的请求
    if not datetime:
        logger.error("Missing 'datetime' parameter.") # 日志：记录错误
        return jsonify({"status": "error", "message": "缺少 'datetime' 参数。"}), 400
    
    result = data_exploration_service.process_simulated_alphas_for_day(datetime, force_process)
    logger.info(f"Data exploration process finished with result: {result}") # 日志：记录处理结果
    return jsonify(result)

@alpha_api_blueprint.route('/data-exploration/query', methods=['GET'])
def query_data_exploration():
    """
    Query data from the data exploration table.

    Example:
    curl "http://localhost:5001/data-exploration/query?region=USA&universe=TOP3000&delay=1&datafield=close"
    """
    region = request.args.get('region')
    universe = request.args.get('universe')
    delay = request.args.get('delay')
    datafield = request.args.get('datafield')
    logger.info(f"Received data exploration query with params: region={region}, universe={universe}, delay={delay}, datafield={datafield}") # 日志：记录收到的查询

    if not all([region, universe, delay, datafield]):
        logger.error("Missing query parameters.") # 日志：记录错误
        return jsonify({"status": "error", "message": "缺少查询参数。"}), 400
    
    data_exploration_dao = DataExplorationDAO()
    results = data_exploration_dao.query_by_filters(region, universe, delay, datafield)
    logger.info(f"Found {len(results)} records for the query.") # 日志：记录查询结果数量
    return jsonify(results)

@alpha_api_blueprint.route('/signal', methods=['GET'])
def get_signal():
    """
    Get alpha signal data by datetime and region.
    Paginates results using limit and offset.

    Query Parameters:
        datetime (str): The datetime string in YYYYMMDD format (e.g., '20251016'). Required.
        region (str): The region (e.g., 'USA'). Required.
        limit (int): Max number of records. Default 1000.
        offset (int): Records to skip. Default 0.

    Examples:
        # Get first 10 records
        curl "http://localhost:5001/alpha/signal?datetime=20251016&region=USA&limit=10"

        # Get next 100 records (page 2)
        curl "http://localhost:5001/alpha/signal?datetime=20251016&region=USA&limit=100&offset=100"
    """
    datetime_str = request.args.get('datetime')
    region = request.args.get('region')
    limit = request.args.get('limit', 1000, type=int)
    offset = request.args.get('offset', 0, type=int)

    if not datetime_str or not region:
        return jsonify({"error": "Missing required parameters: datetime and region"}), 400

    try:
        dao = AlphaSignalDAO()
        results = dao.get_by_region_and_datetime(region, datetime_str, limit, offset)
        return jsonify(results)
    except Exception as e:
        # A basic logger for now
        print(f"Error in get_signal: {e}")
        return jsonify({"error": "An internal server error occurred"}), 500