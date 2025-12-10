# api/alpha_api.py

from flask import Blueprint, request, jsonify
import threading
from data_exploration_service import DataExplorationService
# 直接从 dao 包导入实例化好的对象
from dao import DataExplorationDAO
from dao import AlphaSignalDAO
from config_manager import config_manager
from dao import SimulatedAlphasDAO
from logger import Logger
from alpha_filter import AlphaFilter
import traceback

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
    curl -X POST http://47.111.116.132:5001/data-exploration/process -H "Content-Type: application/json" -d '{"datetime": "20251018", "force_process": True}'
    """
    data = request.get_json()
    datetime = data.get('datetime')
    force_process = data.get('force_process', False)
    logger.info(f"Received data exploration process request for datetime: {datetime}, force_process: {force_process}") # 日志：记录收到的请求
    if not datetime:
        logger.error("Missing 'datetime' parameter.") # 日志：记录错误
        return jsonify({"status": "error", "message": "Missing 'datetime' parameter."}), 400
    
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
        return jsonify({"status": "error", "message": "Missing query parameters."}), 400
    
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

@alpha_api_blueprint.route('/sync-alpha-details', methods=['POST'])
def sync_alpha_details():
    """
    Synchronously fetch alpha details by alphaId and write to simulated_alphas_table.
    
    Request Body:
        alphaId (str): The alpha ID to query. Required.
    
    Returns:
        dict: {"status": "success"} or {"status": "error", "message": "error reason"}
    
    Example:
        curl -X POST http://localhost:5001/sync-alpha-details -H "Content-Type: application/json" -d '{"alphaId": "your_alpha_id"}'
    """
    data = request.get_json()
    alpha_id = data.get('alphaId')
    
    if not alpha_id:
        logger.error("Missing 'alphaId' parameter in sync-alpha-details request.")
        return jsonify({"status": "error", "message": "Missing 'alphaId' parameter."}), 400
    
    try:
        # 导入必要的模块
        
        # 获取会话
        session = config_manager.get_session()
        brain_api_url = "https://api.worldquantbrain.com"
        request_timeout = 30
        
        logger.info(f"Starting sync fetch for alphaId: {alpha_id}")
        
        # 调用WorldQuant BRAIN API获取alpha详情
        response = session.get(f"{brain_api_url}/alphas/{alpha_id}", timeout=request_timeout)
        response.raise_for_status()
        alpha_details = response.json()
        
        if not alpha_details:
            logger.error(f"Alpha details not found for alphaId: {alpha_id}")
            return jsonify({"status": "error", "message": f"AlphaId {alpha_id} does not exist"})
        
        # 格式化数据以适应数据库表结构
        formatted = _format_alpha_details(alpha_details)
        
        # 验证和转换数据类型
        validated_data = _validate_and_convert_data_types(formatted)
        
        # 写入数据库
        simulated_alphas_dao = SimulatedAlphasDAO()
        result = simulated_alphas_dao.insert(validated_data)
        
        if result is not None:
            logger.info(f"Successfully inserted alpha details for alphaId: {alpha_id}")
            return jsonify({"status": "success"})
        else:
            logger.error(f"Failed to insert alpha details for alphaId: {alpha_id}")
            return jsonify({"status": "error", "message": "Database insertion failed"}), 500
            
    except Exception as e:
        logger.error(f"Error in sync_alpha_details for alphaId {alpha_id}: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

def _format_alpha_details(alpha_details):
    """
    将从API获取的alpha详细信息格式化为数据库接受的格式。
    参考自alpha_poller.py中的_format_alpha_details方法
    """
    date_created = alpha_details.get("dateCreated")
    datetime_str = date_created.split("T")[0].replace("-", "") if date_created else None
    
    settings = alpha_details.get("settings", {})
    region = settings.get("region")
    
    favorite = 1 if alpha_details.get("favorite", False) else 0
    hidden = 1 if alpha_details.get("hidden", False) else 0
    
    grade = alpha_details.get("grade")
    if grade is not None:
        try:
            grade = round(float(grade), 2)
        except (ValueError, TypeError):
            grade = None
                    
    return {
        "id": alpha_details.get("id"),
        "type": alpha_details.get("type"),
        "author": alpha_details.get("author"),
        "settings": str(settings),
        "regular": str(alpha_details.get("regular", {})),
        "dateCreated": _to_mysql_datetime(date_created),
        "dateSubmitted": _to_mysql_datetime(alpha_details.get("dateSubmitted")),
        "name": alpha_details.get("name"),
        "favorite": favorite,
        "hidden": hidden,
        "color": alpha_details.get("color"),
        "category": alpha_details.get("category"),
        "tags": str(alpha_details.get("tags", [])),
        "classifications": str(alpha_details.get("classifications", [])),
        "grade": grade,
        "stage": alpha_details.get("stage"),
        "status": alpha_details.get("status"),
        "is": str(alpha_details.get("is", {})),
        "os": alpha_details.get("os"),
        "train": str(alpha_details.get("train")),
        "test": str(alpha_details.get("test")),
        "prod": alpha_details.get("prod"),
        "competitions": alpha_details.get("competitions"),
        "themes": str(alpha_details.get("themes", [])),
        "pyramids": str(alpha_details.get("pyramids", [])),
        "pyramidThemes": str(alpha_details.get("pyramidThemes", [])),
        "team": alpha_details.get("team"),
        "datetime": datetime_str,
        "region": region
    }

def _validate_and_convert_data_types(data_dict):
    """
    验证和转换字典中的数据类型，确保所有值都是数据库可接受的类型。
    对于字典、列表等复杂类型，转换为JSON字符串。
    """
    validated_data = {}
    conversion_log = []
    
    for key, value in data_dict.items():
        if value is None:
            validated_data[key] = None
        elif isinstance(value, (str, int, float, bool)):
            validated_data[key] = value
        else:
            # 其他类型转换为字符串
            validated_data[key] = str(value)
            conversion_log.append(f"Converted {key} from {type(value).__name__} to string")
    
    if conversion_log:
        logger.info(f"Data type conversions performed: {conversion_log}")
    
    return validated_data

def _to_mysql_datetime(dt_str):
    """
    将ISO格式的日期字符串转换为MySQL的DATETIME格式。
    """
    if not dt_str:
        return None
    try:
        # Python 3.7+ fromisoformat可以直接解析带'Z'的格式
        if dt_str.endswith('Z'):
            dt_str = dt_str[:-1] + '+00:00'
        from datetime import datetime
        dt = datetime.fromisoformat(dt_str)
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        logger.error(f"Failed to parse datetime: {dt_str}, error: {e}")
        return None

def _run_process_alphas_worker(date_str, min_fitness, min_sharpe, corr_threshold):
    """
    Worker function to run the long-running process_alphas task.
    Includes its own error handling and logging since it runs in a separate thread.
    """
    try:
        logger.info(f"Background thread started for alpha filtering on date: {date_str}.")
        # Each thread needs its own service instance to ensure thread safety,
        # especially regarding session objects or shared state.
        alpha_filter_service = AlphaFilter()
        alpha_filter_service.process_alphas(
            date_str=date_str,
            min_fitness=min_fitness,
            min_sharpe=min_sharpe,
            corr_threshold=corr_threshold
        )
        logger.info(f"Background alpha filtering task for date {date_str} completed successfully.")
    except Exception as e:
        error_message = f"An unexpected error occurred in the background alpha filtering thread for date {date_str}."
        logger.error(error_message)
        logger.error(traceback.format_exc()) # Log the full traceback

@alpha_api_blueprint.route('/filter/run', methods=['POST'])
def run_alpha_filter():
    """
    Triggers the Alpha filtering task for a specific date in a background thread.

    Request Body (JSON):
    {
        "date": "YYYYMMDD",
        "min_fitness": 0.7,
        "min_sharpe": 1.2,
        "corr_threshold": 0.7
    }

    Example:
    curl -X POST http://localhost:5001/filter/run \
         -H "Content-Type: application/json" \
         -d '{"date": "20251208", "min_fitness": 0.7, "min_sharpe": 1.2, "corr_threshold": 0.7}'
    """
    logger.info("Received request to run alpha filter.")
    
    data = request.get_json()
    if not data:
        logger.error("Request body is empty or not JSON.")
        return jsonify({"status": "error", "message": "Request body must be a valid JSON."}), 400

    # --- Parameter validation ---
    date_str = data.get('date')
    if not date_str or not (isinstance(date_str, str) and len(date_str) == 8 and date_str.isdigit()):
        logger.error(f"Invalid or missing 'date' parameter: {date_str}")
        return jsonify({"status": "error", "message": "Parameter 'date' is required and must be in YYYYMMDD format."}), 400
        
    try:
        min_fitness = float(data.get('min_fitness', 0.7))
        min_sharpe = float(data.get('min_sharpe', 1.2))
        corr_threshold = float(data.get('corr_threshold', 0.7))
    except (ValueError, TypeError):
        logger.error("Invalid numeric parameters.")
        return jsonify({"status": "error", "message": "min_fitness, min_sharpe, and corr_threshold must be valid numbers."}), 400

    # --- Core logic ---
    # Start the long-running task in a background thread
    background_task = threading.Thread(
        target=_run_process_alphas_worker,
        args=(date_str, min_fitness, min_sharpe, corr_threshold)
    )
    background_task.start()
    
    # Immediately return a response
    success_message = f"Alpha filtering task for date {date_str} has been started in the background."
    logger.info(success_message)
    return jsonify({"status": "success", "message": success_message}), 202

