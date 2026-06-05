import configparser
import os
import csv
import pandas as pd
import ast
import re
import time
import random
import json
import requests
from datetime import datetime
from pytz import timezone
from logger import Logger
from typing import List, Tuple, Optional
from requests.exceptions import ConnectionError, Timeout as RequestsTimeout, ReadTimeout
from http.client import RemoteDisconnected
import concurrent.futures
import ace_lib as ace
from cached_data_fetcher import get_datafields_with_cache
from validator import ExpressionValidator

# 创建全局 Logger 实例
logger = Logger()

# 全局算子数据缓存，避免重复调用接口
_OPERATORS_CACHE = None

def fetch_data(session, params, max_retries=3, delay=5):
    """
    使用 session.get 方法调用 API 获取数据集，并返回 DataFrame 格式的 results 数据。
    添加了失败重试机制。

    Args:
        session: requests.Session 对象，用于发送 HTTP 请求。
        params (dict): API 请求参数字典。
        max_retries (int): 最大重试次数。
        delay (int): 重试之间的延迟秒数。

    Returns:
        pd.DataFrame: API 返回的 results 数据，转换为 DataFrame 格式。如果请求失败或无数据，返回空的 DataFrame。
    """
    base_url = "https://api.worldquantbrain.com/data-sets"
    for attempt in range(max_retries):
        try:
            # 使用 session.get 发送请求，params 自动转换为查询字符串
            response = session.get(base_url, params=params)
            response.raise_for_status()  # 检查请求是否成功，若失败抛出异常
            data = response.json()
            results = data.get('results', [])
            if not results:
                print(f"类别 '{params.get('category', 'unknown')}' 未返回数据")
                return pd.DataFrame()  # 返回空的 DataFrame
            return pd.DataFrame(results)
        except requests.exceptions.RequestException as e:
            print(f"获取数据失败 (尝试 {attempt + 1}/{max_retries})，错误: {e}")
            if attempt < max_retries - 1:
                print(f"将在 {delay} 秒后重试...")
                time.sleep(delay)
            else:
                print("已达到最大重试次数，放弃获取数据。")
                return pd.DataFrame()

def fetch_all_datasets(session, params, max_retries=3, delay=5):
    """
    使用分页获取指定类别的全部数据集，自动处理 offset。

    BRAIN API /data-sets 端点的 limit 最大值为 50，超过会返回 400 错误。
    本函数在内部使用 limit=50 分页，直到获取完所有结果。

    Args:
        session: requests.Session 对象。
        params (dict): API 请求参数字典（不需要包含 offset，会自动添加）。
        max_retries (int): 单次请求的最大重试次数。
        delay (int): 重试之间的延迟秒数。

    Returns:
        pd.DataFrame: 所有分页结果的合并 DataFrame。
    """
    page_limit = 50
    all_dfs = []
    offset = 0

    while True:
        page_params = params.copy()
        page_params['limit'] = page_limit
        page_params['offset'] = offset

        df = fetch_data(session, page_params, max_retries=max_retries, delay=delay)
        if df.empty:
            break

        all_dfs.append(df)
        fetched = len(df)
        if fetched < page_limit:
            # 获取数量不足一页，说明已是最后一页
            break

        offset += page_limit

    if not all_dfs:
        return pd.DataFrame()

    return pd.concat(all_dfs, ignore_index=True)

def write_to_csv(df, file_path, mode='w'):
    """
    将 DataFrame 写入 CSV 文件。
    
    Args:
        df (pd.DataFrame): 要写入的数据。
        file_path (str): 文件路径。
        mode (str): 写入模式，'w' 为覆盖，'a' 为追加。
    """
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    header = not os.path.exists(file_path) or mode == 'w'
    df.to_csv(file_path, index=False, mode=mode, header=header)

def get_eastern_time():
    """获取美东时间"""
    eastern = timezone("US/Eastern")
    return datetime.now(eastern)

def is_midnight_eastern():
    """检查是否为美东时间 0 点"""
    eastern_time = get_eastern_time()
    return eastern_time.hour == 0 and eastern_time.minute == 0

def read_csv(file_path):
    """读取 CSV 文件为 DataFrame"""
    if os.path.exists(file_path):
        return pd.read_csv(file_path)
    return pd.DataFrame()

def write_csv(df, file_path):
    """写入 DataFrame 到 CSV 文件"""
    df.to_csv(file_path, index=False)

def sample_alphas_from_file(filename: str, percentage: float) -> List[str]:
    """
    从 JSON 文件中读取 alpha 表达式列表，并返回随机抽样的列表。
    
    Args:
        filename (str): 包含 alpha 列表的 JSON 文件路径。
        percentage (float): 抽样百分比（如 0.03 表示 3%）。
        
    Returns:
        List[str]: 随机抽样的 alpha 表达式列表。
    """
    try:
        if not os.path.exists(filename):
            print(f"File not found: {filename}")
            return []
            
        with open(filename, 'r') as f:
            alphas = json.load(f)
            
        if not isinstance(alphas, list):
            print(f"Error: Expected a list in {filename}, but got {type(alphas)}")
            return []
            
        total_count = len(alphas)
        sample_size = int(total_count * percentage)
        
        # 确保如果列表不为空，至少抽取一个（如果百分比 > 0）
        if sample_size <= 0 and total_count > 0 and percentage > 0:
            sample_size = 1
            
        sampled_alphas = random.sample(alphas, min(sample_size, total_count))
        print(f"Sampled {len(sampled_alphas)} alphas ({percentage*100:.2f}%) from total {total_count} in {filename}")
        return sampled_alphas
    except Exception as e:
        print(f"Error sampling alphas from {filename}: {e}")
        return []


def load_config(config_file="config/config.ini"):
    """
    从配置文件读取 username、password、max_concurrent 和 batch_number_for_every_queue, batch_size。

    Args:
        config_file (str): 配置文件路径，默认为 "config/config.ini"。

    Returns:
        dict: 包含 username, password, max_concurrent, batch_number_for_every_queue, batch_size 的字典，
              如果读取失败则返回 None。
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    config_path = os.path.join(project_root, config_file)

    config = configparser.ConfigParser()

    try:
        if not os.path.exists(config_path):
            logger.error(f"Config file {config_path} not found.")
            return None

        config.read(config_path)
        
        # Parse region_set as a list
        region_set_str = config.get('Credentials', 'region_set', fallback='US')
        try:
            region_set = ast.literal_eval(region_set_str)
            if not isinstance(region_set, list):
                raise ValueError(f"Expected list for region_set, got {type(region_set)}")
        except (ValueError, SyntaxError) as e:
            logger.error(f"Error parsing region_set: {e}. Using default ['US']")
            region_set = ['US']

        config_data = {
            'username': config.get('Credentials', 'username'),
            'password': config.get('Credentials', 'password'),
            'max_concurrent': config.getint('Credentials', 'max_concurrent'),
            'batch_number_for_every_queue': config.getint('Credentials', 'batch_number_for_every_queue'),
            'batch_size': config.getint('Credentials', 'batch_size'),
            'init_date_str': config.get('Credentials', 'init_date_str'),
            'region_set': region_set
        }

        # Load SA simulator configurations if they exist
        if config.has_option('Credentials', 'sa_simulator_region'):
            config_data['sa_simulator_region'] = config.get('Credentials', 'sa_simulator_region')
        
        if config.has_option('Credentials', 'sa_simulator_concurrent_simulations'):
            try:
                config_data['sa_simulator_concurrent_simulations'] = config.getint('Credentials', 'sa_simulator_concurrent_simulations')
            except ValueError:
                # If not an integer, try to get as string
                config_data['sa_simulator_concurrent_simulations'] = config.get('Credentials', 'sa_simulator_concurrent_simulations')

        # Load LLM configuration if section exists
        if config.has_section('LLM'):
            config_data.update({
                'llm_free_base_url': config.get('LLM', 'llm_free_base_url', fallback='https://api.deepseek.com'),
                'llm_free_api_key': config.get('LLM', 'llm_free_api_key', fallback=''),
                'llm_free_model': config.get('LLM', 'llm_free_model', fallback='deepseek-ai/DeepSeek-V3.2-Exp'),
                'llm_paid_base_url': config.get('LLM', 'llm_paid_base_url', fallback='https://api.openai.com/v1'),
                'llm_paid_api_key': config.get('LLM', 'llm_paid_api_key', fallback=''),
                'llm_paid_model': config.get('LLM', 'llm_paid_model', fallback='gpt-4'),
                'llm_base_url': config.get('LLM', 'llm_base_url', fallback='https://api.deepseek.com'),
                'llm_api_key': config.get('LLM', 'llm_api_key', fallback=''),
                'llm_model': config.get('LLM', 'llm_model', fallback='deepseek-chat')
            })

        # 日志记录参数值，方便调试
        logger.info(f"Loaded config from {config_path}: username={config_data['username']}, "
                   f"max_concurrent={config_data['max_concurrent']} (type: {type(config_data['max_concurrent'])}), "
                   f"batch_number={config_data['batch_number_for_every_queue']} (type: {type(config_data['batch_number_for_every_queue'])}), "
                   f"init_date={config_data['init_date_str']}")

        return config_data

    except configparser.NoSectionError:
        logger.error(f"Section 'Credentials' not found in {config_path}")
        return None
    except configparser.NoOptionError as e:
        logger.error(f"Missing key in config: {e}")
        return None
    except ValueError as e:
        logger.error(f"Invalid value in config (must be integer for max_concurrent or batch_number): {e}")
        return None
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        return None

def fix_newline_expression(expression):
    """
    修复表达式中的";n"问题（应该是";\n"被错误转换）
    
    Args:
        expression (str): 输入表达式字符串
        
    Returns:
        str: 修复后的表达式（如果发现问题），否则返回原表达式
    """
    # 检查是否存在";n"问题
    if ";n" in expression:
        # 替换所有";n"为";\n"
        fixed_expression = expression.replace(";n", ";\n")
        return fixed_expression
    return expression

def negate_expression(expression):
    """
    为表达式添加负号
    
    Args:
        expression (str): 输入表达式字符串
        
    Returns:
        str: 处理后的表达式
            如果是单行表达式，直接在前面加负号
            如果是多行表达式，在最后一行前加负号
    """
    if "\n" not in expression:
        # 单行表达式
        return f"-({expression})"
    else:
        # 多行表达式
        lines = expression.split("\n")
        last_line = lines[-1].strip()
        # 在最后一行前加负号
        lines[-1] = f"-({last_line})"
        return "\n".join(lines)

def get_alphas_from_data(data_rows, min_sharpe, min_fitness, mode="track", region_filter=None, single_data_set_filter=None):
    """
    Process database data to generate alpha records in the format:
    [alpha_id, exp, sharpe, turnover, fitness, margin, dateCreated, decay]
    
    Args:
        data_rows: List of database rows containing alpha data
        min_sharpe: Minimum sharpe ratio threshold (e.g. 1.0)
        min_fitness: Minimum fitness threshold (e.g. 0.5)
        mode: Filter mode - 'submit' or 'track' (default: 'track')
        region_filter: Optional region filter (e.g. "USA"). If None, no region filtering.
        single_data_set_filter: Optional boolean to filter Single Data Set Alphas. If None, no filtering.
    
    Returns:
        List of filtered alpha records
    """
    output = []
    
    for row in data_rows:
        try:
            # Parse the necessary fields from the row
            alpha_id = row['id']
            
            # Parse the settings dictionary
            settings = row['settings']
            decay = settings.get('decay', 0)

            if settings.get('decay') == 0 and settings.get('neutralization') == 'NONE':
                continue
            
            # Parse the regular code (expression)
            regular = row['regular']
            exp = fix_newline_expression(regular.get('code', ''))
            operatorCount = regular.get('operatorCount', 0)
            
            # Parse the is dictionary
            is_data = row['is']
            sharpe = is_data.get('sharpe', 0)
            fitness = is_data.get('fitness', 0)
            turnover = is_data.get('turnover', 0)
            margin = is_data.get('margin', 0)
            longCount = is_data.get('longCount', 0)
            shortCount = is_data.get('shortCount', 0)
            returns = is_data.get('returns', 0)
            drawdown = is_data.get('drawdown', 0)
            checks = is_data.get('checks', [])
            
            dateCreated = row['dateCreated']
            has_failed_checks = any(check['result'] == 'FAIL' for check in checks)
            # Apply region filter if specified
            if region_filter is not None and settings.get('region') != region_filter:
                continue
                
            # Apply single data set filter if specified
            if single_data_set_filter is not None:
                classifications = row.get('classifications', '[]')
                is_single_data_set = any(
                    classification.get('id') == 'DATA_USAGE:SINGLE_DATA_SET' 
                    for classification in classifications
                )
                if single_data_set_filter != is_single_data_set:
                    continue
                elif operatorCount > 8:
                    continue
            
            # Apply other filters
            if (longCount + shortCount) > 100:
                if mode == "submit":
                    if (sharpe >= min_sharpe and fitness >= min_fitness) and not has_failed_checks:
                        if sharpe is not None and sharpe < 0:
                            exp = negate_expression(exp)
                        # Create the record
                        rec = [
                            alpha_id,
                            exp,
                            sharpe,
                            turnover,
                            fitness,
                            margin,
                            returns,
                            drawdown,
                            dateCreated,
                            decay
                        ]
                        
                        # Extract pyramids info from checks if available
                        pyramid_checks = [check for check in checks if check.get('name') == 'MATCHES_PYRAMID']
                        if pyramid_checks and 'pyramids' in pyramid_checks[0]:
                            pyramids = pyramid_checks[0]['pyramids']
                            rec.insert(7, pyramids)  # Insert after dateCreated
                        
                        # Add additional decay modifier based on turnover
                        if turnover > 0.7:
                            rec.append(decay * 4)
                        elif turnover > 0.6:
                            rec.append(decay * 3 + 3)
                        elif turnover > 0.5:
                            rec.append(decay * 3)
                        elif turnover > 0.4:
                            rec.append(decay * 2)
                        elif turnover > 0.35:
                            rec.append(decay + 4)
                        elif turnover > 0.3:
                            rec.append(decay + 2)
                        
                        output.append(rec)
                else:  # track mode
                    if (sharpe >= min_sharpe and fitness >= min_fitness) or (sharpe <= min_sharpe * -1.0 and fitness <= min_fitness * -1.0):
                        if sharpe is not None and sharpe < 0:
                            exp = negate_expression(exp)
                        # Create the record
                        rec = [
                            alpha_id,
                            exp,
                            sharpe,
                            turnover,
                            fitness,
                            margin,
                            returns,
                            drawdown,
                            dateCreated,
                            decay
                        ]
                        
                        # Extract pyramids info from checks if available
                        pyramid_checks = [check for check in checks if check.get('name') == 'MATCHES_PYRAMID']
                        if pyramid_checks and 'pyramids' in pyramid_checks[0]:
                            pyramids = pyramid_checks[0]['pyramids']
                            rec.insert(7, pyramids)  # Insert after dateCreated
                        
                        # Add additional decay modifier based on turnover
                        if turnover > 0.7:
                            rec.append(decay * 4)
                        elif turnover > 0.6:
                            rec.append(decay * 3 + 3)
                        elif turnover > 0.5:
                            rec.append(decay * 3)
                        elif turnover > 0.4:
                            rec.append(decay * 2)
                        elif turnover > 0.35:
                            rec.append(decay + 4)
                        elif turnover > 0.3:
                            rec.append(decay + 2)
                        
                        output.append(rec)
            
        except (ValueError, SyntaxError, KeyError) as e:
            # Skip rows with parsing errors or missing required fields
            continue
            
    return output

def safe_api_call(func, *args, max_retries=3, initial_delay=60, call_timeout=300, **kwargs):
    """
    Safely execute a function (usually an API call) with a retry mechanism
    and per-call timeout protection.

    Args:
        func: The function to execute, e.g., ace_lib.get_check_submission.
        *args, **kwargs: Arguments to pass to func.
        max_retries: Maximum number of retry attempts.
        initial_delay: Initial delay between retries in seconds.
        call_timeout: Per-call timeout in seconds (default: 300s = 5 minutes).
    """
    delay = initial_delay
    for attempt in range(max_retries):
        try:
            # Execute with thread-based timeout to prevent hanging
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(func, *args, **kwargs)
                return future.result(timeout=call_timeout)

        except (concurrent.futures.TimeoutError, RequestsTimeout, ReadTimeout, RemoteDisconnected, ConnectionError, json.JSONDecodeError, requests.exceptions.HTTPError) as e:
            # Handle specific HTTP errors that should be retried
            if isinstance(e, requests.exceptions.HTTPError):
                status_code = e.response.status_code if e.response is not None else "Unknown"
                if status_code not in [429, 500, 502, 503, 504]:
                    raise
            
            if attempt + 1 == max_retries:
                print(f"FATAL: API call failed after {max_retries} retries: {e}")
                raise
            
            print(f"Warning: API call failed: {e}. Retrying in {delay}s... (Attempt {attempt + 1}/{max_retries})")
            time.sleep(delay + random.uniform(0, 5))
            delay *= 2

        except Exception as e:
            # Catch-all for other unexpected exceptions that should NOT be retried
            print(f"FATAL: API call failed with unexpected error: {e}")
            raise
def extract_datafields(expression: str, brain_session) -> set:
    """
    (Final encapsulated version) Extracts a candidate set of data fields from an expression string.

    This function internally calls the API to get all operators and their keywords,
    then uses a "subtraction" logic: it finds all words in the expression,
    then subtracts all known "non-field" keywords, and the remaining are data fields.

    Args:
        expression: The alpha expression string.
        brain_session: The authenticated BRAIN platform session object.

    Returns:
        A set containing all candidate data fields.
    """
    # 1. --- 封装的逻辑：获取并构建算子关键词集合 ---
    global _OPERATORS_CACHE
    if _OPERATORS_CACHE is None:
        logger.info("Operators cache is empty. Fetching operators from platform API...")
        _OPERATORS_CACHE = safe_api_call(ace.get_operators, brain_session)
    #else:
        #logger.info("Operators data found in cache. Using cached data.")
    
    raw_operators_data = _OPERATORS_CACHE
    if raw_operators_data is None:
        print("Error: Could not fetch operators. Aborting field extraction.")
        return set()
    
    regular_operators_df = raw_operators_data[raw_operators_data['scope']=='REGULAR']
    regular_operators = regular_operators_df.to_dict('records')

    #print(regular_operators_df)
    
    operator_names = {op['name'] for op in regular_operators if op.get('name')}
    operator_params = set()
    for op in regular_operators:
        definition_str = op.get('definition')
        if definition_str:
            params = re.findall(r'(\w+)\s*=', definition_str)
            if params:
                operator_params.update(params)
    
    operator_keywords = operator_names.union(operator_params)
    
    # Manually add common BRAIN keywords/literals that are not fields
    # Add driver options: gaussian, uniform, cauchy
    non_field_keywords = operator_keywords.union({'true', 'false', 'and', 'or', 'nan', 'inf', 'gaussian', 'uniform', 'cauchy'})

    # print(f"Operator keywords ({len(operator_keywords)})")
    # --- 关键词集合构建完毕 ---

    # 2. 使用正则表达式找出所有可能是标识符的“单词”
    # First, remove string literals (both single and double quoted) to avoid extracting their content as fields.
    expression_without_literals = re.sub(r'"[^"]*"|\'[^\']*\'', '', expression)
    all_identifiers = set(re.findall(r'[a-zA-Z_][a-zA-Z0-9_]*', expression_without_literals))
    
    # 3. 从所有标识符中，减去所有已知的“非字段”关键词
    potential_fields = all_identifiers - non_field_keywords
    
    # 4. 从剩下的候选中，再排除纯数字常量
    found_fields = set()
    for field in potential_fields:
        try:
            float(field) # 尝试转换为浮点数
        except ValueError:
            # 如果转换失败，说明它不是一个数字，因此是一个字段
            found_fields.add(field)
            
    return found_fields


def validate_expression_fields(expression: str, valid_data_fields: set, brain_session) -> Tuple[bool, Optional[str]]:
    """
    Validates that all parsed fields in a single expression exist in the given list of valid fields.
    
    Args:
        expression: The alpha expression string to validate.
        valid_data_fields: A set containing all valid data field names.
        brain_session: The authenticated BRAIN platform session object.

    Returns:
        Tuple[bool, Optional[str]]: A tuple where the first element is True if all fields are valid,
                                    False otherwise. The second element is an error message if validation fails,
                                    otherwise None.
    """
    try:
        # 1. Call internal function to extract candidate data fields used in the expression
        used_fields = extract_datafields(expression, brain_session)
    except Exception as e:
        # If an error occurs during parsing, return False with the error message
        error_message = f"Error parsing expression: {expression}, Error: {e}"
        print(f"❌ {error_message}") # Keep print for debugging/logging
        return False, error_message

    # 2. Check if all extracted fields are present in the 'valid_data_fields' list
    unmatched_fields = used_fields - valid_data_fields
    
    # 3. Return result based on the check
    if not unmatched_fields:
        # If the difference set is empty, all fields matched successfully, expression is valid
        return True, None
    else:
        # Otherwise, return False with the list of unmatched fields
        error_message = f"Expression filtered: Undefined or invalid fields found: {unmatched_fields}"
        print(f"❌ {error_message}") # Keep print for debugging/logging
        return False, error_message


def filter_alpha_by_datafields(alpha_expression: str, settings_dict: dict, brain_session) -> bool:
    """
    Filters an alpha expression based on its datafields to check if it only uses pv1 fields.

    Args:
        alpha_expression: The alpha expression string.
        settings_dict: A dictionary containing settings like region, universe, delay.
        brain_session: The authenticated BRAIN platform session object.

    Returns:
        bool: True if the alpha should be simulated (contains non-pv1 fields), 
              False if it should be filtered out (contains only pv1 fields).
    """
    # 1. Extract datafields from the expression
    try:
        used_fields = extract_datafields(alpha_expression, brain_session)
        if not used_fields:
            # If no fields are found, no need to filter, let it be validated later.
            return True
    except Exception as e:
        print(f"Error parsing expression during pv1 filter, skipping filter: {alpha_expression}, Error: {e}")
        # If parsing fails, don't filter it out, let it fail in the main validation.
        return True

    # 2. Get grouping and price fields from the pv1 dataset
    region = settings_dict.get("region", "GLB")
    universe = settings_dict.get("universe", "TOP3000")
    delay = settings_dict.get("delay", 1)

    try:
        grouping_fields_df = get_datafields_with_cache(brain_session, region=region, universe=universe, delay=delay, data_type='GROUP', dataset_id='pv1')
        price_fields_df = get_datafields_with_cache(brain_session, region=region, universe=universe, delay=delay, data_type='MATRIX', dataset_id='pv1')
    except Exception as e:
        print(f"Error getting grouping/price fields for pv1 filter, skipping filter: {e}")
        # If getting base fields fails, don't filter as a precaution.
        return True
        
    grouping_fields_ids = set(grouping_fields_df['id']) if grouping_fields_df is not None and not grouping_fields_df.empty else set()
    price_fields_ids = set(price_fields_df['id']) if price_fields_df is not None and not price_fields_df.empty else set()

    pv1_fields = grouping_fields_ids.union(price_fields_ids)

    # 3. Check if all used fields are a subset of the pv1 fields
    if pv1_fields and used_fields.issubset(pv1_fields):
        print(f"Expression filtered: '{alpha_expression}' contains only fields from the 'pv1' dataset.")
        return False
    else:
        # If there are fields not in pv1, or if pv1 fields could not be fetched, let it pass
        return True



def complete_expression(expression: str, all_fields_df: pd.DataFrame, brain_session) -> list[str]:
    """
    根据数据字段的类型（VECTOR/MATRIX）自动补全或修正单个表达式。
    规则:
    1. 如果VECTOR类型的数据字段没有被vec_**()形式的算子包裹，则为其添加vec_avg()算子。
    2. 如果MATRIX类型的数据字段被vec_**()形式的算子包裹，则去掉该vec_开头的算子。
    
    Args:
        expression: 要修正的alpha表达式字符串。
        all_fields_df: 包含所有字段及其类型的DataFrame。
        brain_session: 已认证的BRAIN平台会话对象。

    Returns:
        list[str]: 一个包含修正后表达式的列表（为未来扩展性设计）。
    """
    # 1. 从传入的DataFrame创建字段类型映射
    field_type_map = pd.Series(all_fields_df.type.values, index=all_fields_df.id).to_dict()

    # 2. 获取表达式中使用的数据字段
    try:
        used_fields = extract_datafields(expression, brain_session)
    except Exception as e:
        print(f"❌ 在补全前解析表达式时出错: {expression}, 错误: {e}")
        return [expression] # 出错则返回原表达式

    # 3. 应用修正规则
    new_expr = expression
    for field in used_fields:
        field_type = field_type_map.get(field)
        
        if field_type == 'VECTOR':
            # 规则一：为裸露的VECTOR字段添加vec_avg()。
            # 步骤1: 首先将所有出现的该字段用vec_avg()包裹。
            # 例如: vec_sum(v1) + v1  ->  vec_sum(vec_avg(v1)) + vec_avg(v1)
            temp_expr = re.sub(r'\b' + re.escape(field) + r'\b', f'vec_avg({field})', new_expr)
            
            # 步骤2: 修正步骤1中可能造成的双重包裹问题。
            # 例如: vec_sum(vec_avg(v1)) -> vec_sum(v1)
            # 这个正则表达式寻找 vec_xxx(vec_avg(field)) 的模式
            double_wrap_pattern = r'vec_(\w+)\s*\(\s*vec_avg\s*\(\s*(' + re.escape(field) + r')\s*\)\s*\)'
            # 并将其替换为 vec_xxx(field)
            new_expr = re.sub(double_wrap_pattern, r'vec_\1(\2)', temp_expr)

        elif field_type == 'MATRIX':
            # 规则二：为被vec_**()包裹的MATRIX字段移除包裹。
            # 使用while循环来处理可能存在的多层包裹，例如 vec_sum(vec_avg(m1))
            pattern_to_remove = r'vec_\w+\s*\(\s*(' + re.escape(field) + r')\s*\)'
            while re.search(pattern_to_remove, new_expr):
                new_expr = re.sub(pattern_to_remove, r'\1', new_expr)
    
    if new_expr != expression:
        print(f"🔧 表达式已修正: '{expression}' -> '{new_expr}'")
    
    new_expr = update_expression_parameters(new_expr)
    # 以列表形式返回，便于未来扩展
    return [new_expr]


def update_expression_parameters(expression: str) -> str:
    """
    Update expression parameters to explicitly name them.
    """
    # ts_regression with 5 parameters: e.g. ts_regression(y, x, d, lag, rettype) -> ts_regression(y, x, d, lag=1, rettype=2)
    expression = re.sub(r'ts_regression\(([^,]+,\s*[^,]+,\s*[^,]+),\s*(\d+),\s*(\d+)\)$', r'ts_regression(\1, lag=\2, rettype=\3)', expression)
    # ts_regression with 4 parameters: e.g. ts_regression(y, x, d, rettype) -> ts_regression(y, x, d, rettype=1)
    expression = re.sub(r'ts_regression\(([^,]+,\s*[^,]+,\s*[^,]+),\s*(\d+)\)$', r'ts_regression(\1, rettype=\2)', expression)
    # winsorize
    expression = re.sub(r'winsorize\(([^,]+,)\s*(\d+(\.\d+)?)\)$', r'winsorize(\1 std=\2)', expression)
    # group_backfill
    expression = re.sub(r'group_backfill\(([^,]+,)\s*(\d+(\.\d+)?)\)$', r'group_backfill(\1 std=\2)', expression)
    return expression

# Use ace to query datasets based on fields in alpha, return dataset DataFrame
def get_datasets_for_alpha(alpha_id: str, brain_session) -> pd.DataFrame:
    """
    Queries datasets based on the data fields used in a given alpha expression.
    This function is wrapped with safe_api_call for robustness.
    """
    def _get_json(session, url):
        """Helper to get and decode JSON from a URL."""
        response = session.get(url)
        response.raise_for_status()
        return response.json()

    # Safely get alpha details
    alpha_details = safe_api_call(_get_json, brain_session, f"https://api.worldquantbrain.com/alphas/{alpha_id}")
    if not alpha_details:
        logger.warning(f"[get_datasets_for_alpha] EXIT@alpha_details: alpha_details is falsy for {alpha_id} (type={type(alpha_details).__name__}, value={repr(alpha_details)[:200]})")
        return pd.DataFrame(), None

    alpha_expression = alpha_details['regular']['code']
    datafields = extract_datafields(alpha_expression, brain_session)
    logger.info(f"[get_datasets_for_alpha] alpha {alpha_id}: expression fields extracted = {datafields} (count={len(datafields)})")
    settings_dict = extract_alpha_settings(alpha_details)
    logger.info(f"[get_datasets_for_alpha] alpha {alpha_id}: region={settings_dict['region']}, universe={settings_dict['universe']}, delay={settings_dict['delay']}")

    # Use search parameter to get data fields, then keep only exact matches
    data_fields_list = []
    api_fetch_errors = 0
    api_empty_results = 0
    no_exact_match = 0
    for data_field in datafields:
        # Optimization: Use safe_api_call and add delay to avoid rate limiting
        try:
            search_results = safe_api_call(
                ace.get_datafields,
                brain_session,
                region=settings_dict['region'],
                universe=settings_dict['universe'],
                delay=settings_dict['delay'],
                data_type='ALL',
                search=data_field
            )
        except Exception as e:
            logger.warning(f"[get_datasets_for_alpha] safe_api_call FAILED for field '{data_field}': {e}")
            api_fetch_errors += 1
            continue
        time.sleep(1) # Small delay between queries to avoid 429

        # Keep only exact matches in the search results
        if search_results is None:
            logger.warning(f"[get_datasets_for_alpha] search_results is None for field '{data_field}'")
            api_empty_results += 1
            continue
        if search_results.empty:
            logger.warning(f"[get_datasets_for_alpha] search_results is empty DataFrame for field '{data_field}'")
            api_empty_results += 1
            continue

        if 'id' not in search_results.columns:
            logger.warning(f"[get_datasets_for_alpha] 'id' column missing in search_results for field '{data_field}'. Columns: {list(search_results.columns)}")
            no_exact_match += 1
            continue

        exact_matches = search_results[search_results['id'] == data_field]
        if not exact_matches.empty:
            data_fields_list.append(exact_matches)
        else:
            logger.warning(f"[get_datasets_for_alpha] No exact match for field '{data_field}' in {len(search_results)} results")
            no_exact_match += 1

    logger.info(f"[get_datasets_for_alpha] field search summary: matched={len(data_fields_list)}, api_errors={api_fetch_errors}, api_empty={api_empty_results}, no_match={no_exact_match}")

    data_fields = pd.DataFrame()
    if data_fields_list:
        data_fields = pd.concat(data_fields_list, ignore_index=True)

    if data_fields.empty:
        logger.warning(f"[get_datasets_for_alpha] EXIT@empty_data_fields: no exact field matches found for {alpha_id}")
        print(f"Data fields for alpha {alpha_id}: {datafields}")
        print(f"Exact matched data fields details:")
        print(f"Number of matches: {len(data_fields)}")
        return pd.DataFrame(), None

    dataset_ids = list(set(data_fields['dataset_id'].tolist()))
    dataset_ids = [did for did in dataset_ids if did != 'pv1']
    logger.info(f"[get_datasets_for_alpha] dataset_ids (after filtering pv1): {dataset_ids}")

    if not dataset_ids:
        logger.warning(f"[get_datasets_for_alpha] EXIT@no_dataset_ids: all fields belong to pv1 for {alpha_id}")
        return pd.DataFrame(), None

    datasets_list = []
    for dataset_id in dataset_ids:
        dataset_info = get_datafields_with_cache(
            brain_session,
            region=settings_dict['region'],
            universe=settings_dict['universe'],
            delay=settings_dict['delay'],
            data_type='ALL',
            dataset_id=dataset_id
        )

        if dataset_info is not None and not dataset_info.empty:
            datasets_list.append(dataset_info)
        else:
            logger.warning(f"[get_datasets_for_alpha] dataset '{dataset_id}' returned empty/None")

    all_datasets = pd.concat(datasets_list, ignore_index=True) if datasets_list else pd.DataFrame()

    logger.info(f"[get_datasets_for_alpha] SUCCESS for {alpha_id}: {len(all_datasets)} fields from {len(datasets_list)} datasets")
    return all_datasets, dataset_ids[0] if dataset_ids else None

def extract_alpha_settings(alpha_details):
    """
    Extracts setting information from the original signal.

    Args:
        alpha_details: The original signal alpha_details.

    Returns:
        A dictionary where the key is the signal ID and the value is another dictionary
        containing region, universe, neutralization, maxTrade, visualization, and delay.
    """
    try:
        # Parse the setting string into a dictionary
        settings = alpha_details['settings']
        # Extract the required fields
        settings_dict = {
            'region': settings.get('region'),
            'universe': settings.get('universe'),
            'neutralization': settings.get('neutralization'),
            'maxTrade': settings.get('maxTrade'),
            'visualization': settings.get('visualization'),
            'delay': settings.get('delay', 1),  # Default value is 1
            'decay': settings.get('decay', 0)  # Default value is 0
        }
    except Exception as e:
        print(f"Error parsing settings for alpha id {alpha_details['id']}: {e}")
    return settings_dict

def get_dataset_ids(datasets_df: pd.DataFrame) -> list:
    """
    Extracts all field values from the 'id' column of a dataset DataFrame.

    Args:
        datasets_df: The dataset DataFrame returned from the get_datasets_for_alpha method.

    Returns:
        list: A list containing all values from the 'id' field.
    """
    if datasets_df is None or datasets_df.empty:
        return []
    
    # Check if the DataFrame contains an 'id' column
    if 'id' not in datasets_df.columns:
        return []
    
    # Extract all values from the 'id' column and return as a list
    return datasets_df['id'].tolist()

def sort_alphas_by_fitness(alpha_ids: List[str], brain_session) -> List[str]:
    """
    Sorts a list of alpha IDs by their 'fitness' value in descending order.

    Args:
        alpha_ids: A list of alpha ID strings.
        brain_session: The authenticated BRAIN platform session object.

    Returns:
        A list of alpha IDs sorted by fitness score from highest to lowest.
    """
    def _get_json_from_url(session, url):
        """Helper to get and decode JSON from a URL."""
        response = session.get(url)
        response.raise_for_status()
        return response.json()

    alpha_fitness_list = []
    for alpha_id in alpha_ids:
        try:
            alpha_details = safe_api_call(_get_json_from_url, brain_session, f"https://api.worldquantbrain.com/alphas/{alpha_id}")
            if alpha_details:
                is_stats = alpha_details.get('is', {})
                fitness = is_stats.get('fitness')
                if fitness is not None:
                    alpha_fitness_list.append((alpha_id, fitness))
                else:
                    print(f"Warning: 'fitness' not found for alpha {alpha_id}.")
            else:
                print(f"Warning: Could not retrieve details for alpha {alpha_id}.")
        except Exception as e:
            print(f"Error fetching details for alpha {alpha_id}: {e}")

    # Sort the list of tuples by fitness in descending order
    alpha_fitness_list.sort(key=lambda x: x[1], reverse=True)

    # Extract just the alpha IDs from the sorted list
    sorted_alpha_ids = [alpha_id for alpha_id, fitness in alpha_fitness_list]

    return sorted_alpha_ids

def filter_and_fix_expressions(expression_list: List[str], region: str, universe: str, delay: int, dataset_id: List[str], brain_session) -> List[str]:
    """
    Method 1: Filter expressions based on valid fields and fix vector type fields.
    
    Args:
        expression_list (List[str]): List of alpha expressions.
        region (str): Region code.
        universe (str): Universe code.
        delay (int): Delay value.
        dataset_id (List[str]): List of Dataset IDs.
        brain_session: Authenticated BRAIN session.
        
    Returns:
        List[str]: List of filtered and fixed expressions.
    """
    # 1. Get valid datafields
    df_list = []
    if isinstance(dataset_id, str):
        dataset_id = [dataset_id]
        
    for d_id in dataset_id:
        print(f"Fetching datafields for dataset {d_id}...")
        temp_df = get_datafields_with_cache(brain_session, region=region, universe=universe, delay=delay, data_type='ALL', dataset_id=d_id)
        if temp_df is not None and not temp_df.empty:
            df_list.append(temp_df)
            
    if not df_list:
        print(f"Error: No datafields found for datasets {dataset_id}")
        return []
        
    df = pd.concat(df_list, ignore_index=True)
    valid_field_ids = set(df['id'])
    
    fixed_expressions_list = []
    validator = ExpressionValidator()
    
    print(f"Iterating through {len(expression_list)} expressions to validate and fix...")
    for i, expression in enumerate(expression_list):
        if i % 10 == 0:
            print(f"Processing expression {i+1}/{len(expression_list)}...")
        
        # 1. Syntax Validation (Check for basic syntax, operators, parameters, etc.)
        check_result = validator.check_expression(expression)
        if not check_result['valid']:
            print(f"❌ Syntax error in expression: {expression}")
            for error in check_result['errors']:
                print(f"   - {error}")
            continue

        # 2. Validate fields (Check if datafields exist in the target datasets)
        is_valid, _ = validate_expression_fields(expression, valid_field_ids, brain_session)
        
        if is_valid:
            # 3. Fix expression (complete_expression returns a list)
            fixed_exprs = complete_expression(expression, df, brain_session)
            fixed_expressions_list.extend(fixed_exprs)
            
    return fixed_expressions_list

def save_simulatable_alphas(expression_list: List[str], region: str, universe: str, delay: int, dataset_id: str, decay: int, neutralization: str, truncation: float, max_trade):
    """
    Method 2: Assemble expressions and settings into simulatable_alphas.json format and save.
    
    Args:
        expression_list (List[str]): List of alpha expressions.
        region (str): Region code.
        universe (str): Universe code.
        delay (int): Delay value.
        dataset_id (str): Dataset ID (passed for context/consistency, currently unused in output).
        decay (int): Decay value.
        neutralization (str): Neutralization method.
        truncation (float): Truncation value.
        max_trade (float/str): Max trade value.
    """
    print(f"Entering save_simulatable_alphas with {len(expression_list)} expressions")
    # Define output path
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    output_path = os.path.join(project_root, "output", "pending_alpha", f"simulatable_alphas_{region}_{universe}_{delay}_{dataset_id}.json")
    output_path2 = os.path.join(project_root, "output", "pending_alpha", f"alpha_list_{region}_{universe}_{delay}_{dataset_id}.json")
    alphas_to_save = []
    alphas_list = []
    
    for expr in expression_list:
        alpha_entry = {
            "type": "REGULAR",
            "settings": {
                "instrumentType": "EQUITY",
                "region": region,
                "universe": universe,
                "delay": delay,
                "decay": decay,
                "neutralization": neutralization,
                "truncation": truncation,
                "pasteurization": "ON",
                "unitHandling": "VERIFY",
                "nanHandling": "OFF",
                "language": "FASTEXPR",
                "visualization": False,
                "testPeriod": "P0Y",
                "maxTrade": max_trade
            },
            "regular": expr
        }
        alphas_to_save.append(alpha_entry)
        alphas_list.append(expr)
        
    # Assemble into the final structure: Region -> Universe -> List of Alphas
    final_data = {
        region: {
            universe: alphas_to_save
        }
    }
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    os.makedirs(os.path.dirname(output_path2), exist_ok=True)

    
    # Write to file
    try:
        with open(output_path, 'w') as f:
            json.dump(final_data, f, indent=4)
        print(f"Successfully saved {len(alphas_to_save)} alphas to {output_path}")
    except Exception as e:
        print(f"Error saving simulatable alphas to {output_path}: {e}")

    # Write to file
    try:
        with open(output_path2, 'w') as f:
            json.dump(alphas_list, f, indent=4)
        print(f"Successfully saved {len(alphas_list)} alphas to {output_path2}")
    except Exception as e:
        print(f"Error saving simulatable alphas to {output_path2}: {e}")


def main():
    # 1. Load config for credentials
    brain_session = ace.start_session()

    # 2. Define region, universe, delay
    region = "USA"
    universe = "ILLIQUID_MINVOL1M"
    delay = 1

    # 3. 指定要查询的类别 (Categories)
    # 您只需要在此处手动输入类别，例如 ["analyst", "fundamental"]
    categories_to_query = ["option"]
    
    dataset_ids = []
    default_params = {
        'delay': delay,
        'instrumentType': 'EQUITY',
        'region': region,
        'universe': universe,
    }

    print(f"Fetching dataset IDs for categories: {categories_to_query} in {region} {universe}...")
    for category in categories_to_query:
        params = default_params.copy()
        params['category'] = category
        # 使用分页获取完整数据集列表（API limit 最大 50，自动分页）
        results_df = fetch_all_datasets(brain_session, params)
        if not results_df.empty:
            # 提取该类别下的所有 dataset ID
            category_ids = results_df['id'].tolist()
            dataset_ids.extend(category_ids)
            print(f"类别 '{category}' 找到 {len(category_ids)} 个数据集")
        else:
            print(f"类别 '{category}' 未返回数据")

    # 拼上 pv1 得到完整的 dataset_id 列表
    if 'pv1' not in dataset_ids:
        dataset_ids.append('pv1')
    
    # 去重
    dataset_id_list = list(set(dataset_ids))
    print(f"总共生成了 {len(dataset_id_list)} 个唯一的 dataset_id: {dataset_id_list}")

    # 4. Define alpha source and other variables
    # 此处路径需根据实际情况调整
    alpha_source_path = '/Users/tianyuan/.claude/skills/brain-feature-implementation/data/option8_combined_expressions.json'
    expression_list = sample_alphas_from_file(alpha_source_path, 1)
    
    # 如果需要筛选包含特定字符串的 alpha，可以在此设置 substring
    target_substring = ""
    if target_substring:
        expression_list = [e for e in expression_list if target_substring in e]
        print(f"Filtered to {len(expression_list)} alphas containing '{target_substring}'")

    decay = 10
    neutralization = "REVERSION_AND_MOMENTUM"
    truncation = 0.01
    max_trade = "OFF"

    if not expression_list:
        print(f"No expressions found in {alpha_source_path}. Exiting.")
        return

    print(f"Processing {len(expression_list)} expressions...")

    # 5. Filter and fix expressions
    print("Calling filter_and_fix_expressions...")
    fixed_expressions = filter_and_fix_expressions(
        expression_list=expression_list,
        region=region,
        universe=universe,
        delay=delay,
        dataset_id=dataset_id_list,
        brain_session=brain_session
    )

    print(f"Filtered and fixed {len(fixed_expressions)} expressions.")

    # 6. Save simulatable alphas
    # 使用第一个非 pv1 的类别名称或 dataset_id 作为文件名标识
    filename_tag = categories_to_query[0] if categories_to_query else "multi"
    
    print("Calling save_simulatable_alphas...")
    save_simulatable_alphas(
        expression_list=fixed_expressions,
        region=region,
        universe=universe,
        delay=delay,
        dataset_id=filename_tag,
        decay=decay,
        neutralization=neutralization,
        truncation=truncation,
        max_trade=max_trade
    )
    print("Finished.")

if __name__ == "__main__":
    main()
