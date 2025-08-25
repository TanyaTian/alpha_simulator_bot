import configparser
import os
import csv
import pandas as pd
import ast
from datetime import datetime
from pytz import timezone
from logger import Logger  # 导入 Logger 类

# 创建全局 Logger 实例
logger = Logger()

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
            settings = ast.literal_eval(row['settings'])
            decay = settings.get('decay', 0)
            
            # Parse the regular code (expression)
            regular = ast.literal_eval(row['regular'])
            exp = fix_newline_expression(regular.get('code', ''))
            operatorCount = regular.get('operatorCount', 0)
            
            # Parse the is dictionary
            is_data = ast.literal_eval(row['is'])
            sharpe = is_data.get('sharpe', 0)
            fitness = is_data.get('fitness', 0)
            turnover = is_data.get('turnover', 0)
            margin = is_data.get('margin', 0)
            longCount = is_data.get('longCount', 0)
            shortCount = is_data.get('shortCount', 0)
            checks = is_data.get('checks', [])
            
            dateCreated = row['dateCreated']
            has_failed_checks = any(check['result'] == 'FAIL' for check in checks)
            # Apply region filter if specified
            if region_filter is not None and settings.get('region') != region_filter:
                continue
                
            # Apply single data set filter if specified
            if single_data_set_filter is not None:
                classifications = ast.literal_eval(row.get('classifications', '[]'))
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
