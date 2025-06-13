import configparser
import os
import csv
import pandas as pd
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

        config_data = {
            'username': config.get('Credentials', 'username'),
            'password': config.get('Credentials', 'password'),
            'max_concurrent': config.getint('Credentials', 'max_concurrent'),
            'batch_number_for_every_queue': config.getint('Credentials', 'batch_number_for_every_queue'),
            'batch_size': config.getint('Credentials', 'batch_size'),
            'init_date_str': config.get('Credentials', 'init_date_str')
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
