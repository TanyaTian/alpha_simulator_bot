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
    从配置文件读取 username 和 password。

    Args:
        config_file (str): 配置文件路径，默认为 "config/config.ini"。

    Returns:
        tuple: (username, password) 或 (None, None) 如果读取失败。
    """
    # 获取项目根目录
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    config_path = os.path.join(project_root, config_file)

    # 初始化配置解析器
    config = configparser.ConfigParser()

    try:
        # 检查文件是否存在
        if not os.path.exists(config_path):
            logger.error(f"Config file {config_path} not found.")
            return None, None

        # 读取配置文件
        config.read(config_path)

        # 从 [Credentials] 部分读取 username 和 password
        username = config.get('Credentials', 'username')
        password = config.get('Credentials', 'password')

        logger.info(f"Successfully loaded config from {config_path}")
        return username, password

    except configparser.NoSectionError:
        logger.error(f"Section 'Credentials' not found in {config_path}")
        return None, None
    except configparser.NoOptionError as e:
        logger.error(f"Missing key in config: {e}")
        return None, None
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        return None, None


# 示例调用
if __name__ == "__main__":
    username, password = load_config()
    print(f"Username: {username}, Password: {password}")