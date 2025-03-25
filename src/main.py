import os
import time

import schedule
import threading

from alpha_simulator import AlphaSimulator
from src.process_simulated_alphas import ProcessSimulatedAlphas
from utils import load_config
from logger import Logger  # 假设已定义 Logger 类


# 创建全局 Logger 实例
logger = Logger()

def main():
    """
    主函数，初始化 AlphaSimulator 并启动模拟流程。
    从配置文件读取 username、password、max_concurrent 和 batch_number_for_every_queue。
    """
    # 从配置文件读取凭据
    config = load_config()
    if config is None:
        logger.error("Failed to load configuration. Exiting...")
        return

    username = config['username']
    password = config['password']
    max_concurrent = config['max_concurrent']
    batch_number_for_every_queue = config['batch_number_for_every_queue']
    if not all([username, password, max_concurrent, batch_number_for_every_queue]):
        logger.error("One or more config parameters are missing or invalid. Exiting...")
        return

    # 初始化 AlphaSimulator
    try:
        simulator = AlphaSimulator(
            max_concurrent=max_concurrent,
            username=username,
            password=password,
            batch_number_for_every_queue=batch_number_for_every_queue
        )
        logger.info("AlphaSimulator initialized successfully.")
    except FileNotFoundError as e:
        logger.error(f"Initialization failed due to missing file: {e}")
        return
    except Exception as e:
        logger.error(f"Unexpected error during initialization: {e}")
        return

    # 检查输入文件是否存在
    input_file_path = simulator.alpha_list_file_path
    if not os.path.exists(input_file_path):
        logger.error(f"{input_file_path} does not exist. Exiting...")
        return

    # 构建基础路径
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    data_dir = os.path.join(project_root, 'data')
    output_dir = os.path.join(project_root, 'output')
    # 实例化 ProcessSimulatedAlphas 并启动调度
    processor = ProcessSimulatedAlphas(data_dir, output_dir, 1.25, 1.0, username, password)
    processor.start_schedule()

    # 启动模拟管理
    logger.info("Starting simulation management...")
    simulator.manage_simulations()

if __name__ == "__main__":
    main()