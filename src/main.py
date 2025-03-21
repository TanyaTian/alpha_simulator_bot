# main.py
import time
import os
from alpha_simulator import AlphaSimulator
from alpha_simulator import running
from utils import load_config
from logger import Logger  # 导入 Logger 类

# 创建全局 Logger 实例
logger = Logger()

def main():
    """
    主函数，初始化 AlphaSimulator 并管理模拟流程。
    从配置文件读取 username 和 password。
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
    if username is None or password is None:
        logger.error("Failed to load username and password from config. Exiting...")
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
        logger.warning(f"Initialization failed due to missing file: {e}")
        simulator = None
    except Exception as e:
        logger.error(f"Unexpected error during initialization: {e}")
        return

    # 主循环
    while running:
        try:
            if simulator is None or not simulator.session:
                logger.warning("Simulator not initialized or session invalid, attempting to reinitialize...")
                simulator = AlphaSimulator(
                    max_concurrent=max_concurrent,
                    username=username,
                    password=password,
                    batch_number_for_every_queue=batch_number_for_every_queue
                )
                time.sleep(10)
                continue

            input_file_path = simulator.alpha_list_file_path
            if not os.path.exists(input_file_path) or os.path.getsize(input_file_path) == 0:
                logger.warning(f"{input_file_path} is empty or does not exist, waiting for alpha data...")
                time.sleep(60)
                continue

            logger.info("Starting simulation management...")
            simulator.manage_simulations()

        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()