import os
import signal
from alpha_simulator import AlphaSimulator
from process_simulated_alphas import ProcessSimulatedAlphas
from utils import load_config
from logger import Logger 
from signal_manager import SignalManager
from alpha_filter import AlphaFilter


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
    batch_size = config['batch_size']
    init_date = config['init_date_str']
    if not all([username, password, max_concurrent, batch_number_for_every_queue]):
        logger.error("One or more config parameters are missing or invalid. Exiting...")
        return

    # 创建 SignalManager
    signal_manager = SignalManager()
    # 注册信号处理
    signal.signal(signal.SIGTERM, signal_manager.handle_signal)
    signal.signal(signal.SIGINT, signal_manager.handle_signal)
    
    
    # 初始化 AlphaSimulator
    try:
        simulator = AlphaSimulator(
            max_concurrent=max_concurrent,
            username=username,
            password=password,
            batch_number_for_every_queue=batch_number_for_every_queue,
            batch_size=batch_size,
            signal_manager=signal_manager
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
    processor = ProcessSimulatedAlphas(
        data_dir, 
        output_dir, 
        1.58, 1.0, 
        username, 
        password, 
        init_date, 
        signal_manager)          
    processor.manage_process()
    """
    
    # 初始化 AlphaFilter
    alpha_filter = AlphaFilter(username, password, data_dir=data_dir, signal_manager=signal_manager)
    # 启动监控线程
    alpha_filter.start_monitoring(
        interval_minutes=90,
        min_fitness=0.7,
        min_sharpe=1.2,
        corr_threshold=0.75
    )
    """
    # 启动模拟管理
    logger.info("Starting simulation management...") 
    simulator.manage_simulations()
    

if __name__ == "__main__":
    main()