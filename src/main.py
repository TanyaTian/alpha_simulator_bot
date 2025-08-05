import os
import signal
import threading
import time
from alpha_simulator import AlphaSimulator
from process_simulated_alphas import ProcessSimulatedAlphas
from utils import load_config
from logger import Logger 
from signal_manager import SignalManager
from alpha_filter import AlphaFilter
from alpha_poller import AlphaPoller
from config_manager import config_manager, run_config_server


# 创建全局 Logger 实例
logger = Logger()

def main():
    """
    主函数，初始化 AlphaSimulator 并启动模拟流程。
    从配置文件读取 username、password、max_concurrent 和 batch_number_for_every_queue。
    """
    logger.debug("main方法开始执行")

    # 🔥 启动配置中心服务（后台线程）
    config_server_thread = threading.Thread(
        target=run_config_server,
        kwargs={'port': 5001},
        daemon=True
    )
    config_server_thread.start()

    time.sleep(1)
    logger.info("Config center server started on http://localhost:5001")

    # 从配置文件读取凭据
    config = config_manager._config
    if config is None:
        logger.error("Failed to load configuration. Exiting...")
        return
    username = config['username']
    password = config['password']
    max_concurrent = config['max_concurrent']
    batch_number_for_every_queue = config['batch_number_for_every_queue']
    batch_size = config['batch_size']
    init_date = config['init_date_str']
    if not all([username, password, max_concurrent, batch_number_for_every_queue, batch_size, init_date]):
        logger.error("One or more config parameters are missing or invalid. Exiting...")
        return
    
    # 创建 SignalManager
    signal_manager = SignalManager()
    # 注册信号处理
    signal.signal(signal.SIGTERM, signal_manager.handle_signal)
    signal.signal(signal.SIGINT, signal_manager.handle_signal)
    

    # 初始化 AlphaSimulator
    try:
        # 直接使用ConfigManager的类变量访问配置
        simulator = AlphaSimulator(signal_manager=signal_manager)
        logger.info("AlphaSimulator initialized successfully.")
    except KeyError as e:
        logger.error(f"Missing configuration parameter: {e}")
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
    
    
    # 初始化 AlphaFilter
    alpha_filter = AlphaFilter(username, password, data_dir=data_dir, signal_manager=signal_manager)
    # 启动监控线程
    alpha_filter.start_monitoring(
        interval_minutes=90,
        min_fitness=0.7,
        min_sharpe=1.2,
        corr_threshold=0.75
    )
    
    
    # 使用ConfigManager中的配置启动Poller（无需传参）
    poller = AlphaPoller()
    poller.start_polling()  # 启动轮询线程

    # 启动模拟管理
    logger.info("Starting simulation management...") 
    simulator.manage_simulations()
    

if __name__ == "__main__":
    main()
