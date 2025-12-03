# 导入必要的库
import asyncio  # 异步I/O操作的核心库
import os
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor

# 导入项目模块
from alpha_simulator import AlphaSimulator
#from process_simulated_alphas import ProcessSimulatedAlphas
from logger import Logger
from signal_manager import SignalManager
from alpha_filter import AlphaFilter
from alpha_poller import AlphaPoller
from config_manager import config_manager
from api.server import run_server
from alpha_calculator import AlphaCalculator
from alpha_checker import AlphaChecker
from sa_simulator import SASimulator

# 创建全局Logger实例
logger = Logger()

# 异步主函数
async def main():
    """
    主函数，现在是异步的。
    它负责初始化所有组件并管理整个应用的生命周期。
    """
    logger.debug("异步main函数开始执行")

    # 使用run_in_executor在独立的线程中运行Flask API服务器，避免阻塞事件循环
    loop = asyncio.get_running_loop()
    api_server_thread = loop.run_in_executor(
        None,  # 使用默认的ThreadPoolExecutor
        lambda: run_server(port=5001)
    )
    logger.info("API 服务已在后台线程启动，地址 http://localhost:5001")

    # 等待一小段时间，确保配置服务已加载初始配置
    await asyncio.sleep(2)

    # 检查配置是否加载成功
    if config_manager._config is None:
        logger.error("加载配置失败，程序退出。")
        return
    
    # 创建并注册信号管理器，用于优雅地关闭程序
    signal_manager = SignalManager()
    signal.signal(signal.SIGTERM, signal_manager.handle_signal)
    signal.signal(signal.SIGINT, signal_manager.handle_signal)
    
    # 创建一个线程池执行器来运行所有同步阻塞的任务
    executor = ThreadPoolExecutor(max_workers=6, thread_name_prefix='SyncWorker')

    # --- 在独立的线程中运行同步阻塞的组件 ---
    

    # --- 启动新的重构模块 ---

    # 3. 初始化并运行 AlphaCalculator
    try:
        logger.info("正在初始化 AlphaCalculator...")
        alpha_calculator = AlphaCalculator(
            loop=loop,
            specified_sharpe=1.58,
            specified_fitness=1,
            corr_threshold=0.8,
            signal_manager=signal_manager
        )
        loop.run_in_executor(executor, alpha_calculator.start)
        logger.info("AlphaCalculator 调度器已在工作线程中启动。")
    except Exception as e:
        logger.error(f"AlphaCalculator 初始化或启动失败: {e}")
        return

    # 4. 初始化并运行 AlphaChecker
    try:
        logger.info("正在初始化 AlphaChecker...")
        alpha_checker = AlphaChecker(loop=loop, signal_manager=signal_manager)
        loop.run_in_executor(executor, alpha_checker.start)
        logger.info("AlphaChecker 调度器已在工作线程中启动。")
    except Exception as e:
        logger.error(f"AlphaChecker 初始化或启动失败: {e}")
        return

    # 5. 初始化并运行 Super Alpha 模拟器
    try:
        logger.info("正在初始化 SASimulator...")
        sa_simulator = SASimulator()
        # 注册停止处理函数，确保优雅关闭
        signal_manager.add_handler(lambda signum, frame: sa_simulator.stop())
        # 使用 batch_size=30 调用 run_simulation_loop
        loop.run_in_executor(
            executor,
            lambda: sa_simulator.run_simulation_loop(batch_size=30, sleep_interval=60)
        )
        logger.info("SASimulator 服务已在工作线程中启动 (batch_size=30)。")
    except Exception as e:
        logger.error(f"SASimulator 初始化或启动失败: {e}")
        return
    
    
    # 2. 初始化并运行AlphaFilter
    alpha_filter = AlphaFilter(signal_manager=signal_manager)
    loop.run_in_executor(
        executor, 
        lambda: alpha_filter.start_monitoring(
            interval_minutes=180,
            min_fitness=0.7,
            min_sharpe=1.2,
            corr_threshold=0.75
        )
    )
    logger.info("AlphaFilter监控已在工作线程中启动。")
    
    # --- 运行异步组件 ---
    # 3. 初始化AlphaPoller并启动异步轮询
    poller = AlphaPoller()
    polling_task = asyncio.create_task(poller.start_polling_async())
    logger.info("异步AlphaPoller已启动。")

    # 4. 初始化并运行AlphaSimulator (移至此处以确保启动)
    try:
        simulator = AlphaSimulator(signal_manager=signal_manager)
        logger.info("AlphaSimulator初始化成功。")
        loop.run_in_executor(executor, simulator.manage_simulations)
        logger.info("AlphaSimulator管理已在工作线程中启动。")
    except Exception as e:
        logger.error(f"AlphaSimulator初始化或启动失败: {e}")
        return # 如果模拟器启动失败，则退出

    # 等待异步的poller任务完成（此任务会无限运行，从而保持主程序存活）
    try:
        await polling_task
    except asyncio.CancelledError:
        logger.info("主任务被取消，开始关闭...")
    finally:
        # 在关闭前给其他线程一些时间来响应信号
        logger.info("主任务结束，正在关闭线程池执行器...")
        await asyncio.sleep(2)
        executor.shutdown(wait=True)
        logger.info("所有工作线程已关闭。")


if __name__ == "__main__":
    try:
        # 运行异步主函数
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("程序被用户或系统中断，正在关闭...")
