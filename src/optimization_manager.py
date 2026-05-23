import threading
import queue
import time
from logger import Logger
from alpha_optimization_workflow import run_optimization_workflow

# 获取专门的优化日志记录器
logger = Logger.get_logger("optimization")

class OptimizationManager:
    """
    Alpha 优化任务管理器，采用单例模式。
    负责维护任务队列，并开启后台工作线程持续处理优化任务。
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(OptimizationManager, cls).__new__(cls)
                cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self.task_queue = queue.Queue() # 任务等待队列
        self.current_task = None        # 当前正在处理的任务 ID
        self.is_running = False         # 是否正在运行优化
        self.status_info = "Idle"       # 详细状态信息描述
        self.current_iteration = 0      # 当前迭代轮数
        self.simulated_count = 0        # 已模拟的 Alpha 数量
        self._initialized = True
        self._start_worker()            # 初始化时自动开启工作线程

    def _start_worker(self):
        """启动后台工作线程"""
        self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.worker_thread.start()
        logger.info("Alpha 优化后台工作线程已启动。")

    def _worker_loop(self):
        """
        后台消费循环：
        不断从队列中提取 Alpha ID，调用优化工作流进行处理。
        """
        while True:
            try:
                # 阻塞式获取任务，超时 1 秒以便能响应系统退出
                task = self.task_queue.get(timeout=1)
                self.current_task = task
                self.is_running = True
                self.status_info = f"Optimizing Alpha: {task} (Initializing...)"
                self.current_iteration = 0
                self.simulated_count = 0
                logger.info(f"开始执行优化任务: {task}")
                
                try:
                    # 定义状态更新回调函数
                    def update_status(msg, iteration=None, simulated_count=None):
                        self.status_info = f"Optimizing Alpha: {task} ({msg})"
                        if iteration is not None:
                            self.current_iteration = iteration
                        if simulated_count is not None:
                            self.simulated_count = simulated_count
                        logger.debug(f"任务 {task} 进度更新: {msg}, iteration: {iteration}, simulated_count: {simulated_count}")

                    # 调用 LangGraph 构建的核心优化工作流
                    # 传入 status_callback 用于细粒度进度更新
                    result = run_optimization_workflow(task, status_callback=update_status)
                    logger.info(f"Alpha {task} 优化流程执行完毕。")
                except Exception as e:
                    logger.error(f"优化 Alpha {task} 时发生未捕获异常: {e}")
                
                # 重置状态，标记任务完成
                self.current_task = None
                self.is_running = False
                self.status_info = "Idle"
                self.current_iteration = 0
                self.simulated_count = 0
                self.task_queue.task_done()
            except queue.Empty:
                continue

    def add_tasks(self, alpha_ids):
        """
        向队列中添加一个或多个优化任务。
        
        参数:
            alpha_ids: 单个 ID 字符串或 ID 列表。
        """
        if isinstance(alpha_ids, str):
            alpha_ids = [alpha_ids]
        for aid in alpha_ids:
            self.task_queue.put(aid)
            logger.info(f"已将 Alpha {aid} 添加至优化等待队列。")

    def clear_tasks(self):
        """
        清空当前等待队列中的所有任务。
        返回被移除的任务数量。
        """
        removed_count = 0
        while not self.task_queue.empty():
            try:
                self.task_queue.get_nowait()
                self.task_queue.task_done()
                removed_count += 1
            except queue.Empty:
                break
        logger.info(f"已清空优化任务队列，共移除 {removed_count} 个任务。")
        self.status_info = f"Queue cleared (removed {removed_count} tasks)"
        return removed_count

    def get_status(self):
        """
        获取当前管理器的运行状态，供 API 查询。
        """
        # 获取队列中所有待处理的 Alpha ID (非消耗性)
        # 注意：这里使用了内部变量，但在标准库 Queue 中是相对稳定的获取方式
        pending_tasks = list(self.task_queue.queue)
        
        return {
            "status": "Running" if self.is_running else "Idle",
            "current_alpha": self.current_task,
            "pending_tasks": pending_tasks,
            "queue_length": len(pending_tasks),
            "current_iteration": self.current_iteration,
            "simulated_count": self.simulated_count,
            "info": self.status_info
        }

# 实例化单例
optimization_manager = OptimizationManager()
