import queue
import time
import csv
import requests
import os
import ast
import json
import threading
from collections import deque
from pytz import timezone
from logger import Logger  
from datetime import datetime, timedelta
from signal_manager import SignalManager
from dao import SimulationTasksDAO  
from dao import AlphaListPendingSimulatedDAO
from config_manager import config_manager

class AlphaSimulator:
    """Alpha模拟器类，用于管理量化策略的模拟过程"""

    TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"
    FILE_CONFIG = {
        "input_file": "alpha_list_pending_simulated.csv",
        "fail_file": "fail_alphas.csv",
        "output_file": "simulated_alphas.csv",  # 基础文件名
        "state_file": "simulator_state.json"
    }

    def __init__(self, signal_manager=None):
        """初始化模拟器

        Args:
            max_concurrent (int): 最大并发模拟数量
            username (str): 登录用户名
            password (str): 登录密码
            batch_number_for_every_queue (int): 每批处理数量
        """
        self.running = True

        # 创建 Logger 实例
        self.logger = Logger()

        # 注册信号处理
        if signal_manager:
            signal_manager.add_handler(self.signal_handler)
        else:
            self.logger.warning("未提供 SignalManager,AlphaSimulator 无法注册信号处理函数")

        # 注册配置观察者
        self._config_observer_handle = config_manager.on_config_change(self._handle_config_change)
        
        # 从配置中心获取参数
        self._load_config_from_manager()
        
        # 构建基础路径
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)
        self.data_dir = os.path.join(project_root, 'data')

        # 自动创建data目录（如果不存在）
        os.makedirs(self.data_dir, exist_ok=True)

        # 构建文件路径体系
        self.state_file = os.path.join(self.data_dir, self.FILE_CONFIG["state_file"])

        # 创建 Logger 实例
        self.logger = Logger()

        # 注册信号处理
        if signal_manager:
            signal_manager.add_handler(self.signal_handler)
        else:
            self.logger.warning("未提供 SignalManager,AlphaSimulator 无法注册信号处理函数")

        # 初始化DAO
        self.alpha_list_pending_simulated_dao = AlphaListPendingSimulatedDAO()
        self.simulation_task_dao = SimulationTasksDAO()
        
        # 初始化任务队列
        self.active_simulations = []
        self.active_update_time = time.time()
        self.lock = threading.Lock()  # 🔒 文件写入锁

        # 加载上次未完成的 active_simulations
        self._load_previous_state()

    def signal_handler(self, signum, frame):
        self.logger.info(f"Received shutdown signal {signum}, , initiating shutdown...")
        self.running = False
        self.save_state()

    def _load_previous_state(self):
        if os.path.exists(self.state_file):
            with open(self.state_file, 'r') as f:
                state = json.load(f)
                self.active_simulations = state.get("active_simulations", [])
                self.logger.info(f"Loaded {len(self.active_simulations)} previous active simulations from state.")


    def _load_config_from_manager(self):
        """从配置中心加载运行时参数"""
        config = config_manager._config  # 直接访问内部配置
        
        # 加载核心参数（带默认值）
        self.max_concurrent = config.get('max_concurrent', 5)
        self.batch_number_for_every_queue = config.get('batch_number_for_every_queue', 100)
        self.batch_size = config.get('batch_size', 10)
        
        # 加载region_set参数，使用collections.deque实现循环队列
        region_set = config.get('region_set', ['US'])
        if not isinstance(region_set, list):
            region_set = ['US']
        self.region_set = deque(region_set)
        
        # 使用配置中心的session
        self.session = config_manager.get_session()
        
        self.logger.info(f"Loaded config: max_concurrent={self.max_concurrent}, "
                        f"batch_number_for_every_queue={self.batch_number_for_every_queue}, "
                        f"batch_size={self.batch_size}")
    
    def _handle_config_change(self, new_config):
        """配置变更回调处理"""
        self._load_config_from_manager()
        self.logger.info("Configuration reloaded due to config center update")
    
    def __del__(self):
        """析构函数清理观察者注册"""
        if hasattr(self, '_config_observer_handle'):
            observer_list = config_manager._observers
            if self._handle_config_change in observer_list:
                observer_list.remove(self._handle_config_change)


    def simulate_alpha(self, alpha_list):
        """
        模拟一组 alpha 表达式，通过 API 提交批量模拟请求（同步版本）。

        Args:
            alpha_list (list): 包含多个 alpha 数据的列表，每个 alpha 是一个字典，包含 type、settings 和 regular 字段
                - type: 字符串，模拟类型，例如 "REGULAR"
                - settings: 字典，模拟设置，例如 {'instrumentType': 'EQUITY', ...}
                - regular: 字符串，alpha 表达式，例如 "ts_quantile(winsorize(...), 22)"

        Returns:
            str or None: 模拟进度 URL（location_url），如果失败则返回 None
        """
        # 将 alpha_list 转换为 sim_data_list，用于 API 请求
        sim_data_list = self.generate_sim_data(alpha_list)

        # 如果 sim_data_list 为空（例如 alpha_list 中所有 alpha 都无效），记录错误并返回
        if not sim_data_list:
            self.logger.error("No valid simulation data generated from alpha_list")
            # 将整个 alpha_list 写入失败文件
            with open(self.fail_alphas, 'a', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=alpha_list[0].keys())
                for alpha in alpha_list:
                    writer.writerow(alpha)
            return None

        # 初始化重试计数器
        count = 0

        # 重试循环，最多尝试 35 次
        while True:
            try:
                # 使用 self.session 发送 POST 请求到 WorldQuant Brain 平台的 API
                response = self.session.post('https://api.worldquantbrain.com/simulations', json=sim_data_list)

                # 检查 HTTP 状态码，如果失败（例如 4xx 或 5xx），抛出异常
                response.raise_for_status()

                # 检查响应头中是否包含 Location 字段
                if "Location" in response.headers:
                    # 如果成功获取 Location，记录日志并返回
                    self.logger.info("Alpha batch location retrieved successfully.")
                    self.logger.info(f"Location: {response.headers['Location']}")
                    return response.headers['Location']

            except requests.exceptions.RequestException as e:
                # 捕获 HTTP 请求相关的异常（例如网络错误、服务器错误）
                self.logger.error(f"Error in sending simulation request: {e}")

                # 如果重试次数超过 35 次，重新登录并跳出循环
                if count > 35:
                    # 调用 sign_in 方法重新登录
                    
                    self.session = config_manager.get_session()
                    self.logger.error("Error occurred too many times, skipping this alpha batch and re-logging in.")
                    break

                # 记录重试信息，等待 5 秒后继续
                self.logger.error("Error in sending simulation request. Retrying after 5s...")
                time.sleep(5)
                count += 1

        # 如果请求失败（重试次数耗尽），记录错误
        self.logger.error(f"Simulation request failed after {count} attempts for alpha batch")
        return None
    
    def generate_sim_data(self, alpha_list):
        """
        将 alpha_list 转换为 sim_data_list，用于批量模拟。

        Args:
            alpha_list (list): 包含多个 alpha 字典的列表，每个字典包含 type、settings 和 regular 字段
                - type: 字符串，模拟类型，例如 "REGULAR"
                - settings: 字典，模拟设置，例如 {'instrumentType': 'EQUITY', ...}
                - regular: 字符串，alpha 表达式，例如 "ts_quantile(winsorize(...), 22)"

        Returns:
            list: 包含多个 simulation_data 字典的列表，格式符合 API 要求
        """
        sim_data_list = []

        for alpha in alpha_list:
            # 确保 alpha 包含必要的字段
            if not all(key in alpha for key in ['type', 'settings', 'regular']):
                self.logger.error(f"Invalid alpha data, missing required fields: {alpha}")
                continue

            # 直接使用 settings 字段（数据库返回的是字典）
            settings = alpha['settings']
            if not isinstance(settings, dict):
                self.logger.error(f"Unexpected type for settings: {type(settings)}. Expected dict. Skipping this alpha.")
                continue

            # 构造 simulation_data 字典
            simulation_data = {
                'type': alpha['type'],
                'settings': settings,
                'regular': alpha['regular']
            }

            sim_data_list.append(simulation_data)

        self.logger.info(f"Generated sim_data_list with {len(sim_data_list)} entries")
        return sim_data_list
        
    def load_new_alpha_and_simulate(self):
        """
        从数据库批量查询待回测alpha，然后进行模拟
        
        使用新方法fetch_pending_alphas_in_batches从数据库批量获取待回测alpha
        每次查询的数量为self.batch_size，region为region_set的第一个元素
        调用后轮转region_set，将当前region移到最后一个

        Attributes:
            self.batch_size (int): 每次查询的数量
            self.region_set (deque): 地区循环队列
            self.dao (AlphaListPendingSimulatedDAO): 数据访问对象
            self.max_concurrent (int): 最大并发模拟数量
            self.active_simulations (list): 正在进行的模拟任务列表
            self.logger (Logger): 日志记录器
        """
        # 检查运行状态，如果 running 为 False，则退出
        if not self.running:
            return

        # 如果当前正在进行的模拟任务数量达到最大并发限制，等待 2 秒后退出
        if len(self.active_simulations) >= self.max_concurrent:
            self.logger.info(f"Max concurrent simulations reached ({self.max_concurrent}). Waiting 2 seconds")
            time.sleep(2)
            return

        self.logger.info('Loading new alphas for simulation from database...')
        try:
            # 使用新方法从数据库批量获取待回测alpha
            db_records = self.fetch_pending_alphas_in_batches(self.batch_size)
            
            # 如果没有获取到alpha，直接返回
            if not db_records:
                self.logger.info("No pending alphas fetched from database.")
                return

            # 提取需要模拟的数据
            alpha_list = []
            record_ids = []
            for record in db_records:
                try:
                    # 尝试类型转换settings字段
                    if 'settings' in record:
                        if isinstance(record['settings'], str):
                            try:
                                record['settings'] = ast.literal_eval(record['settings'])
                            except (ValueError, SyntaxError) as e:
                                # 转换失败时记录错误并跳过此记录
                                self.logger.error(f"类型转换失败 (record id={record['id']}): {e}")
                                continue
                        elif not isinstance(record['settings'], dict):
                            self.logger.error(f"settings字段类型错误 (record id={record['id']}): 期望字典类型, 实际是 {type(record['settings'])}")
                            continue
                    
                    # 构造alpha对象
                    alpha = {
                        'type': record['type'],
                        'settings': record['settings'],
                        'regular': record['regular']
                    }
                    alpha_list.append(alpha)
                    record_ids.append(record['id'])
                    
                    # 记录当前批次的 alpha 信息
                    self.logger.info(f"  - ID: {record['id']}, Alpha: {record['regular'][:50]}... with settings: {record['settings']}")
                except Exception as e:
                    self.logger.error(f"处理record时发生错误 (id={record['id']}): {e}")
                    continue

            # 调用 simulate_alpha，传入 alpha 列表
            location_url = self.simulate_alpha(alpha_list)

            # 如果模拟成功（返回 location_url），将 URL 添加到 active_simulations
            if location_url:
                self.active_simulations.append(location_url)
                self.active_update_time = time.time()
                self.logger.info(f"Simulation started, location_url: {location_url}")
                # 更新数据库状态为成功
                self.alpha_list_pending_simulated_dao.batch_update_status_by_ids(record_ids, 'sent')
            else:
                self.logger.warning("Simulation failed, no location_url returned")
                # 更新数据库状态为失败
                self.alpha_list_pending_simulated_dao.batch_update_status_by_ids(record_ids, 'failed')

        except Exception as e:
            # 捕获其他异常，记录错误信息
            self.logger.error(f"Error during simulation: {e}")
            # 尝试更新数据库状态为失败
            if 'record_ids' in locals() and record_ids:
                try:
                    self.alpha_list_pending_simulated_dao.batch_update_status_by_ids(record_ids, 'failed')
                except Exception as db_error:
                    self.logger.error(f"Failed to update database status: {db_error}")
    
    def check_simulation_progress(self, simulation_progress_url):
        """
        检查批量模拟的进度，获取 children 列表。

        Args:
            simulation_progress_url (str): 批量模拟的进度 URL，例如 "https://api.worldquantbrain.com/simulations/12345"

        Returns:
            list or None: 如果成功返回 children 列表，否则返回 None
        """
        try:
            simulation_progress = self.session.get(simulation_progress_url)
            simulation_progress.raise_for_status()

            # 检查是否包含 Retry-After 头，表示服务端暂时不可用
            if simulation_progress.headers.get("Retry-After", 0) != 0:
                return None

            # 解析响应，提取 children 和状态
            progress_data = simulation_progress.json()
            children = progress_data.get("children", [])
            if not children:
                self.logger.error("No children found in simulation progress response.")
                return None

            status = progress_data.get("status")
            self.logger.info(f"Simulation batch status: {status}, children count: {len(children)}")

            if status != "COMPLETE":
                self.logger.info("Simulation not complete. Will check again later.")

            # 插入所有child到simulation_tasks_table
            if children:
                data_list = [
                    {
                        'child_id': child,
                        'submit_time': datetime.now(),  # 使用当前时间作为提交时间
                        'status': 'pending',  # 默认状态
                        'query_attempts': 0,  # 初始查询次数为0
                        'last_query_time': None  # 初始无查询时间
                    }
                    for child in children
                ]
                self.simulation_task_dao.batch_insert(data_list)  # 批量插入
            return children

        except requests.exceptions.HTTPError as e:
            remove_status_codes = {400, 403, 404, 410}
            if e.response.status_code in remove_status_codes:
                self.logger.error(f"Simulation request failed with status {e.response.status_code}: {e}")
                # Remove the simulation_progress_url from active_simulations
                if hasattr(self, 'active_simulations') and simulation_progress_url in self.active_simulations:
                    self.active_simulations.remove(simulation_progress_url)
                    self.logger.info(f"Removed {simulation_progress_url} from active_simulations due to status code {e.response.status_code}")
                return None
            else:
                self.logger.error(f"Failed to fetch simulation progress: {e}")
                
                self.session = config_manager.get_session()
                return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to fetch simulation progress: {e}")
            
            self.session = config_manager.get_session()
            return None

    def check_simulation_status(self):
        """检查所有活跃模拟的状态"""
        current_time = time.time()
        time_diff = current_time - self.active_update_time
        if time_diff > 3600 and len(self.active_simulations) > 0:
            self.logger.warning(f"active_update_time exceeds 1 hour (diff: {time_diff:.2f} seconds). Resetting active simulations.")
            
            self.session = config_manager.get_session()
            self.active_simulations.clear()
            self.logger.info("Active simulations cleared after re-signing in.")

        if not self.active_simulations:
            self.logger.info("No active simulations to check.")
            return

        self.logger.info(f"Checking {len(self.active_simulations)} active simulations...")
        count = 0
        for sim_url in self.active_simulations[:]:
            children = self.check_simulation_progress(sim_url)
            if children is None:
                count += 1
                self.logger.debug(f"Simulation {sim_url} still in progress or failed.")
                continue

            self.logger.info(f"Simulation batch {sim_url} completed. Removing from active list.")
            self.active_simulations.remove(sim_url)
            self.active_update_time = time.time()

        self.logger.info(f"Total {count} simulations still in progress for account {config_manager._config['username']}.")

    def save_state(self):
        """保存当前状态"""
        state = {
            "active_simulations": self.active_simulations,
            "timestamp": datetime.now().strftime(self.TIMESTAMP_FORMAT)
        }
        with open(self.state_file, 'w') as f:
            json.dump(state, f)
        self.logger.info(f"Saved {len(self.active_simulations)} active simulations to {self.state_file}")

    def fetch_pending_alphas_in_batches(self, batch_size):
        """从数据库批量查询待回测alpha
        
        Args:
            batch_size (int): 每次查询的数量
            
        Returns:
            list: 待回测的alpha列表
        """
        if not self.region_set:
            self.logger.warning("No regions available in region_set")
            return []
            
        # 获取当前region（第一个元素）
        region = self.region_set[0]
        self.logger.info(f"Querying pending alphas for region: {region}, batch_size: {batch_size}")
        
        # 使用DAO查询数据库
        alphas = self.alpha_list_pending_simulated_dao.fetch_and_lock_pending_by_region(
            region = region,
            limit = batch_size
        )
        
        # 轮转region_set：将当前region移到最后一个
        current_region = self.region_set.popleft()
        self.region_set.append(current_region)
        self.logger.info(f"Rotated region_set: moved {current_region} to the end")
        
        return alphas

    def manage_simulations(self):
        """管理整个模拟过程"""
        if not self.session:
            self.logger.error("Failed to sign in. Exiting...")
            return

        try:
            while self.running:
                self.check_simulation_status()
                self.load_new_alpha_and_simulate()
                time.sleep(3)
        except KeyboardInterrupt:
            self.logger.info("Manual interruption detected.")
