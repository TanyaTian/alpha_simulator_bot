import queue
import time
import csv
import requests
import os
import ast
import json
import threading
import heapq
import random
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Union
from collections import deque
from pytz import timezone
from logger import Logger  
from datetime import datetime, timedelta
from signal_manager import SignalManager
from dao import SimulationTasksDAO  
from dao import AlphaListPendingSimulatedDAO
from config_manager import config_manager
from cache_manager import CacheManager

@dataclass
class PendingSimulation:
    """
    表示一个待检查的模拟任务
    使用最小堆按 next_check_time 排序
    """
    next_check_time: float
    location_url: str
    retry_count: int
    record_ids: List[int]  # 关联的数据库 record id
    backoff_factor: int = 128  # 指数退避因子
    min_delay: int = 3     # 最小延迟 3s

    def __lt__(self, other):
        return self.next_check_time < other.next_check_time

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


        # 初始化DAO
        self.alpha_list_pending_simulated_dao = AlphaListPendingSimulatedDAO()
        self.simulation_task_dao = SimulationTasksDAO()

        # 初始化缓存管理器
        self.cache_manager = CacheManager(
            alpha_dao=self.alpha_list_pending_simulated_dao,
            task_dao=self.simulation_task_dao,
            batch_number_for_every_queue=self.batch_number_for_every_queue
        )
        self.FLUSH_THRESHOLD = self.batch_number_for_every_queue
        
        # 初始化任务队列和映射字典
        self.simulation_heap: List[PendingSimulation] = []  # 优先队列
        self.active_simulations_dict = {}  # 存储location_url到record_ids的映射
        self.active_update_time = time.time()

        # 加载上次未完成的 active_simulations
        self._load_previous_state()

    def signal_handler(self, signum, frame):
        self.logger.info(f"Received shutdown signal {signum}, initiating shutdown...")
        self.running = False
        self.logger.info("Flushing remaining data before exit...")
        self.cache_manager.flush()
        self.save_state()

    def _load_previous_state(self):
        if os.path.exists(self.state_file):
            with open(self.state_file, 'r') as f:
                state = json.load(f)
                urls = state.get("active_simulations", [])
                for url in urls:
                    # 初始 retry_count=0，首次检查延迟 5 秒
                    heapq.heappush(self.simulation_heap, PendingSimulation(
                        next_check_time=time.time() + 5,
                        location_url=url,
                        retry_count=0,
                        record_ids=[]  # 之前的record_ids信息丢失，设置为空
                    ))
                self.logger.info(f"Loaded {len(urls)} previous simulations into heap.")


    def _load_config_from_manager(self):
        """从配置中心加载运行时参数"""
        config = config_manager._config  # 直接访问内部配置
        
        # 加载核心参数（带默认值）
        self.max_concurrent = config.get('max_concurrent', 5)
        self.batch_number_for_every_queue = config.get('batch_number_for_every_queue', 100)
        self.batch_size = config.get('batch_size', 10)
        
        # 加载region_set参数，使用collections.deque实现循环队列
        region_set = config.get('region_set', ['USA'])
        if not isinstance(region_set, list):
            region_set = ['USA']
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
        """析构函数，确保清理和数据刷新"""
        # 清理配置观察者
        if hasattr(self, '_config_observer_handle'):
            try:
                observer_list = config_manager._observers
                if self._handle_config_change in observer_list:
                    observer_list.remove(self._handle_config_change)
            except Exception as e:
                self.logger.error(f"Error removing config observer: {e}")

        # 确保所有挂起的更改都被写入数据库
        if hasattr(self, 'cache_manager') and self.cache_manager.get_dirty_items_count() > 0:
            self.logger.info("Flushing remaining data in destructor...")
            try:
                self.cache_manager.flush()
                self.logger.info("Destructor flush completed.")
            except Exception as e:
                self.logger.error(f"Error flushing data in destructor: {e}")


    def simulate_alpha(self, alpha_list):
        """
        模拟一组 alpha 表达式，通过 API 提交批量模拟请求（同步版本）。
        """
        start_time = time.time()
        backoff = 5 # 初始等待时间
        sim_data_list = self.generate_sim_data(alpha_list)

        if not sim_data_list:
            self.logger.error("No valid simulation data generated from alpha_list")
            return None

        count = 0
        while True:
            try:
                response = self.session.post('https://api.worldquantbrain.com/simulations', json=sim_data_list)
                response.raise_for_status()

                if "Location" in response.headers:
                    duration = time.time() - start_time
                    self.logger.info(f"[Performance] action=simulate_alpha_batch duration={duration:.4f}s batch_size={len(alpha_list)}")
                    self.logger.info("Alpha batch location retrieved successfully.")
                    self.logger.info(f"Location: {response.headers['Location']}")
                    return response.headers['Location']

            except requests.exceptions.RequestException as e:
                self.logger.error(f"Error in sending simulation request: {e}")
                if count > 10:
                    self.session = config_manager.get_session()
                    self.logger.error("Error occurred too many times, skipping this alpha batch and re-logging in.")
                    break
                
                sleep_time = min(backoff + (5 * count), 300)
                sleep_time += random.uniform(0, 3)
                self.logger.error(f"Error in sending simulation request. Retrying after {sleep_time}s...")
                time.sleep(sleep_time)
                count += 1

        duration = time.time() - start_time
        self.logger.error(f"[Performance] action=simulate_alpha_batch status=failed duration={duration:.4f}s batch_size={len(alpha_list)}")
        self.logger.error(f"Simulation request failed after {count} attempts for alpha batch")
        return None
    
    def generate_sim_data(self, alpha_list):
        """
        将 alpha_list 转换为 sim_data_list，用于批量模拟。
        """
        sim_data_list = []
        for alpha in alpha_list:
            if not all(key in alpha for key in ['type', 'settings', 'regular']):
                self.logger.error(f"Invalid alpha data, missing required fields: {alpha}")
                continue

            settings = alpha['settings']
            if not isinstance(settings, dict):
                self.logger.error(f"Unexpected type for settings: {type(settings)}. Expected dict. Skipping this alpha.")
                continue

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
        从缓存批量查询待回测alpha，然后进行模拟。
        """
        if not self.running:
            return

        current_active = len(self.simulation_heap)
        available_slots = self.max_concurrent - current_active

        if available_slots <= 0:
            self.logger.debug(f"Slots full: {current_active}/{self.max_concurrent}")
            return

        self.logger.info(f"Slots available: {available_slots}, trying to fill...")

        for _ in range(available_slots):
            if not self.region_set:
                self.logger.warning("No regions available in region_set")
                continue

            try:
                region = self.region_set[0]
                current_region = self.region_set.popleft()
                self.region_set.append(current_region)
                
                self.logger.info(f"Querying pending alphas for region: {region} from cache, batch_size: {self.batch_size}")
                
                load_cache_start_time = time.time()
                db_records = self.cache_manager.get_pending_alphas_by_region(region, self.batch_size)
                load_cache_duration = time.time() - load_cache_start_time
                self.logger.info(f"[Performance] action=load_alphas_from_cache duration={load_cache_duration:.4f}s region={region} count={len(db_records)}")

                if not db_records:
                    self.logger.info(f"No pending alphas fetched from cache for region: {region}.")
                    continue

                alpha_list = []
                record_ids = []
                for record in db_records:
                    try:
                        if 'settings' in record:
                            if isinstance(record['settings'], str):
                                try:
                                    record['settings'] = ast.literal_eval(record['settings'])
                                except (ValueError, SyntaxError) as e:
                                    self.logger.error(f"类型转换失败 (record id={record['id']}): {e}")
                                    continue
                            elif not isinstance(record['settings'], dict):
                                self.logger.error(f"settings字段类型错误 (record id={record['id']}): 期望字典类型, 实际是 {type(record['settings'])}")
                                continue
                        
                        alpha = {
                            'type': record['type'],
                            'settings': record['settings'],
                            'regular': record['regular']
                        }
                        alpha_list.append(alpha)
                        record_ids.append(record['id'])
                        self.logger.info(f"  - ID: {record['id']}, Alpha: {record['regular'][:50]}... with settings: {record['settings']}")
                    except Exception as e:
                        self.logger.error(f"处理record时发生错误 (id={record['id']}): {e}")
                        continue

                if not alpha_list:
                    self.logger.warning("No valid alphas to simulate in this batch.")
                    continue

                location_url = self.simulate_alpha(alpha_list)

                if location_url:
                    insert_heap_start_time = time.time()
                    heapq.heappush(self.simulation_heap, PendingSimulation(
                        next_check_time=time.time() + 10,
                        location_url=location_url,
                        retry_count=0,
                        record_ids=record_ids
                    ))
                    insert_heap_duration = time.time() - insert_heap_start_time
                    self.logger.info(f"[Performance] action=insert_to_heap duration={insert_heap_duration:.4f}s")

                    self.active_simulations_dict[location_url] = record_ids
                    self.active_update_time = time.time()
                    self.logger.info(f"Simulation started, location_url: {location_url}")
                    
                    self.cache_manager.update_alpha_status(record_ids, 'sent')
                else:
                    self.logger.warning("Simulation failed, no location_url returned")
                    self.cache_manager.update_alpha_status(record_ids, 'failed')
                
                if self.cache_manager.get_dirty_items_count() >= self.FLUSH_THRESHOLD:
                    self.logger.info(f"Dirty items count ({self.cache_manager.get_dirty_items_count()}) reached threshold ({self.FLUSH_THRESHOLD}). Flushing...")
                    flush_start_time = time.time()
                    self.cache_manager.flush()
                    flush_duration = time.time() - flush_start_time
                    self.logger.info(f"[Performance] action=flush_cache_on_threshold duration={flush_duration:.4f}s")

            except Exception as e:
                self.logger.error(f"Error during simulation: {e}")
                if 'record_ids' in locals() and record_ids:
                    try:
                        self.cache_manager.update_alpha_status(record_ids, 'failed')
                        if self.cache_manager.get_dirty_items_count() >= self.FLUSH_THRESHOLD:
                            self.logger.info(f"Dirty items count ({self.cache_manager.get_dirty_items_count()}) reached threshold ({self.FLUSH_THRESHOLD}) on error. Flushing...")
                            flush_err_start_time = time.time()
                            self.cache_manager.flush()
                            flush_err_duration = time.time() - flush_err_start_time
                            self.logger.info(f"[Performance] action=flush_cache_on_error duration={flush_err_duration:.4f}s")
                    except Exception as cache_error:
                        self.logger.error(f"Failed to update cache status on error: {cache_error}")
    
    def check_simulation_progress(self, simulation_progress_url)-> Union[List, None, bool]:
        """
        检查批量模拟的进度，获取 children 列表。
        """
        try:
            simulation_progress = self.session.get(simulation_progress_url)
            simulation_progress.raise_for_status()

            if simulation_progress.headers.get("Retry-After", 0) != 0:
                return None

            progress_data = simulation_progress.json()
            children = progress_data.get("children", [])
            if not children:
                self.logger.error("No children found in simulation progress response.")
                return None

            status = progress_data.get("status")
            self.logger.info(f"Simulation batch status: {status}, children count: {len(children)}")

            if status == "ERROR":
                self.logger.info(f"Simulation failed with ERROR status. Progress URL: {simulation_progress_url}")
            elif status != "COMPLETE":
                self.logger.info("Simulation not complete. Will check again later.")

            record_ids = self.active_simulations_dict.get(simulation_progress_url)
            if record_ids:
                self.logger.info(f"Associated children IDs: {children} with record IDs: {record_ids} for location: {simulation_progress_url}")
                self.active_simulations_dict.pop(simulation_progress_url, None)
            else:
                self.logger.warning(f"No record_ids found for location: {simulation_progress_url}")
            return children

        except requests.exceptions.HTTPError as e:
            remove_status_codes = {400, 403, 404, 410}
            if e.response.status_code in remove_status_codes:
                self.logger.error(f"Simulation request failed with status {e.response.status_code}: {e}")
                self.active_simulations_dict.pop(simulation_progress_url, None)
                return False
            else:
                self.logger.error(f"Failed to fetch simulation progress: {e}")
                self.session = config_manager.get_session()
                return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to fetch simulation progress: {e}")
            self.session = config_manager.get_session()
            return None

    def check_simulation_status(self):
        """
        检查到期任务，处理完成/失败/进行中状态
        """
        now = time.time()
        checked_count = 0
        completed_count = 0
        failed_count = 0

        while self.simulation_heap and self.simulation_heap[0].next_check_time <= now:
            task = heapq.heappop(self.simulation_heap)
            
            check_progress_start_time = time.time()
            result = self.check_simulation_progress(task.location_url)
            check_progress_duration = time.time() - check_progress_start_time
            self.logger.info(f"[Performance] action=check_single_progress duration={check_progress_duration:.4f}s url={task.location_url}")

            if result is None:
                delay = self._calculate_backoff_delay(task.backoff_factor, task.retry_count, task.min_delay)
                task.retry_count += 1
                task.next_check_time = now + delay
                heapq.heappush(self.simulation_heap, task)
                checked_count += 1
                self.logger.debug(
                    f"Simulation {task.location_url} not done. "
                    f"Retry {task.retry_count}, next check in {delay:.1f}s"
                )

            elif result is False:
                self.logger.error(f"❌ Permanently failed: {task.location_url}")
                failed_count += 1

            else:
                self.logger.info(f"✅ Simulation completed: {task.location_url}")
                
                data_list = [
                    {
                        'child_id': child,
                        'submit_time': datetime.now(),
                        'status': 'pending',
                        'query_attempts': 0,
                        'last_query_time': None
                    }
                    for child in result
                ]
                try:
                    buffer_start_time = time.time()
                    self.cache_manager.add_simulation_tasks_batch(data_list)
                    buffer_duration = time.time() - buffer_start_time
                    self.logger.info(f"[Performance] action=buffer_completed_tasks duration={buffer_duration:.4f}s count={len(result)}")
                    
                    self.logger.info(f"💾 Buffered {len(result)} children for {task.location_url}")
                    
                    if self.cache_manager.get_dirty_items_count() >= self.FLUSH_THRESHOLD:
                        self.logger.info(f"Dirty items count ({self.cache_manager.get_dirty_items_count()}) reached threshold ({self.FLUSH_THRESHOLD}). Flushing...")
                        flush_start_time = time.time()
                        self.cache_manager.flush()
                        flush_duration = time.time() - flush_start_time
                        self.logger.info(f"[Performance] action=flush_cache_on_completion duration={flush_duration:.4f}s")
                except Exception as e:
                    self.logger.error(f"Failed to buffer children tasks: {e}")

                completed_count += 1

        current_active = len(self.simulation_heap)
        self.logger.info(
            f"Checked: {checked_count}, "
            f"Completed: {completed_count}, "
            f"Failed: {failed_count}, "
            f"Active: {current_active}"
        )

    def _calculate_backoff_delay(self, backoff_factor, retry_count, min_delay: int) -> float:
        """计算下次检查延迟，带 jitter 防止雪崩"""
        base = max(backoff_factor / (2 ** retry_count), min_delay)
        jitter = random.uniform(0.9, 1.1)
        return base * jitter
    
    def save_state(self):
        urls = [task.location_url for task in self.simulation_heap]
        state = {
            "active_simulations": urls,
            "timestamp": datetime.now().strftime(self.TIMESTAMP_FORMAT)
        }
        with open(self.state_file, 'w') as f:
            json.dump(state, f)
        self.logger.info(f"Saved {len(urls)} active simulations to {self.state_file}")



    def manage_simulations(self):
        """管理整个模拟过程（采用固定轮询模式）"""
        if not self.session:
            self.logger.error("Failed to sign in. Exiting...")
            return

        POLLING_INTERVAL = 3
        self.logger.info(f"Starting simulation management with a fixed polling interval of {POLLING_INTERVAL} seconds.")
        
        last_check_end_time = None

        try:
            while self.running:
                # 1. 检查所有到期任务的状态
                check_start_time = time.time()
                self.check_simulation_status()
                check_end_time = time.time()
                self.logger.info(f"[Performance] action=polling_check_all duration={check_end_time - check_start_time:.4f}s")
                
                # 2. 尝试加载新任务以填充空闲槽位
                load_start_time = time.time()
                self.load_new_alpha_and_simulate()
                load_end_time = time.time()
                self.logger.info(f"[Performance] action=load_and_simulate_all duration={load_end_time - load_start_time:.4f}s")

                # 3. 计算并记录从上次检查结束到本次加载完成的耗时
                if last_check_end_time is not None:
                    idle_to_load_duration = load_end_time - last_check_end_time
                    self.logger.info(f"[Performance] action=cycle_check_to_load_finish duration={idle_to_load_duration:.4f}s")
                
                last_check_end_time = time.time()

                # 4. 固定休眠，等待下一个轮询周期
                self.logger.debug(f"Main loop sleeping for {POLLING_INTERVAL}s...")
                time.sleep(POLLING_INTERVAL)
                
        except KeyboardInterrupt:
            self.logger.info("Manual interruption detected.")