import time
import os
import ast
import json
import threading
import random
import pandas as pd
from typing import List, Dict, Optional, Union, Tuple
from collections import deque
from datetime import datetime

from logger import Logger
from dao import SimulationTasksDAO, AlphaListPendingSimulatedDAO, StageOneSignalDAO
from config_manager import config_manager
from cache_manager import CacheManager
from utils import safe_api_call
from ace_lib import (
    check_session_and_relogin,
    simulate_multi_alpha, get_alpha_yearly_stats, get_check_submission,
    check_prod_corr_test, set_alpha_properties
)
from self_corr_calculator import calc_self_corr, load_data, download_data

from datetime import datetime

class TaskTracker:
    """用于跟踪任务执行时间并在超时时释放信号量"""
    def __init__(self, semaphore, timeout, logger):
        self.semaphore = semaphore
        self.timeout = timeout
        self.logger = logger
        self.start_time = time.time()
        self.released = False
        self.timed_out = False
        self.lock = threading.Lock()

    def release(self, timed_out=False):
        with self.lock:
            if not self.released:
                self.semaphore.release()
                self.released = True
                if timed_out:
                    self.timed_out = True
                return True
            return False

    def check_timeout(self):
        if self.timed_out:
            return True
        if time.time() - self.start_time > self.timeout:
            if self.release(timed_out=True):
                self.logger.warning(f"Task timed out after {self.timeout}s")
            return True
        return False

class AlphaSimulator:
    """Alpha模拟器类，用于管理量化策略的模拟过程"""

    def __init__(self, signal_manager=None):
        """初始化模拟器"""
        self.running = True
        self.logger = Logger()

        # 注册信号处理
        if signal_manager:
            signal_manager.add_handler(self.signal_handler)
        else:
            self.logger.warning("未提供 SignalManager, AlphaSimulator 无法注册信号处理函数")

        # 从配置中心获取参数
        self._load_config_from_manager()
        
        # 注册配置观察者
        self._config_observer_handle = config_manager.on_config_change(self._handle_config_change)
        
        # 初始化DAO
        self.alpha_list_pending_simulated_dao = AlphaListPendingSimulatedDAO()
        self.simulation_task_dao = SimulationTasksDAO()
        self.stage_one_signal_dao = StageOneSignalDAO()

        # 计数器
        self.total_sent_count = 0

        # 初始化缓存管理器
        self.cache_manager = CacheManager(
            alpha_dao=self.alpha_list_pending_simulated_dao,
            task_dao=self.simulation_task_dao,
            batch_number_for_every_queue=self.batch_number_for_every_queue
        )
        
        # 并发控制
        self.active_tasks = []
        self.lock = threading.Lock()
        self.semaphore = threading.Semaphore(self.max_concurrent)

    def signal_handler(self, signum, frame):
        self.logger.info(f"Received shutdown signal {signum}, initiating shutdown...")
        self.running = False
        self.cache_manager.flush()

    def _load_config_from_manager(self):
        """从配置中心加载运行时参数"""
        config = config_manager._config
        self.max_concurrent = config.get('max_concurrent', 5)
        self.batch_number_for_every_queue = config.get('batch_number_for_every_queue', 100)
        self.batch_size = config.get('batch_size', 10)
        
        region_set = config.get('region_set', ['USA'])
        if not isinstance(region_set, list):
            region_set = ['USA']
        self.region_set = deque(region_set)
        
        self.session = config_manager.get_session()

    def _handle_config_change(self, new_config):
        """配置变更回调处理"""
        self._load_config_from_manager()
        self.logger.info("Configuration reloaded due to config center update")

    def simulate_alphas(self, alpha_list: List[Dict]) -> List[Dict]:
        """
        模拟一组 alpha。
        参考 stage_one_workflow.py 的 simulate_alphas 节点逻辑。
        """
        sim_jobs = []
        for alpha in alpha_list:
            sim_jobs.append({
                "type": alpha.get('type', 'REGULAR'),
                "settings": alpha.get('settings', {}),
                "regular": alpha.get('regular', '')
            })
        
        try:
            session = check_session_and_relogin(self.session)
            self.session = session
            # 执行批量仿真
            return safe_api_call(simulate_multi_alpha, session, sim_jobs)
        except Exception as e:
            self.logger.error(f"simulate_alphas failed: {e}")
            return []

    def filter_alphas(self, alpha_ids: List[str], tracker: TaskTracker = None) -> Tuple[List[Dict], List[Dict]]:
        """
        过滤符合条件的 alpha，并生成可能的反转信号。
        参考 stage_one_workflow.py 的 filter_alphas 节点逻辑。
        """
        passed_alphas = []
        reversal_proposals = []
        session = check_session_and_relogin(self.session)
        self.session = session
        
        for alpha_id in alpha_ids:
            if tracker and tracker.timed_out:
                break
            try:
                # 获取表达式 (用于记录历史和生成反转信号)
                expression = ""
                alpha_details = {}
                try:
                    response = session.get(f"https://api.worldquantbrain.com/alphas/{alpha_id}")
                    response.raise_for_status()
                    alpha_details = response.json()
                    expression = alpha_details.get('regular', {}).get('code', "")
                except Exception as e:
                    self.logger.error(f"获取 Alpha {alpha_id} 详情失败: {e}")

                # 获取统计和检查数据
                stats_df = safe_api_call(get_alpha_yearly_stats, session, alpha_id)
                check_df = safe_api_call(get_check_submission, session, alpha_id)
                
                # 1. Year State 检查 (Data History)
                active_years = 0
                if stats_df is not None and not stats_df.empty:
                    active_years = (stats_df['sharpe'] != 0.0).sum()
                
                if active_years < 8:
                    continue
                
                # 2. Check 信息检查
                if check_df is not None and not check_df.empty:
                    checks = check_df.set_index('name')['value'].to_dict()
                    checks_result = check_df.set_index('name')['result'].to_dict()
                    
                    sharpe = checks.get('LOW_SHARPE', 0)
                    fitness = checks.get('LOW_FITNESS', 0)
                    robust = checks.get('LOW_ROBUST_UNIVERSE_SHARPE', 1.0)
                    jpn_robust = checks.get('LOW_ASI_JPN_SHARPE', 1.0)
                    conc_weight_res = checks_result.get('CONCENTRATED_WEIGHT', 'PASS')
                    
                    pass_check = True
                    fail_reason = ""
                    
                    if sharpe <= 1.0:
                        pass_check = False
                        fail_reason = f"Sharpe {sharpe:.2f} <= 1.0"
                    elif fitness <= 0.3:
                        pass_check = False
                        fail_reason = f"Fitness {fitness:.2f} <= 0.3"
                    elif robust <= 0.8:
                        pass_check = False
                        fail_reason = f"Robust {robust:.2f} <= 0.8"
                    elif conc_weight_res in ['FAIL', 'WARNING']:
                        pass_check = False
                        fail_reason = f"Conc Weight {conc_weight_res}"
                    elif jpn_robust <= 0.8:
                        pass_check = False
                        fail_reason = f"JPN Robust {jpn_robust:.2f} <= 0.8"

                    if pass_check:
                        self.logger.info(f"✅ Alpha {alpha_id} 通过过滤 (Fitness: {fitness:.2f})")
                        passed_alphas.append({
                            'id': alpha_id,
                            'fitness': fitness
                        })
                    else:
                        self.logger.info(f"Alpha {alpha_id} 未通过过滤: {fail_reason}")
                        # 3. 生成反转信号
                        # 条件: sharpe < -1 并且 fitness < -0.3
                        if sharpe < -1.0 and fitness < -0.3 and expression:
                            self.logger.info(f"💡 Alpha {alpha_id} 触发反转逻辑 (Sharpe: {sharpe:.2f}, Fitness: {fitness:.2f})")
                            try:
                                settings_from_api = alpha_details.get("settings", {})
                                reversal_proposals.append({
                                    'type': 'REGULAR',
                                    'settings': settings_from_api,
                                    'regular': f"reverse({expression})"
                                })
                            except Exception as e:
                                self.logger.error(f"生成反转信号失败: {e}")
            except Exception as e:
                self.logger.error(f"Error filtering alpha {alpha_id}: {e}")
                
        return passed_alphas, reversal_proposals

    def correlation_and_save(self, passed_alphas: List[Dict], context: Dict):
        """
        相关性检查并保存。
        参考 stage_one_workflow.py 的 correlation_and_save 节点逻辑。
        """
        if not passed_alphas:
            return

        session = check_session_and_relogin(self.session)
        self.session = session
        
        try:
            # 加载相关性参考数据
            download_data(session, flag_increment=True)
            os_alpha_ids, os_alpha_rets = load_data()
            if isinstance(os_alpha_rets, pd.DataFrame) and not os_alpha_rets.empty:
                if os_alpha_rets.columns.duplicated().any():
                    os_alpha_rets = os_alpha_rets.loc[:, ~os_alpha_rets.columns.duplicated()]
        except Exception as e:
            self.logger.error(f"Failed to load correlation data: {e}")
            os_alpha_ids, os_alpha_rets = {}, pd.DataFrame()

        # 排序 (Fitness 从大到小)
        passed_alphas.sort(key=lambda x: x['fitness'], reverse=True)

        for alpha_item in passed_alphas:
            alpha_id = alpha_item['id']
            try:
                # 计算自相关性
                max_corr = calc_self_corr(alpha_id, session, os_alpha_rets, os_alpha_ids)
                
                prod_corr = 1.0
                if max_corr < 0.7:
                    # 检查生产环境相关性
                    prod_corr_result = safe_api_call(check_prod_corr_test, session, alpha_id)
                    prod_corr = prod_corr_result['value'].max() if prod_corr_result is not None and not prod_corr_result.empty else 1.0
                
                if max_corr < 0.7 and prod_corr < 0.7:
                    # 通过检查，保存信号
                    record = {
                        "alpha_id": alpha_id,
                        "region": context['region'],
                        "universe": context['universe'],
                        "delay": context['delay'],
                        "date_time": context['date_time'],
                        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    self.stage_one_signal_dao.upsert_signal(record)
                    # 打标签
                    safe_api_call(set_alpha_properties, session, alpha_id, tags=[f"AlphaSim_{context['date_time']}"])
                    self.logger.info(f"✅ Saved Alpha {alpha_id} (Fitness: {alpha_item['fitness']:.2f})")
            except Exception as e:
                self.logger.error(f"Error in correlation/save for {alpha_id}: {e}")

    def workflow_slot(self, region: str, tracker: TaskTracker):
        """
        单个工作流卡槽的执行流程。
        """
        try:
            self.logger.info(f"🚀 [Workflow Slot] Started for region: {region}")
            
            # 1. 从数据库捞取 batch_size 个 pending alpha
            self.logger.info(f"📋 [Stage 1/4] Fetching {self.batch_size} pending alphas from database for {region}...")
            # 直接从数据库获取并锁定
            db_records = self.alpha_list_pending_simulated_dao.fetch_and_lock_pending_by_region(region, self.batch_size)
            if not db_records:
                self.logger.info(f"ℹ️ No pending alphas found for region: {region}. Slot exiting.")
                return

            # 2. 将这批 alpha 的状态改为 sent
            self.logger.info(f"🔄 [Stage 1/4] Marking {len(db_records)} records as 'sent'...")
            record_ids = [r['id'] for r in db_records]
            # 直接使用 DAO 更新状态
            self.alpha_list_pending_simulated_dao.batch_update_status_by_ids(record_ids, 'sent')
            
            # 累加 total_sent_count
            with self.lock:
                self.total_sent_count += len(record_ids)
                self.logger.info(f"📈 Total sent count updated: {self.total_sent_count}")

            self.logger.info(f"🏗️ [Stage 2/4] Generating simulation job list...")
            alpha_list = []
            for record in db_records:
                self.logger.debug(f"Processing DB record: {record['id']}")
                settings = record.get('settings')
                raw_settings_for_log = settings # Keep a copy for logging
                
                if isinstance(settings, str):
                    try:
                        # Try json.loads first as it's more common for stored JSON
                        settings = json.loads(settings)
                    except json.JSONDecodeError:
                        try:
                            # Fallback to ast.literal_eval if it's a Python literal string
                            settings = ast.literal_eval(settings)
                        except Exception as e:
                            self.logger.error(f"❌ Failed to parse settings for record {record['id']}. "
                                             f"Value: {raw_settings_for_log}, Type: {type(raw_settings_for_log)}, Error: {e}")
                            settings = {}
                elif settings is None:
                    self.logger.warning(f"⚠️ Settings is None for record {record['id']}")
                    settings = {}
                elif not isinstance(settings, dict):
                    self.logger.warning(f"⚠️ Settings is neither string nor dict for record {record['id']}. "
                                       f"Type: {type(settings)}, Value: {settings}")
                    settings = {}
                
                alpha_item = {
                    'type': record.get('type', 'REGULAR'),
                    'settings': settings,
                    'regular': record.get('regular', '')
                }
                alpha_list.append(alpha_item)
            
            self.logger.info(f"✅ [Stage 2/4] Generated {len(alpha_list)} initial jobs.")

            all_passed = []
            # 3. 循环执行 simulate_alphas 和 filter_alphas，直到 batch size 个全部模拟完
            self.logger.info(f"📡 [Stage 3/4] Starting simulation and filtering cycle (Sub-batches of 10)...")
            idx = 0
            sub_batch_count = 0
            while idx < len(alpha_list):
                if tracker.timed_out:
                    self.logger.warning(f"⚠️ [Stage 3/4] Workflow slot for region {region} ABORTED due to timeout during simulation.")
                    break
                
                sub_batch_count += 1
                batch = alpha_list[idx:idx+10]
                idx += 10
                
                self.logger.info(f"🧪 [Sub-batch {sub_batch_count}] Simulating {len(batch)} alphas...")
                sim_results = self.simulate_alphas(batch)
                
                # 提取 alpha id
                ids = [r['alpha_id'] for r in sim_results if r.get('alpha_id')]
                self.logger.info(f"🔍 [Sub-batch {sub_batch_count}] Received {len(ids)} Alpha IDs. Starting filtering...")
                
                if ids:
                    passed, reversals = self.filter_alphas(ids, tracker=tracker)
                    all_passed.extend(passed)
                    self.logger.info(f"✨ [Sub-batch {sub_batch_count}] {len(passed)} passed filter. {len(reversals)} reversals generated.")
                    
                    if reversals:
                        self.logger.info(f"➕ Adding {len(reversals)} reversal proposals to the queue.")
                        alpha_list.extend(reversals)
            
            # 4. 将所有通过 filter_alphas 的 alpha id 传入 correlation_and_save
            if tracker.timed_out:
                self.logger.warning(f"⚠️ [Stage 4/4] Skipping correlation and save for {region} due to timeout.")
            elif all_passed:
                self.logger.info(f"📊 [Stage 4/4] Starting correlation check and save for {len(all_passed)} passed alphas...")
                first_settings = alpha_list[0]['settings']
                context = {
                    'region': region,
                    'universe': first_settings.get('universe', 'TOP3000'),
                    'delay': first_settings.get('delay', 1),
                    'date_time': datetime.now().strftime("%Y%m%d")
                }
                self.correlation_and_save(all_passed, context)
            else:
                self.logger.info(f"ℹ️ [Stage 4/4] No alphas passed filtering for {region}. Skipping correlation.")

            self.logger.info(f"🏁 [Workflow Slot] Finished for region: {region}")

        except Exception as e:
            self.logger.error(f"❌ Error in workflow_slot for region {region}: {e}", exc_info=True)
        finally:
            # 5. 完成后释放卡槽
            tracker.release()

    def _check_timeouts(self):
        """检查所有活动任务的超时情况"""
        with self.lock:
            remaining_tasks = []
            for thread, tracker, region in self.active_tasks:
                if not thread.is_alive():
                    # 确保信号量已释放（如果线程正常结束）
                    tracker.release()
                    continue
                
                if tracker.check_timeout():
                    # 已超时，tracker.check_timeout 会处理信号量释放
                    # 我们停止跟踪它，让线程在下次循环检查 timed_out 时退出
                    continue
                
                remaining_tasks.append((thread, tracker, region))
            self.active_tasks = remaining_tasks

    def manage_simulations(self):
        """管理整个模拟过程"""
        if not self.session:
            self.logger.error("No session available for AlphaSimulator. Exiting...")
            return

        self.logger.info("AlphaSimulator starting simulation management...")
        while self.running:
            # 0. Check limit
            is_limit_reached = False
            with self.lock:
                if self.total_sent_count >= self.batch_number_for_every_queue:
                    is_limit_reached = True
            
            if is_limit_reached:
                self.logger.warning(f"🛑 Limit reached ({self.total_sent_count} >= {self.batch_number_for_every_queue}). Sleeping for 1 hour...")
                
                # Sleep for 1 hour, but check self.running every second
                sleep_start = time.time()
                while time.time() - sleep_start < 3600:
                    if not self.running:
                        break
                    time.sleep(1)
                
                if not self.running:
                    break
                    
                continue

            # 1. 检查超时
            self._check_timeouts()
            
            # 2. 判断 max_concurrent，如果还没有达到配置上限
            if self.semaphore.acquire(timeout=1):
                if not self.running:
                    self.semaphore.release()
                    break
                
                with self.lock:
                    if not self.region_set:
                        self.semaphore.release()
                        time.sleep(5)
                        continue
                    region = self.region_set[0]
                    self.region_set.rotate(-1)
                
                # 计算超时时间：batch_size / 10 * 30 分钟
                timeout_minutes = (self.batch_size / 10) * 30
                timeout_seconds = timeout_minutes * 60
                
                # 创建 Tracker 和 线程
                tracker = TaskTracker(self.semaphore, timeout_seconds, self.logger)
                t = threading.Thread(target=self.workflow_slot, args=(region, tracker))
                t.daemon = True
                t.start()
                
                with self.lock:
                    self.active_tasks.append((t, tracker, region))
            else:
                # 卡槽已满，等待
                time.sleep(3)

    def __del__(self):
        """析构函数，确保清理"""
        if hasattr(self, '_config_observer_handle'):
            try:
                config_manager._observers.remove(self._handle_config_change)
            except:
                pass
        if hasattr(self, 'cache_manager'):
            self.cache_manager.flush()
