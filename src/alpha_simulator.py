import time
import os
import ast
import json
import requests
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
from utils import safe_api_call, get_datasets_for_alpha
from ace_lib import (
    check_session_and_relogin,
    simulate_multi_alpha, get_alpha_yearly_stats, get_check_submission,
    check_prod_corr_test, set_alpha_properties
)
from self_corr_calculator import calc_self_corr, load_data, download_data

class TaskTracker:
    """用于跟踪任务执行时间，提供 should_stop 信号用于内部自愿退出"""
    def __init__(self, timeout, logger):
        self.timeout = timeout
        self.logger = logger
        self.start_time = time.time()
        self._timed_out = False

    @property
    def should_stop(self):
        if self._timed_out:
            return True
        if time.time() - self.start_time > self.timeout:
            self._timed_out = True
            self.logger.warning(f"Task detected timeout after {self.timeout}s, signaling stop.")
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
        self.reserved_count = 0

        # 初始化缓存管理器
        self.cache_manager = CacheManager(
            alpha_dao=self.alpha_list_pending_simulated_dao,
            task_dao=self.simulation_task_dao,
            batch_number_for_every_queue=self.batch_number_for_every_queue
        )
        
        # 并发控制：信号量仅用于限制活跃线程数
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
        self.total_sent_count = 0
        self.logger.info("Configuration reloaded due to config center update. total_sent_count reset.")

    def simulate_alphas(self, alpha_list: List[Dict], tracker: TaskTracker) -> List[Dict]:
        """
        模拟一组 alpha。支持超时检查和重试逻辑。
        """
        sim_jobs = []
        for alpha in alpha_list:
            sim_jobs.append({
                "type": alpha.get('type', 'REGULAR'),
                "settings": alpha.get('settings', {}),
                "regular": alpha.get('regular', '')
            })

        def raise_on_retryable(r, *args, **kwargs):
            if r.status_code in [429, 401]:
                r.raise_for_status()
        
        if not self.session:
            self.session = config_manager.get_session()
            
        if raise_on_retryable not in self.session.hooks['response']:
            self.session.hooks['response'].append(raise_on_retryable)

        max_retries = 3 
        retry_delay = 120 
        
        for attempt in range(max_retries):
            if tracker.should_stop:
                break
                
            try:
                session = check_session_and_relogin(self.session)
                self.session = session
                return simulate_multi_alpha(session, sim_jobs)
                
            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code
                if status_code in [429, 401]:
                    self.logger.warning(f"Simulate Alphas encountered {status_code}. Attempt {attempt + 1}/{max_retries}. Waiting {retry_delay}s...")
                    if attempt < max_retries - 1:
                        for _ in range(retry_delay // 10):
                            if tracker.should_stop: break
                            time.sleep(10)
                        
                        if status_code == 401:
                            try:
                                from ace_lib import start_session
                                self.session = start_session()
                            except Exception as login_err:
                                self.logger.error(f"Force re-login failed: {login_err}")
                        continue
                
                self.logger.error(f"simulate_alphas failed: {e}")
                break
            except Exception as e:
                self.logger.error(f"simulate_alphas unexpected error: {e}")
                break
        
        return []

    def filter_alphas(self, alpha_ids: List[str], tracker: TaskTracker) -> Tuple[List[Dict], List[Dict]]:
        """
        过滤符合条件的 alpha，并提取反转信号建议。
        """
        passed_alphas = []
        reversal_proposals = []
        session = check_session_and_relogin(self.session)
        self.session = session
        
        for alpha_id in alpha_ids:
            if tracker.should_stop:
                break
                
            try:
                response = session.get(f"https://api.worldquantbrain.com/alphas/{alpha_id}", timeout=60)
                response.raise_for_status()
                alpha_details = response.json()
                expression = alpha_details.get('regular', {}).get('code', "")

                stats_df = safe_api_call(get_alpha_yearly_stats, session, alpha_id)
                check_df = safe_api_call(get_check_submission, session, alpha_id)
                
                active_years = 0
                if stats_df is not None and not stats_df.empty:
                    active_years = (stats_df['sharpe'] != 0.0).sum()
                
                if active_years < 6:
                    continue
                
                if check_df is not None and not check_df.empty:
                    checks = check_df.set_index('name')['value'].to_dict()
                    checks_result = check_df.set_index('name')['result'].to_dict()
                    
                    sharpe = checks.get('LOW_SHARPE', 0)
                    fitness = checks.get('LOW_FITNESS', 0)
                    robust = checks.get('LOW_ROBUST_UNIVERSE_SHARPE', 1.0)
                    jpn_robust = checks.get('LOW_ASI_JPN_SHARPE', 1.0)
                    conc_weight_res = checks_result.get('CONCENTRATED_WEIGHT', 'PASS')
                    conc_weight_val = checks.get('CONCENTRATED_WEIGHT')
                    
                    if conc_weight_res in ['FAIL', 'WARNING'] and not pd.isna(conc_weight_val) and conc_weight_val <= 0.3:
                        self.logger.info(f"Alpha {alpha_id} Conc Weight {conc_weight_res} but value {conc_weight_val:.4f} <= 0.3, allowing.")

                    pass_check = True
                    fail_reason = ""
                    
                    if sharpe <= 0.8:
                        pass_check = False
                        fail_reason = f"Sharpe {sharpe:.2f} <= 0.8"
                    elif fitness <= 0.3:
                        pass_check = False
                        fail_reason = f"Fitness {fitness:.2f} <= 0.3"
                    elif robust <= 0.8:
                        pass_check = False
                        fail_reason = f"Robust {robust:.2f} <= 0.8"
                    elif conc_weight_res in ['FAIL', 'WARNING'] and (pd.isna(conc_weight_val) or conc_weight_val > 0.5):
                        pass_check = False
                        val_str = f"{conc_weight_val:.4f}" if not pd.isna(conc_weight_val) else "None"
                        fail_reason = f"Conc Weight {conc_weight_res} (Value: {val_str})"
                    elif jpn_robust <= 0.8:
                        pass_check = False
                        fail_reason = f"JPN Robust {jpn_robust:.2f} <= 0.8"

                    if pass_check:
                        self.logger.info(f"✅ Alpha {alpha_id} passed filtering (Fitness: {fitness:.2f})")
                        passed_alphas.append({'id': alpha_id, 'fitness': fitness})
                    else:
                        self.logger.info(f"Alpha {alpha_id} filtered: {fail_reason}")
                        if sharpe < -0.8 and fitness < -0.3 and expression:
                            self.logger.info(f"💡 Alpha {alpha_id} triggers reversal (Sharpe: {sharpe:.2f}, Fitness: {fitness:.2f})")
                            reversal_proposals.append({
                                'type': 'REGULAR',
                                'settings': alpha_details.get("settings", {}),
                                'regular': f"reverse({expression})"
                            })
            except Exception as e:
                self.logger.error(f"Error filtering alpha {alpha_id}: {e}")
                
        return passed_alphas, reversal_proposals

    def correlation_and_save(self, passed_alphas: List[Dict], context: Dict):
        """
        相关性检查并保存信号记录。
        """
        if not passed_alphas:
            return

        session = check_session_and_relogin(self.session)
        self.session = session
        
        try:
            download_data(session, flag_increment=True)
            os_alpha_ids, os_alpha_rets = load_data()
            if isinstance(os_alpha_rets, pd.DataFrame) and not os_alpha_rets.empty:
                if os_alpha_rets.columns.duplicated().any():
                    os_alpha_rets = os_alpha_rets.loc[:, ~os_alpha_rets.columns.duplicated()]
        except Exception as e:
            self.logger.error(f"Correlation data load failure: {e}")
            os_alpha_ids, os_alpha_rets = {}, pd.DataFrame()

        passed_alphas.sort(key=lambda x: x['fitness'], reverse=True)

        for alpha_item in passed_alphas:
            alpha_id = alpha_item['id']
            try:
                max_corr = calc_self_corr(alpha_id, session, os_alpha_rets, os_alpha_ids)
                if max_corr < 0.5:
                    all_datasets_df, dataset_id = get_datasets_for_alpha(alpha_id, session)
                    category = 'unknown'
                    if all_datasets_df is not None and not all_datasets_df.empty and 'category_id' in all_datasets_df.columns:
                        field_categories = list(set(all_datasets_df['category_id'].dropna().tolist()))
                        if field_categories: category = field_categories[0]

                    record = {
                        "alpha_id": alpha_id,
                        "region": context['region'],
                        "universe": context['universe'],
                        "delay": context['delay'],
                        "dataset_id": dataset_id,
                        "category": category,
                        "date_time": context['date_time'],
                        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    self.stage_one_signal_dao.upsert_signal(record)
                    safe_api_call(set_alpha_properties, session, alpha_id, tags=[f"AlphaSim_{context['date_time']}"])
                    self.logger.info(f"✅ Saved Alpha {alpha_id} (Fitness: {alpha_item['fitness']:.2f}, MaxCorr: {max_corr:.4f})")
                else:
                    self.logger.info(f"❌ Alpha {alpha_id} failed correlation (MaxCorr: {max_corr:.4f} >= 0.5)")
            except Exception as e:
                self.logger.error(f"Correlation/Save error for {alpha_id}: {e}")

    def workflow_slot(self, region: str, tracker: TaskTracker):
        """
        单个区域的处理流水线，作为独立线程运行。
        """
        with self.semaphore: 
            try:
                self.logger.info(f"🚀 [Workflow Slot] Started: {region}")
                
                db_records = self.alpha_list_pending_simulated_dao.fetch_and_lock_pending_by_region(region, self.batch_size)
                if not db_records:
                    self.logger.info(f"ℹ️ No pending alphas for {region}. Exiting.")
                    return

                record_ids = [r['id'] for r in db_records]
                self.alpha_list_pending_simulated_dao.batch_update_status_by_ids(record_ids, 'sent')
                
                with self.lock:
                    self.total_sent_count += len(record_ids)

                alpha_list = []
                for record in db_records:
                    settings = record.get('settings')
                    if isinstance(settings, str):
                        try: settings = json.loads(settings)
                        except:
                            try: settings = ast.literal_eval(settings)
                            except: settings = {}
                    
                    alpha_list.append({
                        'type': record.get('type', 'REGULAR'),
                        'settings': settings or {},
                        'regular': record.get('regular', '')
                    })

                all_passed = []
                reversal_to_db = []
                
                for i in range(0, len(alpha_list), 10):
                    if tracker.should_stop:
                        self.logger.warning(f"⚠️ [Workflow Slot] {region} ABORTED due to timeout.")
                        break
                    
                    batch = alpha_list[i:i+10]
                    self.logger.info(f"🧪 [Batch {i//10 + 1}] Simulating {len(batch)} for {region}...")
                    
                    sim_results = self.simulate_alphas(batch, tracker)
                    ids = [r['alpha_id'] for r in sim_results if r.get('alpha_id')]
                    
                    if ids:
                        passed, reversals = self.filter_alphas(ids, tracker)
                        all_passed.extend(passed)
                        
                        for rev in reversals:
                            reversal_to_db.append({
                                'type': rev['type'],
                                'settings': json.dumps(rev['settings']) if isinstance(rev['settings'], dict) else rev['settings'],
                                'regular': rev['regular'],
                                'region': region,
                                'priority': 0, 
                                'status': 'pending'
                            })
                
                if reversal_to_db:
                    self.logger.info(f"🔄 Injecting {len(reversal_to_db)} reversal signals to DB with priority 0.")
                    self.alpha_list_pending_simulated_dao.batch_insert(reversal_to_db)

                if all_passed and not tracker.should_stop:
                    first_settings = alpha_list[0]['settings']
                    context = {
                        'region': region,
                        'universe': first_settings.get('universe', 'TOP3000'),
                        'delay': first_settings.get('delay', 1),
                        'date_time': datetime.now().strftime("%Y%m%d")
                    }
                    self.correlation_and_save(all_passed, context)

            except Exception as e:
                self.logger.error(f"❌ workflow_slot error ({region}): {e}", exc_info=True)
            finally:
                with self.lock:
                    self.reserved_count -= self.batch_size
                self.logger.info(f"🏁 [Workflow Slot] Finished: {region}")

    def manage_simulations(self):
        """
        主管理循环，负责监控资源并派发线程。包含空跑检测逻辑。
        """
        if not self.session:
            self.logger.error("No valid session. Exiting manager.")
            return

        self.logger.info("AlphaSimulator Manager started (Refactored Threading Mode).")
        active_threads = []
        empty_poll_count = 0 

        while self.running:
            active_threads = [t for t in active_threads if t.is_alive()]
            
            limit_reached = False
            capacity_full = False
            with self.lock:
                if self.total_sent_count >= self.batch_number_for_every_queue:
                    limit_reached = True
                elif self.total_sent_count + self.reserved_count >= self.batch_number_for_every_queue:
                    capacity_full = True
            
            if limit_reached:
                self.logger.warning(f"🛑 Queue limit reached ({self.total_sent_count}). Sleeping 1 hour...")
                for _ in range(3600):
                    if not self.running: break
                    time.sleep(1)
                continue
            
            if capacity_full:
                time.sleep(10)
                continue

            if len(active_threads) < self.max_concurrent:
                found_task_this_round = False
                target_region = None
                
                with self.lock:
                    if not self.region_set:
                        time.sleep(5)
                        continue
                    
                    original_regions = list(self.region_set)
                    for _ in range(len(original_regions)):
                        region = self.region_set[0]
                        self.region_set.rotate(-1)
                        
                        has_pending = self.alpha_list_pending_simulated_dao.fetch_pending_by_region(region, limit=1)
                        if has_pending:
                            found_task_this_round = True
                            target_region = region
                            break
                
                if found_task_this_round:
                    empty_poll_count = 0 
                    
                    with self.lock:
                        self.reserved_count += self.batch_size

                    tracker = TaskTracker(timeout=1200, logger=self.logger)
                    t = threading.Thread(target=self.workflow_slot, args=(target_region, tracker))
                    t.daemon = True
                    t.start()
                    active_threads.append(t)
                    
                    self.logger.info(f"Dispatched thread for {target_region}. Active threads: {len(active_threads)}")
                    time.sleep(3) 
                else:
                    empty_poll_count += 1
                    self.logger.info(f"ℹ️ No pending tasks found in any region. Empty poll count: {empty_poll_count}/10")
                    
                    if empty_poll_count >= 10:
                        self.logger.warning("😴 No tasks found for 10 consecutive polls. Sleeping for 1 hour...")
                        empty_poll_count = 0 
                        for _ in range(3600):
                            if not self.running: break
                            time.sleep(1)
                    else:
                        time.sleep(30) 
            else:
                time.sleep(5)

    def __del__(self):
        """确保资源回收"""
        if hasattr(self, '_config_observer_handle'):
            try: config_manager._observers.remove(self._handle_config_change)
            except: pass
        if hasattr(self, 'cache_manager'):
            self.cache_manager.flush()
