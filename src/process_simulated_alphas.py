import time
import pandas as pd
import requests
import urllib3
import schedule
import threading
import os
import csv
from datetime import datetime, timedelta
import ast
from typing import List
from alpha_prune import calculate_correlations, generate_comparison_data
from logger import Logger
from signal_manager import SignalManager
from io import StringIO
from dao import SimulatedAlphasDAO  
from dao import StoneGoldBagDAO
from config_manager import config_manager


class ProcessSimulatedAlphas:
    def __init__(self, output_dir, specified_sharpe, specified_fitness, signal_manager=None):
        self.output_dir = output_dir
        self.SPECIFIED_SHARPE = specified_sharpe
        self.SPECIFIED_FITNESS = specified_fitness
        self.logger = Logger()

        self.alpha_ids: List[str] = []
        self.total = 0
        self.idx = 0
        self.date_str = self.get_yesterday_date()

        self._load_config_from_manager()

        self._scheduler_running = False
        self.simulated_alphas_dao = SimulatedAlphasDAO()

        self.stone_gold_bag_dao = StoneGoldBagDAO()

        # 注册配置更新回调
        config_manager.on_config_change(self.on_config_update)

        # 注册信号处理
        if signal_manager:
            signal_manager.add_handler(self.handle_exit_signal)
        else:
            self.logger.warning("未提供 SignalManager,ProcessSimulatedAlphas 无法注册信号处理函数")
        
    def handle_exit_signal(self, signum, frame):
        self.logger.info(f"Received shutdown signal {signum}, saving unfinished alpha IDs...")
        self.save_unfinished_alpha_ids()
        self.stop_daily_schedule()

    def _load_config_from_manager(self):
        """从配置中心加载配置参数"""
        config = config_manager._config
        self.username = config.get('username')
        self.password = config.get('password')
        self.init_date = config.get('init_date_str')
        
        # 添加日志记录 - 不打印密码明文
        self.logger.info(f"[ProcessSimulatedAlphas] Loaded config from config manager: username={self.username}, init_date={self.init_date}")
        if self.password:
            self.logger.info("[ProcessSimulatedAlphas] Password is set")
        else:
            self.logger.warning("[ProcessSimulatedAlphas] Password is not set")
            
        self.session = config_manager.get_session()
        

    def get_yesterday_date(self):
        """获取前一天的日期，格式为 YYYYMMDD"""
        return (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    
    def initialize_alpha_ids(self):
        self.logger.info("Initializing alpha_ids...")

        temp_file = os.path.join(self.output_dir, 'unfinished_alpha_ids.temp.csv')

        if os.path.exists(temp_file):
            with open(temp_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                self.alpha_ids = [row[0] for row in reader if row]
                self.logger.info(f"Loaded {len(self.alpha_ids)} unfinished alpha IDs from temp file.")
        else:
            self.logger.info("No temp file. Assuming processing is ongoing.")

        self.total = len(self.alpha_ids)
        self.logger.info(f"Total alpha IDs to process: {self.total}")

    def save_stone_bag(self, filtered_records: List[dict], db_date_str: str) -> None:
        """
        将过滤后的记录保存到 stone_gold_bag_table 数据库表

        Args:
            filtered_records (List[dict]): 过滤后的记录数据
            db_date_str (str): 数据库日期格式 (YYYYMMDD)

        Returns:
            None
        """
        if not filtered_records:
            self.logger.info("No filtered records to save to database")
            return
        
        try:       
            # 批量插入记录
            success_count = self.stone_gold_bag_dao.batch_insert(filtered_records)
            self.logger.info(f"Saved {success_count} records to stone_gold_bag_table for date {db_date_str}")
            
        except Exception as e:
            self.logger.error(f"Error saving to database: {str(e)}")
            # 尝试逐条插入作为回退
            self.logger.info("Attempting record-by-record insertion as fallback")
            success_count = 0
            for record in filtered_records:
                try:
                    self.stone_gold_bag_dao.insert(record)
                    success_count += 1
                except Exception as e:
                    self.logger.error(f"Failed to insert record {record.get('id')}: {str(e)}")
            self.logger.info(f"Inserted {success_count}/{len(filtered_records)} records successfully")
    def load_alpha_data_from_db(self, db_date_str) -> pd.DataFrame:
        """
        从数据库加载 alpha 数据（使用分页查询避免网络延迟）

        返回:
            pd.DataFrame: 包含 alpha 数据的 DataFrame
        """
        try:
            # 获取总记录数
            total_records = self.simulated_alphas_dao.count_by_datetime(db_date_str)
            
            if total_records == 0:
                self.logger.warning(f"No records found in database for date {db_date_str}")
                return pd.DataFrame()
                
            self.logger.info(f"Found {total_records} records for date {db_date_str}, starting paginated query...")
            
            # 设置分页大小
            page_size = 1000
            records = []
            # 计算总页数: (总记录数 + 每页大小 - 1) // 每页大小
            # 使用整数除法(//)确保结果为整数，这是计算分页的标准方法
            page_count = (total_records + page_size - 1) // page_size
            
            for page in range(1, page_count + 1):
                offset = (page - 1) * page_size
                self.logger.info(f"Fetching page {page}/{page_count} (records {offset+1} to {min(offset + page_size, total_records)} of {total_records})")
                page_records = self.simulated_alphas_dao.get_by_datetime_paginated(
                    db_date_str, 
                    limit=page_size, 
                    offset=offset
                )
                records.extend(page_records)
                
            # 将记录转换为 DataFrame
            df = pd.DataFrame(records)
            self.logger.info(f"Successfully loaded {len(df)} records from database in {page_count} pages")
            return df
            
        except Exception as e:
            self.logger.error(f"Error loading data from database: {str(e)}")
            raise

    def read_and_filter_alpha_ids(self, date_str):
        """从数据库读取并筛选符合条件的alpha,保存到 stone_gold_bag_table"""
        # 转换日期格式: YYYY-MM-DD -> YYYYMMDD
        db_date_str = date_str.replace('-', '')
        
        # 检查数据库中是否有数据
        record_count = self.simulated_alphas_dao.count_by_datetime(db_date_str)
        
        if record_count == 0:
            self.logger.error(f"No records found in simulated_alphas_table for date {db_date_str}")
            return
        
        self.logger.info(f"Found {record_count} records in simulated_alphas_table for date {db_date_str}")
        
        # 从数据库加载数据
        df = self.load_alpha_data_from_db(db_date_str)
        
        alpha_ids = []
        filtered_records = []  # 存储符合条件的完整记录
        
        for _, row in df.iterrows():
            try:
                # 解析 is 列（Python 字典字符串）
                is_str = row['is']
                try:
                    is_data = ast.literal_eval(is_str)
                except (SyntaxError, ValueError) as e:
                    self.logger.warning(f"Failed to parse alpha {row['id']} is field: {e}, raw data: {is_str[:100]}...")
                    continue

                # 提取 fitness 和 sharpe，处理 None 或无效值
                try:
                    fitness = float(is_data.get('fitness', 0))
                except (TypeError, ValueError) as e:
                    self.logger.warning(f"Failed to convert fitness for alpha {row['id']}: {e}, raw fitness: {is_data.get('fitness')}")
                    continue

                try:
                    sharpe = float(is_data.get('sharpe', 0))
                except (TypeError, ValueError) as e:
                    self.logger.warning(f"Failed to convert sharpe for alpha {row['id']}: {e}, raw sharpe: {is_data.get('sharpe')}")
                    continue

                # 检查是否有失败的检查项
                try:
                    checks = is_data.get('checks', [])
                    has_failed_checks = any(check['result'] == 'FAIL' for check in checks)
                except Exception as e:
                    self.logger.warning(f"Failed to process checks for alpha {row['id']}: {str(e)}")
                    has_failed_checks = True  # 视为有失败检查
                
                # 检查是否满足条件
                if (fitness >= self.SPECIFIED_FITNESS and 
                    sharpe >= self.SPECIFIED_SHARPE and 
                    not has_failed_checks):
                    
                    alpha_ids.append(row['id'])
                    # 转换为字典格式保存
                    record_dict = row.to_dict()
                    filtered_records.append(record_dict)
                    
            except Exception as e:
                self.logger.warning(f"Failed to process alpha {row.get('id')}: {str(e)}")
                continue
        
        self.logger.info(f"Filtered {len(alpha_ids)} qualified alphaIds.")
        
        # 保存到 stone_gold_bag_table
        self.save_stone_bag(filtered_records, db_date_str)

    def process_alpha_ids_loop(self):
        self.logger.info("Starting alpha ID processing loop...")

        while True:
            if not self.alpha_ids:
                self.logger.info("No alpha IDs to process. Sleeping for 1 hour.")
                time.sleep(3600)
                continue

            alpha_id = self.alpha_ids.pop(0)
            self.idx += 1

            if self.idx % 200 == 0:
                # 调用 sign_in 方法重新登录
                
                self.session = config_manager.get_session()
                self.logger.info(f"Progress: {self.total - len(self.alpha_ids)}/{self.total} processed.")
                if not self.session:
                    continue

            self.logger.info(f"[{self.idx}/{self.total}] Checking alphaId: {alpha_id}")
            try:
                result = self.get_check_submission(self.session, alpha_id)
            except (requests.exceptions.ConnectionError, urllib3.exceptions.ProtocolError) as e:
                self.logger.error(f"Network error processing alphaId {alpha_id}: {str(e)}")
                self.alpha_ids.insert(0, alpha_id)  # 将当前alpha_id放回队列开头
                self.idx -= 1  # 回滚计数
                time.sleep(30)  # 等待30秒后重试
                continue  # 跳过当前循环

            # 处理所有可能的返回情况
            if result == "sleep" or (isinstance(result, tuple) and result[0] != result[0]) or result == "fail" or result == "error":
                if result == "sleep":
                    self.logger.warning(f"alphaId {alpha_id} needs cooldown, re-queuing.")
                    self.alpha_ids.append(alpha_id)
                elif isinstance(result, tuple) and result[0] != result[0]:  # 检查pc是否为NaN
                    self.logger.warning(f"prod-correlation check failed (NaN) for alphaId {alpha_id}, re-queuing.")
                    self.alpha_ids.append(alpha_id)
                elif result == "fail":
                    self.logger.warning(f"alphaId {alpha_id} failed other checks.")
                else:
                    self.logger.error(f"alphaId {alpha_id} error during check.")
                
                wait_time = max(5, min(60, int(300 / (len(self.alpha_ids) + 1))))
                self.logger.info(f"Sleeping for {wait_time} seconds before retrying.")
                time.sleep(wait_time)
                # 调用 sign_in 方法重新登录
                
                self.session = config_manager.get_session()
            elif isinstance(result, tuple):
                # 成功获取三个值的情况
                pc, sharpe, fitness = result
                self.logger.info(f"alphaId {alpha_id} passed check, saving.")
                self.save_gold_bag([(alpha_id, pc, sharpe, fitness)])

    def get_check_submission(self, s, alpha_id):
        """获取alpha检查结果，返回(pc, sharpe, fitness)元组或状态字符串"""
        while True:
            result = s.get("https://api.worldquantbrain.com/alphas/" + alpha_id + "/check")
            if "retry-after" in result.headers:
                time.sleep(float(result.headers["Retry-After"]))
            else:
                break
        
        try:
            result_json = result.json()
            if result_json.get("is", 0) == 0:
                return "sleep"

            checks_df = pd.DataFrame(result_json["is"]["checks"])
            
            # 检查是否有任何检查项的result为"ERROR"
            if any(checks_df["result"] == "ERROR"):
                return "error"
                
            # 获取各个指标值
            pc = checks_df[checks_df.name == "PROD_CORRELATION"]["value"].values[0]
            sharpe = checks_df[checks_df.name == "LOW_SHARPE"]["value"].values[0]
            fitness = checks_df[checks_df.name == "LOW_FITNESS"]["value"].values[0]

            failed_checks = checks_df[checks_df["result"] == "FAIL"]
            if not any(checks_df["result"] == "FAIL"):
                return (pc, sharpe, fitness)
            else:
                self.logger.info(f"alphaId {alpha_id} check failed, skipping.")
                self.logger.info(f"Fail reasons:\n{failed_checks.to_string(index=False)}")
                return "fail"
        except Exception as e:
            self.logger.error(f"Error processing alphaId {alpha_id}: {str(e)}")
            return "error"

    def save_gold_bag(self, gold_bag):
        """更新stone_gold_bag_table表中的数据,将符合条件的alpha标记为gold,并更新相关指标"""
        # 遍历gold_bag，每个元素是(alpha_id, pc, sharpe, fitness)
        for item in gold_bag:
            alpha_id, pc, sharpe, fitness = item
            # 更新数据库记录
            update_data = {
                'gold': 'gold',
                'prodCorrelation': pc,
                'sharpe': sharpe,
                'fitness': fitness
            }
            affected_rows = self.stone_gold_bag_dao.update_by_id(alpha_id, update_data)
            if affected_rows > 0:
                self.logger.info(f"Updated gold bag for alpha {alpha_id} in database. Affected rows: {affected_rows}")
            elif affected_rows == 0:
                self.logger.warning(f"No matching record found for alpha {alpha_id} in database.")
            else:
                self.logger.error(f"Failed to update gold bag for alpha {alpha_id} in database.")
        
        self.logger.info(f"Updated {len(gold_bag)} gold bag records in database.")
        return f"Updated {len(gold_bag)} records in database"
    
    def save_unfinished_alpha_ids(self):
        if self.alpha_ids:
            temp_path = os.path.join(self.output_dir, 'unfinished_alpha_ids.temp.csv')
            if os.path.exists(temp_path):
                os.remove(temp_path)
            os.makedirs(self.output_dir, exist_ok=True)
            with open(temp_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                for aid in self.alpha_ids:
                    writer.writerow([aid])
            self.logger.info(f"Unfinished alpha IDs saved to {temp_path}.")
            
    def process_daily_alpha_ids(self, date_str):
        """处理每日的alpha IDs"""
        
        # 2. 生成stone_bag文件（保存到数据库）
        self.read_and_filter_alpha_ids(date_str)
        
        # 3. 从数据库读取stone_gold_bag_table中对应日期的记录
        record_count = self.stone_gold_bag_dao.count_by_datetime(date_str)
        alpha_result = []
        if record_count > 0:
            records = self.stone_gold_bag_dao.get_by_datetime(date_str)
            for record in records:
                try:
                    # 解析记录中的字段
                    is_data = ast.literal_eval(record['is']) if isinstance(record['is'], str) else record['is']
                    settings_data = ast.literal_eval(record['settings']) if isinstance(record['settings'], str) else record['settings']
                    alpha_result.append({
                        'id': record['id'],
                        'settings': settings_data,
                        'is': is_data
                    })
                except Exception as e:
                    self.logger.warning(f"Failed to parse record from database {record.get('id')}: {e}")
                    continue

            # 按fitness值从小到大排序
            alpha_result.sort(key=lambda x: x['is'].get('fitness', 0))
            self.logger.info(f"Loaded and sorted {len(alpha_result)} alphas from database for date {date_str}")
            if len(alpha_result) > 0:
                self.logger.info("First 3 alphas by fitness:")
                for alpha in alpha_result[:3]:
                    self.logger.info(f"ID: {alpha['id']}, fitness: {alpha['is'].get('fitness', 0)}")
                
                self.logger.info("Last 3 alphas by fitness:") 
                for alpha in alpha_result[-3:]:
                    self.logger.info(f"ID: {alpha['id']}, fitness: {alpha['is'].get('fitness', 0)}")
        
        # 4. 调用alpha_prune.py方法
        if alpha_result:
            try:
                os_alpha_ids, os_alpha_rets = generate_comparison_data(
                    alpha_result, 
                    self.username, 
                    self.password
                )
                filtered_alphas = calculate_correlations(
                    os_alpha_ids,
                    os_alpha_rets,
                    self.username,
                    self.password,
                    corr_threshold=0.8
                )
                self.logger.info(f"Filtered alphas by correlation: {sum(len(v) for v in filtered_alphas.values())} alphas remaining")
                
              
                for region, alpha_ids in filtered_alphas.items():
                    self.alpha_ids.extend(alpha_ids)
                    self.logger.info(f"region: {region},{len(alpha_ids)} alphas remaining")
                self.alpha_ids = list(set(self.alpha_ids))
                
                # 更新total和idx
                self.total = len(self.alpha_ids)
                self.idx = 0
                self.logger.info(f"Total alpha IDs to process: {self.total}")
            except Exception as e:
                self.logger.error(f"Failed to filter alphas by correlation: {e}")
        
        self.logger.info("Resuming alpha ID processing with updated list")

    def run_scheduler(self):
        """
        运行每日调度程序。
        此方法旨在由执行器在单独的线程中运行。
        """
        self.logger.info("Starting daily schedule...")
        self._scheduler_running = True

        def job():
            self.logger.info("Scheduled task triggered at 12:30.")
            self.date_str = self.get_yesterday_date()
            self.logger.info(f"Processing date updated to: {self.date_str}")
            self.process_daily_alpha_ids(self.date_str)

        schedule.clear()
        schedule.every().day.at("12:30").do(job).tag('daily_alpha_task')
        
        self.logger.info(f"Next scheduled task will run at: {schedule.next_run()}")

        i = 0
        while self._scheduler_running:
            schedule.run_pending()
            if i % 300 == 0:
                i = 0
                next_run = schedule.next_run()
                if next_run:
                    self.logger.info(f"Next scheduled task in: {(next_run - datetime.now()).total_seconds() / 3600:.2f} hours")
            i += 1
            time.sleep(1)
        
        self.logger.info("Scheduler loop stopped.")

    def run_processing_loop(self):
        """
        运行主要的alpha ID处理循环。
        此方法旨在由执行器在单独的线程中运行。
        """
        self.logger.info("==== 启动初始化 ====")
        self.initialize_alpha_ids()
        self.logger.info("==== 启动处理循环 ====")
        self.process_alpha_ids_loop()

    def stop_daily_schedule(self):
        """安全停止定时任务"""
        if self._scheduler_running:
            self._scheduler_running = False
            schedule.clear()
            self.logger.info("Daily scheduler stopped")
        
    def on_config_update(self, new_config):
        """
        配置更新回调函数
        当配置文件更新时，检查init_date是否被更新
        如果更新，则使用新的init_date值调用process_daily_alpha_ids方法
        """
        new_init_date = new_config.get('init_date_str')
        if new_init_date and new_init_date != self.init_date:
            self.logger.info(f"init_date updated from {self.init_date} to {new_init_date}, triggering reprocessing")
            
            # 更新当前实例的init_date
            self.init_date = new_init_date
            
            # 在新线程中调用处理函数避免阻塞
            threading.Thread(
                target=self.process_daily_alpha_ids,
                args=(new_init_date,),
                daemon=True
            ).start()
        else:
            self.logger.info("init_date not updated or same as before")
