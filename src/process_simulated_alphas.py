import time
import pandas as pd
import requests
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



class ProcessSimulatedAlphas:
    def __init__(self, data_dir, output_dir, specified_sharpe, specified_fitness, username, password, signal_manager=None):
        self.data_dir = data_dir
        self.output_dir = output_dir
        self.SPECIFIED_SHARPE = specified_sharpe
        self.SPECIFIED_FITNESS = specified_fitness
        self.username = username
        self.password = password
        self.logger = Logger()

        self.alpha_ids: List[str] = []
        self.total = 0
        self.idx = 0
        self.date_str = self.get_yesterday_date()
        self.session = self.sign_in(username, password)
        self._scheduler_running = False
        self._scheduler_thread = None

        # 注册信号处理
        if signal_manager:
            signal_manager.add_handler(self.handle_exit_signal)
        else:
            self.logger.warning("未提供 SignalManager，ProcessSimulatedAlphas 无法注册信号处理函数")

    def handle_exit_signal(self, signum, frame):
        self.logger.info(f"Received shutdown signal {signum}, saving unfinished alpha IDs...")
        self.save_unfinished_alpha_ids()
        self.stop_daily_schedule()

    def sign_in(self, username, password):
        """登录 WorldQuant BRAIN 平台"""
        s = requests.Session()
        s.auth = (username, password)
        count = 0
        count_limit = 30

        while True:
            try:
                response = s.post('https://api.worldquantbrain.com/authentication')
                response.raise_for_status()
                break
            except Exception as e:
                count += 1
                self.logger.error(f"Connection lost, attempting to re-login. Error: {e}")
                time.sleep(15)
                if count > count_limit:
                    self.logger.error(f"Too many failed login attempts for {username}, returning None.")
                    return None
        self.logger.info("Successfully logged in to BRAIN platform.")
        return s

    def get_yesterday_date(self):
        """获取前一天的日期，格式为 YYYY-MM-DD"""
        return (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    def initialize_alpha_ids(self):
        self.logger.info("Initializing alpha_ids...")

        filtered_file = os.path.join(self.output_dir, f'stone_bag.csv.{self.date_str}')
        temp_file = os.path.join(self.output_dir, 'unfinished_alpha_ids.temp.csv')

        if os.path.exists(temp_file):
            with open(temp_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                self.alpha_ids = [row[0] for row in reader if row]
                self.logger.info(f"Loaded {len(self.alpha_ids)} unfinished alpha IDs from temp file.")
        else:
            self.logger.info("No temp file. Assuming processing is ongoing.")

        if not os.path.exists(filtered_file):
            self.logger.info("Filtered file does not exist. Running filter step and loading alpha IDs.")
            self.read_and_filter_alpha_ids(self.date_str)
            stone_bag_file = os.path.join(self.output_dir, f'stone_bag.csv.{self.date_str}')
            
            # 3. 读取stone_bag文件并排序
            alpha_result = []
            if os.path.exists(stone_bag_file):
                with open(stone_bag_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        try:
                            is_data = ast.literal_eval(row['is'].strip().strip('"\''))
                            alpha_result.append({
                                'id': row['id'],
                                'settings': ast.literal_eval(row['settings']),
                                'is': is_data
                            })
                        except Exception as e:
                            self.logger.warning(f"Failed to parse alpha {row.get('id')}: {e}")
                            continue
                
                # 按sharpe值从小到大排序
                alpha_result.sort(key=lambda x: x['is'].get('sharpe', 0))
                self.logger.info(f"Loaded and sorted {len(alpha_result)} alphas from stone_bag file")
            
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
                    
                    # 5. 追加到alpha_ids
                    for region, alpha_ids in filtered_alphas.items():
                        self.alpha_ids.extend(alpha_ids)
                        self.logger.info(f"region: {region},{len(alpha_ids)} alphas remaining")
                    self.alpha_ids = list(set(self.alpha_ids))
                except Exception as e:
                    self.logger.error(f"Failed to filter alphas by correlation: {e}")
        self.total = len(self.alpha_ids)
        self.logger.info(f"Total alpha IDs to process: {self.total}")

    def save_stone_bag(self, filtered_rows: List[list], date_str: str, header: List[str]) -> None:
        """
        将过滤后的行按 sharpe 值降序排列，保存到 stone_bag.csv.{date_str} 文件，使用传入的表头。

        Args:
            filtered_rows (List[list]): 过滤后的行数据，每行包含原始 CSV 的所有列。
            date_str (str): 日期字符串，用于文件名。
            header (List[str]): 输入文件的表头。

        Returns:
            None
        """
        if not filtered_rows:
            self.logger.info("No filtered rows to save to stone_bag.csv")
            return
        
        
        # 按 sharpe 值降序排序
        try:
            sorted_rows = sorted(
                filtered_rows,
                key=lambda row: ast.literal_eval(row[18].strip().strip('"\'')).get('sharpe', float('-inf')),
                reverse=True
            )
        except (SyntaxError, ValueError) as e:
            self.logger.error(f"Error sorting rows by sharpe: {e}")
            return
        
        # 构造输出文件路径
        output_file_path = os.path.join(self.output_dir, f'stone_bag.csv.{date_str}')
        os.makedirs(self.output_dir, exist_ok=True)
        
        # 保存到 CSV 文件
        try:
            with open(output_file_path, 'w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(header)  # 使用传入的表头
                writer.writerows(sorted_rows)
            self.logger.info(f"Saved {len(sorted_rows)} rows to {output_file_path}")
        except Exception as e:
            self.logger.error(f"Error saving to {output_file_path}: {e}")
    def load_alpha_data(self, simulated_alphas_file) -> pd.DataFrame:
        """
        从 simulated_alphas.csv 文件加载 alpha 数据。

        返回:
            pd.DataFrame: 包含 alpha 数据的 DataFrame。

        异常:
            FileNotFoundError: 如果 CSV 文件不存在。
        """
        file_path = simulated_alphas_file
        try:
            # 使用更灵活的CSV解析方式
            df = pd.read_csv(file_path, quoting=csv.QUOTE_ALL, escapechar='\\', on_bad_lines='warn')
            self.logger.info(f"Successfully loaded file: {file_path}")
            return df
        except FileNotFoundError:
            self.logger.error(f"File not found: {file_path}")
            raise

    def read_and_filter_alpha_ids(self, date_str):
        """读取并筛选符合条件的 alphaId，并保存到 output 目录"""
        alpha_ids = []
        filtered_rows = []  # 存储符合条件的完整行
        simulated_alphas_file = os.path.join(self.data_dir, f'simulated_alphas.csv.{date_str}')
        
        if not os.path.exists(simulated_alphas_file):
            self.logger.error(f"File {simulated_alphas_file} not found. Please check file generation process.")
            return
        
        self.logger.info(f"Start processing file: {simulated_alphas_file}")
        df = self.load_alpha_data(simulated_alphas_file=simulated_alphas_file)
        # 从DataFrame获取表头
        header = df.columns.tolist()
            
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
                #self.logger.info(f"read_and_filter_alpha_ids alpha {row['id']}:sharpe={sharpe}, fitness={fitness}")
                # 检查 fitness 和 sharpe 是否满足条件
                if fitness >= self.SPECIFIED_FITNESS and sharpe >= self.SPECIFIED_SHARPE:
                    # 筛选符合条件的alpha Id
                    alpha_ids.append(row['id'])
                    # 保存完整行数据
                    filtered_rows.append(row.tolist())
            except Exception as e:
                self.logger.warning(f"Failed to process alpha {row['id']}: {e}, raw is data: {is_str[:100]}...")
                continue
        
        self.logger.info(f"Filtered {len(alpha_ids)} qualified alphaIds.")

        # 保存符合条件的 alpha ID 到 output 目录
        if alpha_ids:
            output_file_path = os.path.join(self.output_dir, f'filtered_alpha_ids.{date_str}.csv')
            os.makedirs(self.output_dir, exist_ok=True)
            with open(output_file_path, 'w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(['alpha_id'])  # 写入表头
                for alpha_id in alpha_ids:
                    writer.writerow([alpha_id])
            self.logger.info(f"Saved filtered alpha IDs to {output_file_path}")
        else:
            self.logger.info("No alpha IDs to save.")
        
        # 调用 save_stone_bag 保存完整行，传入表头
        self.save_stone_bag(filtered_rows, date_str, header=header)

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
                self.session = self.sign_in(self.username, self.password)
                self.logger.info(f"Progress: {self.total - len(self.alpha_ids)}/{self.total} processed.")
                if not self.session:
                    continue

            self.logger.info(f"[{self.idx}/{self.total}] Checking alphaId: {alpha_id}")
            result = self.get_check_submission(self.session, alpha_id)

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
                self.session = self.sign_in(self.username, self.password)
            elif isinstance(result, tuple):
                # 成功获取三个值的情况
                pc, sharpe, fitness = result
                self.logger.info(f"alphaId {alpha_id} passed check, saving.")
                self.save_gold_bag([(alpha_id, pc, sharpe, fitness)], self.date_str)

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

    def save_gold_bag(self, gold_bag, date_str):
        """保存合格alpha到CSV文件，包含alpha_id, pc, sharpe和fitness"""
        gold_bag_file = os.path.join(self.output_dir, f'gold_bag.csv.{date_str}')
        os.makedirs(self.output_dir, exist_ok=True)

        if os.path.exists(gold_bag_file):
            if os.path.getsize(gold_bag_file) == 0:
                mode = 'w'
            else:
                mode = 'a'
        else:
            mode = 'w'

        with open(gold_bag_file, mode, newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            if mode == 'w':
                writer.writerow(['alpha_id', 'pc', 'sharpe', 'fitness'])
            writer.writerows(gold_bag)

        self.logger.info(f"gold_bag saved to {gold_bag_file}.")
        return gold_bag_file
    
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

    def start_daily_schedule(self):
        """安全启动定时任务"""
        if self._scheduler_running:
            self.logger.warning("Scheduler already running, skipping...")
            return

        self.logger.info("Starting daily schedule...")
        self._scheduler_running = True
        def job():
            self.logger.info("Scheduled task triggered at 12:30.")
            
            # 1. 刷新date_str
            self.date_str = self.get_yesterday_date()
            self.logger.info(f"Processing date updated to: {self.date_str}")
            
            # 2. 生成stone_bag文件
            self.read_and_filter_alpha_ids(self.date_str)
            stone_bag_file = os.path.join(self.output_dir, f'stone_bag.csv.{self.date_str}')
            
            # 3. 读取stone_bag文件并排序
            alpha_result = []
            if os.path.exists(stone_bag_file):
                with open(stone_bag_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        try:
                            is_data = ast.literal_eval(row['is'].strip().strip('"\''))
                            alpha_result.append({
                                'id': row['id'],
                                'settings': ast.literal_eval(row['settings']),
                                'is': is_data
                            })
                        except Exception as e:
                            self.logger.warning(f"Failed to parse alpha {row.get('id')}: {e}")
                            continue
                
                # 按sharpe值从小到大排序
                alpha_result.sort(key=lambda x: x['is'].get('sharpe', 0))
                self.logger.info(f"Loaded and sorted {len(alpha_result)} alphas from stone_bag file")
            
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
                    
                    # 5. 追加到alpha_ids
                    for region, alpha_ids in filtered_alphas.items():
                        self.alpha_ids.extend(alpha_ids)
                        self.logger.info(f"region: {region},{len(alpha_ids)} alphas remaining")
                    self.alpha_ids = list(set(self.alpha_ids))
                except Exception as e:
                    self.logger.error(f"Failed to filter alphas by correlation: {e}")
            
            # 6. 更新total和idx
            self.total = len(self.alpha_ids)
            self.idx = 0
            self.logger.info(f"Total alpha IDs to process: {self.total}")

        # 清除可能存在的旧任务
        schedule.clear()
        schedule.every().day.at("12:30").do(job).tag('daily_alpha_task')

        def run_scheduler():
            while self._scheduler_running:  # 使用标志位控制循环
                schedule.run_pending()
                time.sleep(1)
            self.logger.info("Scheduler thread stopped")

        # 确保旧线程已停止
        if self._scheduler_thread and self._scheduler_thread.is_alive():
            self._scheduler_thread.join(timeout=1)

        self._scheduler_thread = threading.Thread(
            target=run_scheduler,
            name="AlphaScheduler",
            daemon=True
        )
        self._scheduler_thread.start()
        self.logger.info("Scheduler thread started.")
    
    def stop_daily_schedule(self):
        """安全停止定时任务"""
        self._scheduler_running = False
        schedule.clear()
        if self._scheduler_thread:
            self._scheduler_thread.join(timeout=5)
        self.logger.info("Daily scheduler stopped")
    
    def manage_process(self):
        self.logger.info("==== 启动初始化 ====")
        self.initialize_alpha_ids()

        # 调试：打印当前任务数
        self.logger.debug(f"Pre-check jobs: {len(schedule.get_jobs())}")
        
        self.logger.info("==== 启动定时任务 ====")
        self.start_daily_schedule()
        
        # 再次验证
        self.logger.debug(f"Post-check jobs: {len(schedule.get_jobs())}")

        self.logger.info("==== 启动处理循环 ====")
        self.process_thread = threading.Thread(target=self.process_alpha_ids_loop, daemon=True)
        self.process_thread.start()

    

"""
data_dir = "./data"
output_dir = "./output"
specified_sharpe = 1.58
specified_fitness = 1.0
stone_bag_file = os.path.join(output_dir, 'stone_bag.csv.2025-05-25')
logger = Logger()
            
# 3. 读取stone_bag文件并排序
alpha_result = []
if os.path.exists(stone_bag_file):
    with open(stone_bag_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                is_data = ast.literal_eval(row['is'].strip().strip('"\''))
                alpha_result.append({
                    'id': row['id'],
                    'settings': ast.literal_eval(row['settings']),
                    'is': is_data
                })
            except Exception as e:
                logger.warning(f"Failed to parse alpha {row.get('id')}: {e}")
                continue
    
    # 按sharpe值从小到大排序
    alpha_result.sort(key=lambda x: x['is'].get('sharpe', 0))
    logger.info(f"Loaded and sorted {len(alpha_result)} alphas from stone_bag file")
    if len(alpha_result) > 0:
        logger.info("First 3 alphas:")
        for alpha in alpha_result[:3]:
            logger.info(f"ID: {alpha['id']}, sharpe: {alpha['is'].get('sharpe', 0)}")
        
        logger.info("Last 3 alphas:") 
        for alpha in alpha_result[-3:]:
            logger.info(f"ID: {alpha['id']}, sharpe: {alpha['is'].get('sharpe', 0)}")


# 4. 调用alpha_prune.py方法
if alpha_result:
    try:
        os_alpha_ids, os_alpha_rets = generate_comparison_data(
            alpha_result, 
            username, 
            password
        )

        filtered_alphas = calculate_correlations(
            os_alpha_ids,
            os_alpha_rets,
            username,
            password,
            corr_threshold=0.7
        )
        logger.info(f"Filtered alphas by correlation: {sum(len(v) for v in filtered_alphas.values())} alphas remaining")
        
        # 5. 追加到alpha_ids
        for region, alpha_ids in filtered_alphas.items():
            logger.info(f"region:{region}, alpha_ids:{len(alpha_ids)}")
    except Exception as e:
        logger.error(f"Failed to filter alphas by correlation: {e}")
"""
