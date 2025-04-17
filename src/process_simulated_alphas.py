import time
import pandas as pd
import requests
import schedule
import threading
import signal
import os
import csv
from datetime import datetime, timedelta
import ast
from typing import List
from logger import Logger



class ProcessSimulatedAlphas:
    def __init__(self, data_dir, output_dir, specified_sharpe, specified_fitness, username, password):
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

        signal.signal(signal.SIGTERM, self.handle_exit_signal)
        signal.signal(signal.SIGINT, self.handle_exit_signal)

    def handle_exit_signal(self, signum, frame):
        self.logger.info(f"Received shutdown signal {signum}, saving unfinished alpha IDs...")
        self.save_unfinished_alpha_ids()

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

        filtered_file = os.path.join(self.output_dir, f'filtered_alpha_ids.{self.date_str}.csv')
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
            if os.path.exists(filtered_file):
                new_ids = []
                with open(filtered_file, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    next(reader, None)
                    new_ids = [row[0] for row in reader if row]
                self.alpha_ids += new_ids
                self.alpha_ids = list(set(self.alpha_ids))
                self.logger.info(f"Appended {len(new_ids)} new alpha IDs. Total now: {len(self.alpha_ids)}")
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

    def read_and_filter_alpha_ids(self, date_str):
        """读取并筛选符合条件的 alphaId，并保存到 output 目录"""
        alpha_ids = []
        filtered_rows = []  # 存储符合条件的完整行
        simulated_alphas_file = os.path.join(self.data_dir, f'simulated_alphas.csv.{date_str}')
        
        if not os.path.exists(simulated_alphas_file):
            self.logger.error(f"File {simulated_alphas_file} not found. Please check file generation process.")
            return
        
        self.logger.info(f"Start processing file: {simulated_alphas_file}")
        with open(simulated_alphas_file, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            try:
                header = next(reader)  # 读取表头
                if not header:
                    self.logger.error("CSV file header is empty.")
                    return
            except StopIteration:
                self.logger.error("CSV file is empty.")
                return

            
            for row in reader:
                if len(row) != len(header):
                    self.logger.warning(f"Skipping row with mismatched columns: {row}")
                    continue
                alpha_id = row[0]
                try:
                    metrics_str = row[18].strip().strip('"\'')
                    if not metrics_str:
                        self.logger.warning(f"Metrics data empty for alphaId {alpha_id}, skipping.")
                        continue
                    metrics = ast.literal_eval(metrics_str)
                    sharpe = metrics.get('sharpe')
                    fitness = metrics.get('fitness')
                    if sharpe is None or fitness is None:
                        self.logger.warning(f"Missing sharpe or fitness for alphaId {alpha_id}, skipping.")
                        continue
                    if sharpe >= self.SPECIFIED_SHARPE and fitness >= self.SPECIFIED_FITNESS:
                        alpha_ids.append(alpha_id)
                        filtered_rows.append(row)  # 保存完整行
                        self.logger.info(f"alphaId {alpha_id} meets criteria: sharpe={sharpe}, fitness={fitness}")
                except (SyntaxError, ValueError) as e:
                    self.logger.error(f"Error parsing metrics for alphaId {alpha_id}: {e}, raw data: {row[18]}")
                    continue
                except IndexError as e:
                    self.logger.error(f"Index error for alphaId {alpha_id}: {e}, row: {row}")
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
            pc = self.get_check_submission(self.session, alpha_id)

            if pc == "sleep" or pc != pc:
                if pc == "sleep":
                    self.logger.warning(f"alphaId {alpha_id} needs cooldown, re-queuing.")
                else:
                    self.logger.warning(f"prod-correlation check failed for alphaId {alpha_id}, re-queuing.")
                self.alpha_ids.append(alpha_id)
                wait_time = max(5, min(60, int(300 / (len(self.alpha_ids) + 1))))
                self.logger.info(f"Sleeping for {wait_time} seconds before retrying.")
                time.sleep(wait_time)
                self.session = self.sign_in(self.username, self.password)
            elif pc == "fail":
                self.logger.info(f"alphaId {alpha_id} failed check.")
            elif pc == "error":
                self.logger.error(f"alphaId {alpha_id} error during check.")
            else:
                self.logger.info(f"alphaId {alpha_id} passed check, saving.")
                self.save_gold_bag([(alpha_id, pc)], self.date_str)

    def get_check_submission(self, s, alpha_id):
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
            pc = checks_df[checks_df.name == "PROD_CORRELATION"]["value"].values[0]

            failed_checks = checks_df[checks_df["result"] == "FAIL"]
            if not any(checks_df["result"] == "FAIL"):
                return pc
            else:
                self.logger.info(f"alphaId {alpha_id} check failed, skipping.")
                self.logger.info(f"Fail reasons:\n{failed_checks.to_string(index=False)}")
                return "fail"
        except:
            return "error"

    def save_gold_bag(self, gold_bag, date_str):
        """保存 gold_bag 到 CSV 文件，若文件存在则追加，若不存在则新建"""
        gold_bag_file = os.path.join(self.output_dir, f'gold_bag.csv.{date_str}')
        os.makedirs(self.output_dir, exist_ok=True)

        if os.path.exists(gold_bag_file):
            # 文件存在，检查是否为空
            if os.path.getsize(gold_bag_file) == 0:
                mode = 'w'  # 为空，写入表头和数据
            else:
                mode = 'a'  # 不为空，追加数据
        else:
            mode = 'w'  # 不存在，创建新文件

        with open(gold_bag_file, mode, newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            if mode == 'w':
                writer.writerow(['alpha_id', 'pc'])  # 新建文件时写入表头
            writer.writerows(gold_bag)  # 写入数据

        self.logger.info(f"gold_bag saved to {gold_bag_file}.")
        return gold_bag_file
    
    def save_unfinished_alpha_ids(self):
        temp_path = os.path.join(self.output_dir, 'unfinished_alpha_ids.temp.csv')
        os.makedirs(self.output_dir, exist_ok=True)
        with open(temp_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for aid in self.alpha_ids:
                writer.writerow([aid])
        self.logger.info(f"Unfinished alpha IDs saved to {temp_path}.")

    def start_daily_schedule(self):
        self.logger.info("Starting daily schedule...")

        def job():
            self.logger.info("Scheduled task triggered at 00:30.")
            date_str = self.get_yesterday_date()
            self.read_and_filter_alpha_ids(date_str)
            filtered_file = os.path.join(self.output_dir, f'filtered_alpha_ids.{date_str}.csv')
            if os.path.exists(filtered_file):
                new_ids = []
                with open(filtered_file, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    next(reader, None)
                    new_ids = [row[0] for row in reader if row]
                self.alpha_ids += new_ids
                self.alpha_ids = list(set(self.alpha_ids))
                self.total = len(self.alpha_ids)
                self.idx = 0
                self.logger.info(f"Appended {len(new_ids)} new alpha IDs. Total now: {self.total}")

        schedule.every().day.at("00:30").do(job)

        def run_scheduler():
            while True:
                schedule.run_pending()
                time.sleep(1)

        threading.Thread(target=run_scheduler, daemon=True).start()
        self.logger.info("Scheduler thread started.")
    
    def manage_process(self):
        self.logger.info("==== 启动初始化 ====")
        self.initialize_alpha_ids()

        self.logger.info("==== 启动定时任务 ====")
        self.start_daily_schedule()

        self.logger.info("==== 启动处理循环 ====")
        self.process_alpha_ids_loop()

"""
data_dir = "./data"
output_dir = "./output"
specified_sharpe = 1.58
specified_fitness = 1.0
username = "tianyuan411249897@gmail.com"
password = "k9979kui8"
processor = ProcessSimulatedAlphas(data_dir, output_dir, specified_sharpe, specified_fitness, username, password)
processor.manage_process()
"""




