import time
import pandas as pd
import requests
import schedule
import threading
from logger import Logger
import os
import csv
from datetime import datetime, timedelta
import ast

logger = Logger()

class ProcessSimulatedAlphas:
    def __init__(self, data_dir, output_dir, specified_sharpe, specified_fitness, username, password):
        # 初始化参数
        self.data_dir = data_dir
        self.output_dir = output_dir
        self.SPECIFIED_SHARPE = specified_sharpe
        self.SPECIFIED_FITNESS = specified_fitness
        self.username = username
        self.password = password
        self.logger = Logger()  # 初始化 logger
        self.session = self.sign_in(username, password)
        self.alpha_ids = []
        self.gold_bag = []

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

    def read_and_filter_alpha_ids(self, date_str):
        """读取并筛选符合条件的 alphaId"""
        self.alpha_ids = []
        simulated_alphas_file = os.path.join(self.data_dir, f'simulated_alphas.csv.{date_str}')
        if not os.path.exists(simulated_alphas_file):
            self.logger.error(f"File {simulated_alphas_file} not found. Please check file generation process.")
            return
        self.logger.info(f"Start processing file: {simulated_alphas_file}")
        with open(simulated_alphas_file, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            try:
                header = next(reader)  # 跳过表头
                if len(header) < 19:
                    self.logger.error("CSV file header has fewer than 19 columns, cannot process.")
                    return
            except StopIteration:
                self.logger.error("CSV file is empty.")
                return

            for row in reader:
                if len(row) < 19:
                    self.logger.warning(f"Skipping row with insufficient columns: {row}")
                    continue
                alpha_id = row[0]
                try:
                    # 清理字符串，移除首尾引号
                    metrics_str = row[18].strip().strip('"\'')
                    if not metrics_str:
                        self.logger.warning(f"Metrics data empty for alphaId {alpha_id}, skipping.")
                        continue
                    # 使用 ast.literal_eval 解析 Python 字典字符串
                    metrics = ast.literal_eval(metrics_str)
                    sharpe = metrics.get('sharpe')
                    fitness = metrics.get('fitness')
                    if sharpe is None or fitness is None:
                        self.logger.warning(f"Missing sharpe or fitness for alphaId {alpha_id}, skipping.")
                        continue
                    if sharpe >= self.SPECIFIED_SHARPE and fitness >= self.SPECIFIED_FITNESS:
                        self.alpha_ids.append(alpha_id)
                        self.logger.info(f"alphaId {alpha_id} meets criteria: sharpe={sharpe}, fitness={fitness}")
                except (SyntaxError, ValueError) as e:
                    self.logger.error(f"Error parsing metrics for alphaId {alpha_id}: {e}, raw data: {row[18]}")
                    continue
                except IndexError as e:
                    self.logger.error(f"Index error for alphaId {alpha_id}: {e}, row: {row}")
                    continue
        self.logger.info(f"Filtered {len(self.alpha_ids)} qualified alphaIds.")

    def process_alpha_ids(self, alpha_ids):
        """处理 alphaId 列表，获取 gold_bag"""
        self.gold_bag = []
        total = len(alpha_ids)
        if total == 0:
            self.logger.info("No alphaIds to process.")
            return

        self.logger.info(f"Start checking {total} alphaIds...")
        for idx, alpha_id in enumerate(alpha_ids):
            if idx % 200 == 0:
                self.session = self.sign_in(self.username, self.password)
                if not self.session:
                    self.logger.error("Invalid session, skipping current iteration.")
                    continue
            self.logger.info(f"Progress: {idx} checked, {total - idx} remaining.")
            pc = self.get_check_submission(self.session, alpha_id)
            if pc == "sleep":
                self.logger.warning(f"alphaId {alpha_id} requires cooldown, re-queuing.")
                time.sleep(100)
                self.session = self.sign_in(self.username, self.password)
                alpha_ids.append(alpha_id)
            elif pc != pc:  # NaN check
                self.logger.warning(f"Self-correlation check failed for alphaId {alpha_id}, re-queuing.")
                time.sleep(100)
                alpha_ids.append(alpha_id)
            elif pc == "fail":
                self.logger.info(f"alphaId {alpha_id} check failed, skipping.")
                continue
            elif pc == "error":
                self.logger.error(f"Error checking alphaId {alpha_id}, skipping.")
                continue
            else:
                self.logger.info(f"alphaId {alpha_id} check successful, added to gold_bag.")
                self.gold_bag.append((alpha_id, pc))
        self.logger.info(f"Processed {total} alphaIds, {len(self.gold_bag)} successful.")

    def get_check_submission(self, s, alpha_id):
        """获取 alpha 的检查提交结果"""
        while True:
            try:
                result = s.get(f"https://api.worldquantbrain.com/alphas/{alpha_id}/check")
                if "retry-after" in result.headers:
                    time.sleep(float(result.headers["Retry-After"]))
                else:
                    break
            except Exception as e:
                self.logger.error(f"Failed to get check result for alphaId {alpha_id}: {e}")
                return "error"
        try:
            if result.json().get("is", 0) == 0:
                self.logger.warning(f"alphaId {alpha_id} not logged in, returning 'sleep'.")
                return "sleep"
            checks_df = pd.DataFrame(result.json()["is"]["checks"])
            pc = checks_df[checks_df.name == "SELF_CORRELATION"]["value"].values[0]
            if not any(checks_df["result"] == "FAIL"):
                return pc
            else:
                self.logger.info(f"alphaId {alpha_id} failed checks.")
                return "fail"
        except Exception as e:
            self.logger.error(f"Error processing check result for alphaId {alpha_id}: {e}")
            return "error"

    def save_gold_bag(self, date_str):
        """保存 gold_bag 到 CSV 文件"""
        gold_bag_file = os.path.join(self.output_dir, f'gold_bag.csv.{date_str}')
        os.makedirs(self.output_dir, exist_ok=True)
        with open(gold_bag_file, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['alpha_id', 'pc'])
            writer.writerows(self.gold_bag)
        self.logger.info(f"gold_bag saved to {gold_bag_file}.")
        return gold_bag_file

    def run(self):
        """运行每日任务，处理前一天的数据"""
        date_str = self.get_yesterday_date()
        self.read_and_filter_alpha_ids(date_str)
        if not self.alpha_ids:
            self.logger.info("No qualified alphaIds found, task completed.")
            return
        self.process_alpha_ids(self.alpha_ids)
        gold_bag_file = self.save_gold_bag(date_str)
        self.logger.info(f"Processing completed for {date_str}, results saved to {gold_bag_file}.")

    def start_schedule(self):
        """启动每日调度任务，每天 00:30 执行"""
        schedule.every().day.at("00:30").do(self.run)

        def run_schedule():
            while True:
                schedule.run_pending()
                time.sleep(1)

        threading.Thread(target=run_schedule, daemon=True).start()
        self.logger.info("Daily scheduler started, will run at 00:30 every day.")

