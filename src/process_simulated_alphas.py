import tempfile
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

    def load_alpha_ids_batch(self, date_str, batch_size):
        """从 filtered_alpha_ids.{date_str}.csv 文件中加载指定数量的 alphaId，更新文件"""
        # 构造文件路径
        alpha_ids_file = os.path.join(self.output_dir, f'filtered_alpha_ids.{date_str}.csv')
        if not os.path.exists(alpha_ids_file):
            logger.error(f"File {alpha_ids_file} not found.")
            return []

        # 读取所有 alpha ID
        all_alpha_ids = []
        with open(alpha_ids_file, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            try:
                header = next(reader)  # 跳过表头
                if header != ['alpha_id']:
                    logger.error(f"Invalid header in {alpha_ids_file}: {header}")
                    return []
            except StopIteration:
                logger.info(f"File {alpha_ids_file} is empty.")
                return []

            for row in reader:
                if not row or len(row) < 1:
                    logger.warning(f"Skipping invalid row in {alpha_ids_file}: {row}")
                    continue
                all_alpha_ids.append(row[0])

        # 确定加载数量
        load_count = min(batch_size, len(all_alpha_ids))
        loaded_alpha_ids = all_alpha_ids[:load_count]
        remaining_alpha_ids = all_alpha_ids[load_count:]

        # 如果剩余 alpha ID 为空，直接删除文件
        if not remaining_alpha_ids:
            try:
                os.remove(alpha_ids_file)
                logger.info(f"No remaining alpha IDs, deleted {alpha_ids_file}.")
            except Exception as e:
                logger.error(f"Error deleting {alpha_ids_file}: {e}")
                raise
        else:
            # 写入剩余 alpha ID 到临时文件 temp_alpha_ids.{date_str}.csv
            temp_file = os.path.join(self.output_dir, f'temp_alpha_ids.{date_str}.csv')
            try:
                with open(temp_file, 'w', newline='', encoding='utf-8') as file:
                    writer = csv.writer(file)
                    writer.writerow(['alpha_id'])  # 写入表头
                    for alpha_id in remaining_alpha_ids:
                        writer.writerow([alpha_id])
                logger.info(f"Wrote {len(remaining_alpha_ids)} remaining alpha IDs to temporary file {temp_file}")

                # 删除原文件并重命名临时文件
                os.remove(alpha_ids_file)
                os.rename(temp_file, alpha_ids_file)
                logger.info(f"Replaced {alpha_ids_file} with updated alpha IDs, {len(remaining_alpha_ids)} remaining.")
            except Exception as e:
                logger.error(f"Error updating {alpha_ids_file}: {e}")
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                raise

        logger.info(f"Loaded {len(loaded_alpha_ids)} alpha IDs from {alpha_ids_file}")
        return loaded_alpha_ids

    def get_yesterday_date(self):
        """获取前一天的日期，格式为 YYYY-MM-DD"""
        return (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    def read_and_filter_alpha_ids(self, date_str):
        """读取并筛选符合条件的 alphaId，并保存到 output 目录"""
        alpha_ids = []
        simulated_alphas_file = os.path.join(self.data_dir, f'simulated_alphas.csv.{date_str}')
        if not os.path.exists(simulated_alphas_file):
            logger.error(f"File {simulated_alphas_file} not found. Please check file generation process.")
            return
        logger.info(f"Start processing file: {simulated_alphas_file}")
        with open(simulated_alphas_file, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            try:
                header = next(reader)  # 跳过表头
                if len(header) < 19:
                    logger.error("CSV file header has fewer than 19 columns, cannot process.")
                    return
            except StopIteration:
                logger.error("CSV file is empty.")
                return

            for row in reader:
                if len(row) < 19:
                    logger.warning(f"Skipping row with insufficient columns: {row}")
                    continue
                alpha_id = row[0]
                try:
                    metrics_str = row[18].strip().strip('"\'')
                    if not metrics_str:
                        logger.warning(f"Metrics data empty for alphaId {alpha_id}, skipping.")
                        continue
                    metrics = ast.literal_eval(metrics_str)
                    sharpe = metrics.get('sharpe')
                    fitness = metrics.get('fitness')
                    if sharpe is None or fitness is None:
                        logger.warning(f"Missing sharpe or fitness for alphaId {alpha_id}, skipping.")
                        continue
                    if sharpe >= self.SPECIFIED_SHARPE and fitness >= self.SPECIFIED_FITNESS:
                        alpha_ids.append(alpha_id)
                        logger.info(f"alphaId {alpha_id} meets criteria: sharpe={sharpe}, fitness={fitness}")
                except (SyntaxError, ValueError) as e:
                    logger.error(f"Error parsing metrics for alphaId {alpha_id}: {e}, raw data: {row[18]}")
                    continue
                except IndexError as e:
                    logger.error(f"Index error for alphaId {alpha_id}: {e}, row: {row}")
                    continue
        logger.info(f"Filtered {len(alpha_ids)} qualified alphaIds.")

        # 保存符合条件的 alpha ID 到 output 目录
        if alpha_ids:
            output_file_path = os.path.join(self.output_dir, f'filtered_alpha_ids.{date_str}.csv')
            os.makedirs(self.output_dir, exist_ok=True)
            with open(output_file_path, 'w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(['alpha_id'])  # 写入表头
                for alpha_id in alpha_ids:
                    writer.writerow([alpha_id])
            logger.info(f"Saved filtered alpha IDs to {output_file_path}")
        else:
            logger.info("No alpha IDs to save.")

    def process_alpha_ids(self, alpha_ids):
        """Processing alpha IDs and returning gold_bag"""
        gold_bag = []
        total = len(alpha_ids)
        if total == 0:
            logger.info("No alphaIds to process.")
            return gold_bag

        logger.info(f"Start checking {total} alphaIds...")
        for idx, alpha_id in enumerate(alpha_ids):
            if idx % 200 == 0:
                self.session = self.sign_in(self.username, self.password)
                if not self.session:
                    logger.error("Invalid session, skipping current iteration.")
                    continue
            logger.info(f"Progress: {idx} checked, {total - idx} remaining.")
            pc = self.get_check_submission(self.session, alpha_id)
            if pc == "sleep":
                logger.warning(f"alphaId {alpha_id} requires cooldown, re-queuing.")
                time.sleep(100)
                self.session = self.sign_in(self.username, self.password)
                alpha_ids.append(alpha_id)
            elif pc != pc:  # NaN check
                logger.warning(f"Self-correlation check failed for alphaId {alpha_id}, re-queuing.")
                time.sleep(100)
                alpha_ids.append(alpha_id)
            elif pc == "fail":
                logger.info(f"alphaId {alpha_id} check failed, skipping.")
                continue
            elif pc == "error":
                logger.error(f"Error checking alphaId {alpha_id}, skipping.")
                continue
            else:
                logger.info(f"alphaId {alpha_id} check successful, added to gold_bag.")
                gold_bag.append((alpha_id, pc))
        logger.info(f"Processed {total} alphaIds, {len(gold_bag)} successful.")
        return gold_bag

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

        logger.info(f"gold_bag saved to {gold_bag_file}.")
        return gold_bag_file

    def run(self):
        """运行每日任务，处理前一天的数据"""
        date_str = self.get_yesterday_date()
        # 筛选并保存符合条件的 alpha ID 到 filtered_alpha_ids.{date_str}.csv
        self.read_and_filter_alpha_ids(date_str)

        # 定义批处理大小
        batch_size = 5  # 可根据需求调整
        alpha_ids_file = os.path.join(self.output_dir, f'filtered_alpha_ids.{date_str}.csv')

        # 循环处理，直到 filtered_alpha_ids.{date_str}.csv 不存在
        while os.path.exists(alpha_ids_file):
            # 加载一批 alpha ID
            loaded_alpha_ids = self.load_alpha_ids_batch(date_str, batch_size)
            if not loaded_alpha_ids:
                logger.info("No more alpha IDs to process, task completed.")
                break

            # 处理加载的 alpha ID
            gold_bag = self.process_alpha_ids(loaded_alpha_ids)
            if gold_bag:
                # 保存 gold_bag，追加到文件
                self.save_gold_bag(gold_bag, date_str)
                logger.info(
                    f"Processed batch of {len(loaded_alpha_ids)} alpha IDs, saved {len(gold_bag)} to gold_bag.csv.{date_str}")
            else:
                logger.info(f"Processed batch of {len(loaded_alpha_ids)} alpha IDs, no successful results.")

        logger.info(f"Processing completed for {date_str}, all alpha IDs processed.")

    def start_schedule(self):
        """启动每日调度任务，每天 00:30 执行"""
        schedule.every().day.at("00:30").do(self.run)

        def run_schedule():
            while True:
                schedule.run_pending()
                time.sleep(1)

        threading.Thread(target=run_schedule, daemon=True).start()
        self.logger.info("Daily scheduler started, will run at 02:30 every day.")

"""
data_dir = "../data"
output_dir = "../output"
specified_sharpe = 1.0
specified_fitness = 0.8
username = "tianyuan411249897@gmail.com"
password = "k9979kui8"
processor = ProcessSimulatedAlphas(data_dir, output_dir, specified_sharpe, specified_fitness, username, password)
processor.run()
"""
