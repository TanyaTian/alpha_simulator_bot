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
from typing import List

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

    def get_yesterday_date(self):
        """获取前一天的日期，格式为 YYYY-MM-DD"""
        return (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

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
            logger.info("No filtered rows to save to stone_bag.csv")
            return
        
        
        # 按 sharpe 值降序排序
        try:
            sorted_rows = sorted(
                filtered_rows,
                key=lambda row: ast.literal_eval(row[18].strip().strip('"\'')).get('sharpe', float('-inf')),
                reverse=True
            )
        except (SyntaxError, ValueError) as e:
            logger.error(f"Error sorting rows by sharpe: {e}")
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
            logger.info(f"Saved {len(sorted_rows)} rows to {output_file_path}")
        except Exception as e:
            logger.error(f"Error saving to {output_file_path}: {e}")

    def read_and_filter_alpha_ids(self, date_str):
        """读取并筛选符合条件的 alphaId，并保存到 output 目录"""
        alpha_ids = []
        filtered_rows = []  # 存储符合条件的完整行
        simulated_alphas_file = os.path.join(self.data_dir, f'simulated_alphas.csv.{date_str}')
        
        if not os.path.exists(simulated_alphas_file):
            logger.error(f"File {simulated_alphas_file} not found. Please check file generation process.")
            return
        
        logger.info(f"Start processing file: {simulated_alphas_file}")
        with open(simulated_alphas_file, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            try:
                header = next(reader)  # 读取表头
                if not header:
                    logger.error("CSV file header is empty.")
                    return
            except StopIteration:
                logger.error("CSV file is empty.")
                return

            
            for row in reader:
                if len(row) != len(header):
                    logger.warning(f"Skipping row with mismatched columns: {row}")
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
                        filtered_rows.append(row)  # 保存完整行
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
        
        # 调用 save_stone_bag 保存完整行，传入表头
        self.save_stone_bag(filtered_rows, date_str, header=header)

    def process_alpha_ids(self, alpha_ids, date_str):
        """Processing alpha IDs and returning gold_bag"""
        gold_bag = []
        total = len(alpha_ids)
        if total == 0:
            logger.info("No alphaIds to process.")
            return gold_bag

        logger.info(f"Start checking {total} alphaIds...")
        idx = 0
        while alpha_ids:
            alpha_id = alpha_ids.pop(0)
            idx += 1
            if idx % 200 == 0:
                self.session = self.sign_in(self.username, self.password)
                logger.info(f"Progress: {total - len(alpha_ids)}/{total} processed.")
                if not self.session:
                    logger.error("Invalid session, skipping current iteration.")
                    continue
            logger.info(f"Progress: {idx} checked, {len(alpha_ids)} remaining.")
            pc = self.get_check_submission(self.session, alpha_id)
            if pc == "sleep":
                logger.warning(f"alphaId {alpha_id} requires cooldown, re-queuing.")
                time.sleep(60)
                self.session = self.sign_in(self.username, self.password)
                alpha_ids.append(alpha_id)
            elif pc != pc:  # NaN check
                logger.warning(f"prod-correlation check failed for alphaId {alpha_id}, re-queuing.")
                time.sleep(60)
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
                self.save_gold_bag([(alpha_id, pc)], date_str)
        logger.info(f"Processed {total} alphaIds, {len(gold_bag)} successful.")
        return gold_bag

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
                logger.info(f"alphaId {alpha_id} check failed, skipping.")
                logger.info(f"Fail reasons:\n{failed_checks.to_string(index=False)}")
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

        logger.info(f"gold_bag saved to {gold_bag_file}.")
        return gold_bag_file

    def run(self):
        """运行每日任务，处理前一天的数据"""
        date_str = self.get_yesterday_date()
        # 筛选并保存符合条件的 alpha ID 到 filtered_alpha_ids.{date_str}.csv
        self.read_and_filter_alpha_ids(date_str)

        alpha_ids_file = os.path.join(self.output_dir, f'filtered_alpha_ids.{date_str}.csv')
        if not os.path.exists(alpha_ids_file):
                logger.info(f"No filtered alpha ID file found for {date_str}, skipping run.")
                return

        alpha_ids = []
        with open(alpha_ids_file, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)  # 跳过表头
            for row in reader:
                if row:
                    alpha_ids.append(row[0])

        # 步骤 3：处理 alpha ID，并在成功时立即写入 gold_bag.csv
        self.process_alpha_ids(alpha_ids, date_str)
        logger.info(f"Processing completed for {date_str}, all alpha IDs processed.")

    def start_schedule(self):
        """启动每日调度任务，每天 04:30 执行"""
        schedule.every().day.at("04:30").do(self.run)

        def run_schedule():
            while True:
                schedule.run_pending()
                time.sleep(1)

        threading.Thread(target=run_schedule, daemon=True).start()
        self.logger.info("Daily scheduler started, will run at 04:30 every day.")

"""
data_dir = "./data"
output_dir = "./output"
specified_sharpe = 1.58
specified_fitness = 1.0
username = "tianyuan411249897@gmail.com"
password = "k9979kui8"
processor = ProcessSimulatedAlphas(data_dir, output_dir, specified_sharpe, specified_fitness, username, password)
processor.run()
"""



