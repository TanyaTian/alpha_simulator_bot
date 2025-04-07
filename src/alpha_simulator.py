import time
import csv
import requests
import os
import ast
import json
import signal
import schedule
import threading
from pytz import timezone
from logger import Logger  # 假设已定义 Logger 类
from datetime import datetime, timedelta

# 获取美国东部时间
eastern = timezone('US/Eastern')
fmt = '%Y-%m-%d'
loc_dt = datetime.now(eastern)
print("Current time in Eastern is", loc_dt.strftime(fmt))

# 全局退出标志
running = True

def signal_handler(signum, frame):
    """处理 SIGTERM 和 SIGINT 信号"""
    global running
    Logger().info(f"Received signal {signum}, initiating shutdown...")
    running = False

class AlphaSimulator:
    """Alpha模拟器类，用于管理量化策略的模拟过程"""

    TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"
    FILE_CONFIG = {
        "input_file": "alpha_list_pending_simulated.csv",
        "fail_file": "fail_alphas.csv",
        "output_file": "simulated_alphas.csv",  # 基础文件名
        "state_file": "simulator_state.json"
    }

    def __init__(self, max_concurrent, username, password, batch_number_for_every_queue, batch_size=10):
        """初始化模拟器

        Args:
            max_concurrent (int): 最大并发模拟数量
            username (str): 登录用户名
            password (str): 登录密码
            batch_number_for_every_queue (int): 每批处理数量
        """
        # 注册信号处理
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

        # 创建 Logger 实例
        self.logger = Logger()

        # 构建基础路径
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)
        self.data_dir = os.path.join(project_root, 'data')

        # 自动创建data目录（如果不存在）
        os.makedirs(self.data_dir, exist_ok=True)

        # 构建文件路径体系
        self.alpha_list_file_path = os.path.join(self.data_dir, self.FILE_CONFIG["input_file"])
        self.fail_alphas = os.path.join(self.data_dir, self.FILE_CONFIG["fail_file"])
        self.state_file = os.path.join(self.data_dir, self.FILE_CONFIG["state_file"])

        # 关键文件存在性验证
        self._validate_critical_files()

        # 其他属性初始化
        self.max_concurrent = max_concurrent
        self.active_simulations = []
        self.active_update_time = time.time()
        self.username = username
        self.password = password
        self.session = self.sign_in(username, password)
        self.sim_queue_ls = []
        self.batch_number_for_every_queue = batch_number_for_every_queue
        self.batch_size = batch_size  # 存储 batch_size 作为实例变量

        # 加载上次未完成的 active_simulations
        self._load_previous_state()

        self.start_rotation_scheduler()  # 添加轮转调度

    def _validate_critical_files(self):
        """验证关键输入文件是否存在"""
        missing_files = []

        if not os.path.exists(self.alpha_list_file_path):
            missing_files.append(
                f"主输入文件: {self.alpha_list_file_path}\n"
                "可能原因:\n"
                "- 文件尚未生成\n"
                "- 文件名拼写错误"
            )

        if missing_files:
            raise FileNotFoundError(
                "关键文件缺失:\n\n" +
                "\n\n".join(missing_files) +
                "\n\n解决方案建议:\n"
                "1. 检查data目录结构是否符合预期\n"
                "2. 确认文件生成流程已执行"
            )

    def _load_previous_state(self):
        if os.path.exists(self.state_file):
            with open(self.state_file, 'r') as f:
                state = json.load(f)
                self.active_simulations = state.get("active_simulations", [])
                self.logger.info(f"Loaded {len(self.active_simulations)} previous active simulations from state.")

            while self.active_simulations and running:
                try:
                    self.check_simulation_status()
                except Exception as e:
                    self.logger.error(f"Error checking previous simulations: {e}")
                time.sleep(3)
            self.logger.info("All previous active simulations processed.")

    def rotate_output_file(self):
        """每天 0 点轮转 output 文件，将前一天的文件重命名为 YYYY-MM-DD 格式"""
        current_date = datetime.now().date()
        yesterday = current_date - timedelta(days=1)
        yesterday_str = yesterday.strftime('%Y-%m-%d')
        output_file = os.path.join(self.data_dir, self.FILE_CONFIG["output_file"])
        backup_file = os.path.join(self.data_dir, f'simulated_alphas.csv.{yesterday_str}')

        if os.path.exists(output_file):
            if os.path.exists(backup_file):
                os.remove(backup_file)  # 删除旧备份（如果存在）
            os.rename(output_file, backup_file)
            self.logger.info(f"Rotated {output_file} to {backup_file}")
        else:
            self.logger.warning(f"Output file {output_file} does not exist for rotation")

    def start_rotation_scheduler(self):
        """启动文件轮转调度，每天 0 点执行"""
        schedule.every().day.at("00:00").do(self.rotate_output_file)

        def run_schedule():
            while True:
                schedule.run_pending()
                time.sleep(1)

        threading.Thread(target=run_schedule, daemon=True).start()
        self.logger.info("Started output file rotation scheduler at 00:00 daily")

    def sign_in(self, username, password):
        """登录WorldQuant BRAIN平台"""
        s = requests.Session()
        s.auth = (username, password)
        count = 0
        count_limit = 30

        while True:
            try:
                response = s.post('https://api.worldquantbrain.com/authentication')
                response.raise_for_status()
                break
            except:
                count += 1
                self.logger.error("Connection down, trying to login again...")
                time.sleep(15)
                if count > count_limit:
                    self.logger.error(f"{username} failed too many times, returning None.")
                    return None
        self.logger.info("Login to BRAIN successfully.")
        return s

    def read_alphas_from_csv_in_batches(self, batch_size=50):
        """从CSV文件中分批读取alphas"""
        alphas = []
        temp_file_name = self.alpha_list_file_path + '.tmp'

        with open(self.alpha_list_file_path, 'r') as file, open(temp_file_name, 'w', newline='') as temp_file:
            reader = csv.DictReader(file)
            fieldnames = reader.fieldnames
            writer = csv.DictWriter(temp_file, fieldnames=fieldnames)
            writer.writeheader()

            for _ in range(batch_size):
                try:
                    row = next(reader)
                    if 'settings' in row:
                        if isinstance(row['settings'], str):
                            try:
                                row['settings'] = ast.literal_eval(row['settings'])
                            except (ValueError, SyntaxError):
                                print(f"Error evaluating settings: {row['settings']}")
                        elif not isinstance(row['settings'], dict):
                            print(f"Unexpected type for settings: {type(row['settings'])}")
                    alphas.append(row)
                except StopIteration:
                    break

            for remaining_row in reader:
                writer.writerow(remaining_row)

        try:
            os.replace(temp_file_name, self.alpha_list_file_path)
        except OSError as e:
            if e.winerror == 5:  # 拒绝访问
                self.logger.error("Access denied when replacing file. Trying to delete original and rename temporary.")
                try:
                    os.remove(self.alpha_list_file_path)
                    os.rename(temp_file_name, self.alpha_list_file_path)
                except OSError as e2:
                    self.logger.error(f"Failed to delete original or rename temporary file: {e2}")
            else:
                raise

        queue_path = os.path.join(self.data_dir, 'sim_queue.csv')
        if alphas:
            with open(queue_path, 'w', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=alphas[0].keys())
                if file.tell() == 0:
                    writer.writeheader()
                writer.writerows(alphas)
        return alphas

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
                    self.session = self.sign_in(self.username, self.password)
                    self.logger.error("Error occurred too many times, skipping this alpha batch and re-logging in.")
                    break

                # 记录重试信息，等待 5 秒后继续
                self.logger.error("Error in sending simulation request. Retrying after 5s...")
                time.sleep(5)
                count += 1

        # 如果请求失败（重试次数耗尽），记录错误
        self.logger.error(f"Simulation request failed after {count} attempts for alpha batch")
        # 将整个 alpha_list 写入失败文件
        with open(self.fail_alphas, 'a', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=alpha_list[0].keys())
            for alpha in alpha_list:
                writer.writerow(alpha)
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

            # 检查 settings 是否为字典（理论上总是字典）
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
        加载并模拟新的 alpha，每次从队列中弹出 self.batch_size 个 alpha 进行批量模拟。

        Attributes:
            self.sim_queue_ls (list): 包含 alpha 数据的队列，每个元素是一个字典，包含 type、settings 和 regular 字段
            self.batch_size (int): 每次模拟的 alpha 批量大小
            self.max_concurrent (int): 最大并发模拟数量
            self.active_simulations (list): 正在进行的模拟任务列表，存储 location_url
            self.active_update_time (float): 最近一次模拟更新的时间戳
            self.logger (Logger): 日志记录器
        """
        global running
        # 检查运行状态，如果 running 为 False，则退出
        if not running:
            return

        # 如果队列为空，从 CSV 文件中读取新的 alpha 数据
        if len(self.sim_queue_ls) < 1:
            self.logger.info("Simulation queue is empty, reading new alphas from CSV...")
            self.sim_queue_ls = self.read_alphas_from_csv_in_batches(self.batch_number_for_every_queue)

        # 如果当前正在进行的模拟任务数量达到最大并发限制，等待 2 秒后退出
        if len(self.active_simulations) >= self.max_concurrent:
            self.logger.info(f"Max concurrent simulations reached ({self.max_concurrent}). Waiting 2 seconds")
            time.sleep(2)
            return

        self.logger.info('Loading new alphas for simulation...')
        try:
            # 每次从队列中弹出 self.batch_size 个 alpha（如果不足 self.batch_size 个，则弹出所有剩余的）
            alpha_list = []
            for _ in range(min(self.batch_size, len(self.sim_queue_ls))):
                alpha = self.sim_queue_ls.pop(0)
                alpha_list.append(alpha)

            # 如果成功弹出 alpha 列表，执行模拟
            if alpha_list:
                # 记录当前批次的 alpha 信息
                self.logger.info(f"Starting simulation for {len(alpha_list)} alphas:")
                for alpha in alpha_list:
                    self.logger.info(f"  - Alpha: {alpha['regular'][:50]}... with settings: {alpha['settings']}")
                self.logger.info(f"Remaining in sim_queue_ls: {len(self.sim_queue_ls)}")

                # 调用 simulate_alpha，传入 alpha 列表
                location_url = self.simulate_alpha(alpha_list)
                
                # 如果模拟成功（返回 location_url），将 URL 添加到 active_simulations
                if location_url:
                    self.active_simulations.append(location_url)
                    self.active_update_time = time.time()
                    self.logger.info(f"Simulation started, location_url: {location_url}")
                else:
                    self.logger.warning("Simulation failed, no location_url returned")
            else:
                self.logger.info("No alphas available for simulation in this batch")

        except IndexError:
            # 如果队列中没有足够的 alpha，打印信息
            self.logger.info("No more alphas available in the queue.")
        except Exception as e:
            # 捕获其他异常，记录错误信息
            self.logger.error(f"Error during simulation: {e}")

    
    
    def check_simulation_progress(self, simulation_progress_url):
        """
        检查批量模拟的进度，获取每个 child 的 alpha 详情。

        Args:
            simulation_progress_url (str): 批量模拟的进度 URL，例如 "https://api.worldquantbrain.com/simulations/12345"

        Returns:
            list or None: 包含所有 alpha 详情的列表，每个元素是一个字典（alpha 详情的 JSON 数据）；如果失败，返回 None
        """
        try:
            # 请求批量模拟的进度
            simulation_progress = self.session.get(simulation_progress_url)
            simulation_progress.raise_for_status()

            # 检查是否有 Retry-After 头，如果存在，表示需要稍后重试
            if simulation_progress.headers.get("Retry-After", 0) != 0:
                return None

            # 解析响应，获取 children 列表
            progress_data = simulation_progress.json()
            children = progress_data.get("children", [])
            if not children:
                self.logger.error("No children found in simulation progress response")
                return None

            # 检查批量模拟状态
            status = progress_data.get("status")
            if status != "COMPLETE":
                self.logger.info(f"Simulation batch status: {status}. Not complete yet.")
                return None

            # 存储所有 alpha 详情
            alpha_details_list = []
            brain_api_url = "https://api.worldquantbrain.com"

            # 遍历每个 child，获取 alpha_id 和 alpha 详情
            for child in children:
                try:
                    # 请求 child 的模拟进度
                    child_progress = self.session.get(f"{brain_api_url}/simulations/{child}")
                    child_progress.raise_for_status()

                    # 获取 alpha_id
                    child_data = child_progress.json()
                    alpha_id = child_data.get("alpha")
                    if not alpha_id:
                        self.logger.error(f"No alpha_id found for child: {child}")
                        continue

                    # 使用 alpha_id 请求 alpha 详情
                    alpha_response = self.session.get(f"{brain_api_url}/alphas/{alpha_id}")
                    alpha_response.raise_for_status()

                    # 解析 alpha 详情
                    alpha_details = alpha_response.json()
                    alpha_details_list.append(alpha_details)

                except requests.exceptions.RequestException as e:
                    self.logger.error(f"Error fetching child {child} progress or alpha details: {e}")
                    continue  # 跳过失败的 child，继续处理下一个

            return alpha_details_list if alpha_details_list else None

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching simulation progress: {e}")
            self.session = self.sign_in(self.username, self.password)
            return None

    def check_simulation_status(self):
        """
        检查所有活跃模拟的状态，并将结果写入 output 文件。

        Attributes:
            self.active_simulations (list): 包含活跃模拟的 URL 列表
            self.active_update_time (float): 最近一次模拟更新的时间戳
            self.max_concurrent (int): 最大并发模拟数量
            self.session (requests.Session): HTTP 会话对象
            self.logger (Logger): 日志记录器
            self.data_dir (str): 数据目录路径
            self.FILE_CONFIG (dict): 文件配置
        """
        # 检查是否超过 1 小时未更新
        current_time = time.time()
        time_diff = current_time - self.active_update_time
        if time_diff > 3600 and len(self.active_simulations) > 0:  # 1 小时
            self.logger.info(
                f"active_update_time exceeds 1 hour (diff: {time_diff:.2f} seconds), and max concurrent simulations reached ({self.max_concurrent}). Resetting active simulations.")
            self.session = self.sign_in(self.username, self.password)
            self.active_simulations.clear()
            self.logger.info("Active simulations cleared after re-signing in.")

        # 如果没有活跃模拟，直接返回
        if len(self.active_simulations) == 0:
            self.logger.info("No one is in active simulation now")
            return None

        # 准备输出文件路径
        output_file = os.path.join(self.data_dir, self.FILE_CONFIG["output_file"])
        count = 0  # 统计仍在处理中的模拟数量

        # 遍历所有活跃模拟
        for sim_url in self.active_simulations[:]:
            # 检查模拟进度
            alpha_details_list = self.check_simulation_progress(sim_url)

            # 如果返回 None，表示模拟尚未完成，继续处理下一个
            if alpha_details_list is None:
                count += 1
                continue

            # 模拟已完成，从活跃列表中移除
            self.logger.info(f"Simulation batch {sim_url} completed. Removing from active list.")
            self.active_simulations.remove(sim_url)

            # 将所有 alpha 详情写入文件
            with open(output_file, 'a', newline='') as file:
                # 假设 alpha_details_list 中的每个元素有相同的字段
                if alpha_details_list:
                    writer = csv.DictWriter(file, fieldnames=alpha_details_list[0].keys())
                    # 如果文件为空，写入表头
                    if os.stat(output_file).st_size == 0:
                        writer.writeheader()
                    # 写入所有 alpha 详情
                    for alpha_details in alpha_details_list:
                        writer.writerow(alpha_details)

        self.logger.info(f"Total {count} simulations are in process for account {self.username}.")

    def save_state(self):
        """保存当前状态"""
        state = {
            "active_simulations": self.active_simulations,
            "timestamp": datetime.now(eastern).strftime(self.TIMESTAMP_FORMAT)
        }
        with open(self.state_file, 'w') as f:
            json.dump(state, f)
        self.logger.info(f"Saved {len(self.active_simulations)} active simulations to {self.state_file}")

        if self.sim_queue_ls:
            with open(self.alpha_list_file_path, 'a', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=self.sim_queue_ls[0].keys())
                if os.stat(self.alpha_list_file_path).st_size == 0:
                    writer.writeheader()
                writer.writerows(self.sim_queue_ls)
            self.logger.info(f"Reinserted {len(self.sim_queue_ls)} alphas back to {self.alpha_list_file_path}")
            self.sim_queue_ls = []

    def manage_simulations(self):
        """管理整个模拟过程"""
        if not self.session:
            self.logger.error("Failed to sign in. Exiting...")
            return

        try:
            while running:
                self.check_simulation_status()
                self.load_new_alpha_and_simulate()
                time.sleep(3)
        except KeyboardInterrupt:
            self.logger.info("Manual interruption detected.")
        finally:
            self.logger.info("Shutting down, saving state...")
            self.save_state()