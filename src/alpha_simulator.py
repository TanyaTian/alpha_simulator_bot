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

    def __init__(self, max_concurrent, username, password, batch_number_for_every_queue):
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

    def simulate_alpha(self, alpha):
        """模拟单个alpha"""
        count = 0
        while True:
            try:
                response = self.session.post('https://api.worldquantbrain.com/simulations', json=alpha)
                response.raise_for_status()
                if "Location" in response.headers:
                    self.logger.info("Alpha location retrieved successfully.")
                    self.logger.info(f"Location: {response.headers['Location']}")
                    return response.headers['Location']
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Error in sending simulation request: {e}")
                if count > 35:
                    self.session = self.sign_in(self.username, self.password)
                    self.logger.error("Error occurred too many times, skipping this alpha and re-logging in.")
                    break
                self.logger.error("Error in sending simulation request. Retrying after 5s...")
                time.sleep(5)
                count += 1

        self.logger.error(f"Simulation request failed after {count} attempts.")
        with open(self.fail_alphas, 'a', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=alpha.keys())
            writer.writerow(alpha)
        return None

    def load_new_alpha_and_simulate(self):
        """加载并模拟新的alpha"""
        global running
        if not running:
            return

        if len(self.sim_queue_ls) < 1:
            self.sim_queue_ls = self.read_alphas_from_csv_in_batches(self.batch_number_for_every_queue)

        if len(self.active_simulations) >= self.max_concurrent:
            self.logger.info(f"Max concurrent simulations reached ({self.max_concurrent}). Waiting 2 seconds")
            time.sleep(2)
            return

        self.logger.info('Loading new alpha...')
        try:
            alpha = self.sim_queue_ls.pop(0)
            self.logger.info(f"Starting simulation for alpha: {alpha['regular']} with settings: {alpha['settings']}")
            self.logger.info(f"Remaining in sim_queue_ls: {len(self.sim_queue_ls)}")
            location_url = self.simulate_alpha(alpha)
            if location_url:
                self.active_simulations.append(location_url)
                self.active_update_time = time.time()
        except IndexError:
            self.logger.info("No more alphas available in the queue.")

    def check_simulation_progress(self, simulation_progress_url):
        """检查模拟进度"""
        try:
            simulation_progress = self.session.get(simulation_progress_url)
            simulation_progress.raise_for_status()
            if simulation_progress.headers.get("Retry-After", 0) == 0:
                alpha_id = simulation_progress.json().get("alpha")
                if alpha_id:
                    alpha_response = self.session.get(f"https://api.worldquantbrain.com/alphas/{alpha_id}")
                    alpha_response.raise_for_status()
                    return alpha_response.json()
                else:
                    return simulation_progress.json()
            else:
                return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching simulation progress: {e}")
            self.session = self.sign_in(self.username, self.password)
            return None

    def check_simulation_status(self):
        """检查所有活跃模拟的状态，并将结果写入 output 文件"""

        current_time = time.time()
        time_diff = current_time - self.active_update_time
        if time_diff > 3600 and len(self.active_simulations) >= self.max_concurrent:  # 1 小时
            self.logger.info(
                f"active_update_time exceeds 1 hours (diff: {time_diff:.2f} seconds), and max concurrent simulations reached ({self.max_concurrent}). Resetting active simulations.")
            self.session = self.sign_in(self.username, self.password)
            self.active_simulations.clear()
            self.logger.info("Active simulations cleared after re-signing in.")

        count = 0
        if len(self.active_simulations) == 0:
            self.logger.info("No one is in active simulation now")
            return None

        output_file = os.path.join(self.data_dir, self.FILE_CONFIG["output_file"])
        self.session = self.sign_in(self.username, self.password)
        for sim_url in self.active_simulations[:]:
            sim_progress = self.check_simulation_progress(sim_url)
            if sim_progress is None:
                count += 1
                continue
            alpha_id = sim_progress.get("id")
            status = sim_progress.get("status")
            self.logger.info(f"Alpha id: {alpha_id} ended with status: {status}. Removing from active list.")
            self.active_simulations.remove(sim_url)

            # 写入 CSV 文件
            with open(output_file, 'a', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=sim_progress.keys())
                # 如果文件为空，写入表头
                if os.stat(output_file).st_size == 0:
                    writer.writeheader()
                writer.writerow(sim_progress)

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