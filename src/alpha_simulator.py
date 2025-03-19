import time
import csv
import requests
import os
import ast
from datetime import datetime
from pytz import timezone
from logger import Logger  # 导入 Logger 类

# 获取美国东部时间
eastern = timezone('US/Eastern')
fmt = '%Y-%m-%d'
loc_dt = datetime.now(eastern)
print("Current time in Eastern is", loc_dt.strftime(fmt))

class AlphaSimulator:
    """Alpha模拟器类，用于管理量化策略的模拟过程"""

    # 类级别常量配置
    TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"
    FILE_CONFIG = {
        "input_file": "alpha_list_pending_simulated.csv",
        "fail_file": "fail_alphas.csv",
        "output_prefix": "simulated_alphas_"
    }

    def __init__(self, max_concurrent, username, password, batch_number_for_every_queue):
        """初始化模拟器

        Args:
            max_concurrent (int): 最大并发模拟数量
            username (str): 登录用户名
            password (str): 登录密码
            batch_number_for_every_queue (int): 每批处理数量
        """
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
        timestamp = loc_dt.strftime(self.TIMESTAMP_FORMAT)
        self.simulated_alphas = os.path.join(self.data_dir, f'{self.FILE_CONFIG["output_prefix"]}{timestamp}.csv')

        # 关键文件存在性验证
        self._validate_critical_files()

        # 其他属性初始化
        self.max_concurrent = max_concurrent
        self.active_simulations = []
        self.username = username
        self.password = password
        self.session = self.sign_in(username, password)
        self.sim_queue_ls = []
        self.batch_number_for_every_queue = batch_number_for_every_queue

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

        queue_path = os.path.join(os.path.dirname(self.alpha_list_file_path), 'sim_queue.csv')
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
            location_url = self.simulate_alpha(alpha)
            if location_url:
                self.active_simulations.append(location_url)
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
        """检查所有活跃模拟的状态"""
        count = 0
        if len(self.active_simulations) == 0:
            self.logger.info("No one is in active simulation now")
            return None

        for sim_url in self.active_simulations[:]:
            sim_progress = self.check_simulation_progress(sim_url)
            if sim_progress is None:
                count += 1
                continue
            alpha_id = sim_progress.get("id")
            status = sim_progress.get("status")
            self.logger.info(f"Alpha id: {alpha_id} ended with status: {status}. Removing from active list.")
            self.active_simulations.remove(sim_url)
            with open(self.simulated_alphas, 'a', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=sim_progress.keys())
                writer.writerow(sim_progress)
        self.logger.info(f"Total {count} simulations are in process for account {self.username}.")

    def manage_simulations(self):
        """管理整个模拟过程"""
        if not self.session:
            self.logger.error("Failed to sign in. Exiting...")
            return
        while True:
            self.check_simulation_status()
            self.load_new_alpha_and_simulate()
            time.sleep(3)