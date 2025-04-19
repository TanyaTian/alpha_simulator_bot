import queue
import time
import csv
import requests
import os
import ast
import json
import schedule
import threading
from pytz import timezone
from logger import Logger  # 假设已定义 Logger 类
from datetime import datetime, timedelta
from signal_manager import SignalManager

# 获取美国东部时间
eastern = timezone('US/Eastern')
fmt = '%Y-%m-%d'
loc_dt = datetime.now(eastern)
print("Current time in Eastern is", loc_dt.strftime(fmt))

class AlphaSimulator:
    """Alpha模拟器类，用于管理量化策略的模拟过程"""

    TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"
    FILE_CONFIG = {
        "input_file": "alpha_list_pending_simulated.csv",
        "fail_file": "fail_alphas.csv",
        "output_file": "simulated_alphas.csv",  # 基础文件名
        "state_file": "simulator_state.json"
    }

    def __init__(self, max_concurrent, username, password, batch_number_for_every_queue, batch_size=10, signal_manager=None):
        """初始化模拟器

        Args:
            max_concurrent (int): 最大并发模拟数量
            username (str): 登录用户名
            password (str): 登录密码
            batch_number_for_every_queue (int): 每批处理数量
        """
        self.running = True

        # 创建 Logger 实例
        self.logger = Logger()

        # 注册信号处理
        if signal_manager:
            signal_manager.add_handler(self.signal_handler)
        else:
            self.logger.warning("未提供 SignalManager，AlphaSimulator 无法注册信号处理函数")

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

        self.start_rotation_scheduler()  # 添加轮转调度
        self.task_queue = queue.Queue()
        self.lock = threading.Lock()  # 🔒 文件写入锁
        self.worker_thread = threading.Thread(target=self.worker)
        self.worker_thread.daemon = True
        self.worker_thread.start()

        # 加载上次未完成的 active_simulations
        self._load_previous_state()

    def signal_handler(self, signum, frame):
        self.logger.info(f"Received shutdown signal {signum}, , initiating shutdown...")
        self.running = False
        self.save_state()
        self.shutdown()

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

    def manage_input_files(self):
        """管理输入文件，当主文件为空时切换到下一个编号的文件"""
        base_name = self.FILE_CONFIG["input_file"].replace('.csv', '')
        current_file = self.alpha_list_file_path
        
        # 检查当前文件是否为空（只有表头或完全为空）
        try:
            with open(current_file, 'r') as f:
                reader = csv.reader(f)
                header = next(reader, None)  # 读取表头
                if header is None:  # 完全空文件
                    self.logger.info("Current input file is completely empty")
                else:
                    first_data_row = next(reader, None)  # 尝试读取第一行数据
                    if first_data_row is not None:  # 有数据行
                        return False  # 文件不为空，不需要切换
                    self.logger.info("Current input file has only header but no data")
        except FileNotFoundError:
            self.logger.info("Current input file not found")
            pass  # 文件不存在，继续执行下面的逻辑
        
        # 查找下一个可用的文件
        for i in range(1, 20):
            next_file = os.path.join(self.data_dir, f"{base_name}_{i}.csv")
            if os.path.exists(next_file):
                # 检查目标文件是否为空（只有表头）
                with open(next_file, 'r') as f:
                    reader = csv.reader(f)
                    header = next(reader, None)
                    if header is None:  # 完全空文件
                        self.logger.info(f"Found {base_name}_{i}.csv but it's completely empty")
                        continue
                    first_data_row = next(reader, None)
                    if first_data_row is None:  # 只有表头
                        self.logger.info(f"Found {base_name}_{i}.csv but it has only header")
                        continue
                
                try:
                    # 删除当前空文件
                    if os.path.exists(current_file):
                        os.remove(current_file)
                    # 重命名下一个文件为当前文件
                    os.rename(next_file, current_file)
                    self.logger.info(f"Switched input file to {base_name}_{i}.csv")
                    return True
                except OSError as e:
                    self.logger.error(f"Error switching input files: {e}")
                    return False
        
        # 没有找到可用的下一个文件
        self.logger.info("No more valid input files available")
        return False

    def read_alphas_from_csv_in_batches(self, batch_size=50):
        """从CSV文件中分批读取alphas"""
        alphas = []
        temp_file_name = self.alpha_list_file_path + '.tmp'

        # 检查并管理输入文件
        if not os.path.exists(self.alpha_list_file_path):
            if not self.manage_input_files():
                return alphas  # 没有更多文件可用，返回空列表
        else:
            # 检查文件是否只有表头
            with open(self.alpha_list_file_path, 'r') as f:
                reader = csv.reader(f)
                header = next(reader, None)
                if header is None:  # 完全空文件
                    if not self.manage_input_files():
                        return alphas
                else:
                    first_data_row = next(reader, None)
                    if first_data_row is None:  # 只有表头
                        if not self.manage_input_files():
                            return alphas

        with open(self.alpha_list_file_path, 'r') as file, open(temp_file_name, 'w', newline='') as temp_file:
            reader = csv.DictReader(file)
            fieldnames = reader.fieldnames
            if fieldnames is None:  # 完全空文件情况
                return alphas
                
            # 确保表头包含预期字段
            expected_fields = {'type', 'settings', 'regular'}
            if not expected_fields.issubset(set(fieldnames)):
                self.logger.error(f"Input file missing required fields. Expected: {expected_fields}, found: {fieldnames}")
                return alphas
                
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
                                self.logger.error(f"Error evaluating settings: {row['settings']}")
                        elif not isinstance(row['settings'], dict):
                            self.logger.error(f"Unexpected type for settings: {type(row['settings'])}")
                    alphas.append(row)
                except StopIteration:
                    break

            for remaining_row in reader:
                writer.writerow(remaining_row)

        try:
            os.replace(temp_file_name, self.alpha_list_file_path)
        except OSError as e:
            if hasattr(e, 'winerror') and e.winerror == 5:  # 拒绝访问
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
        确保 alpha_list 中的所有 alpha 具有相同的 'region' 和 'universe' 设置。

        Attributes:
            self.sim_queue_ls (list): 包含 alpha 数据的队列，每个元素是一个字典，包含 type、settings 和 regular 字段
            self.batch_size (int): 每次模拟的 alpha 批量大小
            self.max_concurrent (int): 最大并发模拟数量
            self.active_simulations (list): 正在进行的模拟任务列表，存储 location_url
            self.active_update_time (float): 最近一次模拟更新的时间戳
            self.logger (Logger): 日志记录器
        """
        # 检查运行状态，如果 running 为 False，则退出
        if not self.running:
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
            # 每次从队列中弹出 alpha，组成 alpha_list
            alpha_list = []

            # 如果队列为空，直接返回
            if not self.sim_queue_ls:
                self.logger.info("No alphas available in the queue.")
                return

            # 弹出第一个 alpha，设置基准 region 和 universe
            first_alpha = self.sim_queue_ls.pop(0)
            alpha_list.append(first_alpha)
            base_region = first_alpha['settings'].get('region')
            base_universe = first_alpha['settings'].get('universe')

            # 验证第一个 alpha 的 region 和 universe 是否存在
            if base_region is None or base_universe is None:
                self.logger.error(f"First alpha missing 'region' or 'universe': {first_alpha}")
                return

            self.logger.info(f"Base region: {base_region}, Base universe: {base_universe}")

            # 继续弹出 alpha，直到达到 batch_size 或遇到不一致的 region/universe
            while len(alpha_list) < self.batch_size and self.sim_queue_ls:
                # 检查下一个 alpha 的 region 和 universe
                next_alpha = self.sim_queue_ls[0]  # 查看但不弹出
                next_region = next_alpha['settings'].get('region')
                next_universe = next_alpha['settings'].get('universe')

                # 验证 region 和 universe 是否存在
                if next_region is None or next_universe is None:
                    self.logger.error(f"Alpha missing 'region' or 'universe': {next_alpha}")
                    self.sim_queue_ls.pop(0)  # 移除无效 alpha
                    continue

                # 检查是否与基准一致
                if next_region != base_region or next_universe != base_universe:
                    self.logger.info(
                        f"Region/Universe mismatch: {next_region}/{next_universe} does not match base {base_region}/{base_universe}. Stopping batch.")
                    break

                # 如果一致，弹出并添加到 alpha_list
                alpha_list.append(self.sim_queue_ls.pop(0))

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
        检查批量模拟的进度，获取 children 列表。

        Args:
            simulation_progress_url (str): 批量模拟的进度 URL，例如 "https://api.worldquantbrain.com/simulations/12345"

        Returns:
            list or None: 如果成功返回 children 列表，否则返回 None
        """
        try:
            simulation_progress = self.session.get(simulation_progress_url)
            simulation_progress.raise_for_status()

            # 检查是否包含 Retry-After 头，表示服务端暂时不可用
            if simulation_progress.headers.get("Retry-After", 0) != 0:
                return None

            # 解析响应，提取 children 和状态
            progress_data = simulation_progress.json()
            children = progress_data.get("children", [])
            if not children:
                self.logger.error("No children found in simulation progress response.")
                return None

            status = progress_data.get("status")
            self.logger.info(f"Simulation batch status: {status}, children count: {len(children)}")

            if status != "COMPLETE":
                self.logger.info("Simulation not complete. Will check again later.")

            return children

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to fetch simulation progress: {e}")
            self.session = self.sign_in(self.username, self.password)
            return None


    def process_children_async(self, children):
        """异步放入 children 处理任务"""
        if not children:
            self.logger.warning("Empty children list received. Skipping enqueue.")
            return

        try:
            self.task_queue.put(children, timeout=5)
            self.logger.info(f"Enqueued task for {len(children)} children. Queue size: {self.task_queue.qsize()}")
        except queue.Full:
            self.logger.error(f"Task queue is full (maxsize=100). Dropping task for {len(children)} children.")
            temp_file = os.path.join(self.data_dir, f"dropped_children_{int(time.time())}.json")
            with open(temp_file, 'w') as f:
                json.dump(children, f)
            self.logger.info(f"Saved dropped children to {temp_file}")


    def process_children(self, children):
        """同步处理 children，获取 alphaId 和 alpha detail，写入文件"""
        self.logger.info(f"Processing {len(children)} children...")
        if not children:
            self.logger.warning("Empty children list received. Skipping processing.")
            return

        brain_api_url = "https://api.worldquantbrain.com"
        alpha_details_list = []
        max_retries = 3
        retry_delay = 5
        request_timeout = 30

        for child in children:
            self.logger.debug(f"Processing child: {child}")
            if not isinstance(child, str):
                self.logger.error(f"Invalid child format: {child} (Expected string)")
                continue

            for attempt in range(max_retries):
                try:
                    self.logger.debug(f"Fetching child simulation progress: {child} (Attempt {attempt + 1}/{max_retries})")
                    child_progress = self.session.get(f"{brain_api_url}/simulations/{child}", timeout=request_timeout)
                    child_progress.raise_for_status()
                    child_data = child_progress.json()
                    self.logger.debug(f"Child response: {child_data}")

                    alpha_id = child_data.get("alpha")
                    if not alpha_id:
                        self.logger.warning(f"No alpha_id found for child: {child}. Response: {child_data}")
                        break

                    self.logger.debug(f"Fetching alpha detail for alpha_id: {alpha_id}")
                    alpha_response = self.session.get(f"{brain_api_url}/alphas/{alpha_id}", timeout=request_timeout)
                    alpha_response.raise_for_status()
                    alpha_details = alpha_response.json()
                    if not alpha_details:
                        self.logger.warning(f"Empty alpha details for alpha_id: {alpha_id}")
                        break

                    alpha_details_list.append(alpha_details)
                    self.logger.debug(f"Successfully retrieved alpha details for alpha_id: {alpha_id}")
                    break

                except requests.exceptions.RequestException as e:
                    self.logger.error(f"Error fetching child {child} or alpha detail (Attempt {attempt + 1}/{max_retries}): {e}")
                    if attempt < max_retries - 1:
                        self.logger.info(f"Retrying after {retry_delay}s...")
                        time.sleep(retry_delay)
                    else:
                        self.logger.error(f"Max retries reached for child {child}. Skipping...")
                        if "401" in str(e) or "403" in str(e):
                            self.logger.info("Authentication error detected. Re-signing in...")
                            self.session = self.sign_in(self.username, self.password)
                        break
                except ValueError as e:
                    self.logger.error(f"Invalid JSON response for child {child}: {e}")
                    break
                except Exception as e:
                    self.logger.error(f"Unexpected error processing child {child}: {e}")
                    break

        if alpha_details_list:
            output_file = os.path.join(self.data_dir, self.FILE_CONFIG["output_file"])
            try:
                with self.lock:
                    self.logger.info(f"Writing {len(alpha_details_list)} alpha details to file: {output_file}")
                    if not os.access(self.data_dir, os.W_OK):
                        self.logger.error(f"No write permission for directory: {self.data_dir}")
                        return
                    fieldnames = alpha_details_list[0].keys()
                    with open(output_file, 'a', newline='') as file:
                        writer = csv.DictWriter(file, fieldnames=fieldnames)
                        if os.stat(output_file).st_size == 0:
                            writer.writeheader()
                        for alpha_details in alpha_details_list:
                            try:
                                writer.writerow(alpha_details)
                                self.logger.debug(f"Wrote alpha details: {alpha_details.get('id', 'unknown')}")
                            except Exception as e:
                                self.logger.error(f"Error writing alpha details {alpha_details.get('id', 'unknown')}: {e}")
                    self.logger.info(f"Successfully wrote {len(alpha_details_list)} alpha details to {output_file}")
            except PermissionError as e:
                self.logger.error(f"Permission error writing to file {output_file}: {e}")
            except Exception as e:
                self.logger.error(f"Error writing to file {output_file}: {e}")
        else:
            self.logger.warning(f"No alpha details to write for {len(children)} children. alpha_details_list is empty.")

    def worker(self):
        """后台子线程，处理队列中的 children 列表"""
        self.logger.info("Worker thread started. Waiting for tasks...")
        while self.running:
            try:
                children = self.task_queue.get(timeout=10)
                if children is None:
                    self.logger.info("Shutdown signal received. Worker thread exiting.")
                    self.task_queue.task_done()
                    break

                self.logger.info(f"Worker received a task with {len(children)} children.")
                self.process_children(children)
                self.logger.info("Worker finished processing current task.")
                self.task_queue.task_done()

            except queue.Empty:
                self.logger.debug("Task queue is empty. Waiting for new tasks...")
                continue
            except Exception as e:
                self.logger.error(f"Unexpected error in worker thread: {e}")
                self.task_queue.task_done()
                continue

        self.logger.info("Worker thread stopped.")

    def shutdown(self):
        """优雅关闭子线程"""
        self.logger.info("Shutting down the worker thread...")
        self.task_queue.put(None)
        if self.worker_thread.is_alive():
            self.worker_thread.join(timeout=10)
            if self.worker_thread.is_alive():
                self.logger.warning("Worker thread did not terminate within 10 seconds.")
        self.logger.info("Worker thread has been successfully shut down.")

    def check_simulation_status(self):
        """检查所有活跃模拟的状态"""
        current_time = time.time()
        time_diff = current_time - self.active_update_time
        if time_diff > 3600 and len(self.active_simulations) > 0:
            self.logger.warning(f"active_update_time exceeds 1 hour (diff: {time_diff:.2f} seconds). Resetting active simulations.")
            self.session = self.sign_in(self.username, self.password)
            self.active_simulations.clear()
            self.logger.info("Active simulations cleared after re-signing in.")

        if not self.active_simulations:
            self.logger.info("No active simulations to check.")
            return

        self.logger.info(f"Checking {len(self.active_simulations)} active simulations...")
        count = 0
        for sim_url in self.active_simulations[:]:
            children = self.check_simulation_progress(sim_url)
            if children is None:
                count += 1
                self.logger.debug(f"Simulation {sim_url} still in progress or failed.")
                continue

            self.logger.info(f"Simulation batch {sim_url} completed. Removing from active list.")
            self.active_simulations.remove(sim_url)
            self.process_children_async(children)
            self.active_update_time = time.time()

        self.logger.info(f"Total {count} simulations still in progress for account {self.username}.")

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
            while self.running:
                self.check_simulation_status()
                self.load_new_alpha_and_simulate()
                time.sleep(3)
        except KeyboardInterrupt:
            self.logger.info("Manual interruption detected.")