import threading
import os
import time
import requests
import pandas as pd
from utils import load_config
from logger import Logger
import ace_lib
from ace_lib import SingleSession

# =============================================================================
# 🔧 Monkey-Patching Official ace_lib and requests for Stability
# =============================================================================

# 1. 注入默认超时到 requests.Session
# 这样所有使用 requests (包括 SingleSession) 的请求都会有默认超时
original_request = requests.Session.request

def patched_request(self, method, url, *args, **kwargs):
    if 'timeout' not in kwargs:
        kwargs['timeout'] = 120  # 设置默认超时 120 秒
    return original_request(self, method, url, *args, **kwargs)

requests.Session.request = patched_request

# 2. 覆盖 ace_lib.get_credentials 以确保完全非交互
def patched_get_credentials():
    """从配置获取凭据，绝不调用 input()"""
    # 尝试从类属性获取（由 ConfigManager 设置）
    username = getattr(ConfigManager, '_username_cache', None)
    password = getattr(ConfigManager, '_password_cache', None)
    
    if not username or not password:
        username = os.environ.get('BRAIN_USERNAME')
        password = os.environ.get('BRAIN_PASSWORD')
        
    if not username or not password:
        raise ValueError("BRAIN credentials missing. Non-interactive mode active.")
    return (username, password)

ace_lib.get_credentials = patched_get_credentials

# 3. 覆盖 ace_lib.start_session 以跳过 Persona 交互
def patched_start_session():
    """覆盖原版 start_session，移除 Persona 的 input() 逻辑"""
    s = SingleSession()
    s.auth = ace_lib.get_credentials()
    r = s.post(ace_lib.brain_api_url + "/authentication")
    if r.status_code == requests.status_codes.codes.unauthorized:
        if r.headers.get("WWW-Authenticate") == "persona":
            raise RuntimeError("Persona biometrics required. Please login manually in a browser first.")
        else:
            raise RuntimeError("Incorrect BRAIN email or password.")
    return s

ace_lib.start_session = patched_start_session

# 4. 覆盖 get_specified_alpha_stats 处理脆弱的 JSON 解析
original_get_specified_alpha_stats = ace_lib.get_specified_alpha_stats

def patched_get_specified_alpha_stats(s, alpha_id, simulate_data, **kwargs):
    """增加防御性检查，防止 KeyError 导致模拟器线程崩溃"""
    if alpha_id is None:
        return {
            "alpha_id": None,
            "simulate_data": simulate_data,
            "is_stats": None, "pnl": None, "stats": None, "is_tests": None, "train": None, "test": None
        }

    try:
        # 调用原版
        result = original_get_specified_alpha_stats(s, alpha_id, simulate_data, **kwargs)
        return result
    except Exception as e:
        ace_lib.logger.error(f"Error in official get_specified_alpha_stats for {alpha_id}: {e}")
        # 返回一个安全的基础结构，避免上层逻辑崩溃
        return {
            "alpha_id": alpha_id,
            "simulate_data": simulate_data,
            "is_stats": None, "pnl": None, "stats": None, "is_tests": None, "train": None, "test": None,
            "error": str(e)
        }

ace_lib.get_specified_alpha_stats = patched_get_specified_alpha_stats

# =============================================================================

class ConfigManager:
    _instance_lock = threading.Lock()
    _config = None
    _session = None
    _observers = []
    _username_cache = None
    _password_cache = None
    logger = Logger(log_dir='logs/')

    def __init__(self):
        """加载配置文件并初始化内存配置"""
        if ConfigManager._config is None:
            config_data = load_config("config/config.ini")
            if config_data is None:
                ConfigManager._config = {}
                self.logger.warning("Configuration file loading failed, using empty configuration")
            else:
                ConfigManager._config = config_data
                ConfigManager._username_cache = config_data.get('username')
                ConfigManager._password_cache = config_data.get('password')

        self._create_session()

    def _create_session(self):
        """创建并初始化session"""
        if ConfigManager._session is None:
            ConfigManager._session = self._login()
        else:
            if not self._test_session():
                ConfigManager._session = self._login()

    def _login(self):
        """使用配置凭据登录WorldQuant BRAIN"""
        self.logger.info("Starting login attempt")
        count = 0
        count_limit = 30

        while True:
            try:
                s = ace_lib.start_session()
                if s:
                    self.logger.info("Successfully logged in to BRAIN")
                    return s
                raise RuntimeError("Login returned empty session")
            except Exception as e:
                count += 1
                self.logger.error(f"Login attempt {count} failed: {e}")
                if count >= count_limit:
                    self.logger.error("Max login attempts reached")
                    return None
                else:
                    self.logger.info("Login failed. Retry after 1 minute.")
                    time.sleep(60)

    def _test_session(self):
        """测试session是否有效"""
        if not ConfigManager._session:
            return False
        try:
            response = ConfigManager._session.get('https://api.worldquantbrain.com/user')
            return response.status_code == 200
        except:
            return False

    def renew_session(self):
        """强制刷新session"""
        self.logger.info("Forcing session renewal")
        with self._instance_lock:
            session = self._login()
            if session:
                ConfigManager._session = session
            return session is not None

    def get_session(self):
        """获取当前session"""
        if not self._test_session():
            with self._instance_lock:
                if not self._test_session():
                    self._create_session()
        return ConfigManager._session

    def get(self, key, default=None):
        """线程安全的配置获取方法"""
        with self._instance_lock:
            return ConfigManager._config.get(key, default)

    def set(self, key, value):
        """线程安全的配置更新方法"""
        with self._instance_lock:
            ConfigManager._config[key] = value

    def get_all(self):
        """线程安全地获取全部配置"""
        with self._instance_lock:
            return dict(ConfigManager._config)

    def on_config_change(self, callback):
        """注册配置变更回调"""
        if callback not in ConfigManager._observers:
            ConfigManager._observers.append(callback)

    def notify_observers(self):
        """通知所有观察者配置已变更"""
        self.logger.info("Notifying observers of config change")
        for observer in ConfigManager._observers:
            try:
                observer(ConfigManager._config)
            except Exception as e:
                self.logger.error(f"Error notifying observer: {e}")

    def __new__(cls, *args, **kwargs):
        if not hasattr(ConfigManager, "_instance"):
            with ConfigManager._instance_lock:
                if not hasattr(ConfigManager, "_instance"):
                    ConfigManager._instance = super().__new__(cls)
        return ConfigManager._instance

# 创建单例实例
config_manager = ConfigManager()
