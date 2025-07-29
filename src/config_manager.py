from flask import Flask, request, jsonify
import threading
import os
from utils import load_config  # 导入配置加载工具
from logger import Logger 
import requests

class ConfigManager:
    _instance_lock = threading.Lock()
    _config = None  # 用None初始化，确保首次加载配置
    _session = None  # 新增session单例
    _observers = []  # 观察者列表
    # 创建全局 Logger 实例
    logger = Logger(log_dir='logs/')
    
    def __init__(self):
        """加载配置文件并初始化内存配置"""
        if ConfigManager._config is None:
            # 第一次初始化时加载配置文件
            config_data = load_config("config/config.ini")
            
            if config_data is None:
                # 如果加载失败，使用空字典并记录警告
                ConfigManager._config = {}
                self.logger.warning("Configuration file loading failed, using empty configuration")
            else:
                ConfigManager._config = config_data
        
        # 初始化或刷新session
        self._create_session()
    
    def _create_session(self):
        """创建并初始化session"""
        if ConfigManager._session is None:
            ConfigManager._session = self._login()
        else:
            # 现有session可能已过期，尝试刷新
            if not self._test_session():
                ConfigManager._session = self._login()
    
    def _login(self):
        """使用配置凭据登录WorldQuant BRAIN"""
        self.logger.info("Starting login attempt")
        username = ConfigManager._config['username']
        password = ConfigManager._config['password']
        
        s = requests.Session()
        s.auth = (username, password)
        count = 0
        count_limit = 3
        
        while True:
            try:
                response = s.post('https://api.worldquantbrain.com/authentication')
                response.raise_for_status()
                self.logger.info("Successfully logged in to BRAIN")
                return s
            except Exception as e:
                count += 1
                self.logger.error(f"Login attempt {count} failed: {e}")
                if count >= count_limit:
                    self.logger.error("Max login attempts reached")
                    return None
    
    def _test_session(self):
        """测试session是否有效"""
        try:
            response = ConfigManager._session.get('https://api.worldquantbrain.com/user', timeout=5)
            return response.status_code == 200
        except:
            return False
    
    def renew_session(self):
        """强制刷新session"""
        self.logger.info("Forcing session renewal")
        with self._instance_lock:
            session = self._login()
            if session:
                ConfigManager._session = self._login()
            return session is not None
    
    def get_session(self):
        """获取当前session"""
        if not self._test_session():
            self._create_session()
        return ConfigManager._session
    
    def get(self, key, default=None):
        """线程安全的配置获取方法"""
        self.logger.debug(f"Getting config key: {key}")
        with self._instance_lock:
            value = ConfigManager._config.get(key, default)
            self.logger.debug(f"Retrieved value: {value} for key: {key}")
            return value
    
    def set(self, key, value):
        """线程安全的配置更新方法"""
        self.logger.debug(f"Setting config key: {key} to value: {value}")
        with self._instance_lock:
            ConfigManager._config[key] = value
            self.logger.debug(f"Successfully updated key: {key}")
    
    def get_all(self):
        """线程安全地获取全部配置"""
        with self._instance_lock:
            return dict(ConfigManager._config)  # 返回副本，避免外部修改
    
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
                self.logger.debug(f"Successfully notified observer: {observer}")
            except Exception as e:
                self.logger.error(f"Error notifying observer: {e}")
    
    def __new__(cls, *args, **kwargs):
        """单例模式实现"""
        if not hasattr(ConfigManager, "_instance"):
            with ConfigManager._instance_lock:
                if not hasattr(ConfigManager, "_instance"):
                    ConfigManager._instance = super().__new__(cls)
        return ConfigManager._instance

# 创建单例实例
config_manager = ConfigManager()

app = Flask(__name__)

@app.route('/config', methods=['GET'])
def get_config():
    """获取当前内存中的配置"""
    config_manager.logger.debug("GET /config endpoint accessed")
    result = jsonify(config_manager.get_all())
    config_manager.logger.debug(f"Returning config: {result.json}")
    return result

@app.route('/config/update', methods=['POST'])
def update_config():
    """更新配置并通知观察者"""
    config_manager.logger.info("POST /config/update endpoint accessed")
    data = request.json
    if not data:
        config_manager.logger.warning("No JSON data provided in request")
        return jsonify({'status': 'error', 'message': 'No JSON data provided'}), 400

    config_manager.logger.debug(f"Received config update data: {data}")
    updated_keys = []
    for key, value in data.items():
        config_manager.logger.debug(f"Processing update for key: {key}")
        config_manager.set(key, value)
        updated_keys.append(key)
    
    config_manager.logger.info(f"Config updated with keys: {updated_keys}")
    config_manager.notify_observers()
    config_manager.logger.debug("Observers notified of config change")
    
    return jsonify({
        'status': 'success',
        'updated': updated_keys,
        'current_config': config_manager.get_all()
    })

def run_config_server(port=5001):
    """启动配置服务"""
    app.run(host='0.0.0.0', port=port)

if __name__ == '__main__':
    run_config_server()
