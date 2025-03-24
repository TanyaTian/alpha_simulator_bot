import logging
import os
from datetime import datetime, date
from logging.handlers import TimedRotatingFileHandler

class Logger:
    def __init__(self, log_dir="../logs"):
        """
        初始化 Logger 类，支持按天轮转日志文件。
        - log_dir: 日志文件存储目录，默认为 "../logs"
        """
        # 确保日志目录存在
        os.makedirs(log_dir, exist_ok=True)

        # 设置日志文件路径（基础文件名，不含日期）
        log_file_base = os.path.join(log_dir, "simulation.log")

        # 创建 TimedRotatingFileHandler，按天轮转
        handler = TimedRotatingFileHandler(
            filename=log_file_base,
            when="midnight",  # 每天 0 点轮转
            interval=1,       # 轮转间隔为 1 天
            backupCount=30,   # 保留最近 30 天的日志文件（可调整）
            encoding="utf-8"
        )

        # 设置日志格式
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)

        # 配置 logging
        logging.basicConfig(
            level=logging.INFO,
            handlers=[
                handler,              # 按天轮转的文件处理器
                logging.StreamHandler()  # 控制台输出
            ]
        )

        # 创建 logger 实例
        self.logger = logging.getLogger("AlphaSimulatorBot")

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)

    def warning(self, message):
        self.logger.warning(message)