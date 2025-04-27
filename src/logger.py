import logging
import os
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
import pytz

class Logger:
    def __init__(self, log_dir="../logs", timezone="Asia/Shanghai"):
        """
        初始化 Logger 类，支持按天轮转日志文件，并显式指定时区。
        - log_dir: 日志文件存储目录，默认为 "../logs"
        - timezone: 时区名称，默认为 "Asia/Shanghai"
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
            backupCount=30,   # 保留最近 30 天的日志文件
            encoding="utf-8",
            delay=True        # 延迟创建文件，减少 IO 开销
        )

        # 自定义 Formatter，显式指定时区
        class TimezoneFormatter(logging.Formatter):
            def __init__(self, fmt, tz):
                super().__init__(fmt)
                self.tz = pytz.timezone(tz)

            def formatTime(self, record, datefmt=None):
                dt = datetime.fromtimestamp(record.created, self.tz)
                return dt.strftime(datefmt or "%Y-%m-%d %H:%M:%S,%f") + f" {self.tz.zone}"

        # 设置日志格式（包含毫秒和时区）
        log_format = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
        formatter = TimezoneFormatter(log_format, timezone)
        handler.setFormatter(formatter)

        # 创建控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        # 创建 logger 实例
        self.logger = logging.getLogger("AlphaSimulatorBot")
        self.logger.setLevel(logging.INFO)  # 设置默认日志级别
        self.logger.handlers.clear()  # 清除现有处理器，避免重复
        self.logger.addHandler(handler)
        self.logger.addHandler(console_handler)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)

    def warning(self, message):
        self.logger.warning(message)
    
    def debug(self, message):
        """记录 DEBUG 级别的日志"""
        self.logger.debug(message)