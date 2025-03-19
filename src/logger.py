import logging
import os
from datetime import datetime

class Logger:
    def __init__(self, log_dir="../logs"):
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, f"simulation_{datetime.now().strftime('%Y%m%d')}.log")
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.FileHandler(log_file), logging.StreamHandler()]
        )
        self.logger = logging.getLogger("AlphaSimulatorBot")

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)

    def warning(self, message):
        self.logger.warning(message)  # 添加 warning 方法