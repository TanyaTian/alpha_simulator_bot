import signal
import os
from logger import Logger 

class SignalManager:
    def __init__(self):
        self.handlers = []
        self.logger = Logger()

    def add_handler(self, handler):
        self.handlers.append(handler)

    def handle_signal(self, signum, frame):
        self.logger.info(f"在进程 {os.getpid()} 中收到信号 {signum}")
        for handler in self.handlers:
            try:
                handler(signum, frame)
            except Exception as e:
                self.logger.error(f"处理函数 {handler} 出错: {e}")