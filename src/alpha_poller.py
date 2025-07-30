import requests
import os
import time
import threading
from datetime import datetime
from dao import SimulationTasksDAO
from dao import SimulatedAlphasDAO
from logger import Logger
from config_manager import config_manager  # 导入ConfigManager单例


class AlphaPoller:

    def __init__(self):
        # 使用ConfigManager的全局Logger
        self.logger = Logger()
        self.simulation_tasks_dao = SimulationTasksDAO()
        self.simulated_alphas_dao = SimulatedAlphasDAO()
        
        # 从ConfigManager获取session
        self.session = config_manager.get_session()
        
        # 注册配置变更回调
        config_manager.on_config_change(self.handle_config_change)

    # 移除sign_in方法，直接使用ConfigManager的session
    
    # 处理配置变更回调
    def handle_config_change(self, new_config):
        """处理配置变更回调"""
        self.logger.info("Configuration updated, renewing session...")
        self.session = config_manager.get_session()
    
    def process_children(self, children):
        """同步处理 children，获取 alphaId 和 alpha detail，写入文件"""
        self.logger.info(f"Processing {len(children)} children...")
        if not children:
            self.logger.warning("Empty children list received. Skipping processing.")
            return

        brain_api_url = "https://api.worldquantbrain.com"
        alpha_details_list = []
        request_timeout = 30

        for child in children:
            self.logger.debug(f"Processing child: {child}")
            if not isinstance(child, str):
                self.logger.error(f"Invalid child format: {child} (Expected string)")
                continue

            try:
                self.logger.debug(f"Fetching child simulation progress: {child}")
                child_progress = self.session.get(f"{brain_api_url}/simulations/{child}", timeout=request_timeout)
                child_progress.raise_for_status()
                child_data = child_progress.json()
                self.logger.debug(f"Child response: {child_data}")

                alpha_id = child_data.get("alpha")
                if not alpha_id:
                    self.logger.warning(f"No alpha_id found for child: {child}. Response: {child_data}")
                    self._update_task_status(child, 'failed')
                    continue

                self.logger.debug(f"Fetching alpha detail for alpha_id: {alpha_id}")
                alpha_response = self.session.get(f"{brain_api_url}/alphas/{alpha_id}", timeout=request_timeout)
                alpha_response.raise_for_status()
                alpha_details = alpha_response.json()
                if not alpha_details:
                    self.logger.warning(f"Empty alpha details for alpha_id: {alpha_id}")
                    self._update_task_status(child, 'failed')
                    continue
                
                # 转换数据格式
                date_created = alpha_details.get("dateCreated")
                
                datetime_str = date_created.split("T")[0].replace("-", "") if date_created else None
                
                settings = alpha_details.get("settings", {})
                region = settings.get("region")

                favorite = 1 if alpha_details.get("favorite", False) else 0
                hidden = 1 if alpha_details.get("hidden", False) else 0

                grade = alpha_details.get("grade")
                if grade is not None:
                    try:
                        grade = round(float(grade), 2)
                    except:
                        grade = None
                                
                formatted = {
                    "id": alpha_details.get("id"),
                    "type": alpha_details.get("type"),
                    "author": alpha_details.get("author"),
                    "settings": str(alpha_details.get("settings", {})),
                    "regular": str(alpha_details.get("regular", {})),
                    "dateCreated": self.to_mysql_datetime(alpha_details.get("dateCreated")),
                    "dateSubmitted": self.to_mysql_datetime(alpha_details.get("dateSubmitted")),
                    "dateModified": self.to_mysql_datetime(alpha_details.get("dateModified")),
                    "name": alpha_details.get("name"),
                    "favorite": favorite,
                    "hidden": hidden,
                    "color": alpha_details.get("color"),
                    "category": alpha_details.get("category"),
                    "tags": str(alpha_details.get("tags", [])),
                    "classifications": str(alpha_details.get("classifications", [])),
                    "grade": grade,
                    "stage": alpha_details.get("stage"),
                    "status": alpha_details.get("status"),
                    "is": str(alpha_details.get("is", {})),
                    "os": alpha_details.get("os"),
                    "train": alpha_details.get("train"),
                    "test": alpha_details.get("test"),
                    "prod": alpha_details.get("prod"),
                    "competitions": alpha_details.get("competitions"),
                    "themes": str(alpha_details.get("themes", [])),
                    "pyramids": str(alpha_details.get("pyramids", [])),
                    "pyramidThemes": str(alpha_details.get("pyramidThemes", [])),
                    "team": alpha_details.get("team"),
                    "datetime": datetime_str,
                    "region": region
                }
                alpha_details_list.append(formatted)
                self.logger.debug(f"Successfully retrieved alpha details for alpha_id: {alpha_id}")
                self._update_task_status(child, 'completed')

            except requests.exceptions.RequestException as e:
                self.logger.error(f"Error fetching child {child} or alpha detail: {e}")
                if config_manager.renew_session():
                    self.session = config_manager.get_session()
                self._increment_query_attempts(child)
            except ValueError as e:
                self.logger.error(f"Invalid JSON response for child {child}: {e}")
                self._update_task_status(child, 'failed')
            except Exception as e:
                self.logger.error(f"Unexpected error processing child {child}: {e}")
                self._update_task_status(child, 'failed')
        if alpha_details_list:
            try:
                records_inserted = self.simulated_alphas_dao.batch_insert(alpha_details_list)
                self.logger.info(f"Successfully inserted {records_inserted} records into simulated_alphas_table")
            except Exception as e:
                self.logger.error(f"Database batch insert failed: {e}")
        else:
            self.logger.warning(f"No alpha details to insert for {len(children)} children. alpha_details_list is empty.")
    
    def _update_task_status(self, child_id, status):
        """更新任务状态并记录最后查询时间"""
        try:
            self.simulation_tasks_dao.update_status_and_last_query(
                child_id=child_id,
                new_status=status,
                query_time=datetime.now()
            )
        except Exception as dao_e:
            self.logger.error(f"Failed to update task status for child {child_id}: {dao_e}")
    
    def _increment_query_attempts(self, child_id):
        """增加任务查询次数"""
        try:
            self.simulation_tasks_dao.increment_query_attempts(child_id)
        except Exception as dao_e:
            self.logger.error(f"Failed to increment query attempts for child {child_id}: {dao_e}")

    def to_mysql_datetime(self, dt_str):
        if not dt_str:
            return None
        try:
            dt = datetime.fromisoformat(dt_str)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            print(f"Failed to parse datetime: {dt_str}, error: {e}")
            return None
    
    def start_polling(self):
        """启动轮询线程"""
        
        def polling_loop():
            while True:
                try:
                    # 获取100个pending状态的任务
                    pending_tasks = self.simulation_tasks_dao.list_pending_tasks_lite(limit=1)
                    
                    if pending_tasks:
                        self.logger.info(f"Found {len(pending_tasks)} pending tasks")
                        self.process_children(pending_tasks)
                    else:
                        self.logger.info("No pending tasks found. Sleeping for 1 minute...")
                        time.sleep(60)  # 没有待处理任务时休眠1分钟
                        
                    # 短暂休眠防止CPU空转
                    time.sleep(1)
                    
                except Exception as e:
                    self.logger.error(f"Error in polling loop: {e}")
                    time.sleep(60)  # 出现异常时休眠1分钟
        
        # 创建并启动后台线程
        poller_thread = threading.Thread(
            target=polling_loop,
            daemon=True  # 设置为守护线程，主程序退出时自动终止
        )
        poller_thread.start()
        self.logger.info("Started simulation task polling thread")
