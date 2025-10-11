# 导入必要的库
import asyncio  # 异步I/O操作的核心库
import aiohttp  # 异步HTTP客户端/服务器库
import os
import time
from datetime import datetime
from dao import SimulationTasksDAO  # 数据库操作类，用于模拟任务
from dao import SimulatedAlphasDAO  # 数据库操作类，用于模拟的alphas
from logger import Logger  # 日志记录器
from config_manager import config_manager  # 导入全局配置管理器单例

class AlphaPoller:
    """
    AlphaPoller负责轮询处于“待处理”状态的模拟任务，
    通过异步HTTP请求获取任务结果，并将结果存回数据库。
    """

    def __init__(self):
        """
        初始化AlphaPoller。
        """
        self.logger = Logger()  # 初始化日志记录器
        self.simulation_tasks_dao = SimulationTasksDAO()  # 初始化模拟任务DAO
        self.simulated_alphas_dao = SimulatedAlphasDAO()  # 初始化模拟alphas DAO
        
        # 从配置管理器获取会话cookie，用于API认证
        self.session = config_manager.get_session()
        
        # 注册一个回调函数，当配置发生变化时更新session
        config_manager.on_config_change(self.handle_config_change)

    def handle_config_change(self, new_config):
        """
        处理配置变更的回调函数。
        当配置更新时，此函数会被调用，以刷新会话session。
        """
        self.logger.info("Configuration updated, refreshing session...")
        self.session = config_manager.get_session()

    async def fetch_alpha_details(self, session, child):
        """
        异步获取单个childId对应的alpha详细信息。

        Args:
            session (aiohttp.ClientSession): aiohttp会话对象。
            child (str): 要查询的childId。

        Returns:
            dict or None: 如果成功，返回格式化后的alpha详细信息字典；否则返回None。
        """
        brain_api_url = "https://api.worldquantbrain.com"  # API基础URL
        request_timeout = 30  # 请求超时时间

        try:
            # 步骤1: 异步获取模拟进度，拿到alphaId
            self.logger.debug(f"Fetching simulation progress for child: {child}")
            async with session.get(f"{brain_api_url}/simulations/{child}", timeout=request_timeout) as response:
                response.raise_for_status()  # 如果HTTP状态码不是2xx，则抛出异常
                child_data = await response.json()  # 异步解析JSON响应
                self.logger.debug(f"Child response: {child_data}")

            alpha_id = child_data.get("alpha")
            if not alpha_id:
                self.logger.warning(f"Alpha ID not found for child {child}. Response: {child_data}")
                await self._update_task_status_async(child, 'failed')
                return None

            # 步骤2: 异步获取alpha的详细信息
            self.logger.debug(f"Fetching details for alpha_id {alpha_id}")
            async with session.get(f"{brain_api_url}/alphas/{alpha_id}", timeout=request_timeout) as response:
                response.raise_for_status()
                alpha_details = await response.json()

            if not alpha_details:
                self.logger.warning(f"Details for alpha_id {alpha_id} are empty.")
                await self._update_task_status_async(child, 'failed')
                return None
            
            # 步骤3: 格式化数据以适应数据库表结构
            formatted = self._format_alpha_details(alpha_details)
            
            self.logger.debug(f"Successfully fetched details for alpha_id {alpha_id}")
            await self._update_task_status_async(child, 'completed')
            return formatted

        except aiohttp.ClientError as e:
            self.logger.error(f"Network error when fetching details for child {child}: {e}")
            # 网络错误时增加查询次数，以便后续重试
            await self._increment_query_attempts_async(child)
            return None
        except asyncio.TimeoutError:
            self.logger.error(f"Request timed out when fetching details for child {child}")
            await self._increment_query_attempts_async(child)
            return None
        except Exception as e:
            self.logger.error(f"Unknown error when processing child {child}: {e}")
            await self._update_task_status_async(child, 'failed')
            return None

    def _format_alpha_details(self, alpha_details):
        """
        将从API获取的alpha详细信息格式化为数据库接受的格式。
        """
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
            except (ValueError, TypeError):
                grade = None
                        
        return {
            "id": alpha_details.get("id"),
            "type": alpha_details.get("type"),
            "author": alpha_details.get("author"),
            "settings": str(settings),
            "regular": str(alpha_details.get("regular", {})),
            "dateCreated": self.to_mysql_datetime(date_created),
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

    async def process_children_async(self, children):
        """
        异步并发处理一批children。
        """
        self.logger.info(f"Starting to process {len(children)} children asynchronously...")
        if not children:
            self.logger.warning("Children list is empty, skipping processing.")
            return

        async with aiohttp.ClientSession(cookies=self.session.cookies) as session:
            # 创建一组并发任务
            tasks = [self.fetch_alpha_details(session, child) for child in children]
            # 等待所有任务完成
            results = await asyncio.gather(*tasks)
        
        # 过滤掉失败的结果（None）
        alpha_details_list = [res for res in results if res is not None]

        if alpha_details_list:
            try:
                # 在事件循环的executor中运行同步的数据库批量插入操作，避免阻塞
                loop = asyncio.get_running_loop()
                records_inserted = await loop.run_in_executor(
                    None,  # 使用默认的ThreadPoolExecutor
                    lambda: self.simulated_alphas_dao.batch_insert(
                        alpha_details_list,
                        on_duplicate_update=True
                    )
                )
                self.logger.info(f"Successfully inserted/updated {records_inserted} records in the database.")
            except Exception as e:
                self.logger.error(f"Database batch insert failed: {e}")
        else:
            self.logger.warning("No alpha details to insert.")

    async def _update_task_status_async(self, child_id, status):
        """异步更新任务状态。"""
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                None,
                lambda: self.simulation_tasks_dao.update_status_and_last_query(
                    child_id=child_id,
                    new_status=status,
                    query_time=datetime.now()
                )
            )
        except Exception as e:
            self.logger.error(f"Failed to update task status for child {child_id}: {e}")

    async def _increment_query_attempts_async(self, child_id):
        """异步增加任务查询次数。"""
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                None,
                lambda: self.simulation_tasks_dao.increment_query_attempts(child_id)
            )
        except Exception as e:
            self.logger.error(f"Failed to increment query attempts for child {child_id}: {e}")

    def to_mysql_datetime(self, dt_str):
        """将ISO格式的日期字符串转换为MySQL的DATETIME格式。"""
        if not dt_str:
            return None
        try:
            # Python 3.7+ fromisoformat可以直接解析带'Z'的格式
            if dt_str.endswith('Z'):
                dt_str = dt_str[:-1] + '+00:00'
            dt = datetime.fromisoformat(dt_str)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            self.logger.error(f"Failed to parse datetime: {dt_str}, error: {e}")
            return None
    
    async def start_polling_async(self):
        """
        启动异步轮询循环。
        """
        self.logger.info("Starting async polling for simulation tasks...")
        while True:
            try:
                # 在executor中运行同步的数据库查询
                loop = asyncio.get_running_loop()
                pending_tasks = await loop.run_in_executor(
                    None,
                    lambda: self.simulation_tasks_dao.list_pending_tasks_lite(limit=100)
                )
                
                if pending_tasks:
                    self.logger.info(f"Found {len(pending_tasks)} pending tasks.")
                    await self.process_children_async(pending_tasks)
                else:
                    self.logger.info("No pending tasks found. Sleeping for 1 minute...")
                    await asyncio.sleep(60)  # 异步休眠
                    
                # 短暂休眠以防止CPU在没有任务时空转
                await asyncio.sleep(1)
                
            except Exception as e:
                self.logger.error(f"Error in polling loop: {e}")
                await asyncio.sleep(60)  # 发生异常时休眠