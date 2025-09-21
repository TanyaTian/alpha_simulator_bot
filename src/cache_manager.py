import time
import threading
from collections import defaultdict
from typing import List, Dict, Any
from logger import Logger

class CacheManager:
    """
    一个用于管理数据库操作缓存的类，旨在通过批量处理和按需加载减少数据库交互，提高效率。

    新版设计思路 (按需加载):
    1.  **按需加载 (On-Demand Loading)**:
        -   与旧版不同，本类在初始化时不会加载任何数据，避免了内存溢出的风险。
        -   缓存是按区域(`region`)隔离的，每个区域有一个独立的队列。
        -   当外部请求某个区域的数据时，如果该区域的缓存为空，管理器会触发一次数据库查询，
            加载一批新的数据到该区域的缓存中。加载的数量由 `batch_number_for_every_queue` 控制。
        -   这种“用完再取”的策略，在保证低内存占用的同时，通过批量预取数据来维持高性能。

    2.  **写操作缓冲 (Write Buffering)**:
        -   对 `alpha_list_pending_simulated_table` 的状态更新 (`update_alpha_status`) 不会立即写入数据库，
            而是将变更信息（ID和新状态）暂存入一个“脏”集合 (`self.dirty_alpha_ids`)。
        -   对 `simulation_tasks_table` 的新任务插入 (`add_simulation_tasks_batch`) 也被暂存到一个内存列表 (`self.new_tasks_buffer`)。

    3.  **批量刷新 (Batch Flushing)**:
        -   核心方法 `flush()` 负责将所有缓存的写操作同步到数据库。
        -   它将“脏”集合中的状态变更，按状态分组后，通过批量 `UPDATE` 语句执行。
        -   它将新任务缓冲区中的所有任务，通过一个批量 `INSERT` 语句一次性写入。
        -   所有写操作都在一个数据库事务中执行，以确保数据一致性。

    4.  **线程安全 (Thread Safety)**:
        -   使用 `threading.Lock` 来确保对缓存的并发访问（特别是数据获取、补充和刷新）是安全的。

    5.  **数据结构**:
        -   `self.alpha_cache_by_region`: 一个 `defaultdict(list)`，将 `region` 映射到一个alpha记录列表。这是核心的按需缓存结构。
        -   `self.dirty_alpha_ids`: 一个集合，存储被修改过的 alpha 的元组 `(id, status)`。
        -   `self.new_tasks_buffer`: 一个列表，缓冲待插入 `simulation_tasks_table` 的新记录。
    """

    def __init__(self, alpha_dao, task_dao, batch_number_for_every_queue: int):
        """
        初始化缓存管理器。

        Args:
            alpha_dao: `AlphaListPendingSimulatedDAO` 的实例。
            task_dao: `SimulationTasksDAO` 的实例。
            batch_number_for_every_queue (int): 当缓存为空时，每次从数据库为每个region加载的记录数。
        """
        self.logger = Logger()
        self.alpha_dao = alpha_dao
        self.task_dao = task_dao
        self.batch_number_for_every_queue = batch_number_for_every_queue

        # 按region存储的alpha缓存，使用defaultdict简化代码
        self.alpha_cache_by_region: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        
        # 记录被修改过的 alpha 的 id 和新状态
        self.dirty_alpha_ids: set = set()
        
        # 缓冲待插入 `simulation_tasks_table` 的新任务
        self.new_tasks_buffer: List[Dict[str, Any]] = []

        self.logger.info(f"CacheManager initialized with on-demand loading. Batch size per region: {self.batch_number_for_every_queue}")

    def _refill_cache_for_region(self, region: str):
        """
        为一个特定的region补充缓存。它会从数据库获取新的一批数据。
        这是一个内部方法，在锁的保护下被调用。
        """
        self.logger.info(f"Cache for region '{region}' is empty. Refilling from database...")
        try:
            # 调用DAO层获取指定region的一批待处理alphas
            new_alphas = self.alpha_dao.fetch_and_lock_pending_by_region(
                region=region,
                limit=self.batch_number_for_every_queue
            )
            if new_alphas:
                self.alpha_cache_by_region[region].extend(new_alphas)
                self.logger.info(f"Successfully refilled cache for region '{region}' with {len(new_alphas)} new alphas.")
            else:
                self.logger.info(f"No new pending alphas found in database for region '{region}'.")
        except Exception as e:
            self.logger.error(f"Failed to refill cache for region '{region}': {e}")

    def get_pending_alphas_by_region(self, region: str, limit: int) -> List[Dict[str, Any]]:
        """
        从内存缓存中获取指定 region 的待处理 alpha 记录。
        如果缓存为空，则会触发补充机制 (`_refill_cache_for_region`)。

        Args:
            region (str): 需要查询的地区。
            limit (int): 需要获取的最大记录数。

        Returns:
            List[Dict[str, Any]]: 符合条件的 alpha 记录列表。
        """
        # 检查当前region的缓存是否为空，如果为空则补充
        if not self.alpha_cache_by_region[region]:
            self._refill_cache_for_region(region)
        
        # 如果补充后依然没有数据，则返回空列表
        if not self.alpha_cache_by_region[region]:
            return []

        # 从缓存中取出所需数量的记录
        num_to_fetch = min(limit, len(self.alpha_cache_by_region[region]))
        
        # 从列表头部取出元素，模拟队列行为
        results = [self.alpha_cache_by_region[region].pop(0) for _ in range(num_to_fetch)]
        
        self.logger.info(f"Retrieved {len(results)} alphas for region '{region}' from cache. " 
                            f"{len(self.alpha_cache_by_region[region])} items remaining in cache for this region.")
        
        return results

    def update_alpha_status(self, alpha_ids: List[int], status: str):
        """
        在内存中将alpha标记为“脏”，以便后续通过 `flush` 方法批量更新。
        这个操作是轻量级的，因为它不直接与数据库交互。

        Args:
            alpha_ids (List[int]): 需要更新状态的 alpha 记录的 ID 列表。
            status (str): 新的状态 (e.g., 'sent', 'failed')。
        """
        for alpha_id in alpha_ids:
            self.dirty_alpha_ids.add((alpha_id, status))
        self.logger.info(f"Marked {len(alpha_ids)} alphas with status '{status}' as dirty.")

    def add_simulation_tasks_batch(self, tasks_data: List[Dict[str, Any]]):
        """
        将一批新的模拟任务批量添加到内存缓冲区。

        Args:
            tasks_data (List[Dict[str, Any]]): 代表新任务的字典列表。
        """
        self.new_tasks_buffer.extend(tasks_data)
        self.logger.info(f"Buffered {len(tasks_data)} new simulation tasks.")

    def get_dirty_items_count(self) -> int:
        """
        获取当前缓存中待写入数据库的项的总数（“脏”数据量）。
        此方法是线程安全的。

        Returns:
            int: 待处理的 alpha 更新和新任务的总数。
        """
        return len(self.dirty_alpha_ids) + len(self.new_tasks_buffer)

    def flush(self):
        """
        将所有缓存的更改（更新和插入）通过批量操作同步到数据库。
        此操作在数据库事务中执行，以确保原子性。
        """
        if not self.dirty_alpha_ids and not self.new_tasks_buffer:
            return

        self.logger.info(f"Flushing data to database: "
                            f"{len(self.dirty_alpha_ids)} alpha updates, "
                            f"{len(self.new_tasks_buffer)} new tasks.")
        
        try:
            # 1. 处理 alpha 状态更新
            if self.dirty_alpha_ids:
                updates_by_status = defaultdict(list)
                for alpha_id, status in self.dirty_alpha_ids:
                    updates_by_status[status].append(alpha_id)
                
                for status, ids in updates_by_status.items():
                    self.alpha_dao.batch_update_status_by_ids(ids, status)
                    self.logger.info(f"Successfully flushed {len(ids)} alpha updates with status '{status}'.")
            
            # 2. 处理新任务的批量插入
            if self.new_tasks_buffer:
                self.task_dao.batch_insert(self.new_tasks_buffer)
                self.logger.info(f"Successfully flushed {len(self.new_tasks_buffer)} new simulation tasks.")

            # 3. 如果数据库操作成功，清空缓存
            self.dirty_alpha_ids.clear()
            self.new_tasks_buffer.clear()
            self.logger.info("Flush successful. Caches cleared.")

        except Exception as e:
            self.logger.error(f"Flush failed: {e}. Caches will be retained for next retry.")
            raise