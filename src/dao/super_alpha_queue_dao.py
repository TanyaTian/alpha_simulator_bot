from typing import List, Dict, Any
from database import Database
from logger import Logger

class SuperAlphaQueueDAO:
    """
    用于与 super_alpha_queue 表交互的数据访问对象。
    """
    logger = Logger()
    TABLE_NAME = 'super_alpha_queue'

    def __init__(self):
        self.db = Database()
        self.logger.debug("SuperAlphaQueueDAO initialized")

    def batch_insert_tasks(self, tasks: List[Dict[str, Any]], batch_size: int = 1000) -> int:
        """
        批量插入新的 Super Alpha 任务, 支持分批次插入。

        :param tasks: 一个字典列表，每个字典代表一个任务。
                      e.g., [{'priority': 1, 'region': 'USA', 'combo': '...', 'selection': '...', 'settings_json': '{...}'}, ...]
        :param batch_size: 每批次插入数据库的数量。
        :return: 成功插入的记录总行数。
        """
        if not tasks:
            self.logger.warning("batch_insert_tasks: provided task list is empty.")
            return 0
        
        total_inserted = 0
        num_batches = (len(tasks) + batch_size - 1) // batch_size
        
        self.logger.info(f"Starting batch insert of {len(tasks)} tasks in {num_batches} batches (size: {batch_size}) into {self.TABLE_NAME}.")

        try:
            # 确保所有任务都有相同的键，以便批量插入
            if not tasks or not all(isinstance(d, dict) and d.keys() == tasks[0].keys() for d in tasks):
                self.logger.error("All items in tasks must be dictionaries with the same keys for batch insert.")
                return 0

            for i in range(num_batches):
                start_idx = i * batch_size
                end_idx = min((i + 1) * batch_size, len(tasks))
                batch = tasks[start_idx:end_idx]

                self.logger.debug(f"Inserting batch {i+1}/{num_batches} (rows {start_idx}-{end_idx-1})")

                # 直接将字典列表传递给 database 层的 batch_insert
                rowcount = self.db.batch_insert(self.TABLE_NAME, batch)

                if rowcount > 0:
                    total_inserted += rowcount
                    self.logger.debug(f"Batch {i+1} inserted {rowcount} rows successfully.")
                else:
                    self.logger.error(f"Batch {i+1} insert failed.")

            if total_inserted > 0:
                self.logger.info(f"Total batch insert successful. Rows inserted: {total_inserted}/{len(tasks)}")
            else:
                self.logger.error("Total batch insert failed, no rows were inserted.")

            return total_inserted

        except Exception as e:
            self.logger.error(f"An error occurred during batch insert into {self.TABLE_NAME}: {e}", exc_info=True)
            return total_inserted # 返回部分成功的数量

    def fetch_pending_tasks(self, limit: int = 10, region: str = 'USA') -> List[Dict[str, Any]]:
        """
        获取一批待处理的、优先级最高的任务。

        :param limit: 本次获取的任务数量上限。
        :param region: 需要获取任务的地域。
        :return: 一个包含任务完整信息的字典列表。
                 e.g., [{'id': 1, 'status': 'pending', 'combo': '...', 'region': 'USA', ...}, ...]
        """
        sql = f"""
            SELECT * FROM {self.TABLE_NAME}
            WHERE status = 'pending' AND region = %s
            ORDER BY priority DESC, id ASC
            LIMIT %s
        """
        try:
            results = self.db.query(sql, (region, limit))
            return results if results else []
        except Exception as e:
            self.logger.error(f"Error fetching pending tasks from {self.TABLE_NAME}: {e}")
            return []

    def update_task_status_bulk(self, task_ids: List[int], status: str) -> int:
        """
        批量更新一组任务的状态。用于将任务 '锁定' 为 in_progress 或标记为最终状态。

        :param task_ids: 需要更新的任务ID列表。
        :param status: 目标状态 ('in_progress', 'completed', 'failed')。
        :return: 成功更新的记录行数。
        """
        if not task_ids:
            return 0
            
        placeholders = ','.join(['%s'] * len(task_ids))
        sql = f"UPDATE {self.TABLE_NAME} SET status = %s WHERE id IN ({placeholders})"
        params = [status] + task_ids
        
        try:
            rowcount = self.db.execute(sql, tuple(params))
            self.logger.info(f"Successfully updated {rowcount} tasks in {self.TABLE_NAME} to status '{status}'.")
            return rowcount
        except Exception as e:
            self.logger.error(f"Error updating task status in {self.TABLE_NAME}: {e}")
            return 0

    def close(self):
        """关闭数据库连接。"""
        self.db.close()
