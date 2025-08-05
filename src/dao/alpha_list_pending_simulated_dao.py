# dao/alpha_dao.py

from database import Database
from logger import Logger

# 初始化日志
alpha_logger = Logger().logger


class AlphaListPendingSimulatedDAO:
    """
    DAO for alpha_list_pending_simulated_table
    支持批量插入、条件查询、状态更新等操作
    """

    def __init__(self):
        self.db = Database()
        alpha_logger.info("AlphaListPendingSimulatedDAO initialized.")

    def batch_insert(self, alpha_list: list, batch_size: int = 1000):
        """
        批量插入 Alpha 任务（支持分批插入以避免性能问题）
        :param alpha_list: List[dict], 每个 dict 包含 type, settings, regular, priority, region 字段
                           可选包含 created_at，如不提供则使用数据库默认值
        :param batch_size: int, 每批插入的数量，默认1000
        :return: int, 成功插入的总行数
        """
        if not alpha_list:
            alpha_logger.warning("batch_insert: alpha_list is empty.")
            return 0

        # 确保每条数据都有默认 status='pending'
        for alpha in alpha_list:
            if 'status' not in alpha:
                alpha['status'] = 'pending'

        total_inserted = 0
        num_batches = (len(alpha_list) + batch_size - 1) // batch_size
        
        alpha_logger.info(f"Starting batch insert of {len(alpha_list)} records in {num_batches} batches (size: {batch_size})")
        
        for i in range(num_batches):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, len(alpha_list))
            batch = alpha_list[start_idx:end_idx]
            
            alpha_logger.debug(f"Inserting batch {i+1}/{num_batches} (rows {start_idx}-{end_idx-1})")
            
            result = self.db.batch_insert('alpha_list_pending_simulated_table', batch)
            if result > 0:
                total_inserted += result
                alpha_logger.debug(f"✅ Batch {i+1} inserted {result} rows")
            else:
                alpha_logger.error(f"❌ Batch {i+1} insert failed")

        if total_inserted > 0:
            alpha_logger.info(f"✅ Total batch insert successful. Rows inserted: {total_inserted}/{len(alpha_list)}")
        else:
            alpha_logger.error("❌ Total batch insert failed, no rows inserted")
            
        return total_inserted

    def fetch_pending_by_region(self, region: str, limit: int = 100):
        """
        按 region 查询 pending 状态的任务，按 priority 升序、created_at 升序 排序
        :param region: str, 区域（如 'cn', 'us'）
        :param limit: int, 最多返回多少条
        :return: List[dict] or None（失败时）
        """
        sql = """
        SELECT 
            id, type, settings, regular, priority, region, created_at, status
        FROM 
            alpha_list_pending_simulated_table
        WHERE 
            region = %s 
            AND status = 'pending'
        ORDER BY 
            priority ASC, 
            created_at ASC
        LIMIT %s
        """
        params = (region, limit)

        result = self.db.query(sql, params)
        if result is not None:
            alpha_logger.debug(f"Query pending alphas - Region: {region}, Found: {len(result)}, Limit: {limit}")
        else:
            alpha_logger.error(f"❌ Query failed: region={region}, limit={limit}")
        return result

    def fetch_and_lock_pending_by_region(self, region: str, limit: int = 100):
        """
        【推荐】查询并锁定待处理任务（防并发重复读取）
        使用 FOR UPDATE SKIP LOCKED（MySQL 8.0+）
        :param region: str
        :param limit: int
        :return: List[dict]
        """
        sql = """
        SELECT 
            id, type, settings, regular, priority, region, created_at, status
        FROM 
            alpha_list_pending_simulated_table
        WHERE 
            region = %s 
            AND status = 'pending'
        ORDER BY 
            priority ASC, 
            created_at ASC
        LIMIT %s
        FOR UPDATE SKIP LOCKED
        """
        params = (region, limit)

        try:
            with self.db.connection.cursor() as cursor:
                cursor.execute(sql, params)
                result = cursor.fetchall()
                alpha_logger.debug(f"Locked pending alphas - Region: {region}, Locked: {len(result)}, Limit: {limit}")
                return result
        except Exception as e:
            alpha_logger.error(f"❌ Lock query failed: region={region}, error: {e}")
            return None

    def update_status_by_id(self, id, new_status: str):
        """
        更新单条记录的状态
        :param id: int, 数据库主键 id（不是 alpha_id！）
        :param new_status: str
        :return: int, 影响行数
        """
        sql = "UPDATE alpha_list_pending_simulated_table SET status = %s, sent_at = NOW(6) WHERE id = %s AND status = 'pending'"
        params = (new_status, id)
        return self.db.execute(sql, params)


    def batch_update_status_by_ids(self, ids: list, new_status: str):
        """
        批量更新状态
        :param ids: List[int], 数据库主键 id 列表
        :param new_status: str
        :return: int
        """
        if not ids:
            alpha_logger.warning("batch_update_status_by_ids: ids list is empty.")
            return 0

        placeholders = ','.join(['%s'] * len(ids))
        sql = f"""
        UPDATE alpha_list_pending_simulated_table 
        SET status = %s, sent_at = NOW(6) 
        WHERE id IN ({placeholders}) 
        AND status = 'pending'
        """
        params = [new_status] + ids

        result = self.db.execute(sql, tuple(params))
        if result > 0:
            alpha_logger.info(f"✅ Batch status updated to '{new_status}'. DB IDs: {ids}, Rows: {result}")
        return result

    def mark_failed_by_id(self, id, new_status: str = 'failed'):
        """
        标记某条任务失败（可选：增加重试次数字段后可扩展）
        :param alpha_id: int
        :param new_status: str, 默认 'failed'
        :return: int
        """
        sql = "UPDATE alpha_list_pending_simulated_table SET status = %s WHERE id = %s AND status = 'pending'"
        params = (new_status, id)
        return self.db.execute(sql, params)
