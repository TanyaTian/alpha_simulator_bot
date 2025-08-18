# dao/simulated_alphas_dao.py

from database import Database
from logger import Logger

class SimulatedAlphasDAO:
    logger = Logger()
    TABLE_NAME = 'simulated_alphas_table'

    def __init__(self):
        self.db = Database()
        self.logger.debug("SimulatedAlphasDAO initialized")

    def insert(self, data):
        self.logger.debug(f"Inserting simulated alpha: {data}")
        result = self.db.insert(self.TABLE_NAME, data)
        self.logger.debug(f"Insert result: {result}")
        return result
    
    def batch_insert(self, data_list, on_duplicate_update=False, chunk_size=1000):
        """
        批量插入数据，支持分块和重复键处理
        """
        if not data_list:
            self.logger.debug("batch_insert: data_list is empty, skipped.")
            return 0

        self.logger.debug(f"Starting batch insert of {len(data_list)} items, chunk_size={chunk_size}")
        total = 0
        success = True

        for i in range(0, len(data_list), chunk_size):
            chunk = data_list[i:i + chunk_size]
            affected = self.db.batch_insert(
                self.TABLE_NAME,
                chunk,
                on_duplicate_update=on_duplicate_update
            )
            if affected > 0:
                total += affected
                self.logger.debug(f"Chunk {i}-{i+len(chunk)-1} inserted, affected: {affected}")
            else:
                self.logger.error(f"Failed to insert chunk {i}-{i+len(chunk)-1}")
                success = False

        self.logger.info(f"Batch insert completed. Total affected: {total}")
        
        if not success:
            raise RuntimeError("One or more chunks failed during batch insert")
            
        return total

    def get_by_id(self, alpha_id):
        self.logger.debug(f"Querying alpha with ID: {alpha_id}")
        sql = f"SELECT * FROM {self.TABLE_NAME} WHERE id = %s"
        result = self.db.query(sql, (alpha_id,))
        self.logger.debug(f"Query result: {result}")
        return result[0] if result else None

    def update_by_id(self, alpha_id, update_data):
        self.logger.debug(f"Updating alpha {alpha_id} with data: {update_data}")
        where_clause = "id = %s"
        result = self.db.update(self.TABLE_NAME, update_data, where_clause, (alpha_id,))
        self.logger.debug(f"Update affected rows: {result}")
        return result

    def delete_by_id(self, alpha_id):
        where_clause = "id = %s"
        return self.db.delete(self.TABLE_NAME, where_clause, (alpha_id,))

    def get_by_datetime(self, datetime_str):
        self.logger.debug(f"Querying alphas for datetime: {datetime_str}")
        sql = f"SELECT * FROM {self.TABLE_NAME} WHERE datetime = %s"
        result = self.db.query(sql, (datetime_str,))
        self.logger.debug(f"Found {len(result)} records")
        return result

    def count_by_datetime(self, datetime_str):
        sql = f"SELECT COUNT(*) AS count FROM {self.TABLE_NAME} WHERE datetime = %s"
        result = self.db.query(sql, (datetime_str,))
        return result[0]['count'] if result else 0

    def get_by_datetime_paginated(self, datetime_str, limit=1000, offset=0):
        sql = f"SELECT * FROM {self.TABLE_NAME} WHERE datetime = %s LIMIT %s OFFSET %s"
        return self.db.query(sql, (datetime_str, limit, offset))

    # 新增：按 region + datetime 查询
    def get_by_region_and_datetime(self, region, datetime_str, limit=1000, offset=0):
        sql = f"""
            SELECT * FROM {self.TABLE_NAME}
            WHERE region = %s AND datetime = %s
            LIMIT %s OFFSET %s
        """
        return self.db.query(sql, (region, datetime_str, limit, offset))

    def count_by_region_and_datetime(self, region, datetime_str):
        sql = f"""
            SELECT COUNT(*) AS count FROM {self.TABLE_NAME}
            WHERE region = %s AND datetime = %s
        """
        result = self.db.query(sql, (region, datetime_str))
        return result[0]['count'] if result else 0

    # 新增：按 region + datetime 范围查询
    def get_by_region_and_datetime_range(self, region, start_datetime, end_datetime, limit=1000, offset=0):
        sql = f"""
            SELECT * FROM {self.TABLE_NAME}
            WHERE region = %s AND datetime BETWEEN %s AND %s
            LIMIT %s OFFSET %s
        """
        return self.db.query(sql, (region, start_datetime, end_datetime, limit, offset))

    def count_by_region_and_datetime_range(self, region, start_datetime, end_datetime):
        sql = f"""
            SELECT COUNT(*) AS count FROM {self.TABLE_NAME}
            WHERE region = %s AND datetime BETWEEN %s AND %s
        """
        result = self.db.query(sql, (region, start_datetime, end_datetime))
        return result[0]['count'] if result else 0

    def get_max_datetime(self):
        """Return the maximum datetime value from the database"""
        self.logger.debug("Querying maximum datetime from database")
        try:
            sql = f"SELECT MAX(datetime) AS max_datetime FROM {self.TABLE_NAME}"
            result = self.db.query(sql)
            if result and result[0]['max_datetime']:
                return result[0]['max_datetime']
            return None
        except Exception as e:
            self.logger.error(f"Error getting max datetime: {e}")
            raise e

    def close(self):
        """关闭整个数据库连接池（通常在程序退出时调用）"""
        self.db.close()  # ✅ 正确：调用 Database 的 close() 方法
