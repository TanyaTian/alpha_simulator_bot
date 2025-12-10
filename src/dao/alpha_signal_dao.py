# dao/simulated_alphas_dao.py

from database import Database
from logger import Logger

class AlphaSignalDAO:
    logger = Logger()
    TABLE_NAME = 'alpha_signal_table'

    def __init__(self):
        self.db = Database()
        self.logger.debug("AlphaSignalDAO initialized")

    def insert(self, data):
        self.logger.debug(f"Inserting simulated alpha: {data}")
        result = self.db.insert(self.TABLE_NAME, data)
        self.logger.debug(f"Insert result: {result}")
        return result
    
    def batch_insert(self, data_list, chunk_size=1000):
        self.logger.debug(f"Starting batch insert of {len(data_list)} items")
        total = 0
        success = True

        for i in range(0, len(data_list), chunk_size):
            chunk = data_list[i:i + chunk_size]
            affected = self.db.batch_insert(self.TABLE_NAME, chunk, on_duplicate_update=True)
            if affected > 0:
                total += affected
                self.logger.debug(f"Inserted chunk {i}-{i+chunk_size}, affected: {affected}")
            else:
                self.logger.error(f"Failed to insert chunk {i}-{i+chunk_size}")
                success = False

        self.logger.debug(f"Total batch insert affected: {total}")
        
        if not success:
            raise RuntimeError("One or more chunks failed to insert")
            
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

    def datetime_exists(self, datetime_str):
        """检查指定日期时间在数据库中是否已存在
        
        参数:
            datetime_str (str): 要检查的日期时间字符串
            
        返回:
            bool: 如果存在返回True，否则返回False
        """
        self.logger.debug(f"Checking if datetime exists: {datetime_str}")
        try:
            count = self.count_by_datetime(datetime_str)
            return count > 0
        except Exception as e:
            self.logger.error(f"Error checking datetime existence: {e}")
            return False
    
    def region_and_datetime_exists(self, region, datetime_str):
        """检查指定region和日期时间在数据库中是否已存在
        
        参数:
            region (str): 要检查的region
            datetime_str (str): 要检查的日期时间字符串
            
        返回:
            bool: 如果存在返回True，否则返回False
        """
        self.logger.debug(f"Checking if region '{region}' and datetime '{datetime_str}' exist")
        try:
            count = self.count_by_region_and_datetime(region, datetime_str)
            return count > 0
        except Exception as e:
            self.logger.error(f"Error checking region and datetime existence: {e}")
            return False
            
    def close(self):
        # 如果 Database 提供了 close() 方法（它确实提供了！）
        self.db.close()  # ✅ 正确：关闭整个连接池
