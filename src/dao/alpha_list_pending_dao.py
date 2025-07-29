# dao/alpha_list_pending_dao.py

from database import Database
from logger import Logger

class AlphaListPendingDAO:
    logger = Logger()
    TABLE_NAME = 'alpha_list_pending_simulated_table'

    def __init__(self):
        self.db = Database()
        self.logger.debug("AlphaListPendingDAO initialized")

    def insert(self, data):
        self.logger.debug(f"Inserting data: {data}")
        result = self.db.insert(self.TABLE_NAME, data)
        self.logger.debug(f"Insert result: {result}")
        return result

    def batch_insert(self, data_list, chunk_size=1000):
        self.logger.debug(f"Starting batch insert of {len(data_list)} items")
        total = 0
        try:
            for i in range(0, len(data_list), chunk_size):
                chunk = data_list[i:i + chunk_size]
                self.logger.debug(f"Processing chunk {i}-{i+chunk_size}")
                affected = self.db.batch_insert(self.TABLE_NAME, chunk)
                total += affected
                self.logger.debug(f"Chunk {i} inserted, affected: {affected}")
            self.logger.debug(f"Total batch insert affected: {total}")
            return total
        except Exception as e:
            self.db.connection.rollback()
            self.logger.error(f"Batch insert failed: {e}")
            raise e

    def update_by_id(self, alpha_id, update_data):
        self.logger.debug(f"Updating {alpha_id} with data: {update_data}")
        where_clause = "id = %s"
        result = self.db.update(self.TABLE_NAME, update_data, where_clause, (alpha_id,))
        self.logger.debug(f"Update affected rows: {result}")
        return result

    def get_by_priority_and_region(self, priority, region, limit=1000, offset=0):
        self.logger.debug(f"Querying alphas with priority={priority}, region={region}, limit={limit}, offset={offset}")
        sql = f"""
            SELECT * FROM {self.TABLE_NAME}
            WHERE priority = %s AND region = %s
            LIMIT %s OFFSET %s
        """
        result = self.db.query(sql, (priority, region, limit, offset))
        self.logger.debug(f"Found {len(result)} records")
        return result

    def count_by_priority_and_region(self, priority, region):
        sql = f"""
            SELECT COUNT(*) AS count FROM {self.TABLE_NAME}
            WHERE priority = %s AND region = %s
        """
        result = self.db.query(sql, (priority, region))
        return result[0]['count'] if result else 0

    def close(self):
        self.db.connection.close()
