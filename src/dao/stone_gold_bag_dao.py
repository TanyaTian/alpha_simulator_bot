from database import Database
from logger import Logger

class StoneGoldBagDAO:
    logger = Logger()
    TABLE_NAME = 'stone_gold_bag_table'

    def __init__(self):
        self.db = Database()
        self.logger.debug("StoneGoldBagDAO initialized")

    def insert(self, data):
        self.logger.debug(f"Inserting stone_gold_bag record: {data}")
        result = self.db.insert(self.TABLE_NAME, data)
        self.logger.debug(f"Insert result: {result}")
        return result
    
    def batch_insert(self, data_list, chunk_size=1000):
        self.logger.debug(f"Starting batch insert of {len(data_list)} items")
        total = 0
        try:
            for i in range(0, len(data_list), chunk_size):
                chunk = data_list[i:i + chunk_size]
                # Use ON DUPLICATE KEY UPDATE to handle primary key conflicts
                affected = self.db.batch_insert(self.TABLE_NAME, chunk, on_duplicate_update=True)
                total += affected
                self.logger.debug(f"Inserted chunk {i}-{i+chunk_size}, affected: {affected}")
            self.logger.debug(f"Total batch insert affected: {total}")
            return total
        except Exception as e:
            self.db.connection.rollback()
            self.logger.error(f"Batch insert failed: {e}")
            raise e

    def get_by_id(self, bag_id):
        self.logger.debug(f"Querying stone_gold_bag with ID: {bag_id}")
        sql = f"SELECT * FROM {self.TABLE_NAME} WHERE id = %s"
        result = self.db.query(sql, (bag_id,))
        self.logger.debug(f"Query result: {result}")
        return result[0] if result else None

    def update_by_id(self, bag_id, update_data):
        self.logger.debug(f"Updating stone_gold_bag {bag_id} with data: {update_data}")
        where_clause = "id = %s"
        result = self.db.update(self.TABLE_NAME, update_data, where_clause, (bag_id,))
        self.logger.debug(f"Update affected rows: {result}")
        return result

    def delete_by_id(self, bag_id):
        where_clause = "id = %s"
        return self.db.delete(self.TABLE_NAME, where_clause, (bag_id,))

    def get_by_datetime(self, datetime_str):
        self.logger.debug(f"Querying stone_gold_bag for datetime: {datetime_str}")
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

    # Gold-specific methods
    def get_by_gold_value(self, gold_value):
        self.logger.debug(f"Querying stone_gold_bag with gold value: {gold_value}")
        sql = f"SELECT * FROM {self.TABLE_NAME} WHERE gold = %s"
        return self.db.query(sql, (gold_value,))

    def get_by_datetime_and_gold(self, datetime_str, gold_value):
        self.logger.debug(f"Querying stone_gold_bag for datetime {datetime_str} and gold {gold_value}")
        sql = f"SELECT * FROM {self.TABLE_NAME} WHERE datetime = %s AND gold = %s"
        return self.db.query(sql, (datetime_str, gold_value))

    # Performance metric methods
    def get_by_sharpe_range(self, min_sharpe, max_sharpe):
        sql = f"SELECT * FROM {self.TABLE_NAME} WHERE sharpe BETWEEN %s AND %s"
        return self.db.query(sql, (min_sharpe, max_sharpe))

    def get_by_fitness_range(self, min_fitness, max_fitness):
        sql = f"SELECT * FROM {self.TABLE_NAME} WHERE fitness BETWEEN %s AND %s"
        return self.db.query(sql, (min_fitness, max_fitness))

    # Utility methods
    def get_max_datetime(self):
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
        self.logger.debug(f"Checking if datetime exists: {datetime_str}")
        try:
            count = self.count_by_datetime(datetime_str)
            return count > 0
        except Exception as e:
            self.logger.error(f"Error checking datetime existence: {e}")
            return False
    
    def close(self):
        self.db.connection.close()
