# dao/data_exploration_dao.py

from database import Database
from logger import Logger

class DataExplorationDAO:
    """
    用于操作 data_exploration_table 的数据访问对象。
    通过统一的 Database 实例执行数据库操作。
    """
    def __init__(self):
        self.db = Database()
        self.table_name = 'data_exploration_table'
        self.logger = Logger()

    def batch_insert_or_update(self, records: list[dict]):
        """
        批量插入或更新记录。
        """
        if not records:
            return 0
        self.logger.info(f"Batch inserting or updating {len(records)} records into {self.table_name}") # 日志：记录批量插入/更新
        return self.db.batch_insert(self.table_name, records, on_duplicate_update=True)

    def query_by_filters(self, region: str, universe: str, delay: str, datafield: str) -> list[dict]:
        """
        根据指定的筛选条件查询数据。
        """
        sql = f"""SELECT * FROM {self.table_name} 
                 WHERE region = %s AND universe = %s AND delay = %s AND datafield = %s"""
        params = (region, universe, delay, datafield)
        self.logger.info(f"Querying {self.table_name} with filters: {params}") # 日志：记录查询参数
        results = self.db.query(sql, params)
        self.logger.info(f"Found {len(results)} records.") # 日志：记录查询结果数量
        return results

    def count_by_datetime(self, datetime: str) -> int:
        """
        查询指定日期的数据条数。
        """
        sql = f"SELECT COUNT(*) as count FROM {self.table_name} WHERE datetime = %s"
        self.logger.info(f"Counting records in {self.table_name} for datetime: {datetime}") # 日志：记录计数查询
        result = self.db.query(sql, (datetime,))
        count = result[0]['count'] if result and result[0] else 0
        self.logger.info(f"Found {count} records.") # 日志：记录计数结果
        return count