# dao/simulation_tasks_dao.py

from database import Database
from logger import Logger

class SimulationTasksDAO:
    logger = Logger()
    TABLE_NAME = 'simulation_tasks_table'

    def __init__(self):
        self.db = Database()
        self.logger.debug("SimulationTasksDAO initialized")

    def insert(self, data):
        """
        插入一条 simulation task
        :param data: dict,包含 child_id, submit_time, status, query_attempts, last_query_time
        :return: None
        """
        self.logger.debug(f"Inserting simulation task: {data}")
        result = self.db.insert(self.TABLE_NAME, data)
        self.logger.debug(f"Inserted simulation task result: {result}")
        return result

    def batch_insert(self, data_list):
        """
        批量插入 simulation tasks
        :param data_list: List[dict]，每个dict包含 child_id, submit_time, status, query_attempts, last_query_time
        :return: 受影响行数
        """
        self.logger.debug(f"Starting batch insert of {len(data_list)} tasks")
        result = self.db.batch_insert(self.TABLE_NAME, data_list)
        self.logger.debug(f"Batch insert affected rows: {result}")
        return result
    def get_by_child_id(self, child_id):
        """
        根据 child_id 获取任务
        :param child_id: str
        :return: dict or None
        """
        self.logger.debug(f"Querying task for child_id: {child_id}")
        sql = f"SELECT * FROM {self.TABLE_NAME} WHERE child_id = %s"
        result = self.db.query(sql, (child_id,))
        self.logger.debug(f"Query result for {child_id}: {result}")
        return result[0] if result else None

    def update_by_child_id(self, child_id, update_data):
        """
        更新任务信息
        :param child_id: str
        :param update_data: dict
        :return: 受影响行数
        """
        self.logger.debug(f"Updating task {child_id} with data: {update_data}")
        where_clause = "child_id = %s"
        result = self.db.update(self.TABLE_NAME, update_data, where_clause, (child_id,))
        self.logger.debug(f"Update affected rows: {result}")
        return result

    def increment_query_attempts(self, child_id):
        """
        查询次数 +1
        :param child_id: str
        :return: 受影响行数
        """
        self.logger.debug(f"Incrementing query attempts for {child_id}")
        sql = f"""
            UPDATE {self.TABLE_NAME}
            SET query_attempts = query_attempts + 1
            WHERE child_id = %s
        """
        with self.db.connection.cursor() as cursor:
            cursor.execute(sql, (child_id,))
            self.db.connection.commit()
            result = cursor.rowcount
            self.logger.debug(f"Query attempts incremented, affected rows: {result}")
            return result

    def update_status_and_last_query(self, child_id, new_status, query_time):
        """
        更新任务状态和最后查询时间
        :param child_id: str
        :param new_status: str
        :param query_time: datetime
        :return: 受影响行数
        """
        self.logger.debug(f"Updating status and query time for {child_id} to {new_status} at {query_time}")
        update_data = {
            'status': new_status,
            'last_query_time': query_time
        }
        result = self.update_by_child_id(child_id, update_data)
        self.logger.debug(f"Status update affected rows: {result}")
        return result

    def list_pending_tasks_lite(self, limit=100, offset=0):
        """
        只获取 child_id，用于任务调度器初步筛选
        """
        self.logger.debug(f"Listing pending tasks with limit={limit}, offset={offset}")
        sql = f"""
            SELECT child_id FROM {self.TABLE_NAME}
            WHERE status = 'pending'
            AND query_attempts <= 5
            ORDER BY submit_time ASC
            LIMIT %s OFFSET %s
        """
        result = self.db.query(sql, (limit, offset))
        self.logger.debug(f"Found {len(result)} pending tasks")
        return [row['child_id'] for row in result]

    def delete_by_child_id(self, child_id):
        """
        删除任务
        :param child_id: str
        :return: 受影响行数
        """
        where_clause = "child_id = %s"
        return self.db.delete(self.TABLE_NAME, where_clause, (child_id,))

    def close(self):
        self.db.close()
