# dao/stage_one_signal_dao.py

from database import Database
from logger import Logger

class StageOneSignalDAO:
    logger = Logger()
    TABLE_NAME = 'stage_one_base_signals'

    def __init__(self):
        self.db = Database()
        self.logger.debug("StageOneSignalDAO initialized")

    def get_alphas_by_batch(self, region, universe, delay, dataset_id, data_time):
        """
        根据联合键获取已入库的 Alpha 列表。
        用于在挖掘过程中加载"当前批次已存在"的 Alpha 进行相关性检查。
        :return: List[Dict] (包含 alpha_id, status)
        """
        self.logger.debug(f"Querying alphas by batch: region={region}, universe={universe}, "
                         f"delay={delay}, dataset_id={dataset_id}, data_time={data_time}")
        
        sql = f"""
            SELECT alpha_id, status FROM {self.TABLE_NAME}
            WHERE region = %s AND universe = %s AND delay = %s 
            AND dataset_id = %s AND date_time = %s
        """
        params = (region, universe, delay, dataset_id, data_time)
        
        try:
            result = self.db.query(sql, params)
            self.logger.debug(f"Found {len(result)} alphas in batch")
            return result
        except Exception as e:
            self.logger.error(f"Error querying alphas by batch: {e}")
            return []

    def get_oldest_pending_signal(self):
        """
        按照时间顺序查询最久还保持 pending 状态的 alpha。
        :return: Dict 或 None
        """
        self.logger.debug("Querying oldest pending signal")
        
        sql = f"""
            SELECT * FROM {self.TABLE_NAME}
            WHERE status = 'pending'
            ORDER BY created_at ASC
            LIMIT 1
        """
        
        try:
            result = self.db.query(sql)
            return result[0] if result else None
        except Exception as e:
            self.logger.error(f"Error querying oldest pending signal: {e}")
            return None

    def update_signal_status(self, alpha_id, status):
        """
        根据 alpha_id 修改状态。
        :param alpha_id: Alpha ID
        :param status: 新状态 ('pending', 'fail', 'success', 'complete')
        :return: 受影响的行数
        """
        self.logger.debug(f"Updating signal status: alpha_id={alpha_id}, status={status}")
        
        sql = f"UPDATE {self.TABLE_NAME} SET status = %s WHERE alpha_id = %s"
        params = (status, alpha_id)
        
        try:
            result = self.db.execute(sql, params)
            self.logger.debug(f"Update status completed, affected rows: {result}")
            return result
        except Exception as e:
            self.logger.error(f"Error updating signal status: {e}")
            raise

    def upsert_signal(self, alpha_data):
        """
        插入或更新 Alpha 信号。
        逻辑: 如果 alpha_id 已存在，则更新其它字段；否则插入。
        
        :param alpha_data: Dict 包含表结构中的所有字段
        示例:
            {
                'alpha_id': 'USATOP30001_20250116_001',  # 平台生成的唯一 Alpha ID
                'region': 'USA',                         # 地区
                'universe': 'TOP3000',                   # 股票池
                'delay': 1,                              # 延迟
                'dataset_id': 'analyst_estimates',       # 数据集 ID
                'date_time': '20250116',                 # 数据生成/入库日期 (YYYYMMDD)
                'category': 'ANALYST',                   # 数据集类别 (可选)
                'created_at': '2025-01-16 12:00:00'      # 记录创建时间 (可选，数据库有默认值)
            }
        """
        self.logger.debug(f"Upserting alpha signal: {alpha_data}")
        
        # 确保字段名与表结构一致
        required_fields = ['alpha_id', 'region', 'universe', 'delay', 'dataset_id', 'date_time']
        
        # 检查必需字段
        for field in required_fields:
            if field not in alpha_data:
                self.logger.error(f"Missing required field: {field}")
                raise ValueError(f"Missing required field: {field}")
        
        # 构建 INSERT ... ON DUPLICATE KEY UPDATE 语句
        keys = ', '.join(f"`{k}`" for k in alpha_data.keys())
        values = ', '.join(['%s'] * len(alpha_data))
        update_clause = ', '.join([f"`{k}`=VALUES(`{k}`)" for k in alpha_data.keys() if k != 'alpha_id'])
        
        sql = f"""
            INSERT INTO {self.TABLE_NAME} ({keys}) 
            VALUES ({values})
            ON DUPLICATE KEY UPDATE {update_clause}
        """
        
        params = tuple(alpha_data.values())
        
        try:
            result = self.db.execute(sql, params)
            self.logger.debug(f"Upsert completed, affected rows: {result}")
            return result
        except Exception as e:
            self.logger.error(f"Error upserting alpha signal: {e}")
            raise

    def count_signals_in_batch(self, region, universe, delay, dataset_id, data_time):
        """
        统计当前批次已入库的数量，用于判断是否达到停止条件 (如 10 个)。
        """
        self.logger.debug(f"Counting signals in batch: region={region}, universe={universe}, "
                         f"delay={delay}, dataset_id={dataset_id}, data_time={data_time}")
        
        sql = f"""
            SELECT COUNT(*) AS count FROM {self.TABLE_NAME}
            WHERE region = %s AND universe = %s AND delay = %s 
            AND dataset_id = %s AND date_time = %s
        """
        params = (region, universe, delay, dataset_id, data_time)
        
        try:
            result = self.db.query(sql, params)
            count = result[0]['count'] if result else 0
            self.logger.debug(f"Count in batch: {count}")
            return count
        except Exception as e:
            self.logger.error(f"Error counting signals in batch: {e}")
            return 0

    def get_by_alpha_id(self, alpha_id):
        """
        根据 alpha_id 获取 Alpha 信号。
        :param alpha_id: Alpha ID
        :return: Dict 或 None
        """
        self.logger.debug(f"Querying alpha by ID: {alpha_id}")
        
        sql = f"SELECT * FROM {self.TABLE_NAME} WHERE alpha_id = %s"
        params = (alpha_id,)
        
        try:
            result = self.db.query(sql, params)
            return result[0] if result else None
        except Exception as e:
            self.logger.error(f"Error querying alpha by ID: {e}")
            return None

    def delete_by_alpha_id(self, alpha_id):
        """
        根据 alpha_id 删除 Alpha 信号。
        :param alpha_id: Alpha ID
        :return: 受影响的行数
        """
        self.logger.debug(f"Deleting alpha by ID: {alpha_id}")
        
        sql = f"DELETE FROM {self.TABLE_NAME} WHERE alpha_id = %s"
        params = (alpha_id,)
        
        try:
            result = self.db.execute(sql, params)
            self.logger.debug(f"Delete completed, affected rows: {result}")
            return result
        except Exception as e:
            self.logger.error(f"Error deleting alpha by ID: {e}")
            return 0

    def close(self):
        """关闭数据库连接"""
        self.db.close()
        self.logger.debug("StageOneSignalDAO closed")
