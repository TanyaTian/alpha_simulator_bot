# database.py

import os
import pymysql
from configparser import ConfigParser
from pymysql.cursors import DictCursor
from dbutils.pooled_db import PooledDB  # 🔁 引入连接池

# ✅ 导入日志模块
from logger import Logger

db_logger = Logger().logger  # 使用项目统一日志


class Database:
    _instance = None
    _pool = None

    def __new__(cls, config_file='db_config.ini'):
        """单例模式：确保全局只有一个 Database 实例和连接池"""
        if cls._instance is None:
            cls._instance = super(Database, cls).__new__(cls)
        return cls._instance

    def __init__(self, config_file='db_config.ini'):
        # 避免重复初始化
        if hasattr(self, 'initialized') and self.initialized:
            return

        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_path = os.path.join(project_root, 'config', config_file)
        config = ConfigParser()

        try:
            if not os.path.exists(config_path):
                error_msg = f"Database config file not found: {config_path}"
                db_logger.critical(error_msg)
                raise FileNotFoundError(error_msg)

            config.read(config_path)

            if 'database' not in config:
                error_msg = f"Missing 'database' section in {config_path}"
                db_logger.critical(error_msg)
                raise KeyError(error_msg)

            db_config = config['database']

            # 🔧 提取配置
            host = db_config.get('host')
            port = db_config.getint('port', 3306)
            user = db_config.get('user')
            password = db_config.get('password')
            database = db_config.get('database')
            charset = db_config.get('charset', 'utf8mb4')
            max_connections = db_config.getint('max_connections', 20)
            min_cached = db_config.getint('min_cached', 5)
            max_cached = db_config.getint('max_cached', 10)

            # 🏊 创建连接池
            self._pool = PooledDB(
                creator=pymysql,
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
                charset=charset,
                cursorclass=DictCursor,
                autocommit=True,
                # --- 连接池配置 ---
                maxconnections=max_connections,    # 最大连接数
                mincached=min_cached,              # 初始化时创建的空闲连接数
                maxcached=max_cached,              # 最大空闲连接数
                maxshared=5,                       # 最大共享连接数（适用于支持共享的数据库）
                blocking=True,                     # 连接数达到上限时，请求是否等待
                maxusage=1000,                     # 每个连接最多被使用次数（避免长期使用）
                setsession=[],                     # 每次获取连接时执行的命令（如 SET NAMES）
                ping=1,                            # 每次获取连接时 ping 一次，确保连接有效
            )

            db_logger.info(f"✅ Database connection pool created. "
                          f"max={max_connections}, min_cached={min_cached}, max_cached={max_cached}")

            self.initialized = True  # 标记已初始化

        except Exception as e:
            db_logger.critical(f"❌ FATAL: Failed to initialize database pool: {e}")
            raise

    def _get_connection(self):
        """从连接池获取一个连接，并确保其有效"""
        try:
            conn = self._pool.connection()
            # 由于 ping=1 已设置，这里通常不需要再 ping
            # 但为了保险，可以再检查一次
            conn.ping(reconnect=True)
            return conn
        except Exception as e:
            db_logger.error(f"❌ Failed to get connection from pool: {e}")
            return None

    def query(self, sql, params=None):
        conn = self._get_connection()
        if not conn:
            db_logger.error(f"Query skipped: no connection from pool. SQL: {sql}")
            return None

        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, params or ())
                result = cursor.fetchall()
                db_logger.debug(f"Query OK: {sql}, rows: {len(result)}")
                return result
        except Exception as e:
            db_logger.error(f"❌ Query failed: {sql}, params: {params}, error: {e}")
            return None
        finally:
            conn.close()  # 🔐 归还连接到池中（不是真正关闭）

    def execute(self, sql, params=None):
        conn = self._get_connection()
        if not conn:
            db_logger.error(f"Execute skipped: no connection. SQL: {sql}")
            return 0

        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, params or ())
                rowcount = cursor.rowcount
                db_logger.debug(f"Execute OK: {sql}, rows affected: {rowcount}")
                return rowcount
        except Exception as e:
            db_logger.error(f"❌ Execute failed: {sql}, params: {params}, error: {e}")
            return 0
        finally:
            conn.close()

    def insert(self, table, data):
        if not data:
            db_logger.warning("Insert skipped: empty data.")
            return 0

        conn = self._get_connection()
        if not conn:
            db_logger.error(f"Insert skipped: no connection. Table: {table}")
            return 0

        try:
            keys = ', '.join(f"`{k}`" for k in data.keys())
            values = ', '.join(['%s'] * len(data))
            sql = f"INSERT INTO `{table}` ({keys}) VALUES ({values})"

            with conn.cursor() as cursor:
                cursor.execute(sql, tuple(data.values()))
                lastrowid = cursor.lastrowid
                db_logger.debug(f"Insert OK: table={table}, id={lastrowid}")
                return lastrowid
        except Exception as e:
            db_logger.error(f"❌ Insert failed: table={table}, data={data}, error: {e}")
            return 0
        finally:
            conn.close()

    def batch_insert(self, table, data_list, on_duplicate_update=False):
        if not data_list:
            db_logger.warning("Batch insert skipped: empty list.")
            return 0

        conn = self._get_connection()
        if not conn:
            db_logger.error(f"Batch insert skipped: no connection. Table: {table}")
            return 0

        try:
            keys = ', '.join(f"`{k}`" for k in data_list[0].keys())
            values = ', '.join(['%s'] * len(data_list[0]))
            sql = f"INSERT INTO `{table}` ({keys}) VALUES ({values})"

            if on_duplicate_update:
                update_clause = ', '.join([f"`{k}`=VALUES(`{k}`)" for k in data_list[0].keys() if k != 'id'])
                sql += f" ON DUPLICATE KEY UPDATE {update_clause}"
                db_logger.debug(f"Using ON DUPLICATE KEY UPDATE for batch insert")

            with conn.cursor() as cursor:
                affected_rows = cursor.executemany(sql, [tuple(d.values()) for d in data_list])
                db_logger.info(f"Batch insert OK: table={table}, rows={affected_rows}, on_duplicate={on_duplicate_update}")
                return affected_rows
        except Exception as e:
            db_logger.error(f"❌ Batch insert failed: table={table}, error: {e}")
            return 0
        finally:
            conn.close()

    def update(self, table, data, where_clause, params=None):
        if not data:
            db_logger.warning("Update skipped: empty data.")
            return 0

        conn = self._get_connection()
        if not conn:
            db_logger.error(f"Update skipped: no connection. Table: {table}")
            return 0

        try:
            set_clause = ', '.join([f"`{key}` = %s" for key in data.keys()])
            sql = f"UPDATE `{table}` SET {set_clause} WHERE {where_clause}"
            sql_params = tuple(data.values()) + (params if isinstance(params, tuple) else (params,))

            with conn.cursor() as cursor:
                cursor.execute(sql, sql_params)
                rowcount = cursor.rowcount
                db_logger.debug(f"Update OK: table={table}, rows={rowcount}")
                return rowcount
        except Exception as e:
            db_logger.error(f"❌ Update failed: table={table}, where={where_clause}, error: {e}")
            return 0
        finally:
            conn.close()

    def delete(self, table, where_clause, params=None):
        if not where_clause:
            db_logger.error("Delete skipped: missing where_clause (prevent full table delete)")
            return 0

        conn = self._get_connection()
        if not conn:
            db_logger.error(f"Delete skipped: no connection. Table: {table}")
            return 0

        try:
            sql = f"DELETE FROM `{table}` WHERE {where_clause}"
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                rowcount = cursor.rowcount
                db_logger.info(f"Delete OK: table={table}, rows={rowcount}")
                return rowcount
        except Exception as e:
            db_logger.error(f"❌ Delete failed: table={table}, where={where_clause}, error: {e}")
            return 0
        finally:
            conn.close()

    def close(self):
        """关闭整个连接池（通常在程序退出时调用）"""
        if self._pool:
            try:
                self._pool.close()
                db_logger.info("Database connection pool closed.")
            except Exception as e:
                db_logger.error(f"Error during pool close: {e}")
