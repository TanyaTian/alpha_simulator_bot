# database.py

import os
import pymysql
from configparser import ConfigParser
from pymysql.cursors import DictCursor

# ✅ 导入日志模块
from logger import Logger

db_logger = Logger().logger  # 使用项目统一日志


class Database:
    def __init__(self, config_file='db_config.ini'):
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_path = os.path.join(project_root, 'config', 'db_config.ini')
        config = ConfigParser()

        # 🔴 初始化阶段：配置文件错误、连接失败 → 抛出异常，终止程序
        try:
            if not os.path.exists(config_path):
                error_msg = f"Database config file not found: {config_path}"
                db_logger.critical(error_msg)  # critical 表示严重错误，应终止
                raise FileNotFoundError(error_msg)

            config.read(config_path)

            if 'database' not in config:
                error_msg = f"Missing 'database' section in {config_path}"
                db_logger.critical(error_msg)
                raise KeyError(error_msg)

            db_config = config['database']

            self.connection = pymysql.connect(
                host=db_config.get('host'),
                port=db_config.getint('port', 3306),
                user=db_config.get('user'),
                password=db_config.get('password'),
                database=db_config.get('database'),
                charset=db_config.get('charset', 'utf8mb4'),
                cursorclass=DictCursor,
                autocommit=True,
                connect_timeout=10,
            )
            db_logger.info("✅ Database connection established.")

        except Exception as e:
            db_logger.critical(f"❌ FATAL: Failed to initialize database: {e}")
            # 🔥 重新抛出异常，让程序终止
            raise  # 让上层（如 main.py）决定是否退出

    def _ensure_connection(self):
        """检查连接是否有效，自动重连"""
        if not self.connection:
            db_logger.error("Database connection is None.")
            return False
        try:
            self.connection.ping(reconnect=True)
            return True
        except Exception as e:
            db_logger.error(f"Database connection lost: {e}")
            return False

    def query(self, sql, params=None):
        if not self._ensure_connection():
            db_logger.error(f"Query skipped: no database connection. SQL: {sql}")
            return None

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, params or ())
                result = cursor.fetchall()
                db_logger.debug(f"Query OK: {sql}, rows: {len(result)}")
                return result
        except Exception as e:
            db_logger.error(f"❌ Query failed: {sql}, params: {params}, error: {e}")
            return None

    def execute(self, sql, params=None):
        if not self._ensure_connection():
            db_logger.error(f"Execute skipped: no connection. SQL: {sql}")
            return 0

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, params or ())
                self.connection.commit()
                rowcount = cursor.rowcount
                db_logger.debug(f"Execute OK: {sql}, rows affected: {rowcount}")
                return rowcount
        except Exception as e:
            db_logger.error(f"❌ Execute failed: {sql}, params: {params}, error: {e}")
            return 0

    def insert(self, table, data):
        if not data:
            db_logger.warning("Insert skipped: empty data.")
            return 0

        if not self._ensure_connection():
            db_logger.error(f"Insert skipped: no connection. Table: {table}")
            return 0

        try:
            keys = ', '.join(f"`{k}`" for k in data.keys())
            values = ', '.join(['%s'] * len(data))
            sql = f"INSERT INTO `{table}` ({keys}) VALUES ({values})"

            with self.connection.cursor() as cursor:
                cursor.execute(sql, tuple(data.values()))
                self.connection.commit()
                lastrowid = cursor.lastrowid
                db_logger.debug(f"Insert OK: table={table}, id={lastrowid}")
                return lastrowid
        except Exception as e:
            db_logger.error(f"❌ Insert failed: table={table}, data={data}, error: {e}")
            return 0

    def batch_insert(self, table, data_list):
        if not data_list:
            db_logger.warning("Batch insert skipped: empty list.")
            return 0

        if not self._ensure_connection():
            db_logger.error(f"Batch insert skipped: no connection. Table: {table}")
            return 0

        try:
            keys = ', '.join(f"`{k}`" for k in data_list[0].keys())
            values = ', '.join(['%s'] * len(data_list[0]))
            sql = f"INSERT INTO `{table}` ({keys}) VALUES ({values})"

            with self.connection.cursor() as cursor:
                affected_rows = cursor.executemany(sql, [tuple(d.values()) for d in data_list])
                self.connection.commit()
                db_logger.info(f"Batch insert OK: table={table}, rows={affected_rows}")
                return affected_rows
        except Exception as e:
            db_logger.error(f"❌ Batch insert failed: table={table}, error: {e}")
            return 0

    def update(self, table, data, where_clause, params=None):
        if not data:
            db_logger.warning("Update skipped: empty data.")
            return 0

        if not self._ensure_connection():
            db_logger.error(f"Update skipped: no connection. Table: {table}")
            return 0

        try:
            set_clause = ', '.join([f"`{key}` = %s" for key in data.keys()])
            sql = f"UPDATE `{table}` SET {set_clause} WHERE {where_clause}"
            sql_params = tuple(data.values()) + (params if isinstance(params, tuple) else (params,))

            with self.connection.cursor() as cursor:
                cursor.execute(sql, sql_params)
                self.connection.commit()
                rowcount = cursor.rowcount
                db_logger.debug(f"Update OK: table={table}, rows={rowcount}")
                return rowcount
        except Exception as e:
            db_logger.error(f"❌ Update failed: table={table}, where={where_clause}, error: {e}")
            return 0

    def delete(self, table, where_clause, params=None):
        if not where_clause:
            db_logger.error("Delete skipped: missing where_clause (prevent full table delete)")
            return 0

        if not self._ensure_connection():
            db_logger.error(f"Delete skipped: no connection. Table: {table}")
            return 0

        try:
            sql = f"DELETE FROM `{table}` WHERE {where_clause}"
            with self.connection.cursor() as cursor:
                cursor.execute(sql, params)
                self.connection.commit()
                rowcount = cursor.rowcount
                db_logger.info(f"Delete OK: table={table}, rows={rowcount}")
                return rowcount
        except Exception as e:
            db_logger.error(f"❌ Delete failed: table={table}, where={where_clause}, error: {e}")
            return 0

    def close(self):
        if self.connection:
            try:
                self.connection.close()
                db_logger.info("Database connection closed.")
            except Exception as e:
                db_logger.error(f"Error during close: {e}")