import pandas as pd
import ast
from typing import List, Dict, Optional, Tuple
from pathlib import Path
from copy import deepcopy
from self_corr_calculator import calc_self_corr, get_alpha_pnls
from logger import Logger
from datetime import datetime, timedelta
import threading
from signal_manager import SignalManager
import traceback
import os
from config_manager import config_manager
from dao import SimulatedAlphasDAO
from dao import AlphaSignalDAO

class AlphaFilter:
    """
    一个类，用于筛选 alpha 数据，计算相关性，并按区域保存结果。

    该类从 simulated_alphas.csv 文件中读取 alpha 数据，基于 fitness 和 sharpe 筛选 alpha,
    使用筛选后的 alpha 生成 os_alpha_ids 和 os_alpha_rets,逐一计算每个 alpha 的最大相关性
    （移除自身后）,筛选相关性低于阈值的 alpha,并按区域写入 CSV 文件。
    """
    def __init__(self, signal_manager=None):
        """
        初始化 AlphaFilter,自动设置日期为前一天
        
        参数:
            username (str): WorldQuant Brain API 用户名
            password (str): WorldQuant Brain API 密码
            data_dir (str): 数据目录，默认为 "data"
        """
        self.date_str = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

        self.os_alpha_ids: Optional[Dict[str, List]] = None
        self.os_alpha_rets: Optional[pd.DataFrame] = None
        self.logger = Logger()
        self._monitor_thread = None
        self._stop_event = threading.Event()

        self.simulated_alphas_dao = SimulatedAlphasDAO()
        self.alpha_signal_dao = AlphaSignalDAO()
        
        # 注册信号处理
        if signal_manager:
            signal_manager.add_handler(self.handle_exit_signal)
        else:
            self.logger.warning("未提供 SignalManager,AlphaSimulator 无法注册信号处理函数")

        # 从配置中心获取参数
        self._load_config_from_manager()
        
    def _load_config_from_manager(self):
        
        """从配置中心加载运行时参数"""
        config = config_manager._config  # 直接访问内部配置
        
        # 加载核心参数（带默认值）
        self.username = config.get('username', '')
        self.password = config.get('password', '')
        # 使用配置中心的session
        self.sess = config_manager.get_session()
        if not self.sess:
            raise ValueError("Failed to sign in, cannot initialize AlphaFilter")
        
        self.logger.info(f"Loaded config session success in AlphaFilter")
    

    def handle_exit_signal(self, signum, frame):
        self.logger.info(f"Received shutdown signal {signum}, saving unfinished alpha IDs...")
        self.stop_monitoring()


    def load_alpha_data(self) -> pd.DataFrame:
        """
        从数据库中加载指定日期的 alpha 数据，使用分页查询避免一次加载过多数据。

        返回:
            pd.DataFrame: 包含 alpha 数据的 DataFrame。

        异常:
            Exception: 如果数据库查询失败。
        """
        try:
            
            total_count = self.simulated_alphas_dao.count_by_datetime(self.date_str)
            self.logger.info(f"Total alphas in database for date {self.date_str}: {total_count}")
            
            if total_count == 0:
                self.logger.warning(f"No alphas found for date {self.date_str} in database")
                return pd.DataFrame()
            
            page_size = 1000  # 每页加载1000条记录
            all_rows = []
            
            for offset in range(0, total_count, page_size):
                self.logger.debug(f"Loading alphas from database: offset={offset}, limit={page_size}")
                chunk = self.simulated_alphas_dao.get_by_datetime_paginated(self.date_str, limit=page_size, offset=offset)
                all_rows.extend(chunk)
                self.logger.debug(f"Loaded {len(chunk)} alphas (total so far: {len(all_rows)})")
            
            df = pd.DataFrame(all_rows)
            self.logger.info(f"Successfully loaded {len(df)} alphas from database for date {self.date_str}")
            return df
        except Exception as e:
            self.logger.error(f"Failed to load alpha data from database: {e}")
            raise

    def filter_alphas(self, df: pd.DataFrame, min_fitness: float, min_sharpe: float, exclude_operators: List[str] = None) -> List[Dict]:
        """
        筛选符合 fitness 和 sharpe 条件的 alpha,过滤掉包含指定运算符的行,构造 alpha_result,并按 sharpe 值从小到大排序。

        参数:
            df (pd.DataFrame): 包含 alpha 数据的 DataFrame。
            min_fitness (float): 最小 fitness 阈值。
            min_sharpe (float): 最小 sharpe 阈值。
            exclude_operators (List[str], optional): 需要过滤的运算符列表，例如 ['trade_when', 'other_operator']。

        返回:
            List[Dict]: 筛选后的 alpha 元数据列表，每个字典包含 id、settings 和 sharpe,按 sharpe 排序。
        """
        # 如果 exclude_operators 为空，则初始化为空列表
        exclude_operators = exclude_operators or []
        
        alpha_result = []
        for _, row in df.iterrows():
            try:
                # 解析 regular 列（Python 字典字符串）
                regular_str = row['regular']
                try:
                    regular_data = ast.literal_eval(regular_str)
                except (SyntaxError, ValueError) as e:
                    self.logger.warning(f"Failed to parse alpha {row['id']} regular field: {e}, raw data: {regular_str[:100]}...")
                    continue

                # 检查 code 中是否包含需要过滤的运算符
                code = regular_data.get('code', '')
                if exclude_operators:  # 只有当 exclude_operators 不为空时才进行检查
                    if not all(x not in code for x in exclude_operators):
                        self.logger.info(f"Skipping alpha {row['id']} due to presence of operator '{exclude_operators}' in code")
                        continue  # 只要发现一个匹配的运算符就跳过

                # 解析 is 列（Python 字典字符串）
                is_str = row['is']
                try:
                    is_data = ast.literal_eval(is_str)
                except (SyntaxError, ValueError) as e:
                    self.logger.warning(f"Failed to parse alpha {row['id']} is field: {e}, raw data: {is_str[:100]}...")
                    continue

                # 提取 fitness 和 sharpe，处理 None 或无效值
                try:
                    fitness = float(is_data.get('fitness', 0))
                except (TypeError, ValueError) as e:
                    self.logger.warning(f"Failed to convert fitness for alpha {row['id']}: {e}, raw fitness: {is_data.get('fitness')}")
                    continue

                try:
                    sharpe = float(is_data.get('sharpe', 0))
                except (TypeError, ValueError) as e:
                    self.logger.warning(f"Failed to convert sharpe for alpha {row['id']}: {e}, raw sharpe: {is_data.get('sharpe')}")
                    continue

                # 检查 fitness 和 sharpe 是否满足条件
                if (fitness >= min_fitness and sharpe >= min_sharpe) or (fitness <= min_fitness * -1.0 and sharpe <= min_sharpe * -1.0):
                    # 解析 settings 列（Python 字典字符串）
                    settings_str = row['settings']
                    try:
                        settings = ast.literal_eval(settings_str)
                    except (SyntaxError, ValueError) as e:
                        self.logger.warning(f"Failed to parse alpha {row['id']} settings: {e}, raw data: {settings_str[:100]}...")
                        continue
                    
                    # 解析 classifications 列（Python 列表字符串）
                    classifications_str = row['classifications']
                    try:
                        classifications = ast.literal_eval(classifications_str) if classifications_str and classifications_str != '[]' else []
                        self.logger.debug(f"get alpha {row['id']} classifications raw data: {classifications}...")
                    except (SyntaxError, ValueError) as e:
                        self.logger.warning(f"Failed to parse alpha {row['id']} classifications: {e}, raw data: {classifications_str[:100]}...")
                        classifications = []

                    alpha_result.append({
                        'id': row['id'],
                        'settings': settings,
                        'sharpe': sharpe,
                        'fitness': fitness,
                        'abs_fitness': abs(fitness),  # 添加fitness绝对值字段用于排序
                        'classifications': classifications  # 添加解析后的classifications字段
                    })

            except Exception as e:
                self.logger.warning(f"Failed to process alpha {row['id']}: {e}, raw is data: {is_str[:100]}...")
                continue

        # 按 fitness 绝对值从小到大排序
        alpha_result.sort(key=lambda x: x['abs_fitness'])
        
        # 分离含SINGLE_DATA_SET标志的item并重新排序
        single_data_set_items = []
        regular_items = []
        for item in alpha_result:
            # 使用item中的classifications字段检查标志
            if any(cls.get('id') == 'DATA_USAGE:SINGLE_DATA_SET' for cls in item['classifications']):
                single_data_set_items.append(item)
            else:
                regular_items.append(item)
        
        # 对含标志的item单独排序（按fitness绝对值）
        single_data_set_items.sort(key=lambda x: x['abs_fitness'])
        
        # 合并两部分：常规item在前，标志item在后
        alpha_result = regular_items + single_data_set_items
        
        # 移除临时添加的 abs_fitness 和 classifications 字段
        for item in alpha_result:
            item.pop('abs_fitness', None)
            item.pop('classifications', None)  # classifications是临时字段，排序后不再需要
        
        # 添加数量统计日志
        self.logger.info(
            f"Alpha separation: total={len(alpha_result)}, "
            f"regular={len(regular_items)}, "
            f"single_dataset={len(single_data_set_items)}"
        )
        
        # 打印前3个和后3个alpha的详情
        if len(alpha_result) > 0:
            self.logger.info("First 3 alphas by fitness:")
            for alpha in alpha_result[:3]:
                self.logger.info(f"ID: {alpha['id']}, fitness: {alpha['fitness']}, sharpe: {alpha['sharpe']}")
            
            self.logger.info("Last 3 alphas by fitness:") 
            for alpha in alpha_result[-3:]:
                self.logger.info(f"ID: {alpha['id']}, fitness: {alpha['fitness']}, sharpe: {alpha['sharpe']}")

        self.logger.info(f"Filtered {len(alpha_result)} alphas (fitness >= {min_fitness}, sharpe >= {min_sharpe}), sorted by fitness")
        return alpha_result

    def generate_comparison_data(self, alpha_result: List[Dict]) -> None:
        """
        使用筛选后的 alpha 生成 os_alpha_ids 和 os_alpha_rets，保持 os_alpha_ids 与 alpha_result 顺序一致。
        如果 os_alpha_rets 缺少某些 alpha_id 的数据，重试一次查询，仍缺失则从 os_alpha_ids 删除对应 alpha_id。

        参数:
            alpha_result (List[Dict]): 筛选后的 alpha 元数据列表。
        """
        self.logger.info("Generating comparison data (os_alpha_ids and os_alpha_rets)...")
        # 初始化 os_alpha_ids，保持 alpha_result 的顺序
        self.os_alpha_ids = {}
        for alpha in alpha_result:
            region = alpha['settings']['region']
            if region not in self.os_alpha_ids:
                self.os_alpha_ids[region] = []
            if alpha['id'] not in self.os_alpha_ids[region]:  # 避免重复
                self.os_alpha_ids[region].append(alpha['id'])

        # 获取所有 alpha 的盈亏数据
        try:
            if config_manager.renew_session():
                self.sess = config_manager.get_session()
                self.logger.info("Session re-authenticated successfully")
            _, self.os_alpha_rets = get_alpha_pnls(
                alphas = alpha_result, sess = self.sess, username=self.username, password=self.password)
            # 转换为日收益率
            self.os_alpha_rets = self.os_alpha_rets - self.os_alpha_rets.ffill().shift(1)
            # 筛选过去 4 年的数据
            self.os_alpha_rets = self.os_alpha_rets[pd.to_datetime(self.os_alpha_rets.index) >
                                                pd.to_datetime(self.os_alpha_rets.index).max() - pd.DateOffset(years=4)]
        except Exception as e:
            self.logger.error(f"Failed to generate comparison data: {e}")
            raise

        # 检查 os_alpha_rets 是否包含所有 os_alpha_ids 的数据
        missing_alpha_ids = {}
        for region, alpha_ids in self.os_alpha_ids.items():
            missing_alpha_ids[region] = [alpha_id for alpha_id in alpha_ids if alpha_id not in self.os_alpha_rets.columns]
            if missing_alpha_ids[region]:
                self.logger.warning(f"Missing data in os_alpha_rets for region {region}: {missing_alpha_ids[region]}")

        # 对缺失的 alpha_id 重试查询
        for region, alpha_ids in missing_alpha_ids.items():
            if alpha_ids:
                self.logger.info(f"Retrying to fetch data for missing alpha IDs in region {region}: {alpha_ids}")
                try:
                    # 筛选出缺失的 alpha 元数据
                    retry_alphas = [alpha for alpha in alpha_result if alpha['id'] in alpha_ids and alpha['settings']['region'] == region]
                    if retry_alphas:
                        if config_manager.renew_session():
                            self.sess = config_manager.get_session()
                        _, retry_rets = get_alpha_pnls(retry_alphas, self.sess)
                        # 转换为日收益率
                        retry_rets = retry_rets - retry_rets.ffill().shift(1)
                        # 筛选过去 4 年的数据
                        retry_rets = retry_rets[pd.to_datetime(retry_rets.index) >
                                            pd.to_datetime(retry_rets.index).max() - pd.DateOffset(years=4)]
                        # 合并到 os_alpha_rets
                        self.os_alpha_rets = pd.concat([self.os_alpha_rets, retry_rets], axis=1)
                        self.logger.info(f"Successfully fetched retry data for region {region}: {retry_rets.columns.tolist()}")
                except Exception as e:
                    self.logger.error(f"Failed to retry fetching data for region {region}: {e}")
                    self.logger.debug(f"Exception details: {traceback.format_exc()}")

        # 再次检查，删除仍缺失的 alpha_id
        for region, alpha_ids in self.os_alpha_ids.copy().items():  # 使用 copy 避免修改时迭代
            still_missing = [alpha_id for alpha_id in alpha_ids if alpha_id not in self.os_alpha_rets.columns]
            if still_missing:
                self.logger.warning(f"Removing alpha IDs from os_alpha_ids in region {region} due to missing data: {still_missing}")
                self.os_alpha_ids[region] = [alpha_id for alpha_id in alpha_ids if alpha_id not in still_missing]
                if not self.os_alpha_ids[region]:  # 如果区域为空，删除区域
                    del self.os_alpha_ids[region]

        self.logger.info(f"Comparison data generated, regions: {list(self.os_alpha_ids.keys())}")

    def calculate_correlations(self, alpha_result: List[Dict], corr_threshold: float) -> Dict[str, List[Dict]]:
        """
        按区域计算 alpha 的最大相关性，筛选相关性低于阈值的 alpha，并添加 self_corr。
        从 os_alpha_rets 查询 alpha_pnls，减少 API 调用。

        参数:
            alpha_result (List[Dict]): 筛选后的 alpha 元数据列表。
            corr_threshold (float): 相关性阈值，保留相关性小于此值的 alpha。

        返回:
            Dict[str, List[Dict]]: 按区域分组的筛选后 alpha 列表，键为区域，值为 alpha 元数据列表，
                                每个 alpha 包含 id, settings 和 self_corr。
        """
        filtered_alphas = {}
        temp_alpha_ids = deepcopy(self.os_alpha_ids)
        if config_manager.renew_session():
            self.sess = config_manager.get_session()
            self.logger.info("Session re-authenticated successfully")

        for region in temp_alpha_ids:
            alpha_count = len(temp_alpha_ids[region])
            filtered_alphas[region] = []
            self.logger.info(f"Start processing region: {region}, total {alpha_count} alphas")

            idx = 0
            while temp_alpha_ids[region]:
                alpha_id = temp_alpha_ids[region][0]
                alpha = next((a for a in alpha_result if a['id'] == alpha_id), None)
                idx += 1
                self.logger.info(f"[{region}] Processing alpha {idx}/{alpha_count}: {alpha_id}")
                if idx % 100 == 0:
                    if config_manager.renew_session():
                        self.sess = config_manager.get_session()
                        self.logger.info("Session re-authenticated successfully")

                if not alpha:
                    self.logger.warning(f"Alpha {alpha_id} metadata not found, skipping")
                    temp_alpha_ids[region].pop(0)
                    continue

                temp_region_ids = {region: [id for id in temp_alpha_ids[region] if id != alpha_id]}

                try:
                    max_corr = calc_self_corr(
                        alpha_id=alpha_id,
                        sess=self.sess,
                        os_alpha_rets=self.os_alpha_rets,
                        os_alpha_ids=temp_region_ids,
                        alpha_result=alpha,
                        return_alpha_pnls=False
                    )
                    self.logger.info(f"Alpha {alpha_id} (region: {region}) max correlation: {max_corr}")
                    alpha['self_corr'] = max_corr
                    self.logger.info(f"Added self_corr to alpha {alpha_id}: {max_corr}")

                    if max_corr < corr_threshold and not (max_corr == 0.0 and len(temp_alpha_ids[region]) > 1):
                        filtered_alphas[region].append(alpha)
                        self.logger.info(f"Alpha {alpha_id} passed filter (correlation {max_corr} < {corr_threshold})")
                    else:
                        self.logger.info(f"Alpha {alpha_id} filtered out (correlation {max_corr} >= {corr_threshold} or (max_corr=0.0 and region has {len(temp_alpha_ids[region])} alphas))")
                except Exception as e:
                    self.logger.error(f"Failed to calculate correlation for alpha {alpha_id}: {e}")

                temp_alpha_ids[region].pop(0)

            self.logger.info(
                f"Finished region: {region}, selected {len(filtered_alphas[region])}/{alpha_count} alphas"
            )

        return filtered_alphas


    def save_filtered_alphas(self, filtered_alphas: Dict[str, List[Dict]], original_df: pd.DataFrame) -> None:
        """
        将筛选后的 alpha 保存到数据库的 alpha_signal_table 表，包含 self_corr 列。

        参数:
            filtered_alphas (Dict[str, List[Dict]]): 按区域分组的筛选后 alpha 列表。
            original_df (pd.DataFrame): 原始 alpha 数据 DataFrame,用于保留完整信息。
        """
        total_inserted = 0
        for region, alphas in filtered_alphas.items():
            if not alphas:
                self.logger.info(f"No filtered alphas for region {region}, skipping save")
                continue

            # 提取筛选后 alpha 的 ID 和 self_corr
            alpha_ids = [alpha['id'] for alpha in alphas]
            self_corrs = {alpha['id']: alpha['self_corr'] for alpha in alphas}

            # 从原始 DataFrame 中提取对应行
            region_df = original_df[original_df['id'].isin(alpha_ids)].copy()

            # 添加 self_corr 列并重命名为 selfCorrelation
            region_df['selfCorrelation'] = region_df['id'].map(self_corrs)
            self.logger.info(f"Added selfCorrelation column for region {region}, contains {len(region_df)} alphas")

            # 转换为字典列表，准备批量插入
            data_list = region_df.to_dict('records')
            self.logger.info(f"Converted {len(data_list)} records to dict format for region {region}")

            try:
                # 批量插入到数据库
                inserted_count = self.alpha_signal_dao.batch_insert(data_list)
                total_inserted += inserted_count
                self.logger.info(f"Inserted {inserted_count} alphas for region {region} into alpha_signal_table")
            except Exception as e:
                self.logger.error(f"Failed to insert alphas for region {region}: {e}")
                self.logger.debug(f"Exception details: {traceback.format_exc()}")

        self.logger.info(f"Total inserted {total_inserted} filtered alphas into database")

    def process_alphas(self, min_fitness: float = 1.0, min_sharpe: float = 1.0, corr_threshold: float = 0.5) -> None:
        """
        执行完整的 alpha 筛选和相关性分析流程。

        参数:
            min_fitness (float): 最小 fitness 阈值，默认为 1.0。
            min_sharpe (float): 最小 sharpe 阈值，默认为 1.0。
            corr_threshold (float): 最大相关性阈值，默认为 0.5。
        """
        # 加载 alpha 数据
        df = self.load_alpha_data()

        # 筛选符合 fitness 和 sharpe 条件的 alpha
        alpha_result = self.filter_alphas(df, min_fitness, min_sharpe, ['trade_when'])

        # 检查所有region是否已存在数据库中
        regions = {alpha['settings']['region'] for alpha in alpha_result}
        self.logger.info(f"Found regions in alpha_result: {regions}")
        all_region_exist = True
        for region in regions:
            if not self.alpha_signal_dao.region_and_datetime_exists(region, datetime):
                all_region_exist = False
                break

        if all_region_exist:
            self.logger.info(f"All region already exist for date {self.date_str}, skipping processing")
            return

        # 使用筛选后的 alpha 生成比较数据
        self.generate_comparison_data(alpha_result)

        # 计算相关性并筛选
        filtered_alphas = self.calculate_correlations(alpha_result, corr_threshold)

        # 保存结果
        self.save_filtered_alphas(filtered_alphas, df)

    def start_monitoring(self, interval_minutes: int = 90, 
                        min_fitness: float = 0.7, 
                        min_sharpe: float = 1.2, 
                        corr_threshold: float = 0.7):
        """
        启动监控线程，定期检查新数据文件
        
        参数:
            interval_minutes: 检查间隔时间(分钟)
            min_fitness: fitness阈值
            min_sharpe: sharpe阈值
            corr_threshold: 相关性阈值
        """
        if self._monitor_thread and self._monitor_thread.is_alive():
            self.logger.warning("Monitoring thread is already running")
            return

        self._stop_event.clear()
        
        def monitor_loop():
            while not self._stop_event.is_set():
                try:
                    # Get max datetime from database
                    max_datetime = self.simulated_alphas_dao.get_max_datetime()
                    
                    if max_datetime and max_datetime > self.date_str:
                        self.logger.info(f"New data available (max datetime: {max_datetime}), current date: {self.date_str}")
                        self.logger.info(f"Starting processing for date {self.date_str}...")
                        self.process_alphas(min_fitness, min_sharpe, corr_threshold)
                        self._advance_date()
                    else:
                        self.logger.debug(f"No new data found for date {self.date_str}, max datetime: {max_datetime}")
                except Exception as e:
                    self.logger.error(f"Error during monitoring: {e}")
                    self.logger.debug(f"Exception details: {traceback.format_exc()}")
                
                # Wait for interval or until stopped
                self.logger.info(f"No new alpha to filter. Sleeping for {interval_minutes / 60} hour.")
                self._stop_event.wait(timeout=interval_minutes * 60)

        self._monitor_thread = threading.Thread(
            target=monitor_loop,
            name="AlphaFilterMonitor",
            daemon=True
        )
        self._monitor_thread.start()
        self.logger.info(f"Started monitoring thread (checking every {interval_minutes} minutes)")

    def stop_monitoring(self):
        """停止监控线程"""
        if self._monitor_thread and self._monitor_thread.is_alive():
            self._stop_event.set()
            self._monitor_thread.join(timeout=5)
            self.logger.info("Monitoring thread stopped")
        else:
            self.logger.warning("No active monitoring thread to stop")

    def _advance_date(self):
        """将日期前进一天"""
        current_date = datetime.strptime(self.date_str, "%Y%m%d")
        new_date = current_date + timedelta(days=1)
        self.date_str = new_date.strftime("%Y%m%d")
        self.logger.info(f"Advanced processing date to {self.date_str}")

"""
def main():
    # 配置参数
    min_fitness = 0.7
    min_sharpe = 1.2
    corr_threshold = 0.7

    # 初始化并运行
    filter = AlphaFilter(username, password)
    df = filter.load_alpha_data()

        # 筛选符合 fitness 和 sharpe 条件的 alpha
    alpha_result = filter.filter_alphas(df, min_fitness, min_sharpe, ['trade_when'])

    #filter.process_alphas(min_fitness, min_sharpe, corr_threshold)

if __name__ == "__main__":
    main()
"""
