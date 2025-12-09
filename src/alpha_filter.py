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
import requests
from ace_lib import get_alpha_yearly_stats

class AlphaFilter:
    """
    一个类，用于筛选 alpha 数据，计算相关性，并按区域保存结果。
    该类被设计为按需服务，由API调用触发，处理指定日期的alpha数据。
    """
    def __init__(self, signal_manager=None):
        """
        初始化 AlphaFilter。
        """
        self.os_alpha_ids: Optional[Dict[str, List]] = None
        self.os_alpha_rets: Optional[pd.DataFrame] = None
        self.logger = Logger()
        self.alpha_signal_dao = AlphaSignalDAO()
        
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
    
    def _fetch_alphas_from_api(self, date_str: str, per_page: int = 1000) -> pd.DataFrame:
        """
        根据指定日期从API获取原始alpha数据行（自动分页获取所有数据）。
        该实现参考了 D:\repos\alpha_simulator_bot\src\powerpoll_alpha_filter.py 中的 get_alphas_by_datetime 方法。
        
        参数:
            date_str: 日期时间字符串，格式为'%Y%m%d'
            per_page: 每页数量（默认为1000）
            
        返回:
            包含从API获取的alpha数据的DataFrame。
        """
        dt_obj = datetime.strptime(date_str, '%Y%m%d')
        timezone_offset = "-05:00"  # Per reference implementation
        
        start_date = dt_obj.strftime(f'%Y-%m-%dT00:00:00{timezone_offset}')
        end_date = (dt_obj + timedelta(days=1)).strftime(f'%Y-%m-%dT00:00:00{timezone_offset}')

        offset = 0
        all_alphas = []
        
        self.logger.info(f"Fetching alphas from API created between {start_date} and {end_date}")

        while True:
            # URL构建参考了 powerpoll_alpha_filter.py
            url = (
                f"https://api.worldquantbrain.com/users/self/alphas?limit={per_page}&offset={offset}"
                f"&status=UNSUBMITTED%1FIS_FAIL"
                f"&dateCreated>={start_date}&dateCreated<{end_date}"
                f"&order=-dateCreated"
                f"&hidden=false"
                f"&type=REGULAR"
            )
            
            self.logger.info(f"Fetching alphas from URL (offset: {offset})")
            
            try:
                # 重新认证会话，确保会话有效
                self.sess = config_manager.get_session()

                response = self.sess.get(url, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                alphas_page = data.get("results", [])
                
                if not alphas_page:
                    self.logger.info("No more alphas found on this page. Pagination complete.")
                    break
                
                self.logger.info(f"Successfully fetched {len(alphas_page)} alphas from offset {offset}.")
                all_alphas.extend(alphas_page)
                
                offset += per_page
                
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    self.logger.info(f"API returned 404 for date {date_str}, which might be expected if no alphas were created on a given page.")
                    break # 404 意味着没有数据，分页结束
                self.logger.error(f"HTTP error while fetching data: {e}")
                raise  # 将其他HTTP错误向上抛出
            except Exception as e:
                self.logger.error(f"An unexpected error occurred while fetching data from API: {e}")
                self.logger.error("Stopping pagination due to error.")
                raise # 将异常向上抛出

        self.logger.info(f"Total alphas fetched from API for {date_str}: {len(all_alphas)}")
        return pd.DataFrame(all_alphas)

    def process_alphas(self, date_str: str, min_fitness: float, min_sharpe: float, corr_threshold: float):
        """
        为指定日期执行完整的Alpha筛选流程。
        成功执行则正常返回，若发生任何错误则向上抛出异常。
        """
        self.logger.info(f"Processing alphas for {date_str} with params: fitness>={min_fitness}, sharpe>={min_sharpe}, corr<{corr_threshold}")
        
        df = self._fetch_alphas_from_api(date_str)
        if df.empty:
            self.logger.info(f"No data to process for {date_str}. Task finished.")
            return

        # 步骤 1: 初步筛选
        alpha_result = self.filter_alphas(df, min_fitness, min_sharpe, ['trade_when'])
        if not alpha_result:
            self.logger.info(f"No alphas passed initial filtering. Task finished.")
            return

        # 步骤 2: 根据年度统计数据进行过滤
        alpha_result = self._filter_by_yearly_stats(alpha_result)
        if not alpha_result:
            self.logger.info(f"No alphas passed the yearly stats filter. Task finished.")
            return

        # 步骤 3: 准备相关性计算数据
        self.generate_comparison_data(alpha_result)
        
        # 步骤 4: 通过相关性过滤
        filtered_alphas = self.calculate_correlations(alpha_result, corr_threshold)
        if not any(filtered_alphas.values()):
            self.logger.info(f"No alphas passed correlation filtering. Task finished.")
            return
            
        # 步骤 5: 保存最终结果
        self.save_filtered_alphas(filtered_alphas)
        self.logger.info(f"Successfully completed alpha processing for {date_str}")

    def _filter_by_yearly_stats(self, alphas: List[Dict]) -> List[Dict]:
        """
        根据sharpe不为零的年数过滤alpha。
        sharpe不为零的年数少于8年的alpha将被排除。
        此方法参考了powerpoll_alpha_filter.py中的逻辑。
        """
        filtered_alphas = []
        total_alphas = len(alphas)
        if total_alphas == 0:
            return []

        self.logger.info(f"Starting yearly stats filtering for {total_alphas} alphas.")
        for idx, alpha in enumerate(alphas):
            alpha_id = alpha['id']
            
            # 定期重新认证会话以防过期
            if idx > 0 and idx % 50 == 0:
                self.logger.info("Re-authenticating session...")
                self.sess = config_manager.get_session()

            try:
                stats = get_alpha_yearly_stats(self.sess, alpha_id)
                if stats.empty:
                    self.logger.warning(f"Could not retrieve yearly stats for alpha {alpha_id}. Excluding it.")
                    continue
                
                # 计算sharpe不为零的年数
                sharpe_non_zero_years = (stats['sharpe'] != 0.0).sum()

                if sharpe_non_zero_years >= 8:
                    filtered_alphas.append(alpha)
                else:
                    self.logger.info(f"Excluding alpha {alpha_id}: years with non-zero sharpe is {sharpe_non_zero_years} (less than 8).")
            
            except Exception as e:
                self.logger.error(f"Error processing alpha {alpha_id} for yearly stats filter: {e}", exc_info=True)
            
            if (idx + 1) % 50 == 0 or (idx + 1) == total_alphas:
                self.logger.info(f"Processed {idx + 1}/{total_alphas} alphas for yearly stats filter.")

        self.logger.info(f"Finished yearly stats filtering. {len(filtered_alphas)}/{total_alphas} alphas passed.")
        return filtered_alphas

    def filter_alphas(self, df: pd.DataFrame, min_fitness: float, min_sharpe: float, exclude_operators: List[str] = None) -> List[Dict]:
        """
        筛选符合 fitness 和 sharpe 条件的 alpha, 过滤掉包含指定运算符的行, 构造 alpha_result, 并按 fitness 绝对值从小到大排序。
        此版本适配从API直接获取的DataFrame，其中嵌套字段已经是dict或list，无需ast.literal_eval。

        参数:
            df (pd.DataFrame): 包含 alpha 数据的 DataFrame。
            min_fitness (float): 最小 fitness 阈值。
            min_sharpe (float): 最小 sharpe 阈值。
            exclude_operators (List[str], optional): 需要过滤的运算符列表。

        返回:
            List[Dict]: 筛选后的 alpha 元数据列表。
        """
        exclude_operators = exclude_operators or []
        
        alpha_result = []
        for _, row in df.iterrows():
            try:
                row_id = row.get('id', 'N/A')

                # 直接访问字典，不再需要 ast.literal_eval
                regular_data = row.get('regular')
                if not isinstance(regular_data, dict):
                    self.logger.warning(f"Field 'regular' for alpha {row_id} is not a dictionary. Skipping.")
                    continue

                code = regular_data.get('code', '')
                if exclude_operators and any(x in code for x in exclude_operators):
                    self.logger.info(f"Skipping alpha {row_id} due to presence of excluded operator in code.")
                    continue

                # 直接访问字典
                is_data = row.get('is')
                if not isinstance(is_data, dict):
                    self.logger.warning(f"Field 'is' for alpha {row_id} is not a dictionary. Skipping.")
                    continue

                fitness = float(is_data.get('fitness', 0.0))
                sharpe = float(is_data.get('sharpe', 0.0))

                if (fitness >= min_fitness and sharpe >= min_sharpe) or \
                   (fitness <= -min_fitness and sharpe <= -min_sharpe):
                    
                    settings = row.get('settings')
                    if not isinstance(settings, dict):
                        self.logger.warning(f"Field 'settings' for alpha {row_id} is not a dictionary. Skipping.")
                        continue
                        
                    classifications = row.get('classifications', [])
                    if not isinstance(classifications, list):
                        self.logger.warning(f"Field 'classifications' for alpha {row_id} is not a list. Treating as empty.")
                        classifications = []

                    alpha_result.append({
                        'id': row_id,
                        'settings': settings,
                        'sharpe': sharpe,
                        'fitness': fitness,
                        'abs_fitness': abs(fitness),
                        'classifications': classifications
                    })

            except (TypeError, ValueError) as e:
                self.logger.warning(f"Failed to process alpha {row.get('id', 'N/A')} due to data type error: {e}")
            except Exception as e:
                self.logger.error(f"An unexpected error occurred while processing alpha {row.get('id', 'N/A')}: {e}", exc_info=True)

        # 按 fitness 绝对值从小到大排序
        alpha_result.sort(key=lambda x: x['abs_fitness'])
        
        # 分离含SINGLE_DATA_SET标志的item并重新排序
        single_data_set_items = []
        regular_items = []
        for item in alpha_result:
            if any(cls.get('id') == 'DATA_USAGE:SINGLE_DATA_SET' for cls in item.get('classifications', [])):
                single_data_set_items.append(item)
            else:
                regular_items.append(item)
        
        single_data_set_items.sort(key=lambda x: x['abs_fitness'])
        
        # 合并两部分：常规item在前，标志item在后
        alpha_result = regular_items + single_data_set_items
        
        for item in alpha_result:
            item.pop('abs_fitness', None)
            # 'classifications' 可能会在下游使用，暂时保留
        
        self.logger.info(
            f"Alpha separation: total={len(alpha_result)}, "
            f"regular={len(regular_items)}, "
            f"single_dataset={len(single_data_set_items)}"
        )
        
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


    def save_filtered_alphas(self, filtered_alphas: Dict[str, List[Dict]]) -> None:
        """
        获取筛选后的 alpha 的最新详情，添加 self_corr，并保存到数据库。
        此版本不再依赖于旧的DataFrame，而是为每个通过筛选的alpha重新获取最新数据。
        """
        self.logger.info("Starting final save process: fetching latest details for filtered alphas.")
        records_to_insert = []
        
        # 将所有region的alpha收集到一个列表中，方便统一处理
        all_filtered_alphas = [alpha for alphas in filtered_alphas.values() for alpha in alphas]
        total_alphas_to_save = len(all_filtered_alphas)
        
        self.logger.info(f"Total number of alphas to fetch and save: {total_alphas_to_save}")

        for idx, alpha in enumerate(all_filtered_alphas):
            alpha_id = alpha['id']
            self_corr = alpha['self_corr']
            
            # 定期重新认证会话
            if idx > 0 and idx % 50 == 0:
                self.logger.info(f"Re-authenticating session... (processed {idx}/{total_alphas_to_save})")
                self.sess = config_manager.get_session()

            try:
                self.logger.debug(f"Fetching latest details for alpha_id: {alpha_id}")
                brain_api_url = "https://api.worldquantbrain.com"
                response = self.sess.get(f"{brain_api_url}/alphas/{alpha_id}", timeout=30)
                response.raise_for_status()
                alpha_details = response.json()

                if not alpha_details:
                    self.logger.warning(f"Could not fetch latest details for alpha_id {alpha_id}. Skipping.")
                    continue
                
                # 格式化数据
                formatted_alpha = self._format_alpha_details(alpha_details)
                
                # 添加 selfCorrelation 字段
                formatted_alpha['selfCorrelation'] = self_corr
                
                records_to_insert.append(formatted_alpha)
                
            except Exception as e:
                self.logger.error(f"Failed to fetch or format details for alpha_id {alpha_id}: {e}", exc_info=True)

        if not records_to_insert:
            self.logger.warning("No records were successfully prepared for database insertion.")
            return

        self.logger.info(f"Preparing to insert {len(records_to_insert)} records into the database.")
        try:
            inserted_count = self.alpha_signal_dao.batch_insert(records_to_insert)
            self.logger.info(f"Successfully inserted {inserted_count} alphas into alpha_signal_table.")
        except Exception as e:
            self.logger.error(f"Database batch insert failed: {e}", exc_info=True)
            # 根据需要，这里可以增加失败重试或将失败记录写入文件的逻辑
            
    def _format_alpha_details(self, alpha_details: Dict) -> Dict:
        """
        将从API获取的alpha详细信息格式化为数据库接受的格式。
        参考自alpha_poller.py中的_format_alpha_details方法。
        """
        date_created = alpha_details.get("dateCreated")
        datetime_str = date_created.split("T")[0].replace("-", "") if date_created else None
        
        settings = alpha_details.get("settings", {})
        region = settings.get("region")

        favorite = 1 if alpha_details.get("favorite", False) else 0
        hidden = 1 if alpha_details.get("hidden", False) else 0

        grade = alpha_details.get("grade")
        if grade is not None:
            try:
                grade = round(float(grade), 2)
            except (ValueError, TypeError):
                grade = None
                        
        return {
            "id": alpha_details.get("id"),
            "type": alpha_details.get("type"),
            "author": alpha_details.get("author"),
            "settings": str(settings),
            "regular": str(alpha_details.get("regular", {})),
            "dateCreated": self.to_mysql_datetime(date_created),
            "dateSubmitted": self.to_mysql_datetime(alpha_details.get("dateSubmitted")),
            "dateModified": self.to_mysql_datetime(alpha_details.get("dateModified")),
            "name": alpha_details.get("name"),
            "favorite": favorite,
            "hidden": hidden,
            "color": alpha_details.get("color"),
            "category": alpha_details.get("category"),
            "tags": str(alpha_details.get("tags", [])),
            "classifications": str(alpha_details.get("classifications", [])),
            "grade": grade,
            "stage": alpha_details.get("stage"),
            "status": alpha_details.get("status"),
            "is": str(alpha_details.get("is", {})),
            "os": alpha_details.get("os"),
            "train": alpha_details.get("train"),
            "test": alpha_details.get("test"),
            "prod": alpha_details.get("prod"),
            "competitions": alpha_details.get("competitions"),
            "themes": str(alpha_details.get("themes", [])),
            "pyramids": str(alpha_details.get("pyramids", [])),
            "pyramidThemes": str(alpha_details.get("pyramidThemes", [])),
            "team": alpha_details.get("team"),
            "datetime": datetime_str,
            "region": region
        }

    def to_mysql_datetime(self, dt_str: Optional[str]) -> Optional[str]:
        """将ISO格式的日期字符串转换为MySQL的DATETIME格式。"""
        if not dt_str:
            return None
        try:
            if dt_str.endswith('Z'):
                dt_str = dt_str[:-1] + '+00:00'
            dt = datetime.fromisoformat(dt_str)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            self.logger.error(f"Failed to parse datetime: {dt_str}, error: {e}")
            return None


