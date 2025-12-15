import pandas as pd
import ast
import os
import csv
import traceback
from datetime import datetime
from self_corr_calculator import calc_self_corr, download_data, load_data
from logger import Logger
from config_manager import config_manager
from dao import SimulatedAlphasDAO
from utils import get_alphas_from_data
from ace_lib import get_alpha_yearly_stats, check_session_and_relogin

from datetime import datetime, timedelta

class PowerPollAlphaFilter:
    def __init__(self):
        self.logger = Logger()
        self.sess = config_manager.get_session()
        self.username = config_manager._config.get('username', '')
        self.password = config_manager._config.get('password', '')
        self.simulated_alphas_dao = SimulatedAlphasDAO()
        
    def download_and_load_data(self, tag=None):
        """
        下载并加载数据
        
        参数:
            tag: 数据标签（可选），例如'PPAC'或'SelfCorr'
            
        返回:
            os_alpha_ids, os_alpha_rets: 加载的数据
        """
        # 增量下载数据
        download_data(self.sess, flag_increment=True)
        
        # 加载数据，如果需要使用不同的标签，可以传入 tag 参数
        if tag:
            os_alpha_ids, os_alpha_rets = load_data(tag=tag)
        else:
            os_alpha_ids, os_alpha_rets = load_data()
            
        return os_alpha_ids, os_alpha_rets

    def _filter_by_sharpe_non_zero_years(self, alphas, region):
        filtered_alphas = []
        total_alphas = len(alphas)
        if total_alphas == 0:
            return []

        self.logger.info(f"[{region}] Starting sharpe_non_zero_years filtering for {total_alphas} alphas.")
        for idx, alpha in enumerate(alphas):
            alpha_id = alpha[0]
            try:
                self.sess = check_session_and_relogin(self.sess)
                stats = get_alpha_yearly_stats(self.sess, alpha_id)
                if stats.empty:
                    self.logger.warning(f"[{region}] Could not retrieve yearly stats for alpha {alpha_id}. Excluding it.")
                    continue
                
                sharpe_non_zero_years = (stats['sharpe'] != 0.0).sum()

                if sharpe_non_zero_years >= 8:
                    filtered_alphas.append(alpha)
                else:
                    self.logger.info(f"[{region}] Excluding alpha {alpha_id} because sharpe_non_zero_years is {sharpe_non_zero_years} (< 8)")
            except Exception as e:
                self.logger.error(f"[{region}] Error processing alpha {alpha_id} for sharpe filter: {e}")
            
            if (idx + 1) % 50 == 0 or (idx + 1) == total_alphas:
                self.logger.info(f"[{region}] Processed {idx + 1}/{total_alphas} alphas for sharpe filter.")

        self.logger.info(f"[{region}] Finished sharpe_non_zero_years filtering. {len(filtered_alphas)} alphas passed.")
        return filtered_alphas

    def get_alphas_by_datetime(self, datetime_str, per_page=1000):
        """
        根据日期时间从API获取原始alpha数据行（自动分页获取所有数据）
        
        参数:
            datetime_str: 日期时间字符串，格式为'%Y%m%d'
            per_page: 每页数量（默认为1000）
            
        返回:
            从API获取的alpha数据字典列表
        """
        dt_obj = datetime.strptime(datetime_str, '%Y%m%d')
        # Use a fixed offset for consistency with the user's example.
        # It's important to note that '-05:00' is Eastern Time (ET)
        # If the server or user's local timezone varies, this might need dynamic adjustment.
        # However, for now, we follow the provided example string.
        timezone_offset = "-05:00" 
        
        start_date = dt_obj.strftime(f'%Y-%m-%dT00:00:00{timezone_offset}')
        end_date = (dt_obj + timedelta(days=1)).strftime(f'%Y-%m-%dT00:00:00{timezone_offset}')

        offset = 0
        all_alphas = []
        
        self.logger.info(f"Fetching alphas from API created between {start_date} and {end_date}")

        while True:
            # Construct the URL based on the user's provided example for date format and parameters.
            url = (
                f"https://api.worldquantbrain.com/users/self/alphas?limit={per_page}&offset={offset}"
                f"&status=UNSUBMITTED%1FIS_FAIL" # Added status from example
                f"&dateCreated>={start_date}&dateCreated<{end_date}"
                f"&order=-dateCreated" # Added order from example
                f"&hidden=false" # Added hidden from example
                f"&type=REGULAR"
            )
            
            self.logger.info(f"Fetching alphas from URL: {url}")
            
            try:
                self.sess = check_session_and_relogin(self.sess)
                response = self.sess.get(url)
                response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
                
                data = response.json()
                alphas_page = data.get("results", [])
                
                if not alphas_page:
                    self.logger.info("No more alphas found on this page. Pagination complete.")
                    break
                
                self.logger.info(f"Successfully fetched {len(alphas_page)} alphas from offset {offset}.")
                all_alphas.extend(alphas_page)
                
                offset += per_page
                
            except Exception as e:
                self.logger.error(f"An error occurred while fetching data from API: {e}")
                self.logger.error("Stopping pagination due to error.")
                break
                
        self.logger.info(f"Total alphas fetched from API for {datetime_str}: {len(all_alphas)}")
        return all_alphas
    
    def generate_ppac_gold_bags(self, date, keywords_to_exclude, os_alpha_ids, os_alpha_rets):
        regions = ['USA', 'CHN', 'GLB', 'EUR', 'ASI', 'IND']
        stone_bag_atom = {}
        stone_bag_hp = {}
        ppa_tracker_atom_all = []
        ppa_tracker_hp_all = []

        # 获取指定日期的所有数据
        data_rows = self.get_alphas_by_datetime(date, 100)

        # 先生成 stone_bag_atom 和 stone_bag_hp
        for region in regions:
            # atom 筛选
            ppa_tracker_atom_unfiltered = get_alphas_from_data(
                data_rows, 
                min_sharpe=1.0, 
                min_fitness=0.3, 
                mode="submit", 
                region_filter=region, 
                single_data_set_filter=True
            )
            ppa_tracker_atom = self._filter_by_sharpe_non_zero_years(ppa_tracker_atom_unfiltered, region)
            ppa_tracker_atom_all.extend(ppa_tracker_atom)
            region_bag_atom = [
                alpha[0] for alpha in ppa_tracker_atom 
                if not any(keyword in alpha[1] for keyword in keywords_to_exclude)
            ]
            stone_bag_atom[region] = region_bag_atom

            # hp 筛选
            ppa_tracker_hp_unfiltered = get_alphas_from_data(
                data_rows, 
                min_sharpe=1.2, 
                min_fitness=1.0, 
                mode="submit", 
                region_filter=region, 
                single_data_set_filter=None
            )
            ppa_tracker_hp = self._filter_by_sharpe_non_zero_years(ppa_tracker_hp_unfiltered, region)
            ppa_tracker_hp_all.extend(ppa_tracker_hp)
            region_bag_hp = [
                alpha[0] for alpha in ppa_tracker_hp 
                if not any(keyword in alpha[1] for keyword in keywords_to_exclude)
            ]
            stone_bag_hp[region] = region_bag_hp

        # 日志记录统计信息
        for region in regions:
            self.logger.info(f"{region}: {len(stone_bag_atom[region])} atom alphas, {len(stone_bag_hp[region])} high performance alphas")
        self.logger.info(f"Total atom alphas: {len(ppa_tracker_atom_all)}")
        self.logger.info(f"Total high performance alphas: {len(ppa_tracker_hp_all)}")

        # 定义内部处理方法
        def process_stone_bag(stone_bag, output_file_path, os_alpha_ids, os_alpha_rets, ppa_tracker=None):
            valid_alphas_by_region = {}
            ppa_tracker_dict = {item[0]: item[7] for item in ppa_tracker} if ppa_tracker else {}
            
            # 创建输出目录
            os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
            
            # 打开CSV文件准备写入
            with open(output_file_path, mode="w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(["region", "alpha_id", "self_corr", "pyramids"])
                
                # 记录开始处理
                self.logger.info(f"Starting processing for {output_file_path}")
                
                # 处理每个区域
                for region in regions:
                    region_stone_bag = stone_bag.get(region, [])
                    valid_alphas = []
                    total_alpha = len(region_stone_bag)
                    
                    if not total_alpha:
                        self.logger.info(f"[{region}] No alphas to process")
                        continue
                    
                    # 记录区域开始处理
                    self.logger.info(f"[{region}] Processing {total_alpha} alphas")
                    
                    # 处理每个alpha
                    for idx, alpha_id in enumerate(region_stone_bag, start=1):
                        # 每处理10%或最后一个alpha时记录日志
                        progress_interval = max(1, total_alpha // 10)
                        if idx % progress_interval == 0 or idx == total_alpha:
                            # 更新会话（如果需要）
                            self.sess = config_manager.get_session()
                            self.logger.info("Session re-authenticated successfully")
                            
                            # 记录进度
                            self.logger.info(f"[{region}] Processed {idx}/{total_alpha} alphas ({idx/total_alpha:.1%})")

                        # 计算自相关性
                        try:
                            self_corr = calc_self_corr(
                                sess=self.sess, 
                                alpha_id=alpha_id, 
                                os_alpha_rets=os_alpha_rets, 
                                os_alpha_ids=os_alpha_ids
                            )
                        except Exception as e:
                            self.logger.error(f"Error calculating self-correlation for alpha {alpha_id}: {str(e)}")
                            self_corr = 0.0  # 设置为无效值
                        
                        # 筛选有效alpha
                        if self_corr < 0.5 and self_corr != 0.0:
                            pyramids = ppa_tracker_dict.get(alpha_id, None)
                            valid_alphas.append((alpha_id, self_corr, pyramids))
                            writer.writerow([region, alpha_id, self_corr, pyramids])
                        else:
                            self.logger.debug(f"[{region}] Excluded alpha {alpha_id} with self_corr {self_corr:.4f}")
                    
                    valid_alphas_by_region[region] = valid_alphas
                    self.logger.info(f"[{region}] Found {len(valid_alphas)} valid alphas")

            # 记录最终结果摘要
            self.logger.info(f"\n=== Summary for {output_file_path} ===")
            for region in regions:
                valid_alphas = valid_alphas_by_region.get(region, [])
                if valid_alphas:
                    self.logger.info(f"{region}: {len(valid_alphas)} valid alphas")
            
            self.logger.info(f"\n✅ Results saved to: {output_file_path}\n")

        # 生成 CSV 文件
        output_file_atom = f"output/ppac_atom_gold_bag.csv.{date}"
        output_file_hp = f"output/ppac_hp_gold_bag.csv.{date}"
        
        self.logger.info(f"Generating ATOM gold bag for {date}")
        process_stone_bag(stone_bag_atom, output_file_atom, os_alpha_ids, os_alpha_rets, ppa_tracker_atom_all)
        
        self.logger.info(f"Generating HP gold bag for {date}")
        process_stone_bag(stone_bag_hp, output_file_hp, os_alpha_ids, os_alpha_rets, ppa_tracker_hp_all)

def main():
    # 设置日志
    logger = Logger()
    logger.info("Starting PowerPoll Alpha Filter")

    datetimes = ['20251210']  # 示例日期列表
    
    try:
        # 初始化并运行
        ppa_filter = PowerPollAlphaFilter()
        os_alpha_ids, os_alpha_rets = ppa_filter.download_and_load_data()
        for date in datetimes:
            ppa_filter.generate_ppac_gold_bags(
                date=date, 
                keywords_to_exclude=[],
                os_alpha_ids=os_alpha_ids,
                os_alpha_rets=os_alpha_rets
            )
            logger.info("✅ Processing completed successfully")
    except Exception as e:
        logger.error(f"❌ Processing failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
#python src/powerpoll_alpha_filter.py
