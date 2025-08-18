import pandas as pd
import ast
import os
import csv
import traceback
from datetime import datetime
from self_corr_calculator import calc_self_corr,download_data, load_data
from logger import Logger
from config_manager import config_manager
from dao import SimulatedAlphasDAO

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
        
    def fix_newline_expression(self, expression):
        """
        修复表达式中的";n"问题（应该是";\n"被错误转换）
        
        Args:
            expression (str): 输入表达式字符串
            
        Returns:
            str: 修复后的表达式（如果发现问题），否则返回原表达式
        """
        # 检查是否存在";n"问题
        if ";n" in expression:
            # 替换所有";n"为";\n"
            fixed_expression = expression.replace(";n", ";\n")
            return fixed_expression
        return expression

    def negate_expression(self, expression):
        """
        为表达式添加负号
        
        Args:
            expression (str): 输入表达式字符串
            
        Returns:
            str: 处理后的表达式
                如果是单行表达式，直接在前面加负号
                如果是多行表达式，在最后一行前加负号
        """
        if "\n" not in expression:
            # 单行表达式
            return f"-({expression})"
        else:
            # 多行表达式
            lines = expression.split("\n")
            last_line = lines[-1].strip()
            # 在最后一行前加负号
            lines[-1] = f"-({last_line})"
            return "\n".join(lines)
        
    def get_alphas_from_data(self, data_rows, min_sharpe, min_fitness, mode="track", region_filter=None, single_data_set_filter=None):
        """
        Process database data to generate alpha records in the format:
        [alpha_id, exp, sharpe, turnover, fitness, margin, dateCreated, decay]
        
        Args:
            data_rows: List of database rows containing alpha data
            min_sharpe: Minimum sharpe ratio threshold (e.g. 1.0)
            min_fitness: Minimum fitness threshold (e.g. 0.5)
            mode: Filter mode - 'submit' or 'track' (default: 'track')
            region_filter: Optional region filter (e.g. "USA"). If None, no region filtering.
            single_data_set_filter: Optional boolean to filter Single Data Set Alphas. If None, no filtering.
        
        Returns:
            List of filtered alpha records

        Examples:
            # Basic usage - filter by sharpe and fitness only
            results = get_alphas_from_data(data_rows, 1.0, 0.5)
            
            # Filter for USA region only
            results = get_alphas_from_data(data_rows, 1.0, 0.5, 
                                        region_filter="USA")
                                        
            # Filter for Single Data Set Alphas only
            results = get_alphas_from_data(data_rows, 1.0, 0.5,
                                        single_data_set_filter=True)
                                        
            # Combined filter - USA region and Single Data Set Alphas
            results = get_alphas_from_data(data_rows, 1.0, 0.5,
                                        region_filter="USA", single_data_set_filter=True)
        """
        output = []
        
        for row in data_rows:
                try:
                    # Parse the necessary fields from the row
                    alpha_id = row['id']
                    
                    # Parse the settings dictionary
                    settings = ast.literal_eval(row['settings'])
                    decay = settings.get('decay', 0)
                    
                    # Parse the regular code (expression)
                    regular = ast.literal_eval(row['regular'])
                    exp = self.fix_newline_expression(regular.get('code', ''))
                    operatorCount = regular.get('operatorCount', 0)
                    
                    # Parse the is dictionary
                    is_data = ast.literal_eval(row['is'])
                    sharpe = is_data.get('sharpe', 0)
                    fitness = is_data.get('fitness', 0)
                    turnover = is_data.get('turnover', 0)
                    margin = is_data.get('margin', 0)
                    longCount = is_data.get('longCount', 0)
                    shortCount = is_data.get('shortCount', 0)
                    checks = is_data.get('checks', [])
                    
                    dateCreated = row['dateCreated']
                    has_failed_checks = any(check['result'] == 'FAIL' for check in checks)
                    # Apply region filter if specified
                    if region_filter is not None and settings.get('region') != region_filter:
                        continue
                        
                    # Apply single data set filter if specified
                    if single_data_set_filter is not None:
                        classifications = ast.literal_eval(row.get('classifications', '[]'))
                        is_single_data_set = any(
                            classification.get('id') == 'DATA_USAGE:SINGLE_DATA_SET' 
                            for classification in classifications
                        )
                        if single_data_set_filter != is_single_data_set:
                            continue
                        elif operatorCount > 8:
                            continue
                    
                    # Apply other filters
                    if (longCount + shortCount) > 100:
                        if mode == "submit":
                            if (sharpe >= min_sharpe and fitness >= min_fitness) and not has_failed_checks:
                                if sharpe is not None and sharpe < 0:
                                    exp = self.negate_expression(exp)
                                # Create the record
                                rec = [
                                    alpha_id,
                                    exp,
                                    sharpe,
                                    turnover,
                                    fitness,
                                    margin,
                                    dateCreated,
                                    decay
                                ]
                                
                                # Extract pyramids info from checks if available
                                pyramid_checks = [check for check in checks if check.get('name') == 'MATCHES_PYRAMID']
                                if pyramid_checks and 'pyramids' in pyramid_checks[0]:
                                    pyramids = pyramid_checks[0]['pyramids']
                                    rec.insert(7, pyramids)  # Insert after dateCreated
                                
                                # Add additional decay modifier based on turnover
                                if turnover > 0.7:
                                    rec.append(decay * 4)
                                elif turnover > 0.6:
                                    rec.append(decay * 3 + 3)
                                elif turnover > 0.5:
                                    rec.append(decay * 3)
                                elif turnover > 0.4:
                                    rec.append(decay * 2)
                                elif turnover > 0.35:
                                    rec.append(decay + 4)
                                elif turnover > 0.3:
                                    rec.append(decay + 2)
                                
                                output.append(rec)
                        else:  # track mode
                            if (sharpe >= min_sharpe and fitness >= min_fitness) or (sharpe <= min_sharpe * -1.0 and fitness <= min_fitness * -1.0):
                                if sharpe is not None and sharpe < 0:
                                    exp = self.negate_expression(exp)
                                # Create the record
                                rec = [
                                    alpha_id,
                                    exp,
                                    sharpe,
                                    turnover,
                                    fitness,
                                    margin,
                                    dateCreated,
                                    decay
                                ]
                                
                                # Extract pyramids info from checks if available
                                pyramid_checks = [check for check in checks if check.get('name') == 'MATCHES_PYRAMID']
                                if pyramid_checks and 'pyramids' in pyramid_checks[0]:
                                    pyramids = pyramid_checks[0]['pyramids']
                                    rec.insert(7, pyramids)  # Insert after dateCreated
                                
                                # Add additional decay modifier based on turnover
                                if turnover > 0.7:
                                    rec.append(decay * 4)
                                elif turnover > 0.6:
                                    rec.append(decay * 3 + 3)
                                elif turnover > 0.5:
                                    rec.append(decay * 3)
                                elif turnover > 0.4:
                                    rec.append(decay * 2)
                                elif turnover > 0.35:
                                    rec.append(decay + 4)
                                elif turnover > 0.3:
                                    rec.append(decay + 2)
                                
                                output.append(rec)
                        
                except (ValueError, SyntaxError, KeyError) as e:
                    # Skip rows with parsing errors or missing required fields
                    continue
                    
        return output

    def get_alphas_by_datetime(self, datetime_str, per_page=1000):
        """
        根据日期时间获取原始alpha数据行（自动分页获取所有数据）
        
        参数:
            datetime_str: 日期时间字符串，格式为'%Y%m%d'
            per_page: 每页数量（默认为1000）
            
        返回:
            原始alpha数据行列表（包含所有分页的数据）
        """
        # 获取总记录数
        total = self.simulated_alphas_dao.count_by_datetime(datetime_str)
        all_data_rows = []
        offset = 0
        
        # 分页获取所有数据
        while offset < total:
            # 获取当前页的数据
            data_rows = self.simulated_alphas_dao.get_by_datetime_paginated(
                datetime_str, 
                limit=per_page, 
                offset=offset
            )
            all_data_rows.extend(data_rows)
            
            # 更新offset以获取下一页数据
            offset += per_page
        
        return all_data_rows
    
    def generate_ppac_gold_bags(self, date, keywords_to_exclude, os_alpha_ids, os_alpha_rets):
        regions = ['USA', 'CHN', 'GLB', 'EUR', 'ASI']
        stone_bag_atom = {}
        stone_bag_hp = {}
        ppa_tracker_atom_all = []
        ppa_tracker_hp_all = []

        # 获取指定日期的所有数据
        data_rows = self.get_alphas_by_datetime(date)

        # 先生成 stone_bag_atom 和 stone_bag_hp
        for region in regions:
            # atom 筛选
            ppa_tracker_atom = self.get_alphas_from_data(
                data_rows, 
                min_sharpe=1.0, 
                min_fitness=0.3, 
                mode="submit", 
                region_filter=region, 
                single_data_set_filter=True
            )
            ppa_tracker_atom_all.extend(ppa_tracker_atom)
            region_bag_atom = [
                alpha[0] for alpha in ppa_tracker_atom 
                if not any(keyword in alpha[1] for keyword in keywords_to_exclude)
            ]
            stone_bag_atom[region] = region_bag_atom

            # hp 筛选
            ppa_tracker_hp = self.get_alphas_from_data(
                data_rows, 
                min_sharpe=1.2, 
                min_fitness=1.0, 
                mode="submit", 
                region_filter=region, 
                single_data_set_filter=None
            )
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

    datetimes = ['20250814', '20250815', '20250816', '20250817']
    
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
