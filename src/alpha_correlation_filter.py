import pandas as pd
from typing import List, Dict, Optional
from collections import defaultdict
from self_corr_calculator import calc_self_corr, get_alpha_pnls
from logger import Logger
from config_manager import config_manager
from ace_lib import check_session_and_relogin

class AlphaGroupCorrelationFilter:
    def __init__(self):
        self.logger = Logger()
        self.sess = config_manager.get_session()
        self.username = config_manager.get('username', '')
        self.password = config_manager.get('password', '')

    def filter_alphas_by_correlation(self, alpha_ids: List[str], correlation_threshold: float = 0.7) -> List[str]:
        """
        根据相关性筛选 alpha ID 组合。
        
        1. 获取每个 alpha 的 fitness。
        2. 按 fitness 从大到小排序。
        3. 贪心选择：调用 calc_self_corr 计算与当前组合的相关性。
        """
        if not alpha_ids:
            self.logger.warning("Empty alpha_ids list provided.")
            return []

        self.logger.info(f"Starting correlation filtering for {len(alpha_ids)} alphas with threshold {correlation_threshold}")
        
        # 步骤 1: 获取 alpha 详情 (fitness 和 region)
        alpha_data = []
        total = len(alpha_ids)
        self.logger.info(f"Fetching details for {total} alphas...")
        
        for idx, alpha_id in enumerate(alpha_ids, 1):
            try:
                self.sess = check_session_and_relogin(self.sess)
                url = f"https://api.worldquantbrain.com/alphas/{alpha_id}"
                resp = self.sess.get(url)
                resp.raise_for_status()
                data = resp.json()
                
                fitness = data.get('is', {}).get('fitness', 0.0)
                region = data.get('settings', {}).get('region', 'USA')
                
                alpha_data.append({
                    'id': alpha_id,
                    'fitness': fitness,
                    'region': region,
                    'raw_data': data
                })
                
                if idx % 10 == 0 or idx == total:
                    self.logger.info(f"Fetched {idx}/{total} alpha details")
            except Exception as e:
                self.logger.error(f"Failed to fetch details for alpha {alpha_id}: {e}")

        # 步骤 2: 按 fitness 从大到小排序
        alpha_data.sort(key=lambda x: x['fitness'], reverse=True)
        self.logger.info(f"Sorted {len(alpha_data)} alphas by fitness (descending)")

        # 步骤 3: 预先获取所有 alpha 的收益率数据，提高效率
        self.logger.info("Pre-fetching returns data for the entire group...")
        try:
            # 构造 get_alpha_pnls 所需的输入格式
            pnl_input = [{'id': x['id'], 'settings': {'region': x['region']}} for x in alpha_data]
            
            # 使用封装好的 get_alpha_pnls
            _, alpha_pnls = get_alpha_pnls(
                pnl_input, 
                self.sess, 
                username=self.username, 
                password=self.password
            )
            
            # 计算收益率 (Returns)，参考 load_data 中的逻辑
            alpha_rets_all = alpha_pnls - alpha_pnls.ffill().shift(1)
            
            # 过滤最近 4 年数据
            max_date = pd.to_datetime(alpha_rets_all.index).max()
            if not pd.isna(max_date):
                alpha_rets_all = alpha_rets_all[pd.to_datetime(alpha_rets_all.index) > max_date - pd.DateOffset(years=4)]
            
            self.logger.info(f"Returns data ready for {len(alpha_rets_all.columns)} alphas")
        except Exception as e:
            self.logger.error(f"Failed to fetch returns data: {e}")
            return []

        # 步骤 4: 贪心筛选
        selected_ids_by_region = defaultdict(list)
        final_selected_ids = []
        discarded_count = 0
        
        self.logger.info("Starting greedy selection using calc_self_corr...")
        for alpha_item in alpha_data:
            alpha_id = alpha_item['id']
            region = alpha_item['region']
            
            if alpha_id not in alpha_rets_all.columns:
                self.logger.warning(f"Returns data missing for {alpha_id}, skipping")
                continue

            # 获取该 alpha 的 PnL 数据（Series 格式）
            current_pnl = alpha_pnls[alpha_id]

            # 调用封装好的 calc_self_corr
            # 将当前已选中的 alphas 作为组合传入 (selected_ids_by_region)
            try:
                # 重新验证 session
                self.sess = check_session_and_relogin(self.sess)
                
                self_corr = calc_self_corr(
                    alpha_id=alpha_id,
                    sess=self.sess,
                    os_alpha_rets=alpha_rets_all,
                    os_alpha_ids=selected_ids_by_region,
                    alpha_result=alpha_item['raw_data'],
                    alpha_pnls=current_pnl
                )
            except Exception as e:
                self.logger.error(f"Error calling calc_self_corr for {alpha_id}: {e}")
                self_corr = 1.0  # 发生错误则视为高相关性淘汰

            if self_corr < correlation_threshold:
                selected_ids_by_region[region].append(alpha_id)
                final_selected_ids.append(alpha_id)
                self.logger.info(f"Selected: {alpha_id} | fitness: {alpha_item['fitness']:.4f} | self_corr: {self_corr:.4f}")
            else:
                discarded_count += 1
                self.logger.info(f"Discarded: {alpha_id} | fitness: {alpha_item['fitness']:.4f} | self_corr: {self_corr:.4f} > {correlation_threshold}")

        # 打印最终统计日志
        self.logger.info("=" * 50)
        self.logger.info(f"Correlation Filtering Results Summary")
        self.logger.info(f"Threshold: {correlation_threshold}")
        self.logger.info(f"Total Input: {len(alpha_ids)}")
        self.logger.info(f"Discarded: {discarded_count}")
        self.logger.info(f"Remaining: {len(final_selected_ids)}")
        self.logger.info("=" * 50)
        
        return final_selected_ids

def run_filter(alpha_ids: List[str], threshold: float = 0.7) -> List[str]:
    """快捷调用工具"""
    filter_tool = AlphaGroupCorrelationFilter()
    return filter_tool.filter_alphas_by_correlation(alpha_ids, threshold)

if __name__ == "__main__":
    # 配置需要筛选的 alpha ID 列表
    test_ids = [
        "vRdeo7PG",
        "2rJaVdw8",
        "E5kqvapL",
        "rKAbJ5wo",
        "QPEn5xPr",
        "E5kkKnJ9",
        "vRddm7gA",
        "2rJJKVdN",
        "lerr0wX2"
    ]
    # 配置相关性阈值
    threshold = 0.5
    
    # 直接运行处理
    alpha_list = run_filter(test_ids, threshold)
    print(alpha_list)
