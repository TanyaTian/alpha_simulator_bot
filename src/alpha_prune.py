import pandas as pd
from typing import Dict, List, Optional, Tuple
from copy import deepcopy
from self_corr_calculator import sign_in, get_alpha_pnls, calc_self_corr
from logger import Logger

# 创建 Logger 实例
logger = Logger()

def generate_comparison_data(alpha_result: List[Dict], username: str, password: str) -> Tuple[Dict[str, List], pd.DataFrame]:
    """
    生成os_alpha_ids和os_alpha_rets用于后续相关性计算
    
    参数:
        alpha_result (List[Dict]): 筛选后的alpha元数据列表
        username (str): WorldQuant Brain API用户名
        password (str): WorldQuant Brain API密码
        
    返回:
        Tuple[Dict[str, List], pd.DataFrame]: 
            - os_alpha_ids: 按区域分组的alpha ID字典
            - os_alpha_rets: 包含alpha日收益率的DataFrame
    """
    # 初始化os_alpha_ids，保持alpha_result的顺序
    os_alpha_ids = {}
    for alpha in alpha_result:
        region = alpha['settings']['region']
        if region not in os_alpha_ids:
            os_alpha_ids[region] = []
        if alpha['id'] not in os_alpha_ids[region]:  # 避免重复
            os_alpha_ids[region].append(alpha['id'])

    # 获取所有alpha的盈亏数据
    sess = sign_in(username, password)
    if not sess:
        raise ValueError("Failed to sign in, cannot generate comparison data")
    
    _, os_alpha_rets = get_alpha_pnls(
        alphas=alpha_result, 
        sess=sess, 
        username=username, 
        password=password
    )
    
    # 转换为日收益率
    os_alpha_rets = os_alpha_rets - os_alpha_rets.ffill().shift(1)
    # 筛选过去4年的数据
    os_alpha_rets = os_alpha_rets[pd.to_datetime(os_alpha_rets.index) >
                                pd.to_datetime(os_alpha_rets.index).max() - pd.DateOffset(years=4)]

    # 检查os_alpha_rets是否包含所有os_alpha_ids的数据
    missing_alpha_ids = {}
    for region, alpha_ids in os_alpha_ids.items():
        missing_alpha_ids[region] = [alpha_id for alpha_id in alpha_ids if alpha_id not in os_alpha_rets.columns]
        if missing_alpha_ids[region]:
            logger.warning(f"[alpha_prune.py] Missing data in os_alpha_rets for region {region}: {missing_alpha_ids[region]}")

    # 对缺失的alpha_id重试查询
    for region, alpha_ids in missing_alpha_ids.items():
        if alpha_ids:
            logger.info(f"[alpha_prune.py] Retrying to fetch data for missing alpha IDs in region {region}: {alpha_ids}")
            try:
                # 筛选出缺失的alpha元数据
                retry_alphas = [alpha for alpha in alpha_result if alpha['id'] in alpha_ids and alpha['settings']['region'] == region]
                if retry_alphas:
                    sess = sign_in(username, password)
                    _, retry_rets = get_alpha_pnls(retry_alphas, sess, username, password)
                    # 转换为日收益率
                    retry_rets = retry_rets - retry_rets.ffill().shift(1)
                    # 筛选过去4年的数据
                    retry_rets = retry_rets[pd.to_datetime(retry_rets.index) >
                                        pd.to_datetime(retry_rets.index).max() - pd.DateOffset(years=4)]
                    # 合并到os_alpha_rets
                    os_alpha_rets = pd.concat([os_alpha_rets, retry_rets], axis=1)
                    logger.info(f"[alpha_prune.py] Successfully fetched retry data for region {region}: {retry_rets.columns.tolist()}")
            except Exception as e:
                logger.error(f"[alpha_prune.py] Failed to retry fetching data for region {region}: {e}")

    # 再次检查，删除仍缺失的alpha_id
    for region, alpha_ids in os_alpha_ids.copy().items():  # 使用copy避免修改时迭代
        still_missing = [alpha_id for alpha_id in alpha_ids if alpha_id not in os_alpha_rets.columns]
        if still_missing:
            logger.warning(f"[alpha_prune.py] Removing alpha IDs from os_alpha_ids in region {region} due to missing data: {still_missing}")
            os_alpha_ids[region] = [alpha_id for alpha_id in alpha_ids if alpha_id not in still_missing]
            if not os_alpha_ids[region]:  # 如果区域为空，删除区域
                del os_alpha_ids[region]

    logger.info(f"[alpha_prune.py] Comparison data generated, regions: {list(os_alpha_ids.keys())}")
    return os_alpha_ids, os_alpha_rets

def calculate_correlations(
    os_alpha_ids: Dict[str, List], 
    os_alpha_rets: pd.DataFrame, 
    username: str, 
    password: str,
    corr_threshold: float = 0.7
) -> Dict[str, List[Dict]]:
    """
    计算alpha相关性并筛选
    
    参数:
        os_alpha_ids (Dict[str, List]): 按区域分组的alpha ID字典
        os_alpha_rets (pd.DataFrame): 包含alpha日收益率的DataFrame
        username (str): WorldQuant Brain API用户名
        password (str): WorldQuant Brain API密码
        corr_threshold (float): 相关性阈值，保留相关性小于此值的alpha
        
    返回:
        Dict[str, List[Dict]]: 按区域分组的筛选后alpha列表
    """
    filtered_alphas = {}
    temp_alpha_ids = deepcopy(os_alpha_ids)  # 使用临时副本进行操作
    sess = sign_in(username, password)
    if not sess:
        raise ValueError("Failed to sign in, cannot calculate correlations")

    for region in temp_alpha_ids:
        alpha_count = len(temp_alpha_ids[region])
        filtered_alphas[region] = []
        
        logger.info(f"[alpha_prune.py] Start processing region: {region}, total {alpha_count} alphas")

        idx = 0
        while temp_alpha_ids[region]:
            alpha_id = temp_alpha_ids[region][0]
            idx += 1
            logger.info(f"[alpha_prune.py] [{region}] Processing alpha {idx}/{alpha_count}: {alpha_id}")
            
            try:
                max_corr = calc_self_corr(
                    alpha_id=alpha_id,
                    sess=sess,
                    os_alpha_rets=os_alpha_rets,
                    os_alpha_ids={region: [id for id in temp_alpha_ids[region] if id != alpha_id]},
                    return_alpha_pnls=False
                )
                logger.info(f"[alpha_prune.py] Alpha {alpha_id} (region: {region}) max correlation: {max_corr}")
                
                if max_corr < corr_threshold and not (max_corr == 0.0 and len(temp_alpha_ids[region]) > 1):
                    filtered_alphas[region].append(alpha_id)
                    logger.info(f"[alpha_prune.py] Alpha {alpha_id} passed filter (correlation {max_corr} < {corr_threshold})")
                else:
                    logger.info(f"[alpha_prune.py] Alpha {alpha_id} filtered out (correlation {max_corr} >= {corr_threshold} or (max_corr=0.0 and region has {len(temp_alpha_ids[region])} alphas))")
            except Exception as e:
                logger.error(f"[alpha_prune.py] Failed to calculate correlation for alpha {alpha_id}: {e}")

            temp_alpha_ids[region].pop(0)

        logger.info(f"[alpha_prune.py]Finished region: {region}, selected {len(filtered_alphas[region])}/{alpha_count} alphas")

    return filtered_alphas
