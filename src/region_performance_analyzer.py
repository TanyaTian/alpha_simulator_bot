import pandas as pd
import requests
import time
import logging
from typing import List, Dict
import os
import sys

# Add src to path if needed
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from ace_lib import start_session, check_session_and_relogin

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def login():
    """
    使用 ace_lib 的 start_session 进行登录
    """
    return start_session()


def fetch_submitted_alphas(
        session: requests.Session,
        start_date: str,
        end_date: str,
        max_offset: int = 9900  # 最大偏移量限制
) -> List[Dict]:
    """
    拉取指定日期范围内提交的Alpha信息（基于count自动分页）

    参数:
        session: 已登录的requests会话对象
        start_date/end_date: 日期范围（格式：YYYY-MM-DD）
        max_offset: 最大偏移量（防止无限循环）

    返回:
        符合条件的Alpha信息列表
    """
    alpha_list = []
    offset = 0  # 起始偏移量

    while True:
        # 检查并重新登录（如果需要）
        session = check_session_and_relogin(session)
        
        # 构建请求URL（恢复原始筛选条件，移除sharpe等额外筛选）
        # 注意：这里使用的是 api.worldquantbrain.com
        url = (
            f"https://api.worldquantbrain.com/users/self/alphas?limit=100&offset={offset}"
            "&status!=UNSUBMITTED%1FIS_FAIL"
            f"&dateSubmitted%3E={start_date}T00:00:00-04:00"
            f"&dateSubmitted%3C={end_date}T00:00:00-04:00"
            "&order=-is.sharpe&hidden=false&type!=SUPER"
        )

        # 发送请求
        response = session.get(url)

        try:
            logger.info(f"当前偏移量: {offset}")
            # 解析响应数据
            response_data = response.json()
            
            if response.status_code != 200:
                logger.error(f"请求失败: {response.status_code} - {response.text}")
                raise Exception(f"API Error: {response.status_code}")

            total_count = response_data.get("count", 0)
            logger.info(f"符合条件的总数量: {total_count}")

            # 提取当前页结果
            if "results" in response_data:
                alpha_list.extend(response_data["results"])
                logger.info(f"累计获取: {len(alpha_list)} 条Alpha")

            # 判断终止条件：偏移量超过总数或达到最大限制
            offset += 100
            if offset >= total_count or offset > max_offset:
                logger.info("分页拉取完成")
                break

            # 避免请求过于频繁
            time.sleep(1)

        except Exception as e:
            logger.error(f"拉取失败: {str(e)}")
            # 回退偏移量并重试
            offset -= 100
            if offset < 0:
                offset = 0
            logger.info("等待10秒后重试...")
            time.sleep(10)
            # 重新登录获取会话
            session = login()

    return alpha_list


def calculate_region_statistics(alpha_records: List[Dict]) -> pd.DataFrame:
    """按地区统计Alpha的关键指标"""
    if not alpha_records:
        return pd.DataFrame()
        
    analysis_data = []
    for alpha in alpha_records:
        # 提取地区信息
        region = alpha.get("settings", {}).get("region", "UNKNOWN")
        # 提取IS指标
        is_metrics = alpha.get("is", {})

        analysis_data.append({
            "region": region,
            "is_sharpe": is_metrics.get("sharpe"),
            "is_fitness": is_metrics.get("fitness"),
            "is_margin": is_metrics.get("margin")
        })

    # 分组统计
    alpha_df = pd.DataFrame(analysis_data)
    # 确保列是数值类型
    for col in ["is_sharpe", "is_fitness", "is_margin"]:
        alpha_df[col] = pd.to_numeric(alpha_df[col], errors='coerce')
        
    return alpha_df.groupby("region").agg(
        alpha_count=("region", "count"),
        avg_is_sharpe=("is_sharpe", "mean"),
        avg_is_fitness=("is_fitness", "mean"),
        avg_is_margin=("is_margin", "mean")
    ).reset_index()


if __name__ == "__main__":
    from datetime import datetime
    
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = "2025-03-27"
    
    logger.info(f"开始拉取日期范围: {start_date} 至 {end_date}")
    
    try:
        # 1. 登录
        sess = login()
        
        # 2. 拉取数据
        alphas = fetch_submitted_alphas(sess, start_date, end_date)
        
        if alphas:
            # 3. 计算统计数据
            stats_df = calculate_region_statistics(alphas)
            
            # 4. 输出结果
            logger.info("\n各地区Alpha表现统计结果:")
            print(stats_df.to_string(index=False))
            
            # 可选：保存到文件
            output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../output/region_stats.csv")
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            stats_df.to_csv(output_path, index=False)
            logger.info(f"统计结果已保存至: {output_path}")
        else:
            logger.warning("未找到符合条件的Alpha记录")
            
    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")
