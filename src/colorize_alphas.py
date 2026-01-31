# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from collections import Counter
import random
from typing import List, Dict, Any, Optional

from config_manager import config_manager
from logger import Logger

def update_alpha_color(s: Any, alpha_id: str, color: Optional[str], logger: Logger) -> str:
    """
    一个专门用于修改alpha颜色的函数。
    :param s: The session object for API calls.
    :param alpha_id: The ID of the alpha to modify.
    :param color: The color to set. None to clear the color.
    :param logger: The logger instance.
    :return: The color that was set, or "FAILED" if it failed.
    """
    params = {"color": color}
    display_color = "无" if color is None else color
    
    try:
        response = s.patch(
            f"https://api.worldquantbrain.com/alphas/{alpha_id}", json=params
        )
        response.raise_for_status()
        
        logger.info(f"成功将 Alpha {alpha_id} 的颜色修改为 '{display_color}'。")
        return color
    except Exception as e:
        logger.error(f"修改 Alpha {alpha_id} 颜色失败: {e}")
        return "FAILED"

def get_submitted_alphas(s: Any, start_date: str, end_date: str, region: str, logger: Logger, alpha_num_limit: int = 5000) -> List[Dict]:
    """
    获取指定日期范围和区域内提交的常规 Alpha。
    """
    output = []
    logger.info(f"开始获取区域 {region} 从 {start_date} 到 {end_date} 的常规 Alpha...")

    for i in range(0, alpha_num_limit, 100):
        logger.info(f"正在获取第 {i} 到 {i + 100} 个 alpha...")
        url_e = (
            f"https://api.worldquantbrain.com/users/self/alphas?limit=100&offset={i}"
            f"&status!=UNSUBMITTED&status!=IS_FAIL"
            f"&dateSubmitted>={start_date}T00:00:00-04:00"
            f"&dateSubmitted<{end_date}T00:00:00-04:00"
            f"&order=-is.sharpe&hidden=false&type!=SUPER"
            f"&settings.delay=1&&settings.region={region}"
        )
        try:
            response = s.get(url_e)
            response.raise_for_status()
            alpha_list = response.json().get("results", [])

            if not alpha_list:
                logger.info("已获取所有符合条件的 alpha。")
                break

            for alpha in alpha_list:
                rec = {
                    "id": alpha["id"],
                    "region": alpha["settings"]["region"],
                    "name": alpha.get("name"),
                    "color": alpha.get("color"),
                    "dateSubmitted": alpha['dateSubmitted']
                }
                output.append(rec)
                
        except Exception as e:
            logger.error(f"获取alpha时发生异常: {e}")
            break

    logger.info(f"总共获取了 {len(output)} 个符合条件的 {region} Alpha。")
    return output

def main():
    """
    脚本主入口点。
    """
    logger = Logger()
    logger.info("--- 启动 Alpha 染色脚本 ---")

    # --- 参数配置 ---
    # 1. 成为顾问的日期，也是 Alpha 开始计算收益的日期
    start_date_str = "2025-03-27"
    # 2. 您想要操作的目标区域列表
    target_regions = ["USA", "EUR", "ASI", "GLB", "IND"]
    # 3. 用于分配的颜色列表 (None 代表清除颜色)
    colors_to_assign = [None, "RED", "YELLOW", "GREEN", "BLUE", "PURPLE"]
    # 4. 并发修改的线程数
    max_workers = 10
    
    end_date_obj = datetime.now() + timedelta(days=1)
    end_date_str = end_date_obj.strftime("%Y-%m-%d")

    logger.info("-" * 40)
    logger.info("配置信息:")
    logger.info(f"脚本起始日期: {start_date_str}")
    logger.info(f"脚本截止日期: {end_date_str}")
    logger.info(f"目标区域列表: {target_regions}")
    logger.info(f"待分配颜色: {['无' if c is None else c for c in colors_to_assign]}")
    logger.info("-" * 40)

    # --- 获取 Session ---
    sess = config_manager.get_session()
    if not sess:
        logger.error("无法获取 session，脚本退出。")
        return

    # 循环处理每个区域
    for target_region in target_regions:
        logger.info(f"\n{'#' * 60}")
        logger.info(f"开始处理区域: {target_region}")
        logger.info(f"{'#' * 60}")
        
        # --- 获取 Alphas ---
        alphas_to_color = get_submitted_alphas(sess, start_date_str, end_date_str, target_region, logger)

        if not alphas_to_color:
            logger.warning(f"在指定时间范围和区域 {target_region} 内未找到任何 Alpha，跳过该区域。")
            continue

        # --- 随机化并分配颜色 ---
        logger.info(f"找到 {len(alphas_to_color)} 个 Alpha，准备开始随机均衡分配颜色...")

        random.shuffle(alphas_to_color)
        logger.info("Alpha 列表已随机打乱。")

        tasks = []
        color_assignments = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for i, alpha_data in enumerate(alphas_to_color):
                alpha_id = alpha_data["id"]
                target_color = colors_to_assign[i % len(colors_to_assign)]
                color_assignments.append(target_color)
                tasks.append(executor.submit(update_alpha_color, sess, alpha_id, target_color, logger))

        results = [task.result() for task in tasks]

        # --- 打印区域总结报告 ---
        logger.info("\n" + "=" * 50)
        logger.info(f"区域 {target_region} 颜色分配任务已完成。")

        planned_counts = Counter(color_assignments)
        logger.info("\n计划分配的颜色统计:")
        for color, count in planned_counts.items():
            display_color = "无" if color is None else color
            logger.info(f"- {display_color}: {count} 个")

        success_counts = Counter(res for res in results if res != "FAILED")
        failed_count = results.count("FAILED")

        logger.info("\n实际成功分配的颜色统计:")
        for color, count in success_counts.items():
            display_color = "无" if color is None else color
            logger.info(f"- {display_color}: {count} 个")

        if failed_count > 0:
            logger.warning(f"\n失败任务总数: {failed_count} 个")

        logger.info(f"\n区域 {target_region} 处理完毕。")
        logger.info("=" * 50)

    logger.info("\n" + "=" * 50)
    logger.info("所有区域处理完毕，脚本执行完成。")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()
