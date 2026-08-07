# -*- coding: utf-8 -*-
"""
Alpha 时间标签脚本

为已提交的 Alpha 批量添加时间维度标签 (era tag)。
每个 Alpha 获得 3 个时间粒度的标签：年、半年、季度。

处理逻辑（保证不破坏已有标签）：
  1. 获取指定时间范围内已提交的 Alpha 列表
  2. 对每个 Alpha，先 GET 其详情，检查当前 tags 中是否已有 era:* 标签
  3. 若已有 era 标签 → 跳过（避免重复打标）
  4. 若没有 era 标签 → 保留现有标签，追加 era 标签，通过 PATCH 写入
  5. 全程单线程顺序处理，每次 API 调用间隔 1 秒，避免触发限流

示例：
  alpha (dateSubmitted=2025-03-15, tags=["StageOne_pv_20250101"])
    → tags 变为: ["StageOne_pv_20250101", "era:2025", "era:2025H1", "era:2025Q1"]

用法：
  # 预览模式（不修改任何数据）
  python tag_alphas.py --start 2025-03-27 --end 2026-07-12 --dry-run

  # 指定区域执行
  python tag_alphas.py --start 2025-03-27 --end 2026-07-12 --regions USA EUR
"""

import argparse
import time
from collections import Counter
from datetime import datetime, timedelta
from typing import Dict, List

import utils
from config_manager import config_manager
from logger import Logger


# =============================================================================
# 核心时间标签计算
# =============================================================================


def compute_era_tags(date_submitted_str: str) -> List[str]:
    """
    根据 dateSubmitted ISO 字符串，生成三个时间粒度的 era 标签。

    Args:
        date_submitted_str: ISO 8601 格式的时间字符串
            例如 "2025-03-15T10:30:00-04:00"

    Returns:
        ["era:2025", "era:2025H1", "era:2025Q1"]
    """
    dt = datetime.fromisoformat(date_submitted_str)
    year = dt.year
    month = dt.month

    quarter = (month - 1) // 3 + 1
    half = "H1" if month <= 6 else "H2"

    return [
        f"era:{year}",
        f"era:{year}{half}",
        f"era:{year}Q{quarter}",
    ]


# =============================================================================
# API 数据获取
# =============================================================================


def fetch_submitted_alphas(
    session,
    start_date: str,
    end_date: str,
    region: str,
    logger: Logger,
    alpha_limit: int = 10000,
) -> List[Dict]:
    """
    获取指定日期范围和区域内已提交的常规 Alpha。

    与 colorize_alphas.py 中 get_submitted_alphas 保持一致的 API 调用方式。
    """
    output: List[Dict] = []
    logger.info(f"获取区域 {region}: {start_date} ~ {end_date}")

    for i in range(0, alpha_limit, 100):
        logger.info(f"  正在获取第 {i} 到 {i + 100} 个 alpha...")
        url_e = (
            f"https://api.worldquantbrain.com/users/self/alphas?limit=100&offset={i}"
            f"&status!=UNSUBMITTED&status!=IS_FAIL"
            f"&dateSubmitted>={start_date}T00:00:00-04:00"
            f"&dateSubmitted<{end_date}T00:00:00-04:00"
            f"&order=-is.sharpe&hidden=false&type!=SUPER"
            f"&settings.delay=1&&settings.region={region}"
        )
        try:
            resp = session.get(url_e)
            resp.raise_for_status()
            alpha_list = resp.json().get("results", [])

            if not alpha_list:
                logger.info("已获取所有符合条件的 Alpha。")
                break

            for alpha in alpha_list:
                output.append({
                    "id": alpha["id"],
                    "region": alpha["settings"]["region"],
                    "name": alpha.get("name"),
                    "dateSubmitted": alpha.get("dateSubmitted", ""),
                })

        except Exception as e:
            logger.error(f"获取 Alpha 失败 (offset={i}): {e}")
            break

    logger.info(f"区域 {region} 共获取 {len(output)} 个已提交 Alpha。")
    return output


def get_alpha_tags(session, alpha_id: str, logger: Logger) -> List[str]:
    """
    获取单个 Alpha 的当前 tags。
    若获取失败则返回空列表（保守策略：不破坏已有标签，宁可少加）。
    """
    try:
        resp = session.get(
            f"https://api.worldquantbrain.com/alphas/{alpha_id}"
        )
        resp.raise_for_status()
        return resp.json().get("tags", [])
    except Exception as e:
        logger.warning(f"获取 Alpha {alpha_id} 详情失败: {e}")
        return []


# =============================================================================
# API 写入（带重试）
# =============================================================================


def _patch_alpha_tags(session, alpha_id: str, tags: List[str]):
    """
    PATCH 单个 Alpha 的 tags 字段。仅发送 {"tags": [...]}。
    用 response.raise_for_status() 让 safe_api_call 捕获错误并重试。
    """
    resp = session.patch(
        f"https://api.worldquantbrain.com/alphas/{alpha_id}",
        json={"tags": tags},
    )
    resp.raise_for_status()
    return resp


# =============================================================================
# 主入口
# =============================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Alpha 时间标签脚本 — 为已提交 Alpha 批量添加 era 时间标签",
    )
    parser.add_argument(
        "--start", type=str, required=True,
        help="起始日期 YYYY-MM-DD（包含该日）",
    )
    parser.add_argument(
        "--end", type=str, required=True,
        help="结束日期 YYYY-MM-DD（包含该日）",
    )
    parser.add_argument(
        "--regions", type=str, nargs="+",
        default=["USA", "EUR", "ASI", "GLB", "IND"],
        help="目标区域列表（默认: USA EUR ASI GLB IND）",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="仅预览，不实际修改 Alpha 标签",
    )
    args = parser.parse_args()

    logger = Logger(log_name="tag_alphas", log_dir="logs/")
    logger.info("=" * 60)
    logger.info("--- Alpha 时间标签 (era tag) 脚本 ---")
    logger.info(f"时间范围: {args.start} ~ {args.end} (含起止日期)")
    logger.info(f"目标区域: {args.regions}")
    if args.dry_run:
        logger.info("模式: DRY RUN（仅预览，不修改数据）")
    logger.info("=" * 60)

    # API 传入的 end_date 需要 +1 天（API 用 < 过滤）
    start_date_dt = datetime.strptime(args.start, "%Y-%m-%d")
    end_date_dt = datetime.strptime(args.end, "%Y-%m-%d") + timedelta(days=1)
    api_start = start_date_dt.strftime("%Y-%m-%d")
    api_end = end_date_dt.strftime("%Y-%m-%d")

    # 获取 session
    session = config_manager.get_session()
    if not session:
        logger.error("无法获取 BRAIN session，脚本退出。")
        return

    # 全局统计
    total_updated = 0
    total_skipped_has_era = 0
    total_skipped_no_date = 0
    total_skipped_parse_err = 0
    total_failed = 0
    total_unchanged = 0
    all_era_tags: List[str] = []

    for region in args.regions:
        logger.info(f"\n{'#' * 60}")
        logger.info(f"处理区域: {region}")
        logger.info(f"{'#' * 60}")

        alphas = fetch_submitted_alphas(session, api_start, api_end, region, logger)

        if not alphas:
            logger.warning(f"区域 {region} 无符合条件的 Alpha，跳过。")
            continue

        logger.info(f"区域 {region}: 获取到 {len(alphas)} 个 Alpha，开始逐个处理...")

        if args.dry_run:
            # 预览模式：不修改数据，仅打印样本
            sample = alphas[:10]
            logger.info(f"\n预览前 {min(10, len(alphas))} 个 Alpha:")
            for alpha in sample:
                try:
                    era_tags = compute_era_tags(alpha["dateSubmitted"])
                    logger.info(
                        f"  {alpha['id']} | {alpha.get('name') or '-'} | "
                        f"{alpha['dateSubmitted']} → {era_tags}"
                    )
                except Exception as e:
                    logger.warning(f"  {alpha['id']} | 日期解析失败: {e}")
            if len(alphas) > 10:
                logger.info(f"  ... 还有 {len(alphas) - 10} 个 Alpha（预览仅显示前 10）")
            continue

        # 实际执行：单线程逐个处理
        region_updated = 0
        region_skipped_has_era = 0
        region_skipped_no_date = 0
        region_skipped_parse = 0
        region_failed = 0
        region_unchanged = 0

        for idx, alpha in enumerate(alphas):
            alpha_id = alpha["id"]
            date_submitted = alpha.get("dateSubmitted", "")

            # --- 步骤 1: 检查 dateSubmitted ---
            if not date_submitted:
                logger.warning(f"[{idx + 1}/{len(alphas)}] {alpha_id} 缺少 dateSubmitted，跳过。")
                region_skipped_no_date += 1
                time.sleep(0.5)
                continue

            # --- 步骤 2: 计算 era 标签 ---
            try:
                new_era_tags = compute_era_tags(date_submitted)
            except Exception as e:
                logger.error(f"[{idx + 1}/{len(alphas)}] {alpha_id} 日期解析失败 ({date_submitted}): {e}")
                region_skipped_parse += 1
                time.sleep(0.5)
                continue

            # --- 步骤 3: 检查当前是否有 era 标签 ---
            existing_tags = get_alpha_tags(session, alpha_id, logger)

            has_era = any(t.startswith("era:") for t in existing_tags)
            if has_era:
                logger.info(
                    f"[{idx + 1}/{len(alphas)}] {alpha_id} 已有 era 标签: "
                    f"{[t for t in existing_tags if t.startswith('era:')]}，跳过。"
                )
                region_skipped_has_era += 1
                time.sleep(0.5)
                continue

            # --- 步骤 4: 合并标签 ---
            merged_tags = existing_tags + new_era_tags

            # --- 步骤 5: 写入（带 safe_api_call 重试机制）---
            try:
                logger.info(
                    f"[{idx + 1}/{len(alphas)}] {alpha_id} | "
                    f"{date_submitted} | 现有标签 {len(existing_tags)} 个 → "
                    f"追加 era 标签 {new_era_tags}"
                )
                utils.safe_api_call(
                    _patch_alpha_tags, session,
                    alpha_id=alpha_id, tags=merged_tags,
                    max_retries=3, initial_delay=30, call_timeout=120,
                )
                region_updated += 1
                all_era_tags.extend(new_era_tags)
                logger.info(f"  ✓ 写入成功")
            except Exception as e:
                logger.error(f"  ✗ 写入失败: {e}")
                region_failed += 1

            # 每次 API 调用后间隔 1 秒，避免触发限流
            time.sleep(1.0)

        # 区域统计
        total_updated += region_updated
        total_skipped_has_era += region_skipped_has_era
        total_skipped_no_date += region_skipped_no_date
        total_skipped_parse_err += region_skipped_parse
        total_failed += region_failed
        total_unchanged += region_unchanged

        logger.info(
            f"\n区域 {region} 完成: "
            f"更新 {region_updated}, "
            f"已有era {region_skipped_has_era}, "
            f"无日期 {region_skipped_no_date}, "
            f"解析错 {region_skipped_parse}, "
            f"失败 {region_failed}"
        )

    # =========================================================================
    # 全局统计与分布报告
    # =========================================================================
    total_processed = (
        total_updated + total_skipped_has_era + total_skipped_no_date
        + total_skipped_parse_err + total_failed + total_unchanged
    )

    logger.info(f"\n{'=' * 60}")
    logger.info("全部处理完毕。")
    logger.info(
        f"总计 {total_processed} 个 Alpha: "
        f"更新 {total_updated}, "
        f"已有era跳过 {total_skipped_has_era}, "
        f"无日期 {total_skipped_no_date}, "
        f"解析错 {total_skipped_parse_err}, "
        f"失败 {total_failed}"
    )

    if total_updated > 0:
        tag_counts = Counter(all_era_tags)
        logger.info("\n新增 era 标签分布（Top 30）:")
        for tag, count in tag_counts.most_common(30):
            logger.info(f"  {tag}: {count}")

    logger.info(f"{'=' * 60}")


if __name__ == "__main__":
    main()
