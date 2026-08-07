# -*- coding: utf-8 -*-
"""
Alpha 经济学含义标签脚本 (econ tag)

为已提交的 Alpha 批量添加经济学含义标签 (econ tag)。
每批 10 个 Alpha，调用大模型分析表达式、字段含义和表现信息，输出固定 7 类经济学标签。

处理逻辑（保证不破坏已有标签）：
  1. 获取指定时间范围内已提交的 Alpha 列表（按 region）
  2. 检查每个 Alpha 是否已有 econ:* 标签 → 跳过
  3. 无 econ 标签的 Alpha 每 10 个一批：
     a. 获取表达式及字段描述
     b. 调用 LLM 分类为 7 类之一
     c. LLM 返回 JSON：alpha_id, tag, reasoning
  4. GET 当前标签 → 追加新的 econ tag → PATCH 写入

7 个经济学标签定义：
  econ:momentum       - 趋势追踪
  econ:reversal       - 均值回归
  econ:value          - 价值与基本面
  econ:growth         - 成长与预期
  econ:sentiment      - 情绪与资金流
  econ:volatility     - 波动率与风险
  econ:cross_section  - 截面比较

用法：
  # 预览模式（不修改数据）
  python tag_econ_alphas.py --start 2025-01-01 --end 2025-06-30 --dry-run

  # 指定区域执行
  python tag_econ_alphas.py --start 2025-01-01 --end 2025-06-30 --regions USA EUR
  python src/tag_econ_alphas.py --start 2025-01-01 --end 2026-07-12 --regions USA EUR ASI GLB IND

  # 单区域预览
  python tag_econ_alphas.py --start 2025-03-01 --end 2025-03-31 --regions USA --dry-run
"""

import argparse
import json
import os
import re
import time
import sys
from collections import Counter
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

import ace_lib
import utils
from config_manager import config_manager
from langchain_openai import ChatOpenAI
from logger import Logger
from pydantic import BaseModel, Field, ValidationError


# =============================================================================
# 常量：7 个经济学标签定义
# =============================================================================

ECON_TAGS = [
    "econ:momentum",
    "econ:reversal",
    "econ:value",
    "econ:growth",
    "econ:sentiment",
    "econ:volatility",
    "econ:cross_section",
]

ECON_TAG_DEFINITIONS = (
    "econ:momentum: trend-following operators (ts_momentum, ts_corr, ts_rank) on price/volume data.\n"
    "econ:reversal: mean-reversion operators (ts_zscore, ts_delta, ts_mean) on price/volume data.\n"
    "econ:value: fundamental data (valuation, profitability, leverage, book value). Low turnover common.\n"
    "econ:growth: analyst estimates, earnings revisions, growth rates, earnings surprises.\n"
    "econ:sentiment: sentiment, news, institutional holdings, insider trading, capital flow data.\n"
    "econ:volatility: volatility/options/risk data; ts_std_dev/variance/skewness/kurtosis on any source.\n"
    "econ:cross_section: cross-sectional operators (group_rank, group_neutralize, group_zscore)."
)

BATCH_SIZE = 10


# =============================================================================
# Pydantic 模型
# =============================================================================

class EconTagResult(BaseModel):
    alpha_id: str = Field(description="Alpha ID")
    tag: str = Field(description=f"One of: {', '.join(ECON_TAGS)}")
    reasoning: str = Field(description="Brief justification in Chinese (1-2 sentences)")


class EconTagBatchResult(BaseModel):
    results: List[EconTagResult] = Field(description="List of classification results")


# =============================================================================
# Alpha 查询（复用 tag_alphas.py 模式）
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
    复用 tag_alphas.py 的查询逻辑。
    """
    output: List[Dict] = []
    logger.info(f"区域 {region}: {start_date} ~ {end_date}")

    for i in range(0, alpha_limit, 100):
        logger.info(f"  正在获取第 {i} 到 {i + 100} 个 alpha...")
        url = (
            f"https://api.worldquantbrain.com/users/self/alphas?limit=100&offset={i}"
            f"&status!=UNSUBMITTED&status!=IS_FAIL"
            f"&dateSubmitted>={start_date}T00:00:00-04:00"
            f"&dateSubmitted<{end_date}T00:00:00-04:00"
            f"&order=-is.sharpe&hidden=false&type!=SUPER"
            f"&settings.delay=1&settings.region={region}"
        )
        try:
            resp = session.get(url)
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
    """获取单个 Alpha 的当前 tags，失败返回空列表（保守策略）。"""
    try:
        resp = session.get(f"https://api.worldquantbrain.com/alphas/{alpha_id}")
        resp.raise_for_status()
        return resp.json().get("tags", [])
    except Exception as e:
        logger.warning(f"获取 Alpha {alpha_id} tags 失败: {e}")
        return []


# =============================================================================
# Alpha 详情获取（复用 optimization workflow 的 fetch 模式）
# =============================================================================

def get_alpha_details(session, alpha_id: str, logger: Logger) -> Optional[Dict]:
    """
    获取单个 Alpha 的表达式、设置和表现信息。

    Returns:
        {alpha_id, expression, region, universe, neutralization, decay, truncation,
         sharpe, fitness, turnover} 或 None（获取失败）
    """
    try:
        resp = session.get(f"https://api.worldquantbrain.com/alphas/{alpha_id}")
        resp.raise_for_status()
        details = resp.json()

        expression = details.get("regular", {}).get("code", "")
        if not expression:
            logger.warning(f"Alpha {alpha_id}: 无表达式，跳过。")
            return None

        settings = details.get("settings", {})
        is_stats = details.get("is", {})

        return {
            "alpha_id": alpha_id,
            "expression": expression,
            "region": settings.get("region", "USA"),
            "universe": settings.get("universe", "TOP3000"),
            "neutralization": settings.get("neutralization", "NONE"),
            "decay": settings.get("decay", 0),
            "truncation": settings.get("truncation", 0.0),
            "sharpe": round(is_stats.get("sharpe", 0.0), 4),
            "fitness": round(is_stats.get("fitness", 0.0), 4),
            "turnover": round(is_stats.get("turnover", 0.0), 4),
        }
    except Exception as e:
        logger.error(f"获取 Alpha {alpha_id} 详情失败: {e}")
        return None


# =============================================================================
# 字段描述获取（复用 optimization workflow 的 _fetch_field_metadata 模式）
# =============================================================================

def fetch_single_field_description(
    session, field_id: str, settings: Dict, logger: Logger,
    fallback_universes: Optional[Set[str]] = None,
) -> str:
    """
    获取单个字段的描述文本。通过 ace_lib.get_datafields 搜索。

    Args:
        settings: 主查询参数 {region, universe, delay}
        fallback_universes: 主 universe 查不到时回退尝试的其他 universe 集合
    """
    region = settings.get("region", "USA")
    delay = settings.get("delay", 1)
    primary_universe = settings.get("universe", "TOP3000")

    # 构建尝试的 universe 列表：primary + fallbacks（去重）
    universes_to_try = [primary_universe]
    if fallback_universes:
        for u in fallback_universes:
            if u != primary_universe:
                universes_to_try.append(u)

    for universe in universes_to_try:
        try:
            df = utils.safe_api_call(
                ace_lib.get_datafields, session,
                region=region,
                universe=universe,
                delay=delay,
                data_type='ALL',
                search=field_id,
                max_retries=2, initial_delay=5, call_timeout=60,
            )
            if df is not None and not df.empty:
                exact_match = df[df['id'] == field_id]
                if not exact_match.empty:
                    desc = exact_match.iloc[0].get('description', '')
                    if desc and str(desc).strip():
                        return str(desc).strip()
        except Exception as e:
            logger.warning(f"获取字段 {field_id} (universe={universe}) 描述异常: {e}")

    return f"(no description available for {field_id})"


def collect_field_descriptions(
    session, field_ids: Set[str], settings: Dict, logger: Logger,
    cache: Dict[str, str],
    field_universes_map: Optional[Dict[str, Set[str]]] = None,
) -> Dict[str, str]:
    """
    收集字段描述，带缓存复用。若连续多个字段无描述则记录警告但不中断任务。

    Args:
        field_universes_map: field_id -> set of universe strings，主 universe 查不到时回退尝试
    """
    new_fields = field_ids - cache.keys()
    consecutive_empty = 0
    if new_fields:
        logger.info(f"获取 {len(new_fields)} 个新字段的描述...")
        for idx, fid in enumerate(sorted(new_fields)):
            if fid:
                fallback_universes = (
                    field_universes_map.get(fid) if field_universes_map else None
                )
                desc = fetch_single_field_description(
                    session, fid, settings, logger,
                    fallback_universes=fallback_universes,
                )
                cache[fid] = desc
                if desc.startswith("(no description available for"):
                    consecutive_empty += 1
                    if consecutive_empty == 5:
                        logger.warning(
                            f"连续 {consecutive_empty} 个字段无法获取描述，"
                            f"可能是当前 region/universe 组合无数据字段注册。将继续运行（LLM 可从表达式推断分类）。"
                        )
                else:
                    consecutive_empty = 0
                if (idx + 1) % 20 == 0:
                    logger.info(f"  字段描述获取进度: {idx + 1}/{len(new_fields)}")

    return {fid: cache.get(fid, f"(unknown: {fid})") for fid in field_ids if fid}


# =============================================================================
# LLM Prompt 构建
# =============================================================================

def build_classification_prompt(
    alpha_infos: List[Dict],
    field_desc_map: Dict[str, str],
) -> str:
    """
    构建发送给 LLM 的分类 prompt。
    每个 Alpha 呈现：表达式、设置、表现、字段（含描述）。
    """
    alpha_entries = []
    for i, info in enumerate(alpha_infos):
        fields_used = info.get("fields_used", set())
        field_lines = []
        for fid in sorted(fields_used):
            desc = field_desc_map.get(fid, f"(no description for {fid})")
            field_lines.append(f"    - {fid}: {desc}")
        fields_text = "\n".join(field_lines) if field_lines else "    (no fields identified)"

        entry = (
            f"Alpha {i + 1}:\n"
            f"  alpha_id: {info['alpha_id']}\n"
            f"  expression: {info['expression']}\n"
            f"  settings: neutralization={info['neutralization']}, decay={info['decay']}, truncation={info['truncation']}\n"
            f"  IS_performance: sharpe={info['sharpe']}, fitness={info['fitness']}, turnover={info['turnover']}\n"
            f"  fields:\n{fields_text}"
        )
        alpha_entries.append(entry)

    alphas_text = "\n\n".join(alpha_entries)

    json_example = '{"results":[{"alpha_id":"...","tag":"econ:...","reasoning":"中文1-2句' \
        ',提及关键算子/字段"}]}'
    prompt = (
        f"Classify each Alpha into one economic category based on expression, operators, data fields, and performance.\n"
        f"TAGS:\n{ECON_TAG_DEFINITIONS}\n"
        f"RULES:\n"
        f"1. Field semantics > operator pattern. ts_momentum(roe)=value, ts_zscore(returns)=reversal.\n"
        f"2. Volatility operations on any data source -> econ:volatility.\n"
        f"3. Low turnover(<0.2)+fundamentals->value; PV+ts_momentum->momentum; PV+ts_zscore->reversal.\n"
        f"4. Mixed signals: pick the dominant theme.\n"
        f"5. Use field descriptions to understand economic meaning.\n"
        f"\nALPHAS:\n{alphas_text}\n"
        f"OUTPUT valid JSON only (no markdown, no commentary): {json_example}\n"
        f"tag must be: {', '.join(ECON_TAGS)}. Include all alpha_ids. reasoning in Chinese."
    )

    return prompt


# =============================================================================
# LLM 调用与分类（复用 optimization workflow 的 LLM 调用模式）
# =============================================================================

def classify_batch(
    model: ChatOpenAI,
    alpha_infos: List[Dict],
    field_desc_map: Dict[str, str],
    logger: Logger,
) -> Optional[List[EconTagResult]]:
    """
    调用 LLM 对一批 Alpha 进行分类。

    复用 optimization workflow 中 propose_and_generate_batch 的
    model.invoke → extract JSON → Pydantic validate 模式。

    Returns:
        分类结果列表，失败返回 None
    """
    prompt = build_classification_prompt(alpha_infos, field_desc_map)

    # 保存 prompt 到文件便于调试
    prompt_log_file = "data/last_econ_tag_prompt.txt"
    os.makedirs("data", exist_ok=True)
    try:
        with open(prompt_log_file, "w", encoding="utf-8") as f:
            f.write(prompt)
        logger.info(f"Prompt 已保存到 {prompt_log_file} ({len(prompt)} 字符)")
    except Exception:
        pass

    max_retries = 3
    attempts = 0
    while attempts < max_retries:
        try:
            logger.info(f"调用 LLM 分类 {len(alpha_infos)} 个 Alpha (attempt {attempts + 1}/{max_retries})...")
            response = model.invoke(prompt)
            content = response.content.strip()

            # 尝试多种方式提取 JSON（复用 optimization workflow 的提取逻辑）
            json_str = None
            json_match = re.search(r'```json\s*(.*?)\s*```', content, re.DOTALL | re.IGNORECASE)
            if json_match:
                json_str = json_match.group(1).strip()
            if not json_str:
                json_match = re.search(r'```\s*(.*?)\s*```', content, re.DOTALL | re.IGNORECASE)
                if json_match:
                    json_str = json_match.group(1).strip()
            if not json_str:
                json_match = re.search(r'(\{[\s\S]*\})', content)
                if json_match:
                    json_str = json_match.group(1).strip()

            if not json_str:
                preview = content[:500] + ("..." if len(content) > 500 else "")
                logger.warning(f"LLM 返回中未找到 JSON。前 500 字符:\n{preview}")
                raise ValueError("No JSON in response")

            parsed = json.loads(json_str)
            validated = EconTagBatchResult.model_validate(parsed)

            # 验证返回的 Alpha ID 与输入一致
            input_ids = {info["alpha_id"] for info in alpha_infos}
            output_ids = {r.alpha_id for r in validated.results}
            missing = input_ids - output_ids
            extra = output_ids - input_ids
            if missing:
                logger.warning(f"LLM 未返回以下 Alpha ID: {missing}")
            if extra:
                logger.warning(f"LLM 返回了未知 Alpha ID: {extra}")

            # 过滤无效 tag
            valid_results = []
            for r in validated.results:
                if r.tag not in ECON_TAGS:
                    logger.warning(f"Alpha {r.alpha_id}: 无效标签 '{r.tag}'，跳过。")
                    continue
                valid_results.append(r)

            logger.info(f"LLM 分类成功: {len(valid_results)}/{len(alpha_infos)} 个结果有效。")
            return valid_results

        except (json.JSONDecodeError, ValidationError, ValueError) as e:
            attempts += 1
            logger.error(f"LLM 提取/解析失败 (attempt {attempts}/{max_retries}): {e}")
            if attempts >= max_retries:
                logger.error("LLM 分类重试耗尽，此批次失败。")
                return None
            logger.info(f"等待 30s 后重试...")
            time.sleep(30)

    return None


# =============================================================================
# Tag 写入（复用 tag_alphas.py 的 PATCH 模式）
# =============================================================================

def _patch_alpha_tags(session, alpha_id: str, tags: List[str]):
    """PATCH 单个 Alpha 的 tags 字段。让 raise_for_status 触发 safe_api_call 的重试。"""
    resp = session.patch(
        f"https://api.worldquantbrain.com/alphas/{alpha_id}",
        json={"tags": tags},
    )
    resp.raise_for_status()
    return resp


def apply_econ_tag(session, alpha_id: str, new_tag: str, logger: Logger) -> bool:
    """
    为单个 Alpha 添加 econ tag（保护已有标签不变）。

    1. GET 当前 tags
    2. 合并: existing_tags + [new_tag]
    3. PATCH 写入

    Returns:
        成功 True，失败 False
    """
    try:
        existing_tags = get_alpha_tags(session, alpha_id, logger)

        # 二次确认：是否已有 econ tag（防并发写入）
        already_has = any(t.startswith("econ:") for t in existing_tags)
        if already_has:
            existing_econ = [t for t in existing_tags if t.startswith("econ:")]
            logger.info(f"Alpha {alpha_id}: 已有 econ 标签 {existing_econ}，跳过写入。")
            return True

        merged_tags = existing_tags + [new_tag]

        logger.info(f"Alpha {alpha_id}: 写入标签 -> 现有 {len(existing_tags)} 个 + '{new_tag}'")
        utils.safe_api_call(
            _patch_alpha_tags, session,
            alpha_id=alpha_id, tags=merged_tags,
            max_retries=3, initial_delay=30, call_timeout=120,
        )
        logger.info(f"  ✓ {alpha_id} -> {new_tag} 写入成功")
        return True

    except Exception as e:
        logger.error(f"  ✗ {alpha_id} 写入失败: {e}")
        return False


# =============================================================================
# 批次处理
# =============================================================================

def process_alpha_batch(
    session,
    batch: List[Dict],
    model: ChatOpenAI,
    logger: Logger,
    dry_run: bool,
    region: str,
    field_cache: Dict[str, str],
) -> Tuple[int, int, List[str]]:
    """
    处理一批 Alpha（最多 BATCH_SIZE 个）：

    1. 获取每个 Alpha 的详情（表达式、设置、表现）
    2. 提取每个 Alpha 使用的字段
    3. 收集字段描述（带全局缓存）
    4. 调用 LLM 分类
    5. 写入标签

    Args:
        session: BRAIN session
        batch: Alpha 基本信息列表 [{"id": ..., "region": ..., ...}]
        model: ChatOpenAI 实例
        logger: 日志器
        dry_run: 预览模式
        region: 当前区域
        field_cache: 字段描述缓存 dict（跨批次复用）

    Returns:
        (成功数, 失败数, 已应用的 tag 列表)
    """
    alpha_infos = []
    no_detail_count = 0

    for alpha in batch:
        details = get_alpha_details(session, alpha["id"], logger)
        if details is None:
            no_detail_count += 1
            continue

        # 提取表达式中的字段 ID
        try:
            fields_used = utils.extract_datafields(details["expression"], session)
            details["fields_used"] = fields_used if fields_used else set()
        except Exception as e:
            logger.warning(f"Alpha {alpha['id']}: 提取字段 ID 失败: {e}")
            details["fields_used"] = set()

        alpha_infos.append(details)

    if not alpha_infos:
        logger.warning("此批次无有效 Alpha（全部详情获取失败）。")
        return 0, len(batch), []

    # 收集去重字段的描述
    all_fields: Set[str] = set()
    # 构建 field -> alpha universes 映射，用于回退查询
    field_universes_map: Dict[str, Set[str]] = {}
    alpha_universes: List[str] = []
    for info in alpha_infos:
        fields = info.get("fields_used", set())
        all_fields.update(fields)
        alpha_uni = info.get("universe", "TOP3000")
        alpha_universes.append(alpha_uni)
        for f in fields:
            field_universes_map.setdefault(f, set()).add(alpha_uni)

    # 以出现最多的 universe 作为主查询 universe
    primary_universe = Counter(alpha_universes).most_common(1)[0][0] if alpha_universes else "TOP3000"
    ref_settings = {"region": region, "universe": primary_universe, "delay": 1}
    logger.info(f"批次: {len(alpha_infos)} 个 Alpha, {len(all_fields)} 个去重字段, 主 universe={primary_universe}。")
    field_desc_map = collect_field_descriptions(
        session, all_fields, ref_settings, logger, field_cache,
        field_universes_map=field_universes_map,
    )

    # 调用 LLM 分类
    results = classify_batch(model, alpha_infos, field_desc_map, logger)
    if results is None:
        logger.error("LLM 分类失败，跳过此批次。")
        return 0, len(alpha_infos), []

    # 应用标签
    success_tags = []
    success_count = 0
    fail_count = no_detail_count

    for result in results:
        if result.tag not in ECON_TAGS:
            logger.warning(f"Alpha {result.alpha_id}: LLM 返回无效标签 '{result.tag}'，跳过。")
            fail_count += 1
            continue

        if dry_run:
            logger.info(f"[DRY RUN] {result.alpha_id} -> {result.tag}: {result.reasoning}")
            success_tags.append(result.tag)
            success_count += 1
        else:
            if apply_econ_tag(session, result.alpha_id, result.tag, logger):
                success_tags.append(result.tag)
                success_count += 1
            else:
                fail_count += 1

            # API 限流保护
            time.sleep(1.0)

    return success_count, fail_count, success_tags


# =============================================================================
# 主入口
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Alpha 经济学标签脚本 — 为已提交 Alpha 批量添加 econ 标签",
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
        help="仅预览 LLM 分类结果，不实际修改 Alpha 标签",
    )
    parser.add_argument(
        "--batch-size", type=int, default=BATCH_SIZE,
        help=f"每批处理的 Alpha 数量（默认: {BATCH_SIZE}）",
    )
    args = parser.parse_args()

    logger = Logger(log_name="tag_econ_alphas", log_dir="logs/")
    logger.info("=" * 60)
    logger.info("--- Alpha 经济学标签 (econ tag) 脚本 ---")
    logger.info(f"时间范围: {args.start} ~ {args.end} (含起止日期)")
    logger.info(f"目标区域: {args.regions}")
    logger.info(f"每批数量: {args.batch_size}")
    if args.dry_run:
        logger.info("模式: DRY RUN（仅预览，不修改数据）")
    logger.info(f"经济学标签 ({len(ECON_TAGS)} 类): {', '.join(ECON_TAGS)}")
    logger.info("=" * 60)

    # API 的 end_date 需要 +1 天（API 使用 < 过滤）
    start_dt = datetime.strptime(args.start, "%Y-%m-%d")
    end_dt = datetime.strptime(args.end, "%Y-%m-%d") + timedelta(days=1)
    api_start = start_dt.strftime("%Y-%m-%d")
    api_end = end_dt.strftime("%Y-%m-%d")

    # 获取 BRAIN session
    session = config_manager.get_session()
    if not session:
        logger.error("无法获取 BRAIN session，脚本退出。")
        sys.exit(1)

    # 初始化 LLM（复用 optimization workflow 的配置模式）
    model = ChatOpenAI(
        model=config_manager.get('llm_paid_model', 'deepseek-chat'),
        base_url=config_manager.get('llm_paid_base_url', 'https://api.deepseek.com'),
        api_key=config_manager.get('llm_paid_api_key'),
        temperature=0.3,
        request_timeout=300.0,
        max_tokens=4096,
    )
    logger.info(f"LLM 模型: {config_manager.get('llm_paid_model', 'deepseek-chat')}")

    # 全局统计
    total_tagged = 0
    total_already_has_econ = 0
    total_failed = 0
    total_no_detail = 0
    all_applied_tags: List[str] = []
    field_cache: Dict[str, str] = {}  # 跨批次的字段描述缓存

    for region in args.regions:
        logger.info(f"\n{'#' * 60}")
        logger.info(f"处理区域: {region}")
        logger.info(f"{'#' * 60}")

        alphas = fetch_submitted_alphas(session, api_start, api_end, region, logger)
        if not alphas:
            logger.warning(f"区域 {region} 无符合条件的 Alpha，跳过。")
            continue

        logger.info(f"区域 {region}: 共获取 {len(alphas)} 个 Alpha，检查已有 econ 标签...")

        # 筛选需要打标的 Alpha（无 econ 标签的）
        need_tagging = []
        region_has_econ = 0
        for idx, alpha in enumerate(alphas):
            if (idx + 1) % 200 == 0:
                logger.info(f"  已检查 {idx + 1}/{len(alphas)} 个 Alpha...")

            existing_tags = get_alpha_tags(session, alpha["id"], logger)
            has_econ = any(t.startswith("econ:") for t in existing_tags)
            if has_econ:
                region_has_econ += 1
            else:
                need_tagging.append(alpha)

        logger.info(
            f"区域 {region}: {region_has_econ} 个已有 econ 标签, "
            f"{len(need_tagging)} 个需要打标。"
        )
        total_already_has_econ += region_has_econ

        if not need_tagging:
            logger.info(f"区域 {region} 所有 Alpha 均已有 econ 标签，跳过。")
            continue

        # 分批处理
        region_tagged = 0
        region_failed = 0
        total_batches = (len(need_tagging) - 1) // args.batch_size + 1

        for batch_idx in range(0, len(need_tagging), args.batch_size):
            batch = need_tagging[batch_idx:batch_idx + args.batch_size]
            batch_num = batch_idx // args.batch_size + 1
            logger.info(f"\n{'~' * 50}")
            logger.info(f"区域 {region} - 第 {batch_num}/{total_batches} 批 ({len(batch)} 个 Alpha)")
            logger.info(f"{'~' * 50}")

            success, failed, applied_tags = process_alpha_batch(
                session, batch, model, logger, args.dry_run, region, field_cache
            )
            region_tagged += success
            region_failed += failed
            all_applied_tags.extend(applied_tags)

            # 批次间短暂间隔
            if batch_idx + args.batch_size < len(need_tagging):
                time.sleep(3.0)

        total_tagged += region_tagged
        total_failed += region_failed

        logger.info(
            f"\n区域 {region} 完成: "
            f"成功 {region_tagged}, 失败 {region_failed}, "
            f"已有标签跳过 {region_has_econ}, "
            f"字段缓存条目 {len(field_cache)}"
        )

    # =========================================================================
    # 全局统计
    # =========================================================================
    total_processed = total_tagged + total_already_has_econ + total_failed + total_no_detail
    logger.info(f"\n{'=' * 60}")
    logger.info("全部处理完毕。")
    logger.info(
        f"总计 {total_processed} 个 Alpha: "
        f"新打标 {total_tagged}, "
        f"已有标签跳过 {total_already_has_econ}, "
        f"失败 {total_failed}, "
        f"无详情 {total_no_detail}"
    )

    if all_applied_tags:
        tag_counts = Counter(all_applied_tags)
        logger.info(f"\n新增 econ 标签分布:")
        for tag in ECON_TAGS:
            count = tag_counts.get(tag, 0)
            bar = "█" * min(count // max(1, total_tagged) * 40, 40) if total_tagged else ""
            logger.info(f"  {tag:25s}: {count:5d}  {bar}")

        # 写分布结果到文件
        dist_dir = "data"
        os.makedirs(dist_dir, exist_ok=True)
        dist_path = os.path.join(dist_dir, "econ_tag_distribution.json")
        try:
            with open(dist_path, "w", encoding="utf-8") as f:
                json.dump(dict(tag_counts), f, ensure_ascii=False, indent=2)
            logger.info(f"标签分布已保存到 {dist_path}")
        except Exception as e:
            logger.warning(f"保存分布文件失败: {e}")

    logger.info(f"{'=' * 60}")


if __name__ == "__main__":
    main()
