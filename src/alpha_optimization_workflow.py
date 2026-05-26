import ace_lib
import ace_lib_ext
import utils
import pandas as pd
from pydantic import BaseModel, Field, ValidationError
from langchain_openai import ChatOpenAI
from langgraph.graph import StateGraph, END
from typing import TypedDict, List, Optional, Tuple, Callable
import json
import time
import os
import re
import sys
from http.client import RemoteDisconnected
from requests.exceptions import ConnectionError
from llm_calls import test_llm_connection
import concurrent.futures
from alpha_score import evaluate_and_select_best_alpha, select_best_from_candidates, _passes_hard_filters
from validator import ExpressionValidator
from cached_data_fetcher import get_datafields_with_cache
from logger import Logger
from config_manager import config_manager

# --- 日志记录器 ---
logger = Logger.get_logger("optimization")

# --- 文件路径常量 ---
_KB_ROOT = "data"
_FIELD_METADATA_CACHE_DIR = os.path.join("data", "field_metadata")

FILE_PATHS = {
    "IMPROVE_METHODS": {
        'LOW_SHARPE': f'{_KB_ROOT}/ImproveMethods/How to improve Sharpe?.md',
        'LOW_FITNESS': f'{_KB_ROOT}/ImproveMethods/How to increase fitness of alphas.md',
        'LOW_TURNOVER': f'{_KB_ROOT}/ImproveMethods/How to improve Turnover?.md',
        'HIGH_TURNOVER': f'{_KB_ROOT}/ImproveMethods/How to improve Turnover?.md',
        'CONCENTRATED_WEIGHT': f'{_KB_ROOT}/ImproveMethods/Weight Coverage common issues and advice.md',
        'LOW_SUB_UNIVERSE_SHARPE': f'{_KB_ROOT}/ImproveMethods/How do I resolve this error Sub-universe Sharpe NaN is not above cutoff.md',
        'LOW_ROBUST_UNIVERSE_SHARPE': f'{_KB_ROOT}/ImproveMethods/Details about "robust universe" criteria in CHN,ASI,IND region.md',
        'LOW_ROBUST_UNIVERSE_SHARPE.WITH_RATIO': f'{_KB_ROOT}/ImproveMethods/Details about "robust universe" criteria in CHN,ASI,IND region.md',
        'LOW_ASI_JPN_SHARPE': f'{_KB_ROOT}/ImproveMethods/How to handle LOW_ASI_JPN_SHARPE.md',
        'LOW_ROBUST_UNIVERSE_RETURNS': f'{_KB_ROOT}/ImproveMethods/Details about "robust universe" criteria in CHN,ASI,IND region.md',
        'LOW_2Y_SHARPE': f'{_KB_ROOT}/ImproveMethods/How to improve Sharpe?.md',
        'PROD_CORRELATION': f'{_KB_ROOT}/ImproveMethods/How do you reduce correlation of a good Alpha?.md',
        'LOW_GLB_AMER_SHARPE': f'{_KB_ROOT}/ImproveMethods/How to improve Sharpe?.md',
        'LOW_GLB_EMEA_SHARPE': f'{_KB_ROOT}/ImproveMethods/How to improve Sharpe?.md',
        'LOW_GLB_APAC_SHARPE': f'{_KB_ROOT}/ImproveMethods/How to improve Sharpe?.md',
        'LOW_INVESTABILITY_CONSTRAINED_SHARPE': f'{_KB_ROOT}/ImproveMethods/Investability Constrained Metrics: Optimizing Alpha for Real-World Trading.md'
    },
    "HOW_TO_USE_DATASETS": {
        'analyst': f'{_KB_ROOT}/HowToUseAllDatasets/Getting started with Analyst Datasets.md',
        'earnings': f'{_KB_ROOT}/HowToUseAllDatasets/Getting started with Earnings Datasets.md',
        'institutions': f'{_KB_ROOT}/HowToUseAllDatasets/Getting started with Institutions Datasets.md',
        'macro': f'{_KB_ROOT}/HowToUseAllDatasets/Getting started with Macro Datasets.md',
        'model': f'{_KB_ROOT}/HowToUseAllDatasets/Getting started with Model Datasets.md',
        'news': f'{_KB_ROOT}/HowToUseAllDatasets/Getting started with News Datasets.md',
        'option': f'{_KB_ROOT}/HowToUseAllDatasets/Getting Started with Option Datasets.md',
        'other': f'{_KB_ROOT}/HowToUseAllDatasets/Getting started with Other Datasets.md',
        'pv': f'{_KB_ROOT}/HowToUseAllDatasets/Getting started with Price Volume Datasets.md',
        'risk': f'{_KB_ROOT}/HowToUseAllDatasets/Getting started with Risk Datasets.md',
        'sentiment': f'{_KB_ROOT}/HowToUseAllDatasets/Getting started with Sentiment Datasets.md',
        'shortinterest': f'{_KB_ROOT}/HowToUseAllDatasets/Getting started with Short Interest Datasets.md',
        'socialmedia': f'{_KB_ROOT}/HowToUseAllDatasets/Getting started with Social Media Datasets.md'
    },
    "PROMPT_PRINCIPLES": f"{_KB_ROOT}/prompt.md",
    "PROMPT_EXAMPLES": f"{_KB_ROOT}/prompt2.md",
    "PROMPT_TEMPLATE": f"{_KB_ROOT}/prompt2.md",
    "REGION_DOCS_ROOT": f"{_KB_ROOT}/regions"
}

# --- NEUTRALIZATION 可选项 ---
NEUTRALIZATION_OPTIONS = ["REVERSION_AND_MOMENTUM", "CROWDING", "FAST", "SLOW", "SLOW_AND_FAST", "MARKET", "SECTOR", "INDUSTRY", "SUBINDUSTRY", "STATISTICAL"]

# --- 知识库文档最大长度限制 (防止 Prompt 过长) ---
MAX_TOTAL_KB = 8000

# ========================================================================
# Pydantic 模型
# ========================================================================

class ProposedAlpha(BaseModel):
    reasoning: str = Field(description="The reasoning behind the proposed changes.")
    new_expression: str = Field(description="The new, modified Alpha expression.")
    new_setting: dict = Field(description="A dictionary with new settings for 'neutralization', 'decay', and 'truncation'.")

class AlphaProposal(BaseModel):
    proposals: List[ProposedAlpha] = Field(description="A list of 20 proposed alpha candidates.")

# ========================================================================
# WorkflowState 定义
# ========================================================================

class WorkflowState(TypedDict):
    session: object
    seed_alpha_id: str
    valid_data_fields: set
    all_fields_df: object
    seed_expression: str
    seed_setting: dict
    row_setting: dict
    seed_performance_report: str
    yearly_performance_report: str
    proposed_alphas: List[dict]
    valid_candidates: List[dict]
    historical_alphas: List[str]
    repeat_historical_alphas: List[str]
    knowledge_base: dict
    status: str
    success_id: Optional[str]
    error_log: List[str]
    best_alpha: dict
    initial_best_alpha: dict
    iteration_count: int
    no_valid_candidates_counter: int
    status_callback: Optional[Callable] # 状态更新回调函数

# ========================================================================
# 辅助函数
# ========================================================================



def _load_field_metadata_cache(settings: dict) -> dict:
    """Load the field metadata cache from disk."""
    region = settings.get("region", "UNKNOWN")
    universe = settings.get("universe", "UNKNOWN")
    delay = settings.get("delay", 1)
    cache_dir = os.path.join(_FIELD_METADATA_CACHE_DIR, region)
    cache_filename = f"{universe}_{delay}.json"
    cache_filepath = os.path.join(cache_dir, cache_filename)
    if os.path.exists(cache_filepath):
        try:
            with open(cache_filepath, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load field metadata cache {cache_filepath}: {e}")
    return {}

def _save_field_metadata_cache(settings: dict, cache: dict):
    """Save the field metadata cache to disk."""
    region = settings.get("region", "UNKNOWN")
    universe = settings.get("universe", "UNKNOWN")
    delay = settings.get("delay", 1)
    cache_dir = os.path.join(_FIELD_METADATA_CACHE_DIR, region)
    os.makedirs(cache_dir, exist_ok=True)
    cache_filename = f"{universe}_{delay}.json"
    cache_filepath = os.path.join(cache_dir, cache_filename)
    try:
        with open(cache_filepath, 'w') as f:
            json.dump(cache, f, indent=2)
        logger.info(f"Field metadata cache saved to {cache_filepath} ({len(cache)} entries)")
    except Exception as e:
        logger.error(f"Failed to save field metadata cache {cache_filepath}: {e}")

def _fetch_field_metadata(session, field_ids: List[str], state: WorkflowState) -> pd.DataFrame:
    """从平台获取字段元数据，优先使用文件缓存"""
    settings = state["row_setting"]
    all_results = []
    cache_dirty = False
    total = len(field_ids)

    # Load existing cache
    cache = _load_field_metadata_cache(settings)
    logger.info(f"Loaded field metadata cache: {len(cache)} entries")

    cached_count = 0
    fetch_count = 0
    for idx, fid in enumerate(field_ids):
        # Check cache first
        if fid in cache:
            entry = cache[fid]
            if entry is not None:
                all_results.append(pd.DataFrame([entry]))
                cached_count += 1
                # Log progress every 20 cached items
                if cached_count % 20 == 0:
                    logger.info(f"[{idx+1}/{total}] Cached: {cached_count}, fetched: {fetch_count}")
                continue

        # Not in cache, fetch from API
        logger.info(f"[{idx+1}/{total}] Fetching field: {fid} (cached: {cached_count}, fetched: {fetch_count})")
        try:
            df = utils.safe_api_call(ace_lib.get_datafields, session,
                                     region=settings.get("region"),
                                     universe=settings.get("universe"),
                                     delay=settings.get("delay", 1),
                                     data_type='ALL', search=fid)
            if df is not None and not df.empty:
                exact_match = df[df['id'] == fid]
                if not exact_match.empty:
                    record = exact_match.iloc[0].to_dict()
                    all_results.append(pd.DataFrame([record]))
                    cache[fid] = record
                    cache_dirty = True
                    fetch_count += 1
                else:
                    # Field not found on platform, cache as None to avoid repeated lookups
                    cache[fid] = None
                    cache_dirty = True
            else:
                cache[fid] = None
                cache_dirty = True
        except Exception as e:
            logger.error(f"Error fetching field {fid}: {e}")

    # Persist cache if any new entries were added
    if cache_dirty:
        _save_field_metadata_cache(settings, cache)

    logger.info(f"Field metadata summary: {cached_count} from cache, {fetch_count} fetched, {total} total")
    return pd.concat(all_results, ignore_index=True).drop_duplicates(subset=['id']) if all_results else pd.DataFrame()

def _verify_and_load_region_fields(session, fields: set, state: WorkflowState):
    """验证并加载区域相关字段"""
    if not fields:
        return
    region = state["row_setting"].get("region", "UNKNOWN")
    logger.info(f"[Region: {region}] Verifying {len(fields)} regional fields via platform API...")
    metadata_df = _fetch_field_metadata(session, list(fields), state)
    if not metadata_df.empty:
        verified_ids = metadata_df['id'].unique()
        state["valid_data_fields"].update(verified_ids)
        state["all_fields_df"] = pd.concat([state["all_fields_df"], metadata_df], ignore_index=True)
        logger.info(f"[Region: {region}] Verified and added {len(verified_ids)} regional fields.")
    else:
        logger.info(f"[Region: {region}] None of the fields from region docs could be verified.")

def _load_region_knowledge(session, state: WorkflowState):
    """加载区域知识文档并提取字段"""
    region = state["row_setting"].get("region", "USA")
    doc_path = os.path.join(FILE_PATHS["REGION_DOCS_ROOT"], f"{region}.md")

    # Load markdown content for knowledge reference
    if os.path.exists(doc_path):
        try:
            with open(doc_path, 'r') as f:
                state['knowledge_base']['region_knowledge'] = f.read()
        except Exception as e:
            logger.error(f"Error loading region doc {doc_path}: {e}")
            state['knowledge_base']['region_knowledge'] = ""
    else:
        logger.warning(f"No region-specific doc found for {region} at {doc_path}")
        state['knowledge_base']['region_knowledge'] = ""

    # Load field IDs from JSON definition file (instead of regex-parsing markdown)
    fields_json_path = os.path.join(FILE_PATHS["REGION_DOCS_ROOT"], f"{region}_fields.json")
    if os.path.exists(fields_json_path):
        try:
            with open(fields_json_path, 'r') as f:
                field_def = json.load(f)
            region_fields = set(field_def.get("fields", []))
            if region_fields:
                _verify_and_load_region_fields(session, region_fields, state)
                logger.info(f"[Region: {region}] Loaded {len(region_fields)} fields from {region}_fields.json")
        except Exception as e:
            logger.error(f"Error loading region fields JSON {fields_json_path}: {e}")
    else:
        logger.warning(f"No region fields JSON found for {region} at {fields_json_path}")

# ========================================================================
# Alpha 信息获取
# ========================================================================

def _fetch_alpha_details(session, seed_alpha_id: str, state: WorkflowState) -> bool:
    """获取 Alpha 详情（代码、设置）"""
    try:
        session = ace_lib.check_session_and_relogin(session)
        alpha_details = utils.safe_api_call(ace_lib.get_simulation_result_json, session, seed_alpha_id)

        if not alpha_details:
            state["status"] = "failed"
            state["error_log"].append(f"Failed to fetch details for seed_alpha_id: {seed_alpha_id}")
            return False

        state["seed_expression"] = alpha_details.get("regular", {}).get("code", "")
        full_settings = alpha_details.get("settings", {})
        state["row_setting"] = full_settings
        state["seed_setting"] = {
            k: v for k, v in full_settings.items()
            if k in ["neutralization", "decay", "truncation"] and v is not None
        }
        return True
    except Exception as e:
        state["status"] = "failed"
        state["error_log"].append(f"FATAL: Could not fetch details for {seed_alpha_id}. Error: {e}")
        return False

def _get_performance_report(session, seed_alpha_id: str, state: WorkflowState) -> List[str]:
    """获取提交检查报告和年度统计，返回 FAIL 项名称列表"""
    session = ace_lib.check_session_and_relogin(session)
    check_df = utils.safe_api_call(ace_lib.get_check_submission, session, seed_alpha_id)
    fail_names = []

    if check_df is not None and not check_df.empty:
        filtered_df = check_df[check_df['result'].isin(['PASS', 'FAIL', 'WARNING'])]
        perf_lines = [f"name:{row['name']},result:{row['result']},limit:{row.get('limit', 'N/A')},value:{row.get('value', 'N/A')}"
                      for _, row in filtered_df.iterrows()]
        state["seed_performance_report"] = "\n".join(perf_lines)
        fail_names = filtered_df[filtered_df['result'] == 'FAIL']['name'].tolist()

        # 如果所有检查都是 PASS，额外调用平台 API 计算真实相关性
        if not fail_names:
            corr_parts = []
            prod_corr_result = utils.safe_api_call(ace_lib.check_prod_corr_test, session, alpha_id=seed_alpha_id, call_timeout=900)
            if prod_corr_result is not None and not prod_corr_result.empty:
                pc = prod_corr_result['value'].max()
                corr_parts.append(f"name:PROD_CORRELATION,result:{'PASS' if pc < 0.7 else 'FAIL'},limit:0.7,value:{pc:.4f}")
                if pc >= 0.7:
                    fail_names.append('PROD_CORRELATION')
            self_corr_result = utils.safe_api_call(ace_lib.check_self_corr_test, session, alpha_id=seed_alpha_id, call_timeout=900)
            if self_corr_result is not None and not self_corr_result.empty:
                sc = self_corr_result['value'].max()
                corr_parts.append(f"name:SELF_CORRELATION,result:{'PASS' if sc < 0.5 else 'FAIL'},limit:0.5,value:{sc:.4f}")
                if sc >= 0.5:
                    fail_names.append('SELF_CORRELATION')
            if corr_parts:
                state["seed_performance_report"] += "\n" + "\n".join(corr_parts)
    else:
        state["seed_performance_report"] = f"Could not retrieve submission checks for {seed_alpha_id}."

    stats_df = utils.safe_api_call(ace_lib.get_alpha_yearly_stats, session, seed_alpha_id)
    if stats_df is not None and not stats_df.empty:
        if 'alpha_id' in stats_df.columns:
            stats_df = stats_df.drop(columns=['alpha_id'])
        numeric_cols = stats_df.select_dtypes(include=['number']).columns
        stats_df[numeric_cols] = stats_df[numeric_cols].round(4)
        report_lines = [",".join([f"{col}:{val}" for col, val in row.items()]) for _, row in stats_df.iterrows()]
        state["yearly_performance_report"] = "\n".join(report_lines)
    else:
        state["yearly_performance_report"] = f"Could not retrieve yearly stats for {seed_alpha_id}."

    return fail_names

# ========================================================================
# 数据字段构建
# ========================================================================

def _build_data_fields(session, alpha_id: str, state: WorkflowState) -> List[str]:
    """构建可用字段列表和字段类别，强化数据获取的稳定性"""
    session = ace_lib.check_session_and_relogin(session)

    # 获取 Alpha 所使用的数据集，增加重试机制以应对网络/API不稳定
    # get_datasets_for_alpha 是一个复合过程，任何环节失效都会导致结果不完整
    all_datasets_df = pd.DataFrame()
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # get_datasets_for_alpha 内部已使用 safe_api_call，在此增加外层重试
            all_datasets_df, _ = utils.get_datasets_for_alpha(alpha_id, session)
            
            if all_datasets_df is not None and not all_datasets_df.empty:
                logger.info(f"Successfully fetched {len(all_datasets_df)} fields for alpha {alpha_id}")
                break
            else:
                logger.warning(f"Attempt {attempt + 1}: get_datasets_for_alpha returned empty for {alpha_id}")
        except Exception as e:
            logger.error(f"Attempt {attempt + 1}: get_datasets_for_alpha failed: {e}")
        
        if attempt < max_retries - 1:
            sleep_time = (attempt + 1) * 60 # 60s, 120s
            logger.info(f"Retrying get_datasets_for_alpha in {sleep_time}s...")
            time.sleep(sleep_time)
            session = ace_lib.check_session_and_relogin(session)

    field_categories = []
    if all_datasets_df is not None and not all_datasets_df.empty and 'category_id' in all_datasets_df.columns:
        field_categories = list(set(all_datasets_df['category_id'].dropna().tolist()))
    logger.info(f"Identified {len(field_categories)} dataset categories: {field_categories}")

    # 如果已缓存字段数据，直接返回
    if ("all_fields_df" in state and state["all_fields_df"] is not None
            and hasattr(state["all_fields_df"], 'empty') and not state["all_fields_df"].empty
            and "valid_data_fields" in state and state["valid_data_fields"]):
        logger.info(f"Using {len(state['valid_data_fields'])} existing valid data fields from cache.")
        return field_categories

    settings = state["row_setting"]
    region = settings.get("region")
    universe = settings.get("universe")
    delay = settings.get("delay", 1)

    grouping_fields_df = get_datafields_with_cache(session, region=region, universe=universe,
                                                    delay=delay, data_type='GROUP', dataset_id='pv1')
    price_fields_df = get_datafields_with_cache(session, region=region, universe=universe,
                                                 delay=delay, data_type='MATRIX', dataset_id='pv1')

    dfs_to_concat = []
    if all_datasets_df is not None and not all_datasets_df.empty:
        dfs_to_concat.append(all_datasets_df)
    if grouping_fields_df is not None and not grouping_fields_df.empty:
        dfs_to_concat.append(grouping_fields_df)
    if price_fields_df is not None and not price_fields_df.empty:
        dfs_to_concat.append(price_fields_df)

    all_fields_df = pd.concat(dfs_to_concat, ignore_index=True) if dfs_to_concat else pd.DataFrame()

    state["all_fields_df"] = all_fields_df
    state["valid_data_fields"] = set(all_fields_df['id'].unique()) if not all_fields_df.empty else set()

    # 添加额外字段
    extra_field_ids = []
    if region == 'ASI':
        extra_field_ids.extend(['rsk70_mfm2_asetrd_cnt_jpn', 'oth428_z_4_digit_number_for_jp_listed_companies'])

    if extra_field_ids:
        logger.info(f"Fetching platform metadata for {len(extra_field_ids)} extra fields...")
        extra_metadata_df = _fetch_field_metadata(session, extra_field_ids, state)
        if not extra_metadata_df.empty:
            state["all_fields_df"] = pd.concat([state["all_fields_df"], extra_metadata_df], ignore_index=True)
            state["valid_data_fields"].update(extra_metadata_df['id'].unique())

    logger.info(f"Loaded {len(state['valid_data_fields'])} total valid data fields.")
    return field_categories

# ========================================================================
# 知识库加载
# ========================================================================

def _load_knowledge_base(session, field_categories: List[str], fail_names: List[str], state: WorkflowState):
    """加载并整合知识库内容"""

    # 1. 加载数据集使用指南（去重）
    dataset_tips = []
    loaded_dataset_paths = set()
    for category in field_categories:
        doc_path = FILE_PATHS["HOW_TO_USE_DATASETS"].get(category.lower())
        if doc_path and os.path.exists(doc_path) and doc_path not in loaded_dataset_paths:
            with open(doc_path, 'r') as f:
                dataset_tips.append(f.read())
            loaded_dataset_paths.add(doc_path)

    # 2. 加载改进方法（去重）
    improvement_methods = []
    loaded_improve_paths = set()
    for fail in fail_names:
        doc_path = FILE_PATHS["IMPROVE_METHODS"].get(fail)
        if doc_path and os.path.exists(doc_path) and doc_path not in loaded_improve_paths:
            with open(doc_path, 'r') as f:
                improvement_methods.append(f.read())
            loaded_improve_paths.add(doc_path)

    # 3. 合并与长度控制（改进方法优先）
    imp_text = "\n\n".join(improvement_methods)
    tips_text = "\n\n".join(dataset_tips)

    state['knowledge_base'] = {}

    # 确保基础通用字段在 valid_data_fields 中
    universal_fields = {'cap', 'assets', 'returns', 'close', 'volume', 'open', 'high', 'low',
                        'vwap', 'market', 'sector', 'industry', 'subindustry', 'country', 'exchange'}
    state["valid_data_fields"].update(universal_fields)

    _load_region_knowledge(session, state)

    total_len = len(imp_text) + len(tips_text)
    if total_len > MAX_TOTAL_KB:
        if len(imp_text) >= MAX_TOTAL_KB:
            combined_kb = imp_text[:MAX_TOTAL_KB - 3] + '...'
        else:
            remaining = MAX_TOTAL_KB - len(imp_text)
            combined_kb = imp_text + "\n\n" + tips_text[:remaining - 3] + '...'
        state['knowledge_base'].update({
            'dataset_tips': "",
            'improvement_methods': combined_kb
        })
    else:
        state['knowledge_base'].update({
            'dataset_tips': tips_text,
            'improvement_methods': imp_text
        })

    kb_len = (len(state['knowledge_base'].get('dataset_tips', ''))
              + len(state['knowledge_base'].get('improvement_methods', ''))
              + len(state['knowledge_base'].get('region_knowledge', '')))
    logger.info(f"Knowledge base loaded. Total length: {kb_len} chars.")

# ========================================================================
# 相关性检查
# ========================================================================

def _check_correlations(session, alpha_id: str):
    """检查 prod/self/power pool 相关性，返回 (是否通过, 相关性数值字典)"""
    corr_values = {"pc": 0.0, "sc": 0.0, "ppc": 0.0}

    # Production correlation
    prod_corr_result = utils.safe_api_call(ace_lib.check_prod_corr_test, session, alpha_id=alpha_id, call_timeout=900)
    pc = prod_corr_result['value'].max() if prod_corr_result is not None and not prod_corr_result.empty else 1.0
    corr_values["pc"] = pc
    logger.info(f"Alpha {alpha_id} - Production correlation: {pc:.4f}")
    if pc >= 0.7:
        logger.info(f"Alpha {alpha_id} FAILED: pc={pc:.4f} >= 0.7")
        return False, corr_values

    # Self correlation
    self_corr_result = utils.safe_api_call(ace_lib.check_self_corr_test, session, alpha_id=alpha_id, call_timeout=900)
    sc = self_corr_result['value'].max() if self_corr_result is not None and not self_corr_result.empty else 1.0
    corr_values["sc"] = sc
    logger.info(f"Alpha {alpha_id} - Self correlation: {sc:.4f}")
    if sc >= 0.5:
        logger.info(f"Alpha {alpha_id} FAILED: sc={sc:.4f} >= 0.5")
        return False, corr_values

    # Power pool correlation
    ppc_result = utils.safe_api_call(ace_lib_ext.check_power_pool_corr_test, session, alpha_id=alpha_id, call_timeout=900)
    ppc = ppc_result['value'].max() if (ppc_result is not None and not ppc_result.empty
                                         and ppc_result['value'].notna().any()) else 0.0
    corr_values["ppc"] = ppc
    logger.info(f"Alpha {alpha_id} - Power Pool correlation: {ppc:.4f}")
    if ppc >= 0.5:
        logger.info(f"Alpha {alpha_id} FAILED: ppc={ppc:.4f} >= 0.5")
        return False, corr_values

    logger.info(f"Alpha {alpha_id} PASSED all correlation checks.")
    return True, corr_values

# ========================================================================
# LangGraph 节点 1: 获取种子详情
# ========================================================================

def fetch_seed_details(state: WorkflowState) -> WorkflowState:
    """获取初始种子 Alpha 信息及周边知识"""
    logger.info("--- Starting node 'fetch_seed_details' ---")
    iteration = state.get('iteration_count', 0)
    simulated_count = len(state.get('historical_alphas', []))
    if state.get("status_callback"):
        state["status_callback"]("Fetching seed details and related knowledge", 
                                  iteration=iteration + 1, 
                                  simulated_count=simulated_count)

    hist_alphas = state.get('historical_alphas', [])
    repeat_alphas = state.get('repeat_historical_alphas', [])
    total_hist = len(hist_alphas)
    total_repeat = len(repeat_alphas)
    total_generated = total_hist + total_repeat
    repeat_rate = (total_repeat / total_generated * 100) if total_generated > 0 else 0.0

    seed_score = -1.0
    if state.get('best_alpha') and state['best_alpha'].get('scores'):
        seed_score = state['best_alpha']['scores'].get('final_score', -1.0)

    logger.info(f"{'='*60}")
    logger.info(f"Iteration: {iteration + 1} | Seed: {state.get('seed_alpha_id')} | Score: {seed_score:.4f}")
    logger.info(f"History: {total_hist} alphas | Repeat rate: {repeat_rate:.2f}%")
    logger.info(f"{'='*60}")

    session = ace_lib.check_session_and_relogin(state["session"])
    state["session"] = session
    seed_alpha_id = state["seed_alpha_id"]

    # 重置本轮字段
    state.update({"proposed_alphas": [], "valid_candidates": [], "error_log": []})

    # 1. 获取 Alpha 详情和性能报告
    if not _fetch_alpha_details(session, seed_alpha_id, state):
        return state
    fail_names = _get_performance_report(session, seed_alpha_id, state)

    # 2. 构建数据字段和加载知识库
    field_categories = _build_data_fields(session, seed_alpha_id, state)
    _load_knowledge_base(session, field_categories, fail_names, state)

    # 3. 首轮初始化 best_alpha
    if state.get("iteration_count", 0) == 0:
        logger.info(f"First iteration: evaluating initial seed alpha {seed_alpha_id}")
        initial_alpha_package = evaluate_and_select_best_alpha(session, [seed_alpha_id])

        if initial_alpha_package:
            state['best_alpha'] = initial_alpha_package
            is_stats = initial_alpha_package['alpha_info'].get('is', {})
            state['initial_best_alpha'] = {
                'alpha_id': seed_alpha_id,
                'sharpe': is_stats.get('sharpe', 0),
                'fitness': is_stats.get('fitness', 0),
                'turnover': is_stats.get('turnover', 0)
            }
            logger.info(f"Initial best score: {state['best_alpha']['scores']['final_score']:.4f}")
        else:
            logger.warning(f"Initial seed alpha {seed_alpha_id} did not pass evaluation filters.")
            alpha_details = ace_lib.get_simulation_result_json(session, seed_alpha_id)
            is_stats = {}
            if alpha_details and 'is' in alpha_details:
                is_stats = {
                    'sharpe': alpha_details['is'].get('sharpe', 0),
                    'fitness': alpha_details['is'].get('fitness', 0),
                    'turnover': alpha_details['is'].get('turnover', 0)
                }
            state['best_alpha'] = {
                'alpha_info': {'id': seed_alpha_id, 'is': is_stats},
                'scores': {'final_score': -1.0}
            }
            state['initial_best_alpha'] = {
                'alpha_id': seed_alpha_id,
                'sharpe': is_stats.get('sharpe', 0),
                'fitness': is_stats.get('fitness', 0),
                'turnover': is_stats.get('turnover', 0)
            }

    logger.info("--- Node 'fetch_seed_details' completed. ---")
    return state

# ========================================================================
# LangGraph 节点 2: LLM 生成变体
# ========================================================================

def propose_and_generate_batch(state: WorkflowState) -> WorkflowState:
    """调用 LLM 生成 20 个 Alpha 表达式变体"""
    logger.info("--- Starting node 'propose_and_generate_batch' ---")
    if state.get("status") == "failed":
        logger.warning("Node 'propose_and_generate_batch' skipped due to failed status.")
        return state

    iteration = state.get('iteration_count', 0)
    simulated_count = len(state.get('historical_alphas', []))
    if state.get("status_callback"):
        state["status_callback"]("Calling LLM to generate variants", 
                                  iteration=iteration + 1, 
                                  simulated_count=simulated_count)
    logger.info("--- Proposing and generating alpha batch via LLM ---")

    session = ace_lib.check_session_and_relogin(state["session"])
    state["session"] = session

    # 1. 获取算子信息
    operators_summary = "Common mathematical and statistical operators."
    try:
        operators_df = utils.safe_api_call(ace_lib.get_operators, session)
        regular_ops = operators_df[operators_df['scope'].apply(
            lambda x: 'REGULAR' in x if isinstance(x, list) else x == 'REGULAR')]
        summary_lines = []
        for _, row in regular_ops.iterrows():
            summary_lines.append(f"{row['name']}: {row.get('definition', 'No definition')}")
        operators_summary = "\n".join(summary_lines)
    except Exception as e:
        logger.warning(f"Error fetching operators: {e}")

    # 2. 加载提示词模板
    try:
        with open(FILE_PATHS["PROMPT_PRINCIPLES"], "r") as f:
            prompt_principles = f.read()
        with open(FILE_PATHS["PROMPT_TEMPLATE"], "r") as f:
            prompt_examples = f.read()
    except FileNotFoundError as e:
        state["status"] = "failed"
        state["error_log"].append(f"FATAL: Could not read prompt file: {e}")
        logger.info("--- Node 'propose_and_generate_batch' completed (failed to read prompt). ---")
        return state

    # 3. 准备数据字段样本
    fields_df = state.get('all_fields_df')
    if fields_df is not None and not fields_df.empty:
        sample_size = min(1500, len(fields_df))
        datafields_sample = fields_df.sample(n=sample_size)[['id', 'description']].to_string(index=False)
    else:
        datafields_sample = "No data fields available."

    # 3.5 提取种子表达式中的字段，用于在 Prompt 中显式告知 LLM 保留 these 字段
    seed_expression = state.get('seed_expression', '')
    if seed_expression:
        try:
            seed_fields = utils.extract_datafields(seed_expression, session)
            seed_fields_list = sorted(seed_fields)
            logger.info(f"Seed expression fields to preserve: {seed_fields_list}")
        except Exception as e:
            seed_fields_list = []
            logger.warning(f"Failed to extract seed expression fields: {e}")
    else:
        seed_fields_list = []

    # 4. 历史记录
    hist_list = state.get('historical_alphas', [])
    hist_str = ""
    if hist_list:
        recent_hist = hist_list[-20:]
        hist_str = ("## Previous Attempts and Statistical Analysis\n"
                    "Analyze these previous attempts to avoid repeating mistakes. "
                    "Each entry records check metrics, correlation values, and margin.\n"
                    "- 'pc' (Production Correlation), 'sc' (Self Correlation), 'ppc' (Power Pool Correlation): "
                    "LOWER is better. Target pc < 0.7, sc < 0.5, ppc < 0.5.\n"
                    "- 'margin': HIGHER is better. Target > 0.0007.\n"
                    "Use this data to propose variants with improved (lower) correlations and higher margin:\n"
                    + "\n".join([f"- {item}" for item in recent_hist]))

    # 5. 知识库内容整合
    kb_content = ""
    fail_names = []
    seed_perf = state.get('seed_performance_report', '')
    if seed_perf:
        for line in seed_perf.split('\n'):
            if 'result:FAIL' in line:
                fail_names.append(line.split(',')[0].replace('name:', ''))

    if fail_names:
        kb_content += f"## Issues to Address\nFocus on resolving: {', '.join(fail_names)}\n\n"

    if state.get('knowledge_base', {}).get('improvement_methods'):
        kb_content += f"## Targeted Optimization Advice\n{state['knowledge_base']['improvement_methods']}\n\n"
    if state.get('knowledge_base', {}).get('dataset_tips'):
        kb_content += f"## Dataset Usage Tips\n{state['knowledge_base']['dataset_tips']}\n\n"
    if state.get('knowledge_base', {}).get('region_knowledge'):
        kb_content += f"## Region-Specific Advice ({state.get('row_setting', {}).get('region', 'USA')})\n{state['knowledge_base']['region_knowledge']}\n\n"

    # 6. 构造完整 Prompt
    prompt_parts = [
        "# Statistical Analysis Task: Mathematical Expression Variants\n\n",
        "You are a research assistant specializing in statistical modeling and mathematical expression analysis. "
        "Your task is to analyze the given mathematical expression and propose modified variants that may improve statistical properties.\n\n",
        "## 1. Input Expression\n",
        f"```\n{state.get('seed_expression', '')}\n```\n\n",
        "## 2. Configuration Parameters\n",
        f"```json\n{json.dumps(state.get('seed_setting', {}), indent=2)}\n```\n\n",
        "## 3. Performance Metrics\n",
        f"```\n{state.get('seed_performance_report', 'N/A')}\n```\n\n",
        "## 4. Historical Data Analysis\n",
        f"```\n{state.get('yearly_performance_report', 'N/A')}\n```\n\n",
        f"{hist_str}\n\n" if hist_str else "",
        f"{kb_content}\n" if kb_content else "",
        "## 5. Available Mathematical Operators\n",
        f"```\n{operators_summary}\n```\n\n",
        "## 6. Available Data Fields (Sample)\n",
        "Use these exact field IDs in your expressions:\n",
        f"```\n{datafields_sample}\n```\n\n",
        "## 7. CRITICAL: Data Field Preservation Rule\n",
        f"The seed expression in Section 1 uses these exact field IDs: {', '.join(seed_fields_list) if seed_fields_list else '(none)'}\n",
        "These field IDs MUST be preserved in your variants. The task is to optimize the expression by modifying "
        "operators, parameters, window sizes, neutralization methods, and expression structure — "
        "NOT by substituting data fields with different field IDs from the Available Data Fields list.\n"
        "Replacing a seed field with a different field ID changes the fundamental signal and defeats "
        "the purpose of optimization. Only replace a field ID if you have a specific, well-justified reason "
        "and you clearly explain it in your reasoning.\n\n",
        "## 8. Optimization Targets\n",
        "When proposing variants, please prioritize optimization goals in the following order:\n",
        "- Primary and mandatory: Resolve all 'FAIL' items listed in the Performance Metrics section.",
        "This is a prerequisite for optimization — any unresolved FAIL item is unacceptable.\n",
        "- Bonus items (optimize where possible)\n",
        "Minimize correlation metrics (pc, sc, ppc): lower correlation means the alpha is more unique and less redundant.\n",
        "Maximize margin: higher margin indicates stronger predictive signal and better profitability.\n\n",
        "## 9. Guidelines\n",
        f"{prompt_principles}\n\n",
        "## 10. Reference Examples\n",
        f"{prompt_examples}\n\n",
        "## 11. Output Format\n",
        "Generate 20 expression variants. For each variant, you MUST propose a 'new_setting' dictionary with:\n",
        f"- 'neutralization': MUST be chosen from: {NEUTRALIZATION_OPTIONS}\n",
        "- 'decay': MUST be an integer between 0 and 504 (inclusive).\n",
        "- 'truncation': MUST be a float between 0.0 and 1.0 (exclusive). Common values: 0.1 to 0.8.\n\n",
        "Return ONLY a JSON object with this structure:\n",
        "```json\n",
        "{\n",
        '  "proposals": [\n',
        "    {\n",
        '      "reasoning": "Brief technical justification for the modification",\n',
        '      "new_expression": "valid_mathematical_expression_here",\n',
        '      "new_setting": {"neutralization": "SLOW", "decay": 4, "truncation": 0.01}\n',
        "    }\n",
        "  ]\n",
        "}\n",
        "```\n"
    ]
    prompt = "\n".join(prompt_parts)

    # 保存 Prompt 到文件便于调试
    prompt_log_file = "data/last_prompt_sent_to_llm.txt"
    os.makedirs("data", exist_ok=True)
    with open(prompt_log_file, "w", encoding="utf-8") as f:
        f.write(prompt)
    logger.info(f"Prompt saved to {prompt_log_file} ({len(prompt)} chars)")

    # 7. 调用 LLM（带重试）
    model = ChatOpenAI(
        model=config_manager.get('llm_paid_model', 'deepseek-chat'),
        base_url=config_manager.get('llm_paid_base_url', 'https://api.deepseek.com'),
        api_key=config_manager.get('llm_paid_api_key'),
        temperature=0.7,
        request_timeout=600.0,
        max_tokens=4096,
    )

    max_retries = 3
    attempts = 0
    while attempts < max_retries:
        try:
            response = model.invoke(prompt)
            content = response.content.strip()

            # 尝试提取 JSON
            json_match = re.search(r'```json\s*(.*?)\s*```', content, re.DOTALL | re.IGNORECASE)
            if not json_match:
                json_match = re.search(r'```\s*(.*?)\s*```', content, re.DOTALL | re.IGNORECASE)
            if json_match:
                json_str = json_match.group(1).strip()
            else:
                json_match = re.search(r'(\{.*\})', content, re.DOTALL)
                json_str = json_match.group(1).strip() if json_match else None

            if json_str:
                parsed_json = json.loads(json_str)
                validated_proposal = AlphaProposal.model_validate(parsed_json)
                state["proposed_alphas"] = [p.model_dump() for p in validated_proposal.proposals]

                logger.info(f"LLM generated {len(state['proposed_alphas'])} candidates:")
                for i, p in enumerate(state["proposed_alphas"]):
                    logger.info(f"  >>> Candidate {i+1} >>>")
                    logger.info(f"  Reasoning: {p.get('reasoning')}")
                    logger.info(f"  Expression: {p.get('new_expression')}")
                    logger.info(f"  Settings: {p.get('new_setting')}")
                    logger.info(f"  <<< Candidate {i+1} <<<")
                logger.info("--- Node 'propose_and_generate_batch' completed. ---")
                return state
            else:
                truncated_resp = content[:500] + "..." if len(content) > 500 else content
                logger.warning(f"No JSON block found in response. First 500 chars:\n{truncated_resp}")
                raise ValueError("No JSON block found.")

        except (json.JSONDecodeError, ValidationError, ValueError) as e:
            attempts += 1
            logger.error(f"LLM attempt {attempts}/{max_retries} failed: {e}")
            if attempts >= max_retries:
                state["error_log"].append(f"LLM call failed after {max_retries} attempts: {e}")
            else:
                time.sleep(30)

    logger.info("--- Node 'propose_and_generate_batch' completed (all attempts failed). ---")
    return state

# ========================================================================
# LangGraph 节点 3: 批量验证
# ========================================================================

def batch_validate_and_process(state: WorkflowState) -> WorkflowState:
    """批量验证表达式合法性、字段存在性并进行类型自动补全"""
    logger.info("--- Starting node 'batch_validate_and_process' ---")
    iteration = state.get('iteration_count', 0)
    simulated_count = len(state.get('historical_alphas', []))
    if state.get("status_callback"):
        state["status_callback"](f"Validating {len(state['proposed_alphas'])} candidate expressions", 
                                  iteration=iteration + 1, 
                                  simulated_count=simulated_count)
    logger.info(f"--- Validating {len(state['proposed_alphas'])} candidates ---")

    session = state["session"]
    proposed_alphas = state["proposed_alphas"]
    valid_data_fields = state["valid_data_fields"]
    all_fields_df = state["all_fields_df"]
    row_setting = state["row_setting"]

    valid_candidates = []

    for candidate in proposed_alphas:
        # 验证 neutralization 选项
        new_setting = candidate.get("new_setting", {})
        neutralization = new_setting.get("neutralization")
        if neutralization and neutralization not in NEUTRALIZATION_OPTIONS:
            logger.warning(f"Invalid neutralization '{neutralization}' - removing from settings")
            new_setting.pop("neutralization")

        expression = candidate.get("new_expression")
        if not expression:
            logger.warning(f"Skipping candidate with no expression: {candidate.get('reasoning')}")
            continue

        # 过滤仅使用 PV1 字段的简单 Alpha
        if not utils.filter_alpha_by_datafields(expression, row_setting, session):
            logger.info(f"Alpha filtered out by pv1-only check: {expression[:80]}...")
            continue

        # 字段白名单验证
        is_fields_valid, field_error = utils.validate_expression_fields(expression, valid_data_fields, session)
        if not is_fields_valid:
            logger.warning(f"Field validation failed: {field_error}")
            continue

        # 语法验证
        validator = ExpressionValidator()
        validation_result = validator.check_expression(expression)
        is_syntax_valid = validation_result['valid']
        syntax_errors = validation_result['errors']

        if not is_syntax_valid:
            logger.warning(f"Syntax validation failed: {syntax_errors}")
            # Append the expression and error to a log file
            with open("syntax_error_log.txt", "a") as f:
                f.write(f"--- Syntax Error ---\n")
                f.write(f"Expression: {expression}\n")
                f.write(f"Reason: {syntax_errors}\n\n")
            continue

        # 类型自动补全
        try:
            completed_expression = utils.complete_expression(expression, all_fields_df, session)[0]
            candidate['processed_expression'] = completed_expression
            valid_candidates.append(candidate)
            logger.info(f"Valid candidate: {completed_expression}")
        except Exception as e:
            logger.warning(f"Type processing failed for '{expression}': {e}")

    state["valid_candidates"] = valid_candidates
    logger.info(f"Found {len(valid_candidates)} valid candidates for simulation.")
    logger.info("--- Node 'batch_validate_and_process' completed. ---")
    return state

def _log_iteration_summary(state: WorkflowState):
    """打印迭代汇总日志"""
    hist_alphas = state.get('historical_alphas', [])
    repeat_alphas = state.get('repeat_historical_alphas', [])
    total_hist = len(hist_alphas)
    total_repeat = len(repeat_alphas)
    total_generated = total_hist + total_repeat
    repeat_rate = (total_repeat / total_generated * 100) if total_generated > 0 else 0.0

    seed_score = -1.0
    if state.get('best_alpha') and state['best_alpha'].get('scores'):
        seed_score = state['best_alpha']['scores'].get('final_score', -1.0)

    logger.info(f"\n{'='*80}")
    logger.info(f"Iteration {state.get('iteration_count', 0)} Summary")
    logger.info(f"  Historical Alphas Total: {total_hist}")
    logger.info(f"  Repeated Alphas Total: {total_repeat}")
    logger.info(f"  Repeat Rate: {repeat_rate:.2f}%")
    logger.info(f"  Current Seed Alpha ID: {state.get('seed_alpha_id')}")
    logger.info(f"  Current Seed Alpha Score: {seed_score:.4f}")
    if state.get('best_alpha') and state['best_alpha'].get('alpha_info'):
        best_id = state['best_alpha']['alpha_info'].get('id', 'N/A')
        is_stats = state['best_alpha']['alpha_info'].get('is', {})
        logger.info(f"  Best Alpha ID: {best_id}")
        logger.info(f"    Sharpe: {is_stats.get('sharpe', 0):.4f}, "
                    f"Fitness: {is_stats.get('fitness', 0):.4f}, "
                    f"Turnover: {is_stats.get('turnover', 0):.4f}")
    logger.info(f"{'='*80}\n")

# ========================================================================
# LangGraph 节点 4: 批量模拟并选优
# ========================================================================

def batch_simulate_and_select_best(state: WorkflowState) -> WorkflowState:
    """批量提交模拟、收集结果、更新历史、选出最优"""
    logger.info("--- Starting node 'batch_simulate_and_select_best' ---")
    if state.get("status") == "failed":
        logger.warning("Node 'batch_simulate_and_select_best' skipped due to failed status.")
        return state
    session = ace_lib.check_session_and_relogin(state["session"])
    state["session"] = session
    state["iteration_count"] += 1

    iteration = state.get('iteration_count', 0)
    simulated_count = len(state.get('historical_alphas', []))
    if state.get("status_callback"):
        state["status_callback"](f"Simulating and selecting best candidates (total: {len(state['valid_candidates'])})", 
                                  iteration=iteration, 
                                  simulated_count=simulated_count)
    logger.info("--- Batch simulating and selecting best ---")

    valid_candidates = state["valid_candidates"]
    if not valid_candidates:
        logger.info("No valid candidates to simulate. Incrementing counter.")
        state['no_valid_candidates_counter'] = state.get('no_valid_candidates_counter', 0) + 1
        if state.get('best_alpha') and state['best_alpha'].get('alpha_info'):
            state['seed_alpha_id'] = state['best_alpha']['alpha_info'].get('id', state['seed_alpha_id'])
        _log_iteration_summary(state)
        logger.info("--- Node 'batch_simulate_and_select_best' completed (no valid candidates). ---")
        return state

    state['no_valid_candidates_counter'] = 0

    # 1. 准备模拟任务
    sim_jobs = []
    for candidate in valid_candidates:
        sim_settings = state["row_setting"].copy()
        sim_settings.update(candidate["new_setting"])
        sim_settings['testPeriod'] = 'P0Y'
        sim_settings['max_trade'] = 'OFF'
        sim_jobs.append({
            "type": "REGULAR",
            "settings": sim_settings,
            "regular": candidate["processed_expression"]
        })

    # 2. 分块批量模拟（每批 3 个，带超时和重试）
    chunk_size = 3
    chunks = [sim_jobs[i:i + chunk_size] for i in range(0, len(sim_jobs), chunk_size)]
    batch_results = []

    for idx, chunk in enumerate(chunks):
        max_retries = 5
        attempts = 0
        timeout_duration = 30 * 60  # 30 分钟超时

        logger.info(f"--- Batch {idx + 1}/{len(chunks)} ({len(chunk)} alphas) ---")

        while attempts < max_retries:
            try:
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(ace_lib.simulate_multi_alpha, session, chunk)
                    chunk_results = future.result(timeout=timeout_duration)

                if chunk_results:
                    batch_results.extend(chunk_results)
                    logger.info(f"Batch {idx + 1} completed successfully.")
                    break
                else:
                    raise ValueError(f"Batch {idx + 1} returned no results.")

            except (concurrent.futures.TimeoutError, RemoteDisconnected, ConnectionError, Exception) as e:
                attempts += 1
                error_msg = f"Batch {idx + 1}, attempt {attempts}/{max_retries}: {e}"
                logger.error(error_msg)
                state["error_log"].append(error_msg)
                if attempts >= max_retries:
                    logger.error(f"Batch {idx + 1} FAILED after {max_retries} attempts.")
                    break
                time.sleep(300)
                session = ace_lib.check_session_and_relogin(session)
                state["session"] = session

    logger.info(f"All batches processed. Results: {len(batch_results)}/{len(sim_jobs)}")

    # 3. 处理模拟结果
    candidates_for_evaluation = []  # 收集预获取的候选数据，避免后续重复 API 调用
    historical_alphas = state.get('historical_alphas', [])
    repeat_historical_alphas = state.get('repeat_historical_alphas', [])
    expressions_added_count = 0
    session = ace_lib.check_session_and_relogin(session)

    # 历史去重集合
    def _parse_hist_entry(entry):
        parts = entry.split(' | ')
        exp = parts[0].replace('Expression: ', '')
        neut = "UNKNOWN"
        for p in parts:
            if p.startswith('Neutralization: '):
                neut = p.replace('Neutralization: ', '')
                break
        return (exp, neut)

    seen_configs = {_parse_hist_entry(entry) for entry in historical_alphas}

    for result in batch_results:
        alpha_id = result.get("alpha_id")
        if not alpha_id:
            logger.warning("A simulation job failed to produce an alpha_id.")
            continue

        try:
            alpha_details = utils.safe_api_call(
                lambda s, aid: s.get(f"https://api.worldquantbrain.com/alphas/{aid}").json(),
                session, alpha_id
            )
            if not alpha_details:
                logger.warning(f"Failed to fetch details for alpha {alpha_id}")
                continue
            expression = alpha_details.get('regular', {}).get('code')
            if not expression:
                logger.warning(f"Could not find expression in details for alpha {alpha_id}")
                continue
            neutralization = alpha_details.get('settings', {}).get('neutralization', 'UNKNOWN')
        except Exception as e:
            logger.error(f"Failed to fetch details for alpha {alpha_id}: {e}")
            continue

        check_df = utils.safe_api_call(ace_lib.get_check_submission, session, alpha_id)
        has_fail = 'FAIL' in check_df['result'].unique() if check_df is not None else True

        # 仅当其他检查全部通过时才调用相关性检查（避免浪费平台计算资源）
        corr_values = None
        corr_passed = True
        if not has_fail:
            corr_passed, corr_values = _check_correlations(session, alpha_id)

        stats_df = utils.safe_api_call(ace_lib.get_alpha_yearly_stats, session, alpha_id)

        # 自动标记通过硬过滤的 Alpha，同时收集候选数据供后续评分
        if alpha_details and check_df is not None and stats_df is not None:
            candidate_for_tagging = {
                "id": alpha_id,
                "details": alpha_details,
                "check_df": check_df,
                "yearly_stats": stats_df,
                "is": alpha_details.get("is", {})
            }
            candidates_for_evaluation.append(candidate_for_tagging)
            if _passes_hard_filters(candidate_for_tagging):
                try:
                    if corr_values is not None:
                        pc_name = f"{corr_values.get('pc', 0.0):.4f}"
                        utils.safe_api_call(ace_lib.set_alpha_properties, session,
                                            alpha_id=alpha_id, tags=["deep_search_result"], name=pc_name)
                        logger.info(f"Alpha {alpha_id} PASSED hard filters - tagged and named {pc_name}")
                    else:
                        utils.safe_api_call(ace_lib.set_alpha_properties, session,
                                            alpha_id=alpha_id, tags=["deep_search_result"])
                        logger.info(f"Alpha {alpha_id} PASSED hard filters - tagged 'deep_search_result'")
                except Exception as e:
                    logger.warning(f"Could not tag alpha {alpha_id}: {e}")
            else:
                logger.info(f"Alpha {alpha_id} FAILED hard filters - skipping tag.")

        # 更新历史记录（去重）
        current_config = (expression, neutralization)
        if current_config in seen_configs:
            repeat_historical_alphas.append(f"Expression: {expression} | Neutralization: {neutralization}")
        else:
            # 提取关键检查指标
            stats_str_parts = []
            added_prod_corr = False
            added_self_corr = False
            if check_df is not None and not check_df.empty:
                check_dict = check_df.set_index('name')['value'].to_dict()
                metrics_to_include = [
                    'LOW_SHARPE', 'LOW_FITNESS', 'LOW_TURNOVER',
                    'CONCENTRATED_WEIGHT', 'LOW_SUB_UNIVERSE_SHARPE',
                    'LOW_ROBUST_UNIVERSE_SHARPE', 'LOW_2Y_SHARPE',
                    'LOW_ASI_JPN_SHARPE', 'LOW_INVESTABILITY_CONSTRAINED_SHARPE',
                    'LOW_ROBUST_UNIVERSE_SHARPE.WITH_RATIO', 'LOW_ROBUST_UNIVERSE_RETURNS',
                    'IS_LADDER_SHARPE', 'SELF_CORRELATION', 'PROD_CORRELATION'
                ]
                for metric in metrics_to_include:
                    value = check_dict.get(metric)
                    if pd.notna(value):
                        if isinstance(value, float):
                            val_str = f"{value:.4f}"
                        else:
                            val_str = f"{value}"
                        stats_str_parts.append(f"'{metric}': {val_str}")

                        if metric == 'PROD_CORRELATION':
                            added_prod_corr = True
                            logger.info(f"Alpha {alpha_id} - Extracted PROD_CORRELATION from checks: {val_str}")
                        elif metric == 'SELF_CORRELATION':
                            added_self_corr = True
                            logger.info(f"Alpha {alpha_id} - Extracted SELF_CORRELATION from checks: {val_str}")
            # 追加 margin（始终包含）
            margin = alpha_details.get("is", {}).get("margin")
            if margin is not None and pd.notna(margin):
                stats_str_parts.append(f"'margin': {margin:.6f}")
            # 仅当实际调用了相关性检查时才追加相关性数据
            if corr_values is not None:
                if not added_prod_corr:
                    stats_str_parts.append(f"'pc': {corr_values['pc']:.4f}")
                if not added_self_corr:
                    stats_str_parts.append(f"'sc': {corr_values['sc']:.4f}")
                stats_str_parts.append(f"'ppc': {corr_values['ppc']:.4f}")
            stats_str = ", ".join(stats_str_parts)
            historical_alpha_entry = f"Expression: {expression} | Neutralization: {neutralization} | Checks: {{ {stats_str} }}"
            historical_alphas.append(historical_alpha_entry)
            seen_configs.add(current_config)
            expressions_added_count += 1

        # 检查是否直接成功（零 FAIL + 充足数据 + 通过相关性检查）
        if stats_df is not None:
            active_years = (stats_df['sharpe'] != 0.0).sum()

            if not has_fail and active_years >= 8:
                if corr_passed:
                    state["status"] = "succeeded"
                    state["success_id"] = alpha_id
                    state['historical_alphas'] = historical_alphas
                    state['repeat_historical_alphas'] = repeat_historical_alphas
                    logger.info(f"SUCCESS! Alpha {alpha_id} passed all checks and correlations!")
                    _log_iteration_summary(state)
                    logger.info("--- Node 'batch_simulate_and_select_best' completed (success). ---")
                    return state
                else:
                    logger.info(f"Alpha {alpha_id} passed checks but failed correlations.")

            # 数据不足：移除不良字段
            if 0 < active_years < 8:
                logger.info(f"Alpha {alpha_id} has insufficient data ({active_years} years). Removing problematic fields...")
                settings = state["row_setting"]
                region = settings.get("region")
                universe = settings.get("universe")
                delay = settings.get("delay", 1)

                grouping_fields_df = get_datafields_with_cache(session, region=region, universe=universe,
                                                                delay=delay, data_type='GROUP', dataset_id='pv1')
                price_fields_df = get_datafields_with_cache(session, region=region, universe=universe,
                                                             delay=delay, data_type='MATRIX', dataset_id='pv1')

                pv1_fields = set()
                if grouping_fields_df is not None:
                    pv1_fields.update(grouping_fields_df['id'].tolist())
                if price_fields_df is not None:
                    pv1_fields.update(price_fields_df['id'].tolist())

                used_fields = utils.extract_datafields(expression, session)
                fields_to_remove = {field for field in used_fields if field not in pv1_fields}

                if fields_to_remove:
                    logger.info(f"Removing {len(fields_to_remove)} fields with insufficient history")
                    state["valid_data_fields"].difference_update(fields_to_remove)
                    state["all_fields_df"] = state["all_fields_df"][
                        ~state["all_fields_df"]['id'].isin(fields_to_remove)]

    state['historical_alphas'] = historical_alphas
    state['repeat_historical_alphas'] = repeat_historical_alphas
    if expressions_added_count > 0:
        logger.info(f"Added {expressions_added_count} new expressions. Total history: {len(historical_alphas)}")

    if not candidates_for_evaluation:
        logger.info("No candidates available for evaluation.")
        if state.get('best_alpha') and state['best_alpha'].get('alpha_info'):
            state['seed_alpha_id'] = state['best_alpha']['alpha_info'].get('id', state['seed_alpha_id'])
        _log_iteration_summary(state)
        logger.info("--- Node 'batch_simulate_and_select_best' completed (no candidates to evaluate). ---")
        return state

    # 4. 使用务实评分选出最优（基于已获取数据，无需额外 API 调用）
    logger.info(f"Scoring {len(candidates_for_evaluation)} candidates from pre-fetched data...")
    best_of_batch = select_best_from_candidates(candidates_for_evaluation)

    if best_of_batch:
        batch_best_score = best_of_batch['scores']['final_score']
        global_best_score = state.get('best_alpha', {}).get('scores', {}).get('final_score', -1.0)

        logger.info(f"Best of batch score: {batch_best_score:.4f} (ID: {best_of_batch['alpha_info']['id']})")
        logger.info(f"Current global best score: {global_best_score:.4f}")

        if batch_best_score >= global_best_score:
            state['best_alpha'] = best_of_batch
            logger.info(f"NEW global best! ID: {best_of_batch['alpha_info']['id']}, Score: {batch_best_score:.4f}")
        else:
            logger.info("No improvement over global best alpha.")
    else:
        logger.info("No alphas in batch passed evaluation filters.")

    # 5. 设置下一轮种子
    if state.get('best_alpha') and state['best_alpha'].get('alpha_info'):
        state['seed_alpha_id'] = state['best_alpha']['alpha_info'].get('id', state['seed_alpha_id'])

    _log_iteration_summary(state)
    logger.info("--- Node 'batch_simulate_and_select_best' completed. ---")
    return state

# ========================================================================
# 条件判断：继续或结束
# ========================================================================

def continue_or_end(state: WorkflowState) -> str:
    """判断工作流是否继续循环"""
    if state.get("status") == "succeeded":
        logger.info("Workflow SUCCEEDED.")
        return "end"

    if state.get("status") == "failed" and state.get("error_log"):
        logger.info("Workflow FAILED due to errors.")
        return "end"

    if len(state.get('historical_alphas', [])) > 100:
        logger.info("Max iterations reached (100+ historical alphas).")
        state["status"] = "max_iterations_reached"
        return "end"

    if state.get('no_valid_candidates_counter', 0) >= 3:
        logger.info("No valid candidates for 3 consecutive iterations. Ending workflow.")
        state["status"] = "no_valid_candidates_repeatedly"
        return "end"

    return "fetch_seed_details"

# ========================================================================
# 构建 LangGraph 工作流
# ========================================================================

def build_graph():
    """构建 LangGraph 状态图"""
    workflow = StateGraph(WorkflowState)

    workflow.add_node("fetch_seed_details", fetch_seed_details)
    workflow.add_node("propose_and_generate_batch", propose_and_generate_batch)
    workflow.add_node("batch_validate_and_process", batch_validate_and_process)
    workflow.add_node("batch_simulate_and_select_best", batch_simulate_and_select_best)

    workflow.set_entry_point("fetch_seed_details")

    workflow.add_edge("fetch_seed_details", "propose_and_generate_batch")
    workflow.add_edge("propose_and_generate_batch", "batch_validate_and_process")
    workflow.add_edge("batch_validate_and_process", "batch_simulate_and_select_best")

    workflow.add_conditional_edges(
        "batch_simulate_and_select_best",
        continue_or_end,
        {"end": END, "fetch_seed_details": "fetch_seed_details"}
    )

    return workflow.compile()

# ========================================================================
# 入口函数
# ========================================================================

def run_optimization_workflow(seed_alpha_id: str, status_callback: Optional[Callable] = None) -> dict:
    """
    运行完整的 Alpha 优化工作流。

    Args:
        seed_alpha_id: 种子 Alpha ID
        status_callback: 状态更新回调函数 (可选)

    Returns:
        包含最终状态和结果信息的字典
    """
    # 1. 测试 LLM 连接
    if not test_llm_connection():
        logger.error("LLM connection test failed.")
        return {"error": "LLM connection test failed. Check configuration and network."}

    logger.info(f"Initializing optimization workflow for seed: {seed_alpha_id}")

    # 2. 创建会话
    try:
        session = ace_lib.start_session()
        if not session:
            return {"error": "ACE session creation failed"}
    except Exception as e:
        logger.error(f"Session creation failed: {e}")
        return {"error": f"Session creation failed: {e}"}

    # 3. 构建工作流图
    app = build_graph()

    # 4. 初始化状态
    initial_state = {
        "session": session,
        "seed_alpha_id": seed_alpha_id,
        "valid_data_fields": set(),
        "all_fields_df": pd.DataFrame(),
        "seed_expression": "",
        "seed_setting": {},
        "row_setting": {},
        "seed_performance_report": "",
        "yearly_performance_report": "",
        "proposed_alphas": [],
        "valid_candidates": [],
        "historical_alphas": [],
        "repeat_historical_alphas": [],
        "knowledge_base": {},
        "status": "running",
        "success_id": None,
        "error_log": [],
        "best_alpha": {},
        "initial_best_alpha": {},
        "iteration_count": 0,
        "no_valid_candidates_counter": 0,
        "status_callback": status_callback,
    }

    final_state = None
    logger.info("Starting workflow execution...")

    try:
        for output in app.stream(initial_state, config={'recursion_limit': 100}):
            for key, value in output.items():
                logger.info(f"Node '{key}' completed.")
                final_state = value
        logger.info("Workflow finished successfully.")
    except Exception as e:
        logger.error(f"Workflow execution crashed: {e}")
        if final_state:
            final_state.setdefault('error_log', []).append(f"Workflow crashed: {e}")
        else:
            final_state = {"error": f"Workflow crashed: {e}"}

    return final_state

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python src/alpha_optimization_workflow.py <alpha_id>")
        sys.exit(1)
    
    alpha_id = sys.argv[1]
    result = run_optimization_workflow(alpha_id)
    print(f"\nWorkflow finished. Final status: {result.get('status')}")
    if result.get('success_id'):
        print(f"Success Alpha ID: {result.get('success_id')}")
    elif result.get('best_alpha'):
        best_id = result['best_alpha'].get('alpha_info', {}).get('id', 'N/A')
        print(f"Best candidate found: {best_id}")

