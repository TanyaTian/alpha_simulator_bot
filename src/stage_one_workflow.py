import pandas as pd
from pydantic import BaseModel, Field, ValidationError
from typing import TypedDict, List, Optional, Dict, Any, Union
from langgraph.graph import StateGraph, END
from langchain_openai import ChatOpenAI
import json
import time
import re
from datetime import datetime
import os
from requests.exceptions import ConnectionError
from http.client import RemoteDisconnected
import concurrent.futures
import logging

# 导入本地模块
from ace_lib import (
    start_session, check_session_and_relogin, get_datafields,
    simulate_multi_alpha, get_check_submission, get_alpha_yearly_stats,
    get_simulation_result_json, get_operators, check_prod_corr_test,
    set_alpha_properties, get_alpha_pnl
)
from utils import extract_datafields, load_config, filter_alpha_by_datafields, validate_expression_fields, complete_expression, safe_api_call
from expression_validator import check_expression_syntax
from dao import StageOneSignalDAO
from logger import Logger
from self_corr_calculator import calc_self_corr, load_data, download_data, get_alpha_pnls
from cached_data_fetcher import get_datafields_with_cache

logger = Logger()
# 设置日志级别为 DEBUG 以方便调试
#logger.logger.setLevel(logging.DEBUG)

# --- 0. 定义常量 ---
# 定义允许的算子列表，用于过滤 operators
ALLOWED_OPERATORS = [
    # --- Arithmetic ---
    #"add",
    #"subtract",
    #"multiply",
    #"divide",
    "abs",
    "sign",
    "log",
    "power",
    "signed_power",
    "sqrt",
    #"inverse",
    #"reverse",
    "max",
    "min",
    "round",
    "round_down",
    "arc_tan",

    # --- Logical / Comparison ---
    #"less",
    #"less_equal",
    #"greater",
    #"greater_equal",
    #"equal",
    #"not_equal",
    "and",
    "or",
    "not",
    "if_else",
    "is_nan",
    "is_not_finite",

    # --- Data Cleaning ---
    "pasteurize",
    "purify",
    "to_nan",
    "nan_mask",

    # --- Time Series (Stats & Structure) ---
    "ts_mean",
    "ts_std_dev",
    "ts_sum",
    "ts_product",
    "ts_max",
    "ts_min",
    "ts_delta",
    "ts_returns",
    "ts_decay_linear",
    "ts_av_diff",
    "ts_max_diff",
    "ts_kurtosis",
    "ts_delay",
    "days_from_last_change",
    "last_diff_value",
    "ts_step",
    "ts_count_nans",
    "ts_arg_max",
    "ts_arg_min",
    "ts_backfill",
    "jump_decay",
    "hump",
    "kth_element",
    "ts_zscore",
    "ts_skewness",

    # --- Vector (Intraday/Array) ---
    "vec_avg",
    "vec_sum",
    "vec_min",
    "vec_max",
    "vec_stddev",
    "vec_range",
    "vec_norm",
    "vec_count",

    # --- Transformation ---
    "clamp",
    "tail",
    "filter",
    "keep",
    "zscore",

    # --- 多字段关系 ---
    "ts_corr",
    "ts_regression"
]


# --- Global Constants ---
NEUTRALIZATION_OPTIONS = ["REVERSION_AND_MOMENTUM",  "CROWDING", "FAST", "SLOW",  "SLOW_AND_FAST", "MARKET", "SECTOR", "INDUSTRY", "SUBINDUSTRY"]


# --- 1. 定义 Pydantic 模型用于 LLM 结构化输出 ---

class SearchDirection(BaseModel):
    """Stage1 节点输出：纯数学统计驱动的 Alpha 搜索方向"""
    core_idea: str = Field(description="核心数学/统计直觉，不含任何经济学解释")
    differentiation: str = Field(description="与已尝试alpha的差异点：窗口/变换路径/结构类型")
    target_structure: str = Field(description="预期捕捉的结构类型：如波动异常/多尺度背离/分布偏斜")
    constraints: str = Field(description="需要避免的陷阱：过拟合形态/重复路径/分布坍缩")

class DirectionNodeOutput(BaseModel):
    directions: List[SearchDirection] = Field(description="三个搜索方向")

class ProposedAlpha(BaseModel):
    """表示 LLM 提出的单个 Alpha 候选"""
    reasoning: str = Field(description="提出更改的原因")
    new_expression: str = Field(description="新的、修改后的 Alpha 表达式")
    new_setting: dict = Field(description="包含 'neutralization', 'decay', 'truncation' 新设置的字典")

class AlphaProposal(BaseModel):
    """表示一批提出的 Alpha 候选"""
    proposals: List[ProposedAlpha] = Field(description="生成的 alpha 候选列表")

# --- 2. 定义图的状态 ---
class StageOneState(TypedDict):
    """表示 stage one 工作流的状态"""
    # --- 输入字段 ---
    session: Any  # 认证的 ace_lib 会话
    dataset_ids: List[str]  # 数据集 ID 列表
    category: str # 数据集类别
    region: str  # 区域
    universe: str  # 股票池
    delay: int  # 延迟
    date_time: str  # 数据时间 YYYYMMDD
    
    # --- 数据字段 ---
    valid_data_fields: Any  # 有效数据字段 (DataFrame 或 List[Dict])
    search_directions: List[SearchDirection]  # 搜索方向列表
    
    # --- Alpha 候选 ---
    generated_expressions: List[ProposedAlpha]  # 节点 3 生成的 Alpha 候选（包含表达式、设置和原因）
    valid_expressions: List[ProposedAlpha]  # 节点 3.5 验证通过的 Alpha 候选，准备提交仿真
    
    # --- 中间状态字段 (拆分 simulate_filter_and_save 后新增) ---
    current_batch_alpha_ids: List[str] # 当前批次仿真生成的 alpha id 列表
    passed_filter_alphas: List[Dict[str, Any]] # 通过过滤等待相关性检查的 alpha 信息 [{'id': 'xxx', 'fitness': 1.2}, ...]

    # --- 历史记录 ---
    historical_alphas: List[str]  # 历史生成的 alpha 表达式列表
    repeat_historical_alphas: List[str]  # 重复生成的 alpha 表达式列表
    saved_alphas_ids: List[str]  # 已保存的 alpha ID 列表
    
    # --- 工作流状态和结果 ---
    iteration_count: int  # 迭代计数器
    total_saved_count: int  # 总共保存的 alpha 数量
    insufficient_data_count: int  # 数据历史不足的 alpha 数量
    consecutive_empty_candidates: int # 连续产生的无效候选批次数
    status: str  # 状态：'running', 'completed', 'failed'
    error_log: List[str]  # 错误日志

# --- LLM Setup ---
model = None

def load_llm_config():
    """Loads LLM configuration from config.ini using utils.load_config."""
    try:
        config_data = load_config("config/config.ini")
        if not config_data:
            logger.error(f"config_data is enpty")
            return {{}}
        # Use general LLM configuration (matches user's DeepSeek config)
        return {
            "model": config_data.get("llm_model"),
            "base_url": config_data.get("llm_base_url"),
            "api_key": config_data.get("llm_api_key")
        }
    except Exception as e:
        logger.error(f"Error loading LLM config: {e}")
        return {}

def initialize_model():
    """Initializes the single LLM used for batch generation."""
    global model
    llm_config = load_llm_config()
    if not all(llm_config.values()):
        logger.error("❌ LLM configuration is missing or incomplete. Cannot initialize model.")
        return False
    try:
        model = ChatOpenAI(
            model=llm_config["model"],
            base_url=llm_config["base_url"],
            api_key=llm_config["api_key"],
            temperature=0.7, 
            request_timeout=180.0,
        )
        logger.info("✅ LLM model initialized successfully.")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to initialize LLM model: {e}")
        return False

def get_model(temperature=0.7):
    """Helper to get a model instance with specific temperature if needed, or reuse global."""
    # Since ChatOpenAI is immutable regarding temperature after init usually,
    # and we want different temperatures, we might need to re-instantiate or use `bind` if supported.
    # For simplicity, we can just re-instantiate lightly or use the global one if temp doesn't matter too much.
    # But strictly following the logic:
    llm_config = load_llm_config()
    return ChatOpenAI(
        model=llm_config["model"],
        base_url=llm_config["base_url"],
        api_key=llm_config["api_key"],
        temperature=temperature,
        request_timeout=180.0,
    )

# --- 3. 定义图的节点（函数） ---

def initialize_and_load_data(state: StageOneState) -> StageOneState:
    """
    节点 1: 初始化工作流状态并加载数据
    如果 valid_data_fields 为空，则根据配置加载数据集
    """
    task_info = f"[Region: {state.get('region')} | Universe: {state.get('universe')} | Delay: {state.get('delay')} | Dataset: {state.get('dataset_ids')}]"
    logger.info(f"{task_info} --- 开始节点 1: 初始化与数据加载 ---")
    logger.debug(f"Input State Keys: {state.keys()}")
    
    try:
        # 4. 加载数据字段
        if state.get("valid_data_fields") is None or (isinstance(state["valid_data_fields"], (list, set)) and len(state["valid_data_fields"]) == 0) or (isinstance(state["valid_data_fields"], pd.DataFrame) and state["valid_data_fields"].empty):
            logger.info("开始加载数据字段...")
            session = check_session_and_relogin(state["session"])
            state["session"] = session
            
            all_fields_dfs = []
            
            # 4.1 加载目标数据集字段
            for ds_id in state["dataset_ids"]:
                logger.info(f"获取数据集 {ds_id} 的字段...")
                logger.debug(f"Calling get_datafields: region={state['region']}, universe={state['universe']}, delay={state['delay']}, dataset_id={ds_id}")
                df = get_datafields_with_cache(
                    session,
                    region=state["region"],
                    delay=state["delay"],
                    universe=state["universe"],
                    dataset_id=ds_id,
                    data_type="ALL"
                )
                if df is not None and not df.empty:
                    logger.debug(f"Retrieved {len(df)} fields for dataset {ds_id}")
                    all_fields_dfs.append(df)
                else:
                    logger.warning(f"No fields found for dataset {ds_id}")
            
            if not all_fields_dfs:
                error_msg = "未获取到任何有效数据字段"
                logger.error(error_msg)
                state["status"] = "failed"
                state["error_log"].append(error_msg)
                logger.info(f"{task_info} --- 节点 1 结束: 失败 (无有效字段) ---")
                return state
            
            # 合并所有字段
            combined_df = pd.concat(all_fields_dfs, ignore_index=True).drop_duplicates(subset=['id'])
            
            # 过滤掉 GROUP 类型的字段，因为 Stage One 不使用分组算子
            combined_df = combined_df[combined_df['type'] != 'GROUP']
            
            state["valid_data_fields"] = combined_df
            logger.info(f"数据加载完成，共 {len(combined_df)} 个字段")
            logger.debug(f"Combined fields sample: {combined_df.head(2).to_dict(orient='records')}")
        else:
            logger.info("数据字段已存在，跳过加载")

    except Exception as e:
        logger.error(f"节点 1 执行失败: {e}", exc_info=True)
        state["status"] = "failed"
        state["error_log"].append(f"节点 1 执行失败: {e}")
        logger.info(f"{task_info} --- 节点 1 结束: 异常失败 ---")
        return state
    
    field_count = len(state["valid_data_fields"]) if isinstance(state["valid_data_fields"], pd.DataFrame) else 0
    logger.info(f"{task_info} --- 节点 1 结束: 成功 (加载 {field_count} 个字段) ---")
    return state

def generate_search_directions(state: StageOneState) -> StageOneState:
    """
    节点 2: 生成搜索方向
    调用 LLM 生成纯数学统计驱动的 Alpha 搜索方向
    """
    task_info = f"[Region: {state.get('region')} | Universe: {state.get('universe')} | Delay: {state.get('delay')} | Dataset: {state.get('dataset_ids')}]"
    
    # 打印更显著的迭代汇总日志（放在每次迭代的最开始）
    total_historical = len(state.get("historical_alphas", []))
    total_repeat = len(state.get("repeat_historical_alphas", []))
    total_generated = total_historical + total_repeat
    repeat_rate = (total_repeat / total_generated * 100) if total_generated > 0 else 0
    
    logger.info(f"\n{'='*80}\n"
                f"🔄 迭代汇总 | Iteration: {state.get('iteration_count', 0)}\n"
                f"📈 历史 Alpha 总数: {total_historical}\n"
                f"🔄 重复 Alpha 总数: {total_repeat}\n"
                f"📉 数据不足移除: {state.get('insufficient_data_count', 0)}\n"
                f"📊 Alpha 表达式重复生成率: {repeat_rate:.2f}%\n"
                f"✅ 当前累计保存: {state.get('total_saved_count', 0)}\n"
                f"{'='*80}")
    
    logger.info(f"{task_info} --- 开始节点 2: 生成搜索方向 ---")
    
    try:
        # 准备数据
        valid_fields_df = state["valid_data_fields"]
        if isinstance(valid_fields_df, pd.DataFrame):
            dataset_fields_str = valid_fields_df[['id', 'description', 'type', 'coverage']].to_string(index=False)
        else:
             dataset_fields_str = str(valid_fields_df)
        
        logger.debug(f"Using dataset fields string length: {len(dataset_fields_str)}")

        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        
        try:
            with open(os.path.join(project_root, "data/prompt/search_direction_prompt_v2.md"), "r") as f:
                prompt_template = f.read()
            with open(os.path.join(project_root, "data/prompt/math_stat_thinking_v2.md"), "r") as f:
                math_stat_thinking = f.read()
        except Exception as e:
             logger.error(f"读取 Prompt 文件失败: {e}")
             raise e

        # 填充 Prompt
        prompt = prompt_template.replace("{dataset_fields}", dataset_fields_str)
        prompt = prompt.replace("{math_stat_thinking}", math_stat_thinking)
        
        # Add explicit JSON instruction
        prompt += "\n\nCRITICAL: Return the output as a valid JSON object with a single key 'directions' containing a list of objects. Each object must have 'core_idea', 'differentiation', 'target_structure', and 'constraints' fields."
        
        logger.debug(f"Prompt constructed. Length: {len(prompt)}")

        logger.info("调用 LLM 生成搜索方向 (Temperature=0.8)...")
        
        # Instantiate model with specific temp
        llm = get_model(temperature=0.8)
        
        max_retries = 3
        attempts = 0
        while attempts < max_retries:
            try:
                logger.debug(f"Calling LLM invoke (attempt {attempts + 1})...")
                response = llm.invoke(prompt)
                
                # Parsing logic
                content = response.content
                logger.debug(f"Raw LLM response (first 500 chars): {content[:500]}...")

                json_start = content.find('```json')
                if json_start != -1:
                    # Try to find the closing backticks after the opening tag
                    json_end = content.find('```', json_start + 7)
                    
                    if json_end != -1:
                        json_str = content[json_start + 7:json_end]
                    else:
                        # Fallback: assume the rest of the string is JSON (maybe missing closing backticks)
                        logger.warning("Closing backticks not found, attempting to parse from start tag to end.")
                        json_str = content[json_start + 7:]
                else:
                    # Try to find { or [
                    json_match = re.search(r'\{.*\}', content, re.DOTALL)
                    if json_match:
                        json_str = json_match.group()
                    else:
                         raise ValueError("No JSON found in response")
                
                json_str = json_str.strip()
                logger.debug(f"Extracted JSON string length: {len(json_str)}")
                
                if not json_str:
                     raise ValueError("Extracted JSON string is empty")

                parsed_json = json.loads(json_str)
                # If parsed_json is a list, wrap it
                if isinstance(parsed_json, list):
                    parsed_json = {"directions": parsed_json}
                
                validated = DirectionNodeOutput.model_validate(parsed_json)
                state["search_directions"] = validated.directions
                logger.info(f"生成了 {len(state['search_directions'])} 个搜索方向:\n{json.dumps([d.model_dump() for d in state['search_directions']], indent=2, ensure_ascii=False)}")
                break

            except (json.JSONDecodeError, ValueError, ValidationError, RemoteDisconnected, ConnectionError) as e:
                attempts += 1
                logger.warning(f"Attempt {attempts}/{max_retries}: Failed to parse or validate LLM response: {e}")
                if attempts >= max_retries:
                    state["error_log"].append(f"Final attempt failed to parse LLM response in Node 2: {e}")
                time.sleep(5)
            except Exception as e:
                attempts += 1
                logger.error(f"Attempt {attempts}/{max_retries}: Error during LLM call in Node 2: {e}")
                if attempts >= max_retries:
                    state["error_log"].append(f"Final attempt failed during LLM call in Node 2: {e}")
                time.sleep(5)

    except Exception as e:
        logger.error(f"节点 2 执行失败: {e}", exc_info=True)
        state["error_log"].append(f"节点 2 执行失败: {e}")
        logger.info(f"{task_info} --- 节点 2 结束: 异常失败 ---")
        return state
    
    logger.info(f"{task_info} --- 节点 2 结束: 成功 (生成 {len(state.get('search_directions', []))} 个方向) ---")
    return state

def generate_alpha_candidates(state: StageOneState) -> StageOneState:
    """
    节点 3: 生成 Alpha 候选
    基于搜索方向生成具体的 Alpha 表达式
    """
    task_info = f"[Region: {state.get('region')} | Universe: {state.get('universe')} | Delay: {state.get('delay')} | Dataset: {state.get('dataset_ids')}]"
    logger.info(f"{task_info} --- 开始节点 3: 生成 Alpha 候选 (广度搜索模式) ---")
    
    # 0. 检查 search_directions，如果为空则跳过大模型调用
    search_directions = state.get("search_directions", [])
    if not search_directions:
        logger.warning(f"{task_info} --- 节点 3: 跳过大模型调用 (无搜索方向) ---")
        state["generated_expressions"] = []
        logger.info(f"{task_info} --- 节点 3 结束: 跳过 (无搜索方向) ---")
        return state
        
    try:
        session = check_session_and_relogin(state["session"])
        state["session"] = session
        
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        # --- 准备静态资源 (只做一次) ---
        
        # 1. 准备 OPERATORS
        operators_df = safe_api_call(get_operators, session)
        regular_ops = operators_df[operators_df['scope'] == 'REGULAR']
        filtered_ops = regular_ops[regular_ops['name'].isin(ALLOWED_OPERATORS)]
        operators_json = filtered_ops.to_json(orient='records')
        logger.debug(f"Available operators count after filtering: {len(filtered_ops)}")
        
        # 2. 准备 DATAFIELDS
        valid_fields_df = state["valid_data_fields"]
        datafields_str = valid_fields_df[['id', 'description', 'type', 'coverage']].to_string(index=False) if isinstance(valid_fields_df, pd.DataFrame) else str(valid_fields_df)
        
        # 3. 准备 ALPHA_EXAMPLES
        try:
            with open(os.path.join(project_root, "data/prompt/alpha_examples_v2.md"), "r") as f:
                alpha_examples_str = f.read()
            if not alpha_examples_str.strip():
                alpha_examples_str = "无示例"
        except Exception:
            alpha_examples_str = "无示例"

        # 4. 读取 Prompt 模板
        try:
            with open(os.path.join(project_root, "data/prompt/generate_alpha_prompt_v2.md"), "r") as f:
                prompt_template_raw = f.read()
        except Exception as e:
            logger.error(f"读取 Prompt 文件失败: {e}")
            raise e

        # 5. 初始化结果列表
        all_proposals = []
        
        # 6. 循环处理每个搜索方向
        total_directions = len(search_directions)
        
        for idx, direction in enumerate(search_directions):
            logger.info(f"👉 正在处理方向 {idx + 1}/{total_directions}: {direction.core_idea[:50]}...")
            
            # 构造该方向的描述字符串
            direction_str = f"Search Direction:\n"
            direction_str += f"  Core Idea: {direction.core_idea}\n"
            direction_str += f"  Differentiation: {direction.differentiation}\n"
            direction_str += f"  Target Structure: {direction.target_structure}\n"
            direction_str += f"  Constraints: {direction.constraints}\n"
            
            # 填充 Prompt
            prompt = prompt_template_raw.replace("{SEARCH_DIRECTIONS}", direction_str)
            prompt = prompt.replace("{OPERATORS}", operators_json)
            prompt = prompt.replace("{DATAFIELDS}", datafields_str)
            prompt = prompt.replace("{ALPHA_EXAMPLES}", alpha_examples_str)
            prompt = prompt.replace("{NEUTRALIZATION_OPTIONS}", " ".join(NEUTRALIZATION_OPTIONS))
            
            # 添加广度搜索和数量指令
            # 动态调整请求数量，如果有大量字段，限制上限以防 token 溢出
            field_count = len(valid_fields_df) if isinstance(valid_fields_df, pd.DataFrame) else 0
            if field_count > 50:
                prompt = prompt.replace("{COUNT}", "100")
            else:
                prompt = prompt.replace("{COUNT}", "50")

            prompt += "\n\nCRITICAL: Return the output as a valid JSON object with a single key 'proposals' containing a list of objects. Each object must have 'reasoning', 'new_expression', and 'new_setting'."

            # 调用 LLM
            llm = get_model(temperature=0.2) # 稍微提高温度以增加多样性，但保持足够的结构稳定性
            
            max_retries = 3
            attempts = 0
            success = False
            
            while attempts < max_retries:
                try:
                    logger.debug(f"Calling LLM invoke for direction {idx + 1} (attempt {attempts + 1})...")
                    response = llm.invoke(prompt)
                    
                    # 解析响应
                    content = response.content
                    json_start = content.find('```json')
                    if json_start != -1:
                        json_end = content.find('```', json_start + 7)
                        if json_end != -1:
                            json_str = content[json_start + 7:json_end]
                        else:
                            json_str = content[json_start + 7:]
                    else:
                        json_match = re.search(r'\{.*\}', content, re.DOTALL) # 尝试匹配最外层大括号 (可能不包含 proposals 列表，需小心)
                        # 更健壮的 fallback: 找列表开始
                        list_match = re.search(r'\[.*\]', content, re.DOTALL)
                        if json_match:
                             json_str = json_match.group()
                        elif list_match:
                             # 这种情况下模型可能直接返回了列表
                             json_str = f'{{"proposals": {list_match.group()}}}'
                        else:
                             # 最后的尝试：假设整个 content 就是 json
                             json_str = content

                    json_str = json_str.strip()
                    if not json_str:
                        raise ValueError("Extracted JSON string is empty")

                    parsed_json = json.loads(json_str)
                    
                    # 兼容性处理：如果返回的是列表而非 dict
                    if isinstance(parsed_json, list):
                        parsed_json = {"proposals": parsed_json}
                        
                    validated_proposals = AlphaProposal.model_validate(parsed_json)
                    new_proposals = validated_proposals.proposals
                    
                    if new_proposals:
                        all_proposals.extend(new_proposals)
                        logger.info(f"方向 {idx + 1} 生成了 {len(new_proposals)} 个 Alpha 候选。")
                        success = True
                        break
                    else:
                        logger.warning(f"方向 {idx + 1} 返回了空的 proposals 列表。")
                        break

                except (json.JSONDecodeError, ValueError, ValidationError, RemoteDisconnected, ConnectionError) as e:
                    attempts += 1
                    logger.warning(f"Direction {idx + 1} Attempt {attempts}/{max_retries}: Failed to parse LLM response: {e}")
                    time.sleep(3)
                except Exception as e:
                    attempts += 1
                    logger.error(f"Direction {idx + 1} Attempt {attempts}/{max_retries}: Error during LLM call: {e}")
                    time.sleep(3)
            
            if not success:
                logger.error(f"方向 {idx + 1} 生成失败，跳过。")
                state["error_log"].append(f"方向 {idx + 1} 生成失败")

        state["generated_expressions"] = all_proposals
        logger.info(f"所有方向处理完毕。总共生成 {len(state['generated_expressions'])} 个 Alpha 候选。")

    except Exception as e:
        logger.error(f"节点 3 执行失败: {e}", exc_info=True)
        state["error_log"].append(f"节点 3 执行失败: {e}")
        logger.info(f"{task_info} --- 节点 3 结束: 异常失败 ---")
        return state
    
    logger.info(f"{task_info} --- 节点 3 结束: 成功 (生成 {len(state.get('generated_expressions', []))} 个候选) ---")
    return state

def validate_alpha_candidates(state: StageOneState) -> StageOneState:
    """
    节点 3.5: 验证 Alpha 候选
    """
    task_info = f"[Region: {state.get('region')} | Universe: {state.get('universe')} | Delay: {state.get('delay')} | Dataset: {state.get('dataset_ids')}]"
    logger.info(f"{task_info} --- 开始节点 3.5: 验证 Alpha 候选 ---")
    
    try:
        session = state["session"]
        generated_expressions = state["generated_expressions"]
        logger.debug(f"Starting validation for {len(generated_expressions)} candidates.")
        
        # 1. 准备验证所需的字段集合
        valid_fields_df = state["valid_data_fields"]
        if isinstance(valid_fields_df, pd.DataFrame):
            valid_field_ids = set(valid_fields_df['id'].unique())
        else:
            valid_field_ids = set()
        logger.debug(f"Number of valid field IDs: {len(valid_field_ids)}")
            
        # 2. 构造 settings 字典
        settings_dict = {
            "region": state["region"],
            "universe": state["universe"],
            "delay": state["delay"]
        }
        
        # 预先获取 operators，避免在 check_expression_syntax 中多次调用导致连接风险
        stage_one_operators_df = None
        try:
            operators_df = safe_api_call(get_operators, session)
            regular_operators_df = operators_df[operators_df['scope'] == 'REGULAR']
            stage_one_operators_df = regular_operators_df[regular_operators_df['name'].isin(ALLOWED_OPERATORS)]
            logger.debug("Successfully pre-fetched operators for validation.")
        except Exception as e:
            logger.warning(f"Failed to pre-fetch operators: {e}. Validation might fail or be slow.")

        valid_candidates = []
        for candidate in generated_expressions:
            expression = candidate.new_expression
            logger.debug(f"Validating candidate expression: {expression}")
            
            if not expression: 
                logger.warning(f"跳过无表达式的候选: {candidate.reasoning}")
                continue
            
            # 3. 过滤掉仅使用 pv1 字段的 alpha
            if not filter_alpha_by_datafields(expression, settings_dict, session):
                logger.info(f"Alpha '{expression}' 仅包含 pv1 字段，被过滤。")
                continue
                
            # 4. 验证字段是否全部在有效字段集中
            is_fields_valid, field_error = validate_expression_fields(expression, valid_field_ids, session)
            if not is_fields_valid:
                logger.warning(f"字段验证失败 '{expression}': {field_error}")
                continue
                
            # 5. 验证语法
            # 传入预先获取的 operators_df
            is_syntax_valid, syntax_errors = check_expression_syntax(expression, session, operators_df=stage_one_operators_df)
            if not is_syntax_valid:
                logger.warning(f"语法验证失败 '{expression}': {syntax_errors}")
                # 记录详细错误
                with open("syntax_error_log.txt", "a") as f:
                    f.write(f"--- Syntax Error ---\n")
                    f.write(f"Expression: {expression}\n")
                    f.write(f"Reason: {syntax_errors}\n\n")
                continue
                
            # 6. 自动补全和修正表达式 (如添加 vec_avg 等)
            try:
                # complete_expression 返回一个列表，我们取第一个
                # 注意：complete_expression 需要 valid_data_fields 为 DataFrame 以获取类型信息
                completed_expressions = complete_expression(expression, valid_fields_df, session)
                if completed_expressions:
                    processed_expression = completed_expressions[0]
                    logger.info(f"验证通过并处理: {processed_expression}")
                    logger.debug(f"Original: {expression} -> Processed: {processed_expression}")
                    
                    # 更新候选对象的表达式
                    # 由于 ProposedAlpha 是 Pydantic 模型，我们可以创建一个新的实例或者直接修改
                    # 这里选择更新 new_expression，因为后续步骤直接使用它
                    candidate.new_expression = processed_expression
                    valid_candidates.append(candidate)
                else:
                    logger.info(f"表达式处理返回空列表: {expression}")
            except Exception as e:
                logger.error(f"表达式处理异常 '{expression}': {e}")
        
        state["valid_expressions"] = valid_candidates
        logger.info(f"验证通过 {len(valid_candidates)}/{len(generated_expressions)}")
        
        # 更新连续空转计数
        if not valid_candidates:
            state["consecutive_empty_candidates"] += 1
        else:
            state["consecutive_empty_candidates"] = 0
            
    except Exception as e:
        logger.error(f"验证过程发生异常: {e}", exc_info=True)
        state["error_log"].append(f"验证过程发生异常: {e}")
        logger.info(f"{task_info} --- 节点 3.5 结束: 异常失败 ---")
        return state
    
    logger.info(f"{task_info} --- 节点 3.5 结束: 成功 (通过 {len(valid_candidates)}/{len(generated_expressions)} 个) ---")
    return state

def simulate_alphas(state: StageOneState) -> StageOneState:
    """
    节点 4a: 模拟节点
    职责：将 valid_expressions 拼成 sim_jobs，每次只取 10 个 alpha。
    """
    task_info = f"[Region: {state.get('region')} | Universe: {state.get('universe')} | Delay: {state.get('delay')} | Dataset: {state.get('dataset_ids')}]"
    logger.info(f"{task_info} --- 开始节点 4a: 模拟 Alphas ---")
    
    try:
        session = check_session_and_relogin(state["session"])
        state["session"] = session
        
        # 获取待仿真的表达式列表
        all_valid_expressions = state.get("valid_expressions", [])
        
        if not all_valid_expressions:
            logger.info("无待仿真表达式。")
            state["current_batch_alpha_ids"] = []
            logger.info(f"{task_info} --- 节点 4a 结束: 跳过 (无表达式) ---")
            return state
            
        # 每次只取前 10 个
        batch_size = 10
        current_batch = all_valid_expressions[:batch_size]
        remaining_expressions = all_valid_expressions[batch_size:]
        
        # 更新 state 中的 valid_expressions (移除已取出的)
        state["valid_expressions"] = remaining_expressions
        
        logger.info(f"本轮仿真 {len(current_batch)} 个表达式，剩余 {len(remaining_expressions)} 个待仿真。")

        # 1. 准备仿真任务
        sim_jobs = []
        for candidate in current_batch:
            sim_settings = {
                "instrumentType": "EQUITY",
                "region": state["region"],
                "universe": state["universe"],
                "delay": state["delay"],
                "decay": candidate.new_setting.get("decay", 0),
                "neutralization": candidate.new_setting.get("neutralization", "INDUSTRY"),
                "truncation": candidate.new_setting.get("truncation", 0.08),
                "pasteurization": "ON",
                "testPeriod": "P2Y",
                "unitHandling": "VERIFY",
                "nanHandling": "OFF",
                "maxTrade": "OFF",
                "language": "FASTEXPR",
                "visualization": False
            }
            # 确保表达式是字符串
            expression = candidate.new_expression
            sim_jobs.append({
                "type": "REGULAR",
                "settings": sim_settings,
                "regular": expression
            })
            
        # 2. 执行批量仿真
        batch_results = []
        max_retries = 3
        attempts = 0
        timeout_duration = 30 * 60 # 30分钟超时

        while attempts < max_retries:
            executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
            try:
                logger.info(f"运行批量仿真... (尝试 {attempts + 1}/{max_retries})")
                future = executor.submit(simulate_multi_alpha, session, sim_jobs)
                batch_results = future.result(timeout=timeout_duration)
                executor.shutdown(wait=True)
                logger.info("批量仿真完成。")
                break
            except concurrent.futures.TimeoutError as e:
                # 关键修改：超时后不等待线程结束，直接关闭 executor 并抛出异常触发重试
                logger.warning(f"仿真超时 (尝试 {attempts + 1})，正在强制继续...")
                executor.shutdown(wait=False) 
                attempts += 1
                error_msg = f"仿真尝试 {attempts}/{max_retries} 失败: {e}"
                logger.error(error_msg)
                state["error_log"].append(error_msg)
                if attempts >= max_retries:
                    logger.error("仿真最终失败。")
                    batch_results = []
                    break
                time.sleep(60)
                session = start_session()
                state["session"] = session
            except (RemoteDisconnected, ConnectionError, Exception) as e:
                executor.shutdown(wait=False)
                attempts += 1
                error_msg = f"仿真尝试 {attempts}/{max_retries} 失败: {e}"
                logger.error(error_msg)
                state["error_log"].append(error_msg)
                if attempts >= max_retries:
                    logger.error("仿真最终失败。")
                    batch_results = []
                    break
                time.sleep(60)
                session = start_session()
                state["session"] = session

        # 提取 Alpha IDs
        current_alpha_ids = []
        for res in batch_results:
            aid = res.get("alpha_id")
            if aid and aid not in current_alpha_ids:
                current_alpha_ids.append(aid)
        
        state["current_batch_alpha_ids"] = current_alpha_ids
        
    except Exception as e:
        logger.error(f"模拟节点执行失败: {e}", exc_info=True)
        state["error_log"].append(f"模拟节点执行失败: {e}")
        state["current_batch_alpha_ids"] = []
        logger.info(f"{task_info} --- 节点 4a 结束: 异常失败 ---")
        return state

    logger.info(f"{task_info} --- 节点 4a 结束: 成功 (获得 {len(state.get('current_batch_alpha_ids', []))} 个 ID) ---")
    return state

def filter_alphas(state: StageOneState) -> StageOneState:
    """
    节点 4b: 过滤节点
    职责：检查 year state 和 check 信息，生成反转信号，筛选通过的 alpha。
    """
    task_info = f"[Region: {state.get('region')} | Universe: {state.get('universe')} | Delay: {state.get('delay')} | Dataset: {state.get('dataset_ids')}]"
    logger.info(f"{task_info} --- 开始节点 4b: 过滤 Alphas ---")
    
    try:
        session = check_session_and_relogin(state["session"])
        state["session"] = session
        
        current_alpha_ids = state.get("current_batch_alpha_ids", [])
        if not current_alpha_ids:
            logger.info("本批次无 Alpha ID，跳过过滤。")
            logger.info(f"{task_info} --- 节点 4b 结束: 跳过 (无 ID) ---")
            return state

        # 初始化 passed_filter_alphas (如果不存在)
        if "passed_filter_alphas" not in state or state["passed_filter_alphas"] is None:
            state["passed_filter_alphas"] = []

        def _get_json_from_url(sess, url):
            response = sess.get(url)
            response.raise_for_status()
            return response.json()
        
        passed_count_this_run = 0

        for alpha_id in current_alpha_ids:
            logger.debug(f"正在检查 Alpha: {alpha_id}")
            
            # 获取表达式 (用于记录历史和生成反转信号)
            expression = ""
            try:
                alpha_details = safe_api_call(_get_json_from_url, session, f"https://api.worldquantbrain.com/alphas/{alpha_id}")
                if alpha_details:
                    expression = alpha_details.get('regular', {}).get('code', "")
            except Exception as e:
                logger.error(f"获取 Alpha {alpha_id} 详情失败: {e}")

            # 获取统计和检查数据
            stats_df = safe_api_call(get_alpha_yearly_stats, session, alpha_id)
            check_df = safe_api_call(get_check_submission, session, alpha_id)

            # --- 1. Year State 检查 (Data History) ---
            active_years = 0
            if stats_df is not None and not stats_df.empty:
                active_years = (stats_df['sharpe'] != 0.0).sum()
            
            if active_years < 8:
                logger.info(f"Alpha {alpha_id} 数据历史不足 ({active_years} 年)，直接放弃并从历史记录移除。")
                
                # 增加计数
                state["insufficient_data_count"] = state.get("insufficient_data_count", 0) + 1

                # 尝试移除导致数据不足的字段
                if 0 < active_years < 8 and expression:
                    try:
                        used_fields = extract_datafields(expression, session)
                        valid_fields_df = state["valid_data_fields"]
                        if isinstance(valid_fields_df, pd.DataFrame):
                             fields_to_remove = [f for f in used_fields if f in valid_fields_df['id'].values]
                             if fields_to_remove:
                                 logger.info(f"移除导致数据不足的字段: {fields_to_remove}")
                                 state["valid_data_fields"] = valid_fields_df[~valid_fields_df['id'].isin(fields_to_remove)]
                    except Exception as e:
                        logger.error(f"移除字段失败: {e}")
                continue # Skip processing this alpha
            else:
                if expression:
                    if expression in state["historical_alphas"]:
                        # 如果已存在，添加到 repeat_historical_alphas 列表
                        state["repeat_historical_alphas"].append(expression)
                    else:
                        # 如果不存在，添加到 historical_alphas 列表
                        state["historical_alphas"].append(expression)

            # --- 2. Check 信息检查 ---
            pass_check = True
            fail_reason = ""
            fitness = 0.0
            sharpe = 0.0
            
            if check_df is not None and not check_df.empty:
                checks = check_df.set_index('name')['value'].to_dict()
                checks_result = check_df.set_index('name')['result'].to_dict()
                
                sharpe = checks.get('LOW_SHARPE', 0)
                fitness = checks.get('LOW_FITNESS', 0)
                robust = checks.get('LOW_ROBUST_UNIVERSE_SHARPE', 1.0)
                jpn_robust = checks.get('LOW_ASI_JPN_SHARPE', 1.0)
                conc_weight_res = checks_result.get('CONCENTRATED_WEIGHT', 'PASS')
                
                if sharpe <= 1.0:
                    pass_check = False
                    fail_reason = f"Sharpe {sharpe:.2f} <= 1.0"
                elif fitness <= 0.3: # 注意：Prompt 中说 fitness < -0.3 触发反转，这里是通过标准 fitness > 0.3? Prompt 没细说通过标准，沿用旧逻辑 > 0.3
                    pass_check = False
                    fail_reason = f"Fitness {fitness:.2f} <= 0.3"
                elif robust <= 0.8:
                    pass_check = False
                    fail_reason = f"Robust {robust:.2f} <= 0.8"
                elif conc_weight_res in ['FAIL', 'WARNING']:
                    pass_check = False
                    fail_reason = f"Conc Weight {conc_weight_res}"
                elif jpn_robust <= 0.8:
                    pass_check = False
                    fail_reason = f"JPN Robust {jpn_robust:.2f} <= 0.8"
            else:
                pass_check = False
                fail_reason = "No Check Data"

            if pass_check:
                # 通过检查 -> 放入 passed_filter_alphas
                logger.info(f"✅ Alpha {alpha_id} 通过过滤 (Fitness: {fitness:.2f})")
                state["passed_filter_alphas"].append({
                    "id": alpha_id,
                    "fitness": fitness
                })
                passed_count_this_run += 1
            else:
                logger.info(f"Alpha {alpha_id} 未通过过滤: {fail_reason}")
                
                # --- 3. 生成反转信号 ---
                # 条件: sharpe < -1 并且 fitness < -0.3
                if sharpe < -1.0 and fitness < -0.3 and expression:
                    logger.info(f"💡 Alpha {alpha_id} 触发反转逻辑 (Sharpe: {sharpe:.2f}, Fitness: {fitness:.2f})")
                    try:
                        new_expression = f"reverse({expression})"
                        # 保持原有参数设置
                        # 我们需要找到这个 alpha 对应的原始 ProposedAlpha 里的设置
                        # 这是一个难点，因为我们这里只有 ID。
                        # 简化处理：使用默认设置，或者尝试从 alpha details 获取 (settings 字段)
                        
                        # 尝试从 API 详情获取设置
                        settings_from_api = alpha_details.get("settings", {})
                        
                        # 构造新的 ProposedAlpha
                        new_proposal = ProposedAlpha(
                            reasoning=f"Generated from reversal of {alpha_id} (Sharpe {sharpe}, Fitness {fitness})",
                            new_expression=new_expression,
                            new_setting={
                                "neutralization": settings_from_api.get("neutralization", "INDUSTRY"),
                                "decay": settings_from_api.get("decay", 0),
                                "truncation": settings_from_api.get("truncation", 0.08)
                            }
                        )
                        
                        # 放入 valid_expressions 等待下一轮模拟
                        state["valid_expressions"].append(new_proposal)
                        logger.info(f"反转信号已加入待仿真队列: {new_expression}")
                        
                    except Exception as e:
                        logger.error(f"生成反转信号失败: {e}")

        # 清空当前批次 ID
        state["current_batch_alpha_ids"] = []

    except Exception as e:
        logger.error(f"过滤节点执行失败: {e}", exc_info=True)
        state["error_log"].append(f"过滤节点执行失败: {e}")
        logger.info(f"{task_info} --- 节点 4b 结束: 异常失败 ---")
        return state
    
    logger.info(f"{task_info} --- 节点 4b 结束: 成功 (本轮通过 {passed_count_this_run} 个) ---")
    return state

def correlation_and_save(state: StageOneState) -> StageOneState:
    """
    节点 4c: 相关性检查并保存
    职责：对所有通过过滤的 alpha 进行相关性检查，通过则入库。
    """
    task_info = f"[Region: {state.get('region')} | Universe: {state.get('universe')} | Delay: {state.get('delay')} | Dataset: {state.get('dataset_ids')}]"
    logger.info(f"{task_info} --- 开始节点 4c: 相关性检查并保存 ---")
    state["iteration_count"] += 1
    
    try:
        session = check_session_and_relogin(state["session"])
        state["session"] = session
        
        passed_alphas = state.get("passed_filter_alphas", [])
        if not passed_alphas:
            logger.info("无待检查相关性的 Alpha。")
            logger.info(f"{task_info} --- 节点 4c 结束: 跳过 (无 Alpha) ---")
            return state

        # 1. 准备相关性数据
        logger.info("加载相关性参考数据...")
        try:
            download_data(session, flag_increment=True)
            os_alpha_ids, os_alpha_rets = load_data()
            
            # --- Fix: Ensure no duplicate columns in os_alpha_rets ---
            if isinstance(os_alpha_rets, pd.DataFrame) and not os_alpha_rets.empty:
                if os_alpha_rets.columns.duplicated().any():
                    logger.warning("os_alpha_rets contains duplicate columns. Removing duplicates.")
                    os_alpha_rets = os_alpha_rets.loc[:, ~os_alpha_rets.columns.duplicated()]
            # ---------------------------------------------------------
            
        except Exception as e:
            logger.error(f"相关性数据加载失败: {e}")
            os_alpha_ids, os_alpha_rets = {}, pd.DataFrame()

        region = state["region"]
        
        # 2. 排序 (Fitness 从大到小)
        passed_alphas.sort(key=lambda x: x['fitness'], reverse=True)
        
        logger.info(f"开始检查 {len(passed_alphas)} 个 Alpha 的相关性...")
        
        dao = StageOneSignalDAO()
        new_saved_count = 0

        # 3. 循环检查
        for alpha_item in passed_alphas:
            alpha_id = alpha_item['id']
            fitness = alpha_item['fitness']
            
            # 计算相关性
            try:
                max_corr, candidate_pnls_df = calc_self_corr(
                    alpha_id=alpha_id,
                    sess=session,
                    os_alpha_rets=os_alpha_rets,
                    os_alpha_ids=os_alpha_ids,
                    return_alpha_pnls=True
                )
            except Exception as e:
                logger.error(f"计算相关性出错 {alpha_id}: {e}")
                max_corr = 1.0
                candidate_pnls_df = None

            prod_corr = 1.0
            if max_corr < 0.7:
                 prod_corr_result = safe_api_call(check_prod_corr_test, session, alpha_id=alpha_id)
                 prod_corr = prod_corr_result['value'].max() if prod_corr_result is not None and not prod_corr_result.empty else 1.0
            
            logger.info(f"Alpha {alpha_id} (Fit {fitness:.2f}): Self={max_corr:.2f}, Prod={prod_corr:.2f}")

            if max_corr < 0.7 and prod_corr < 0.7:
                # --- Pass: Save ---
                logger.info(f"✅ Alpha {alpha_id} 相关性检查通过，保存。")
                
                signal_record = {
                    "alpha_id": alpha_id,
                    "region": state["region"],
                    "universe": state["universe"],
                    "delay": state["delay"],
                    "dataset_id": state["dataset_ids"][0] if state["dataset_ids"] else "unknown",
                    "category": state.get("category", "unknown"),
                    "date_time": state["date_time"],
                    "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                
                try:
                    dao.upsert_signal(signal_record)
                    if alpha_id not in state["saved_alphas_ids"]:
                        state["saved_alphas_ids"].append(alpha_id)
                    state["total_saved_count"] += 1
                    new_saved_count += 1

                    # Tag
                    tag = f"StageOne_{state.get('category', 'unknown')}_{state['date_time']}"
                    safe_api_call(set_alpha_properties, session, alpha_id, tags=[tag])

                except Exception as e:
                    logger.error(f"保存 Alpha {alpha_id} 失败: {e}")
            else:
                logger.info(f"Alpha {alpha_id} 相关性过高，丢弃。")

        # 清空 passed_filter_alphas
        state["passed_filter_alphas"] = []

    except Exception as e:
        logger.error(f"相关性检查节点执行失败: {e}", exc_info=True)
        state["error_log"].append(f"相关性检查节点执行失败: {e}")
        logger.info(f"{task_info} --- 节点 4c 结束: 异常失败 ---")
        return state

    logger.info(f"{task_info} --- 节点 4c 结束: 成功 (新保存 {new_saved_count} 个，累计 {state['total_saved_count']} 个) ---")
    return state
# --- 4. 定义条件边 ---

def should_continue_simulation(state: StageOneState) -> str:
    """
    决定是否继续仿真（批处理）
    逻辑：如果 valid_expressions 还有剩余，或者刚刚生成了反转信号放入了 valid_expressions，则继续仿真
    """
    valid_exprs = state.get("valid_expressions", [])
    if valid_exprs:
        logger.info(f"队列中仍有 {len(valid_exprs)} 个待仿真表达式，继续仿真循环。")
        return "continue_simulation"
    logger.info("所有待仿真表达式处理完毕，进入相关性检查与保存阶段。")
    return "go_to_save"

def should_continue_workflow(state: StageOneState) -> str:
    """决定是否继续整个工作流迭代"""
    if state.get("status") == "failed":
        return "end"
    
    # 停止条件 1: 连续 3 次无有效候选
    if state.get("consecutive_empty_candidates", 0) >= 3:
        logger.info("连续 3 次未生成有效候选，停止工作流")
        state["status"] = "completed"
        return "end"

    # 停止条件 2: 目标数量达成
    target_saved_count = 20
    if state.get("total_saved_count", 0) >= target_saved_count:
        logger.info(f"达到目标保存数量 {target_saved_count}，完成")
        state["status"] = "completed"
        return "end"
        
    # 停止条件 3: 历史尝试过多
    if len(state.get("historical_alphas", [])) > 2000:
        logger.info(f"尝试 Alpha 超过 2000 个 (含无效数据 {state.get('insufficient_data_count', 0)})，停止")
        state["status"] = "completed"
        return "end"
        
    return "continue"

# --- 5. 构建和运行图 ---

def build_stage_one_graph():
    """构建 LangGraph 工作流"""
    workflow = StateGraph(StageOneState)
    
    workflow.add_node("initialize_and_load_data", initialize_and_load_data)
    workflow.add_node("generate_search_directions", generate_search_directions)
    workflow.add_node("generate_alpha_candidates", generate_alpha_candidates)
    workflow.add_node("validate_alpha_candidates", validate_alpha_candidates)
    
    # 新拆分的节点
    workflow.add_node("simulate_alphas", simulate_alphas)
    workflow.add_node("filter_alphas", filter_alphas)
    workflow.add_node("correlation_and_save", correlation_and_save)
    
    workflow.set_entry_point("initialize_and_load_data")
    
    workflow.add_edge("initialize_and_load_data", "generate_search_directions")
    workflow.add_edge("generate_search_directions", "generate_alpha_candidates")
    workflow.add_edge("generate_alpha_candidates", "validate_alpha_candidates")
    workflow.add_edge("validate_alpha_candidates", "simulate_alphas")
    
    workflow.add_edge("simulate_alphas", "filter_alphas")
    
    # 仿真/过滤循环 vs 进入保存
    workflow.add_conditional_edges(
        "filter_alphas",
        should_continue_simulation,
        {
            "continue_simulation": "simulate_alphas",
            "go_to_save": "correlation_and_save"
        }
    )
    
    # 主循环
    workflow.add_conditional_edges(
        "correlation_and_save",
        should_continue_workflow,
        {
            "end": END,
            "continue": "generate_search_directions" 
        }
    )
    
    return workflow.compile()

def run_stage_one_workflow(
    dataset_ids: List[str] = ["pv1"],
    category: str = "general",
    region: str = "USA",
    universe: str = "TOP3000",
    delay: int = 1,
    date_time: str = ""
) -> Dict[str, Any]:
    """运行 Stage One 工作流"""
    logger.info("🚀 启动 Stage One 工作流")
    
    try:
        session = start_session()
        if not session:
            return {"error": "ACE 会话启动失败"}
        
        # 初始化模型
        if not initialize_model():
             return {"error": "LLM 初始化失败"}

        # 初始化 date_time
        if not date_time:
            date_time = datetime.now().strftime("%Y%m%d")
        
        # 恢复逻辑：查找已存在的 Alphas
        dao = StageOneSignalDAO()
        existing_alphas = dao.get_alphas_by_batch(region, universe, delay, dataset_ids[0], date_time)
        
        historical_alphas = []
        saved_alphas_ids = []
        
        def _get_json_from_url(sess, url):
            """Helper to get and decode JSON from a URL within a safe_api_call."""
            response = sess.get(url)
            response.raise_for_status()
            return response.json()

        if existing_alphas:
            logger.info(f"发现 {len(existing_alphas)} 个已存在的 Alpha，正在恢复上下文...")
            for item in existing_alphas:
                alpha_id = item['alpha_id']
                saved_alphas_ids.append(alpha_id)
                
                # Fetch expression
                alpha_details = safe_api_call(_get_json_from_url, session, f"https://api.worldquantbrain.com/alphas/{alpha_id}")
                if alpha_details:
                    expression = alpha_details.get('regular', {}).get('code')
                    if expression:
                        historical_alphas.append(expression)
                    else:
                        logger.warning(f"恢复时无法获取 Alpha {alpha_id} 的表达式")
                else:
                    logger.warning(f"恢复时无法获取 Alpha {alpha_id} 的详情")
            
            logger.info(f"成功恢复 {len(historical_alphas)} 个历史表达式")

        app = build_stage_one_graph()
        
        initial_state = {
            "session": session,
            "dataset_ids": dataset_ids,
            "category": category,
            "region": region,
            "universe": universe,
            "delay": delay,
            "date_time": date_time, 
            "valid_data_fields": None,
            "search_directions": [],
            "generated_expressions": [],
            "valid_expressions": [],
            "current_batch_alpha_ids": [],
            "passed_filter_alphas": [],
            "historical_alphas": historical_alphas,
            "repeat_historical_alphas": [],
            "saved_alphas_ids": saved_alphas_ids,
            "iteration_count": 0,
            "total_saved_count": len(saved_alphas_ids),
            "insufficient_data_count": 0,
            "consecutive_empty_candidates": 0,
            "status": "running",
            "error_log": []
        }
        
        final_state = app.invoke(initial_state)
        
        # 保存工作流状态结果
        try:
            output_dir = "output/workflow_state_result"
            os.makedirs(output_dir, exist_ok=True)
            
            # 创建可序列化的状态副本
            serializable_state = {}
            for k, v in final_state.items():
                if k == "session":
                    continue
                # 不需要保存 search_directions 和 generated_expressions
                if k in ["search_directions", "generated_expressions"]:
                    continue
                    
                if k == "valid_data_fields":
                    # 如果是 DataFrame，只记录数量或转换为字典
                    if isinstance(v, pd.DataFrame):
                        serializable_state[k] = f"DataFrame with {len(v)} rows"
                    else:
                        serializable_state[k] = str(v)
                elif k == "valid_expressions": # 保持 valid_expressions 的处理 (虽然 generated_expressions 移除了，但 valid 也许有用，不过用户没明确说移除 valid，只说了 generated)
                    # 处理 Pydantic 模型列表
                    serializable_state[k] = [item.model_dump() if hasattr(item, "model_dump") else item for item in v]
                else:
                    serializable_state[k] = v
            
            file_name = f"{category}_{region}_{date_time}_{int(time.time())}.json"
            file_path = os.path.join(output_dir, file_name)
            
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(serializable_state, f, indent=2, ensure_ascii=False)
            logger.info(f"✅ 工作流最终状态已保存至: {file_path}")
            
        except Exception as e:
            logger.error(f"保存工作流状态失败: {e}")

        return final_state
        
    except Exception as e:
        logger.error(f"工作流运行异常: {e}")
        return {"success": False, "error": str(e)}

def main():
    # 可以在此处修改运行参数
    #dataset_ids = ["other534", "shortinterest3", "shortinterest38", "shortinterest5", "shortinterest55"]
    #category = "shortinterest"
    dataset_ids = ["fundamental28"]
    category = "fundamental"
    region = "ASI"
    universe = "MINVOL1M"
    delay = 1
    date_time = "" # 留空则自动使用当前日期

    
    session = start_session()

    """
    df = get_datafields_with_cache(
                    session,
                    region=region,
                    delay=delay,
                    universe=universe,
                    dataset_id="analyst10",
                    data_type="ALL"
                )
    datafields_str = df[['id', 'description', 'type', 'coverage']].to_string(index=False) if isinstance(df, pd.DataFrame) else str(df)
    
    print(datafields_str)
    """
    operators_df = safe_api_call(get_operators, session)
    regular_operators_df = operators_df[operators_df['scope'] == 'REGULAR']
    stage_one_operators_df = regular_operators_df[regular_operators_df['name'].isin(ALLOWED_OPERATORS)]

    print(stage_one_operators_df.to_string(
    max_cols=None,      # 显示所有列
    max_colwidth=300,   # 每列最大宽度
    show_dimensions=True  # 显示维度信息
    ))

    """
    print(operators_df.to_string(
    max_cols=None,      # 显示所有列
    max_colwidth=100,   # 每列最大宽度
    show_dimensions=True  # 显示维度信息
    ))

    result = run_stage_one_workflow(
        dataset_ids=dataset_ids,
        category=category,
        region=region,
        universe=universe,
        delay=delay,
        date_time=date_time
    )

    if result.get("status") != "failed":
        print(f"✅ 工作流运行成功，保存数量: {result.get('total_saved_count')}")
    else:
        print(f"❌ 工作流运行结束，状态: {result.get('status')}")
        if result.get("error_log"):
            print(f"错误日志: {result.get('error_log')}")
    """

if __name__ == "__main__":
    main()
