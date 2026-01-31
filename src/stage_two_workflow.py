import pandas as pd
import ace_lib
import ace_lib_ext
import utils
import alpha_score
from pydantic import BaseModel, Field, ValidationError
from langchain_openai import ChatOpenAI
from langgraph.graph import StateGraph, END
from typing import TypedDict, List, Optional, Tuple, Dict, Any
import json
import time
import os
import re
from datetime import datetime
from langchain_core.prompts import PromptTemplate
from requests.exceptions import ConnectionError
from http.client import RemoteDisconnected
import concurrent.futures
from expression_validator import check_expression_syntax
from self_corr_calculator import calc_self_corr,download_data, load_data
from logger import Logger
from cached_data_fetcher import get_datafields_with_cache

logger = Logger()

# --- File Constants ---
FILE_PATHS = {
    "IMPROVE_METHODS": {
        'LOW_SHARPE': 'data/ImproveMethods/How to improve Sharpe?.md',
        'LOW_FITNESS': 'data/ImproveMethods/How to increase fitness of alphas.md',
        'LOW_TURNOVER': 'data/ImproveMethods/How to improve Turnover?.md',
        'HIGH_TURNOVER': 'data/ImproveMethods/How to improve Turnover?.md',
        'CONCENTRATED_WEIGHT': 'data/ImproveMethods/Weight Coverage common issues and advice.md',
        'LOW_SUB_UNIVERSE_SHARPE': 'data/ImproveMethods/How do I resolve this error Sub-universe Sharpe NaN is not above cutoff.md',
        'LOW_ROBUST_UNIVERSE_SHARPE': 'data/ImproveMethods/Details about "robust universe" criteria in CHN,ASI,IND region.md',
        'LOW_ROBUST_UNIVERSE_SHARPE.WITH_RATIO': 'data/ImproveMethods/Details about "robust universe" criteria in CHN,ASI,IND region.md',
        'LOW_ASI_JPN_SHARPE': 'data/ImproveMethods/Details about "robust universe" criteria in CHN,ASI,IND region.md',
        'LOW_ROBUST_UNIVERSE_RETURNS': 'data/ImproveMethods/Details about "robust universe" criteria in CHN,ASI,IND region.md',
        'LOW_2Y_SHARPE': 'data/ImproveMethods/How to improve Sharpe?.md',
        'PROD_CORRELATION': 'data/ImproveMethods/How do you reduce correlation of a good Alpha?.md?',
        'LOW_GLB_AMER_SHARPE':'data/ImproveMethods/How to improve Sharpe?.md',
        'LOW_GLB_EMEA_SHARPE':'data/ImproveMethods/How to improve Sharpe?.md',
        'LOW_GLB_APAC_SHARPE':'data/ImproveMethods/How to improve Sharpe?.md',
        'LOW_INVESTABILITY_CONSTRAINED_SHARPE':'data/ImproveMethods/Asian_Region_Investability_Constrained_Sharpe_Analysis.md'
    },
    "HOW_TO_USE_DATASETS": {
        'analyst': 'data/HowToUseAllDatasets/Getting started with Analyst Datasets.md',
        'earnings': 'data/HowToUseAllDatasets/Getting started with Earnings Datasets.md',
        'institutions': 'data/HowToUseAllDatasets/Getting started with Institutions Datasets.md',
        'macro': 'data/HowToUseAllDatasets/Getting started with Macro Datasets.md',
        'model': 'data/HowToUseAllDatasets/Getting started with Model Datasets.md',
        'news': 'data/HowToUseAllDatasets/Getting started with News Datasets.md',
        'option': 'data/HowToUseAllDatasets/Getting Started with Option Datasets.md',
        'other': 'data/HowToUseAllDatasets/Getting started with Other Datasets.md',
        'pv': 'data/HowToUseAllDatasets/Getting started with Price Volume Datasets.md',
        'risk': 'data/HowToUseAllDatasets/Getting started with Risk Datasets.md',
        'sentiment': 'data/HowToUseAllDatasets/Getting started with Sentiment Datasets.md',
        'shortinterest': 'data/HowToUseAllDatasets/Getting started with Short Interest Datasets.md',
        'socialmedia': 'data/HowToUseAllDatasets/Getting started with Social Media Datasets.md'
    },
    "PROMPT_DIRECTION": "data/prompt/stage_two_direction_prompt.md",
    "PROMPT_GENERATION": "data/prompt/stage_two_generation_prompt.md",
    "ESSENCE_EXAMPLES": "data/prompt/essence_prompt2_zh.md",
}

# --- Global Constants ---
NEUTRALIZATION_OPTIONS = ["REVERSION_AND_MOMENTUM", "CROWDING", "FAST", "SLOW", "SLOW_AND_FAST", "MARKET", "SECTOR", "INDUSTRY", "SUBINDUSTRY"]

# --- Pydantic Models ---

class SearchDirection(BaseModel):
    """Stage 2: Improvement Direction"""
    core_idea: str = Field(description="The main financial or statistical logic.")
    differentiation: str = Field(description="How it differs from the original.")
    target_structure: str = Field(description="What market structure it targets.")
    constraints: str = Field(description="What to avoid.")

class DirectionNodeOutput(BaseModel):
    """Stage 2: Improvement Direction"""
    directions: List[SearchDirection] = Field(description="List of improvement directions")

class ProposedAlpha(BaseModel):
    """Stage 2: Proposed Alpha Candidate"""
    reasoning: str = Field(description="Explain why this change implements the direction.")
    new_expression: str = Field(description="The new, modified Alpha expression.")
    new_setting: dict = Field(description="Update 'neutralization', 'decay', 'truncation' if needed.")

class AlphaProposal(BaseModel):
    """Batch of proposed alphas"""
    proposals: List[ProposedAlpha] = Field(description="List of new alpha expressions")

# --- Workflow State ---

class WorkflowState(TypedDict):
    """Represents the state of our batch-oriented workflow."""
    # --- Input fields ---
    session: Any  # The authenticated ace_lib session
    seed_alpha_id: str  # The ID of the alpha to use as a seed for the current iteration
    all_fields_df: Any  # Assuming pandas DataFrame
    valid_data_fields: set

    # --- Iteration-specific state ---
    seed_expression: str  # The code of the seed alpha
    seed_setting: dict  # The settings of the seed alpha
    row_setting: dict # To store the full original settings for simulation
    seed_performance_report: str  # Performance report of the seed alpha
    yearly_performance_report: str # Yearly performance report of the seed alpha
    
    # --- Generation ---
    search_directions: List[SearchDirection] # Directions generated by LLM

    # --- Batch processing fields ---
    proposed_alphas: List[dict]  # Stores a batch of proposed alphas from the LLM (raw dicts)
    valid_candidates: List[dict]  # Stores alphas that passed local validation (ProposedAlpha objects or dicts)
    historical_alphas: List[str] # Stores a list of previously generated alpha expressions
    repeat_historical_alphas: List[str] # Stores duplicated alpha expressions

    # --- Knowledge Base ---
    knowledge_base: dict  # Stores dynamically loaded knowledge {'dataset_tips': str, 'improvement_methods': str}

    # --- Workflow status and results ---
    status: str
    success_id: List[str]
    error_log: List[str]
    best_alpha: dict  # Tracks the best alpha found so far {'alpha_id', 'sharpe', 'fitness'}
    initial_best_alpha: dict # Stores the original seed alpha's performance for comparison

    # --- Internal control flow ---
    iteration_count: int  # Main loop counter
    no_valid_candidates_counter: int # Counter for consecutive empty valid_candidates
    
    # --- Intermediate ---
    current_batch_alpha_ids: List[str]
    passed_filter_alphas: List[Dict[str, Any]] # Alphas that passed hard filters, waiting for ranking

# --- Helper Functions ---

def load_llm_config():
    """Loads LLM configuration from config/config.ini using utils.load_config."""
    try:
        config_data = utils.load_config("config/config.ini")
        if not config_data:
            return {}
        return {
            "model": config_data.get("llm_model"),
            "base_url": config_data.get("llm_base_url"),
            "api_key": config_data.get("llm_api_key")
        }
    except Exception as e:
        logger.error(f"Error loading LLM config: {e}")
        return {}

def get_model(temperature=0.7):
    llm_config = load_llm_config()
    if not all(llm_config.values()):
        logger.error("LLM config missing.")
        return None
    return ChatOpenAI(
        model=llm_config["model"],
        base_url=llm_config["base_url"],
        api_key=llm_config["api_key"],
        temperature=temperature,
        request_timeout=180.0,
    )

def _get_performance_report(session, seed_alpha_id: str, state: WorkflowState) -> Tuple[pd.DataFrame, List[str]]:
    """Fetches the performance report and returns the check DataFrame and a list of fail names."""
    session = ace_lib.check_session_and_relogin(session)
    check_df = utils.safe_api_call(ace_lib.get_check_submission, session, seed_alpha_id)
    fail_names = []
    
    if check_df is not None and not check_df.empty:
        filtered_df = check_df[check_df['result'].isin(['PASS', 'FAIL', 'WARNING'])]
        perf_lines = [f"name:{row['name']},result:{row['result']},limit:{row.get('limit', 'N/A')},value:{row.get('value', 'N/A')}"
                      for _, row in filtered_df.iterrows()]
        state["seed_performance_report"] = "\n".join(perf_lines)
        fail_names = filtered_df[filtered_df['result'] == 'FAIL']['name'].tolist()
        
        if not fail_names:
            for corr_check in ['PROD_CORRELATION', 'SELF_CORRELATION']:
                check_row = check_df[check_df['name'] == corr_check]
                if not check_row.empty:
                     if check_row.iloc[0]['result'] != 'PASS':
                         fail_names.append(corr_check)
    else:
        state["seed_performance_report"] = f"Could not retrieve submission checks for {seed_alpha_id}."
        
    stats_df = utils.safe_api_call(ace_lib.get_alpha_yearly_stats, session, seed_alpha_id)
    if stats_df is not None and not stats_df.empty:
        # Drop alpha_id column as requested
        if 'alpha_id' in stats_df.columns:
            stats_df = stats_df.drop(columns=['alpha_id'])
        
        # Round numeric columns to 4 decimal places for readability
        numeric_cols = stats_df.select_dtypes(include=['number']).columns
        stats_df[numeric_cols] = stats_df[numeric_cols].round(4)

        report_lines = []
        for _, row in stats_df.iterrows():
            line = ",".join([f"{col}:{val}" for col, val in row.items()])
            report_lines.append(line)
        state["yearly_performance_report"] = "\n".join(report_lines)
    else:
        state["yearly_performance_report"] = f"Could not retrieve yearly stats for {seed_alpha_id}."
        
    return check_df, fail_names

def _build_data_fields(session, alpha_id: str, state: WorkflowState) -> List[str]:
    """
    Identifies data fields, categorizes them, and updates the state with a comprehensive
    list of valid data fields by combining the alpha's datasets with pv1 fields.
    """
    session = ace_lib.check_session_and_relogin(session)
    # 1. Get datasets specifically used by the alpha
    all_datasets_df, _ = utils.get_datasets_for_alpha(alpha_id, session)

    # 2. Extract field categories for the knowledge base from the alpha's datasets
    field_categories = []
    if all_datasets_df is not None and not all_datasets_df.empty and 'category_id' in all_datasets_df.columns:
        field_categories = list(set(all_datasets_df['category_id'].dropna().tolist()))
    logger.info(f"Identified {len(field_categories)} dataset categories for knowledge base: {field_categories}")

    # 3. If data fields are already in the state, use them
    if "all_fields_df" in state and not state["all_fields_df"].empty and "valid_data_fields" in state and state["valid_data_fields"]:
        logger.info(f"Using {len(state['valid_data_fields'])} existing valid data fields from the workflow state.")
        return field_categories

    # 4. If not, get pv1 fields and combine them with the alpha's datasets
    settings = state["row_setting"]
    region = settings.get("region")
    universe = settings.get("universe")
    delay = settings.get("delay", 1)

    grouping_fields_df = get_datafields_with_cache(
        session, region=region, universe=universe, delay=delay, data_type='GROUP', dataset_id='pv1'
    )
    price_fields_df = get_datafields_with_cache(
        session, region=region, universe=universe, delay=delay, data_type='MATRIX', dataset_id='pv1'
    )

    # 5. Concatenate all dataframes to build the complete list of available fields
    dfs_to_concat = []
    if all_datasets_df is not None and not all_datasets_df.empty:
        dfs_to_concat.append(all_datasets_df)
    if grouping_fields_df is not None and not grouping_fields_df.empty:
        dfs_to_concat.append(grouping_fields_df)
    if price_fields_df is not None and not price_fields_df.empty:
        dfs_to_concat.append(price_fields_df)
    
    if dfs_to_concat:
        all_fields_df = pd.concat(dfs_to_concat, ignore_index=True)
    else:
        all_fields_df = pd.DataFrame()

    # 6. Update the state with the comprehensive field lists
    state["all_fields_df"] = all_fields_df
    if not all_fields_df.empty:
        state["valid_data_fields"] = set(all_fields_df['id'].unique())
    else:
        state["valid_data_fields"] = set()

    logger.info(f"Loaded {len(state['valid_data_fields'])} total valid data fields for the workflow.")
    
    return field_categories

def _load_knowledge_base(field_categories: List[str], fail_names: List[str], state: WorkflowState):
    """
    Loads knowledge base content based on dataset categories and performance failures.
    """
    dataset_tips = []
    loaded_dataset_paths = set()
    # Get the project root assuming the script is in src/
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    for category in field_categories:
        doc_path = FILE_PATHS["HOW_TO_USE_DATASETS"].get(category.lower())
        if doc_path:
            full_path = os.path.join(project_root, doc_path)
            if os.path.exists(full_path) and full_path not in loaded_dataset_paths:
                with open(full_path, 'r') as f:
                    dataset_tips.append(f.read())
                loaded_dataset_paths.add(full_path)
    
    improvement_methods = []
    loaded_improve_paths = set()
    for fail in fail_names:
        doc_path = FILE_PATHS["IMPROVE_METHODS"].get(fail)
        if doc_path:
            full_path = os.path.join(project_root, doc_path)
            if os.path.exists(full_path) and full_path not in loaded_improve_paths:
                with open(full_path, 'r') as f:
                    improvement_methods.append(f.read())
                loaded_improve_paths.add(full_path)

    state['knowledge_base'] = {
        'dataset_tips': "\n\n".join(dataset_tips),
        'improvement_methods': "\n\n".join(improvement_methods)
    }
    logger.info("Knowledge base loaded with dataset tips and improvement methods.")


def _check_correlations(session, alpha_id: str) -> dict:
    """
    Checks production, self, and power pool correlations with short-circuiting.
    Returns a dict with values and boolean result.
    """
    # 1. Check production correlation (Most critical)
    prod_corr_result = utils.safe_api_call(ace_lib.check_prod_corr_test, session, alpha_id=alpha_id)
    pc = prod_corr_result['value'].max() if prod_corr_result is not None and not prod_corr_result.empty else 1.0
    
    if pc >= 0.7:
        logger.info(f"Alpha {alpha_id} FAIL PC ({pc:.4f} >= 0.7) - Short-circuiting")
        return {"pc": pc, "sc": None, "ppc": None, "pass": False}

    # 2. Check self correlation
    self_corr_result = utils.safe_api_call(ace_lib.check_self_corr_test, session, alpha_id=alpha_id)
    sc = self_corr_result['value'].max() if self_corr_result is not None and not self_corr_result.empty else 1.0
    
    if sc >= 0.5:
        logger.info(f"Alpha {alpha_id} FAIL SC ({sc:.4f} >= 0.5) - Short-circuiting")
        return {"pc": pc, "sc": sc, "ppc": None, "pass": False}

    # 3. Check power pool correlation
    ppc_result = utils.safe_api_call(ace_lib_ext.check_power_pool_corr_test, session, alpha_id=alpha_id)
    ppc = ppc_result['value'].max() if ppc_result is not None and not ppc_result.empty and ppc_result['value'].notna().any() else 0.0
    
    if ppc >= 0.5:
        logger.info(f"Alpha {alpha_id} FAIL PPC ({ppc:.4f} >= 0.5)")
        return {"pc": pc, "sc": sc, "ppc": ppc, "pass": False}
    
    return {
        "pc": pc,
        "sc": sc,
        "ppc": ppc,
        "pass": True
    }

# --- Nodes ---

def initialize_context(state: WorkflowState) -> WorkflowState:
    """Node 1: Initialize Context, Performance, Knowledge Base, and Scoring."""
    
    session = ace_lib.check_session_and_relogin(state["session"])
    state["session"] = session
    seed_alpha_id = state["seed_alpha_id"]
    
    # 1. Fetch Alpha Details
    alpha_details = utils.safe_api_call(ace_lib.get_simulation_result_json, session, seed_alpha_id)
    if not alpha_details:
        state["status"] = "failed"
        state["error_log"].append(f"Failed to fetch details for {seed_alpha_id}")
        return state
    
    state["seed_expression"] = alpha_details.get("regular", {}).get("code", "")
    full_settings = alpha_details.get("settings", {})
    state["row_setting"] = full_settings
    state["seed_setting"] = {
        k: v for k, v in full_settings.items()
        if k in ["neutralization", "decay", "truncation"]
    }
    
    # 2. Get Performance Report and Fail Names
    _, fail_names = _get_performance_report(session, seed_alpha_id, state)
    
    # 3. Build Data Fields and Load Knowledge Base
    field_categories = _build_data_fields(session, seed_alpha_id, state)
    _load_knowledge_base(field_categories, fail_names, state)

    # 4. Score Seed Alpha (if first iteration)
    if "best_alpha" not in state or not state["best_alpha"]:
        logger.info(f"Scoring initial seed {seed_alpha_id} to set baseline.")
        score_res = alpha_score.get_alpha_score(session, seed_alpha_id)
        
        initial_score = score_res.get("final_score", -1.0)
        
        is_stats = alpha_details.get("is", {})
        initial_alpha_info = {
            "alpha_id": seed_alpha_id,
            "sharpe": is_stats.get("sharpe", 0),
            "fitness": is_stats.get("fitness", 0),
            "score": initial_score
        }
        state["initial_best_alpha"] = initial_alpha_info
        state["best_alpha"] = initial_alpha_info
        logger.info(f"Initial Seed Score set: {initial_score:.4f}")
    
    # 5. Reset iteration-specific lists
    state["proposed_alphas"] = []
    state["valid_candidates"] = []
    state["current_batch_alpha_ids"] = []
    state["passed_filter_alphas"] = []

    # 6. Final Detailed Logging
    hist_count = len(state.get('historical_alphas', []))
    repeat_count = len(state.get('repeat_historical_alphas', []))
    total_generated = hist_count + repeat_count
    repeat_rate = (repeat_count / total_generated * 100) if total_generated > 0 else 0.0
    no_valid_counter = state.get('no_valid_candidates_counter', 0)
    
    best_alpha_info = state.get('best_alpha', {})
    seed_score = best_alpha_info.get('score', -1.0)
    seed_sharpe = best_alpha_info.get('sharpe', 'N/A')
    seed_fitness = best_alpha_info.get('fitness', 'N/A')

    logger.info(f"\n{ '='*80}")
    logger.info(f"--- ITERATION {state.get('iteration_count', 0) + 1} SUMMARY ---")
    logger.info(f"  Seed Alpha ID: {seed_alpha_id}")
    logger.info(f"  Seed Score: {seed_score:.4f} | Sharpe: {seed_sharpe:.4f} | Fitness: {seed_fitness:.4f}")
    logger.info(f"  Seed Expression: {state['seed_expression']}")
    logger.info(f"  History: {hist_count} Alphas | Repeats: {repeat_count} ({repeat_rate:.1f}%)")
    logger.info(f"  Consecutive No-Valid-Candidates: {no_valid_counter}")
    logger.info(f"{ '='*80}\n")
    
    return state

def generate_improvement_directions(state: WorkflowState) -> WorkflowState:
    """Node 2: Generate Improvement Directions (Temperature 0.7)"""
    logger.info("--- Node 2: Generate Directions ---")
    
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Read prompt template
    try:
        prompt_path = os.path.join(project_root, FILE_PATHS["PROMPT_DIRECTION"])
        with open(prompt_path, "r") as f:
            template = f.read()
    except Exception as e:
        logger.error(f"Failed to read prompt: {e}")
        return state

    # Read essence examples
    try:
        essence_path = os.path.join(project_root, FILE_PATHS["ESSENCE_EXAMPLES"])
        with open(essence_path, "r") as f:
            essence_examples = f.read()
    except Exception as e:
        logger.warning(f"Failed to read essence examples: {e}")
        essence_examples = "No essence examples available."

    # Format prompt
    prompt = template.replace("{seed_expression}", state["seed_expression"])
    prompt = prompt.replace("{seed_setting}", json.dumps(state["seed_setting"], indent=2))
    prompt = prompt.replace("{seed_performance_report}", state["seed_performance_report"])
    prompt = prompt.replace("{yearly_performance_report}", state["yearly_performance_report"])
    
    kb = state.get("knowledge_base", {})
    prompt = prompt.replace("{dataset_tips}", kb.get("dataset_tips", "No tips available."))
    prompt = prompt.replace("{improvement_methods}", kb.get("improvement_methods", "No methods available."))
    prompt = prompt.replace("{essence_examples}", essence_examples)

    # 获取数据字段信息用于辅助理解和逻辑注入
    fields_df = state.get('all_fields_df')
    if fields_df is not None and not fields_df.empty:
        # 选取关键列并限制样本数量
        selected_columns = ['id', 'description', 'type', 'coverage']
        # 确保列存在
        actual_columns = [col for col in selected_columns if col in fields_df.columns]
        datafields_sample_string = fields_df[actual_columns].to_string(index=False)
    else:
        datafields_sample_string = "No data fields available."
    
    prompt = prompt.replace("{dataset_fields}", datafields_sample_string)
    
    # Call LLM
    llm = get_model(temperature=0.7)
    if not llm:
        return state
        
    max_retries = 3
    attempts = 0
    
    while attempts < max_retries:
        try:
            response = llm.invoke(prompt)
            # Parse JSON (basic logic)
            content = response.content
            
            # Use robust extraction
            json_str = extract_json(content)
            if not json_str:
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                if json_match:
                    json_str = json_match.group()
            
            if json_str:
                try:
                    data = json.loads(json_str)
                except json.JSONDecodeError as e:
                    logger.warning(f"JSON parse failed: {e}. Attempting repair...")
                    json_str_fixed = re.sub(r',\s*([\]}])', r'\1', json_str)
                    data = json.loads(json_str_fixed)
                    
                if "directions" in data:
                    # Validate with Pydantic
                    validated = DirectionNodeOutput(directions=data["directions"])
                    state["search_directions"] = validated.directions
                    logger.info(f"Generated {len(state['search_directions'])} directions.")
                    
                    # Log detailed directions
                    for i, d in enumerate(state["search_directions"]):
                        logger.info(f"Direction {i+1}:")
                        logger.info(f"  Core Idea: {d.core_idea}")
                        logger.info(f"  Differentiation: {d.differentiation}")
                        logger.info(f"  Target Structure: {d.target_structure}")
                        logger.info(f"  Constraints: {d.constraints}")
                    break
                else:
                    logger.warning("No 'directions' key in JSON.")
                    raise ValueError("No 'directions' key in JSON")
            else:
                logger.warning("No JSON found in response.")
                raise ValueError("No JSON found in response")
                
        except (json.JSONDecodeError, ValueError, ValidationError, RemoteDisconnected, ConnectionError) as e:
            attempts += 1
            logger.warning(f"Attempt {attempts}/{max_retries}: Failed to parse or validate LLM response: {e}")
            time.sleep(3)
        except Exception as e:
            attempts += 1
            logger.error(f"Attempt {attempts}/{max_retries}: Error during LLM call: {e}")
            time.sleep(3)

    if attempts >= max_retries:
        state["error_log"].append(f"Direction gen failed after {max_retries} attempts.")
        
    return state

def extract_json(text):
    """
    Attempts to extract a valid JSON object or a list of proposal objects from text.
    Handles truncated JSON by parsing individual objects within the 'proposals' list.
    """
    text = text.strip()
    
    # 1. Try to extract the full JSON object first (balanced braces)
    idx = text.find('{')
    if idx != -1:
        brace_count = 0
        in_string = False
        escape = False
        
        for i in range(idx, len(text)):
            char = text[i]
            if in_string:
                if char == '"' and not escape:
                    in_string = False
                elif char == '\\':
                    escape = not escape
                else:
                    escape = False
            else:
                if char == '"':
                    in_string = True
                elif char == '{':
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        return text[idx:i+1] # Found full valid object
    
    # 2. If full object not found (likely truncated), try to extract partial proposals
    # Look for "proposals": [
    match = re.search(r'"proposals"\s*:\s*\[', text)
    if match:
        valid_proposals = []
        start_scan = match.end()
        current_pos = start_scan
        
        while current_pos < len(text):
            # Find start of next object
            obj_start = text.find('{', current_pos)
            if obj_start == -1:
                break
            
            # Try to extract one balanced object
            obj_text = None
            brace_count = 0
            in_string = False
            escape = False
            
            for i in range(obj_start, len(text)):
                char = text[i]
                if in_string:
                    if char == '"' and not escape:
                        in_string = False
                    elif char == '\\':
                        escape = not escape
                    else:
                        escape = False
                else:
                    if char == '"':
                        in_string = True
                    elif char == '{':
                        brace_count += 1
                    elif char == '}':
                        brace_count -= 1
                        if brace_count == 0:
                            obj_text = text[obj_start:i+1]
                            current_pos = i + 1
                            break
            
            if obj_text:
                try:
                    obj = json.loads(obj_text)
                    if "new_expression" in obj:
                        valid_proposals.append(obj)
                except:
                    pass
            else:
                # Could not find closing brace for this object
                break
        
        if valid_proposals:
            return json.dumps({"proposals": valid_proposals})

    return None

def generate_alpha_candidates(state: WorkflowState) -> WorkflowState:
    """Node 3: Generate Alpha Candidates (Temperature 0.2)"""
    logger.info("--- Node 3: Generate Alphas ---")
    directions = state.get("search_directions", [])
    if not directions:
        logger.warning("No directions to generate alphas from.")
        return state
        
    # Prepare resources
    session = state["session"]
    try:
        operators_df = utils.safe_api_call(ace_lib.get_operators, session)
        regular_ops = operators_df[operators_df['scope'] == 'REGULAR']
        operators_json = regular_ops.to_json(orient='records')
    except:
        operators_json = "[]"
        
    # Prepare datafields string
    fields_df = state.get('all_fields_df')
    if fields_df is not None and not fields_df.empty:
        selected_columns = ['id', 'description', 'type', 'coverage']
        actual_columns = [col for col in selected_columns if col in fields_df.columns]
        datafields_string = fields_df[actual_columns].to_string(index=False)
    else:
        datafields_string = "See ACE data fields documentation."
    
    # Read prompt template
    try:
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        prompt_path = os.path.join(project_root, FILE_PATHS["PROMPT_GENERATION"])
        with open(prompt_path, "r") as f:
            template_raw = f.read()
    except Exception as e:
        logger.error(f"Failed to read gen prompt: {e}")
        return state
        
    llm = get_model(temperature=0.2)
    all_proposals = []
    
    # 获取精华示例用于生成阶段的参考
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    try:
        essence_path = os.path.join(project_root, FILE_PATHS["ESSENCE_EXAMPLES"])
        with open(essence_path, "r") as f:
            essence_examples = f.read()
    except:
        essence_examples = "No essence examples available."

    kb = state.get("knowledge_base", {})
    dataset_tips = kb.get("dataset_tips", "No tips available.")
    improvement_methods = kb.get("improvement_methods", "No methods available.")

    for direction in directions:
        logger.info(f"Generating for direction: {direction.core_idea[:30]}...")
        prompt = template_raw.replace("{direction_core_idea}", direction.core_idea)
        prompt = prompt.replace("{direction_differentiation}", direction.differentiation)
        prompt = prompt.replace("{direction_target_structure}", direction.target_structure)
        prompt = prompt.replace("{direction_constraints}", direction.constraints)
        prompt = prompt.replace("{seed_expression}", state["seed_expression"])
        
        # Inject seed settings
        seed_settings = state.get("seed_setting", {})
        prompt = prompt.replace("{seed_neutralization}", str(seed_settings.get("neutralization", "N/A")))
        prompt = prompt.replace("{seed_decay}", str(seed_settings.get("decay", "N/A")))
        prompt = prompt.replace("{seed_truncation}", str(seed_settings.get("truncation", "N/A")))

        prompt = prompt.replace("{operators_json}", operators_json)
        prompt = prompt.replace("{datafields_string}", datafields_string)
        prompt = prompt.replace("{NEUTRALIZATION_OPTIONS}", " ".join(NEUTRALIZATION_OPTIONS))
        
        # 注入缺失的知识库字段
        prompt = prompt.replace("{dataset_tips}", dataset_tips)
        prompt = prompt.replace("{improvement_methods}", improvement_methods)
        prompt = prompt.replace("{essence_examples}", essence_examples)
        
        max_retries = 3
        attempts = 0
        success = False
        
        while attempts < max_retries:
            try:
                response = llm.invoke(prompt)
                content = response.content
                
                # Use robust extraction
                json_str = extract_json(content)
                if not json_str:
                    # Fallback to regex
                    json_match = re.search(r'\{.*\}', content, re.DOTALL)
                    if json_match:
                        json_str = json_match.group()
                
                if json_str:
                    try:
                        data = json.loads(json_str)
                    except json.JSONDecodeError as e:
                        logger.warning(f"JSON parse failed: {e}. Attempting repair...")
                        # Fix trailing commas
                        json_str_fixed = re.sub(r',\s*([\]}])', r'\1', json_str)
                        try:
                            data = json.loads(json_str_fixed)
                        except:
                             raise e # Raise original error if repair fails

                    if "proposals" in data:
                        validated = AlphaProposal(proposals=data["proposals"])
                        all_proposals.extend(validated.proposals)
                        logger.info(f"Generated {len(validated.proposals)} alphas for direction: {direction.core_idea[:30]}...")
                        success = True
                        break
                    else:
                        logger.warning(f"No 'proposals' key in JSON for direction: {direction.core_idea[:30]}...")
                        raise ValueError("No 'proposals' key in JSON")
                else:
                    logger.warning("No JSON found in response.")
                    raise ValueError("No JSON found in response")
            except (json.JSONDecodeError, ValueError, ValidationError, RemoteDisconnected, ConnectionError) as e:
                attempts += 1
                logger.warning(f"Attempt {attempts}/{max_retries}: Failed to parse LLM response for direction: {e}")
                logger.error(f"LLM Raw Output:\n{content}")
                time.sleep(3)
            except Exception as e:
                attempts += 1
                logger.error(f"Attempt {attempts}/{max_retries}: Error during LLM call for direction: {e}")
                time.sleep(3)
        
        if not success:
             logger.error(f"Failed to generate alphas for direction: {direction.core_idea[:30]}... after {max_retries} attempts.")
            
    # Convert ProposedAlpha objects to dicts for state
    state["proposed_alphas"] = [p.dict() for p in all_proposals]
    logger.info(f"Total proposed alphas: {len(state['proposed_alphas'])}")
    return state

def validate_candidates(state: WorkflowState) -> WorkflowState:
    """Node 4: Validate Syntax & Fields"""
    logger.info("--- Node 4: Validate ---")
    session = state["session"]
    valid_candidates = []
    
    # Pre-fetch operators for syntax check
    try:
        operators_df = utils.safe_api_call(ace_lib.get_operators, session)
        regular_operators_df = operators_df[operators_df['scope'] == 'REGULAR']
    except:
        regular_operators_df = None

    # Prepare validation resources
    valid_fields_df = state.get("all_fields_df")
    valid_field_ids = state.get("valid_data_fields", set())
    
    # Construct settings dict for filter_alpha_by_datafields
    settings_dict = state.get("row_setting", {}).copy()

    proposed_count = len(state["proposed_alphas"])
    logger.info(f"Validating {proposed_count} proposed alphas...")

    for idx, alpha in enumerate(state["proposed_alphas"]):
        # proposed_alphas are dicts
        expr = alpha.get("new_expression")
        
        if not expr:
            logger.warning(f"Alpha {idx+1}/{proposed_count}: Skipping empty expression.")
            continue

        logger.debug(f"Alpha {idx+1}/{proposed_count}: Checking expression: {expr}")

        # 1. Filter out alphas using only pv1 fields
        # This prevents generating simple price-volume alphas when we want more complex logic
        if not utils.filter_alpha_by_datafields(expr, settings_dict, session):
            logger.info(f"Alpha {idx+1}/{proposed_count} FILTERED (only pv1 fields): {expr}")
            continue

        # 2. Validate fields exist in valid_field_ids
        is_fields_valid, field_error = utils.validate_expression_fields(expr, valid_field_ids, session)
        if not is_fields_valid:
            logger.warning(f"Alpha {idx+1}/{proposed_count} FIELD FAIL: {expr} | Error: {field_error}")
            continue
            
        # 3. Syntax Check
        is_syntax_valid, syntax_error = check_expression_syntax(expr, session, operators_df=regular_operators_df)
        if not is_syntax_valid:
            logger.warning(f"Alpha {idx+1}/{proposed_count} SYNTAX FAIL: {expr} | Error: {syntax_error}")
            # Log to syntax error log file
            try:
                with open("syntax_error_log.txt", "a") as f:
                    f.write(f"--- Stage 2 Syntax Error ---\nExpression: {expr}\nError: {syntax_error}\n\n")
            except:
                pass
            continue

        # 4. Auto-completion / Correction (e.g. adding vec_avg for vector fields)
        try:
            # complete_expression requires dataframe to check types
            completed_expressions = utils.complete_expression(expr, valid_fields_df, session)
            if completed_expressions:
                processed_expr = completed_expressions[0]
                if processed_expr != expr:
                    logger.info(f"Alpha {idx+1}/{proposed_count} CORRECTED:\n  Original: {expr}\n  Processed: {processed_expr}")
                
                # Update the candidate expression
                alpha["new_expression"] = processed_expr
                valid_candidates.append(alpha)
                logger.info(f"✅ Alpha {idx+1}/{proposed_count} PASSED: {processed_expr}")
            else:
                 logger.info(f"Alpha {idx+1}/{proposed_count} Completion Empty for: {expr}")
        except Exception as e:
            logger.error(f"Alpha {idx+1}/{proposed_count} Completion Error for {expr}: {e}")
            pass
        
    state["valid_candidates"] = valid_candidates
    
    # Update no_valid_candidates_counter
    if not valid_candidates:
        state["no_valid_candidates_counter"] += 1
        logger.warning(f"No valid candidates found in this batch. Counter: {state['no_valid_candidates_counter']}")
    else:
        state["no_valid_candidates_counter"] = 0
        
    logger.info(f"Validation summary: {len(valid_candidates)}/{proposed_count} passed.")
    return state

def simulate_candidates(state: WorkflowState) -> WorkflowState:
    """Node 5: Batch Simulation (10 at a time)"""
    logger.info("--- Node 5: Simulate ---")
    session = ace_lib.check_session_and_relogin(state["session"])
    state["session"] = session
    
    candidates_to_run = state["valid_candidates"][:10]
    state["valid_candidates"] = state["valid_candidates"][10:] # Remove picked 
    
    if not candidates_to_run:
        logger.info("No candidates to simulate.")
        state["current_batch_alpha_ids"] = []
        return state
        
    sim_jobs = []
    for cand in candidates_to_run:
        settings = state["row_setting"].copy()
        if cand.get("new_setting"):
            new_val = cand["new_setting"]
            # 1. Neutralization
            neut = new_val.get("neutralization")
            if neut in NEUTRALIZATION_OPTIONS or neut == "NONE":
                settings["neutralization"] = neut
            
            # 2. Decay
            decay = new_val.get("decay")
            if isinstance(decay, (int, float)):
                settings["decay"] = decay
            
            # 3. Truncation
            trunc = new_val.get("truncation")
            if isinstance(trunc, (int, float)):
                settings["truncation"] = trunc
            
        settings["testPeriod"] = "P2Y"
        settings["maxTrade"] = "OFF"
        settings["visualization"] = False
        
        sim_jobs.append({
            "type": "REGULAR",
            "settings": settings,
            "regular": cand["new_expression"]
        })
        
    # Run simulation with timeout and retries
    logger.info(f"Simulating batch of {len(sim_jobs)}. Remaining in valid_candidates: {len(state['valid_candidates'])}")
    batch_results = []
    max_retries = 3
    attempts = 0
    timeout_duration = 30 * 60 # 30 minutes

    while attempts < max_retries:
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        try:
            logger.info(f"Running batch simulation... (Attempt {attempts + 1}/{max_retries})")
            future = executor.submit(ace_lib.simulate_multi_alpha, session, sim_jobs)
            batch_results = future.result(timeout=timeout_duration)
            executor.shutdown(wait=True)
            logger.info("Batch simulation completed.")
            break
        except concurrent.futures.TimeoutError as e:
            logger.warning(f"Simulation timed out (Attempt {attempts + 1}). forcing retry...")
            executor.shutdown(wait=False)
            attempts += 1
            if attempts >= max_retries:
                logger.error("Simulation failed after max retries due to timeout.")
                state["error_log"].append("Simulation timed out after max retries.")
                batch_results = []
                break
            time.sleep(60)
            session = ace_lib.start_session() # Re-login
            state["session"] = session
        except (RemoteDisconnected, ConnectionError, Exception) as e:
            logger.error(f"Simulation failed: {e} (Attempt {attempts + 1})")
            executor.shutdown(wait=False)
            attempts += 1
            if attempts >= max_retries:
                logger.error("Simulation failed after max retries.")
                state["error_log"].append(f"Simulation failed: {e}")
                batch_results = []
                break
            time.sleep(60)
            session = ace_lib.start_session()
            state["session"] = session

    ids = [r["alpha_id"] for r in batch_results if r.get("alpha_id")]
    state["current_batch_alpha_ids"] = ids
    logger.info(f"Simulation done. IDs: {len(ids)}")
        
    return state

def filter_candidates(state: WorkflowState) -> WorkflowState:
    """Node 6: Hard Rule Filter"""
    logger.info("--- Node 6: Hard Rule Filter ---")
    session = ace_lib.check_session_and_relogin(state["session"])
    state["session"] = session
    
    batch_ids = state.get("current_batch_alpha_ids", [])
    if not batch_ids:
        logger.info("No alpha IDs to filter.")
        return state
        
    passed_alphas = state.get("passed_filter_alphas", [])
    
    # Use Initial Best Alpha ID for Tagging
    init_best = state.get("initial_best_alpha", {})
    init_id = init_best.get("alpha_id", state["seed_alpha_id"]) # Fallback to current seed if missing
    tag = f"stage_two_{init_id}"

    filtered_out_count = 0
    success_reached_count = 0
    
    # Pre-load Self-Correlation Data
    try:
        logger.info("Loading self-correlation data...")
        download_data(session, flag_increment=True)
        os_alpha_ids, os_alpha_rets = load_data()
    except Exception as e:
        logger.error(f"Failed to load self-correlation data: {e}")
        os_alpha_ids, os_alpha_rets = {}, pd.DataFrame()

    for alpha_id in batch_ids:
        logger.info(f"Processing Alpha: {alpha_id}")
        
        # 1. Tag
        utils.safe_api_call(ace_lib.set_alpha_properties, session, alpha_id, tags=[tag])
        
        # 2. Check Submission & Correlations (Only if Submission Check Passes)
        check_df = utils.safe_api_call(ace_lib.get_check_submission, session, alpha_id)
        
        has_fail = False
        if check_df is not None:
            if 'FAIL' in check_df['result'].unique():
                has_fail = True
        else:
            has_fail = True 
            
        if not has_fail:
            # Only check correlations if submission check passed
            corr_res = _check_correlations(session, alpha_id)
            if corr_res["pass"]:
                logger.info(f"🎉 Alpha {alpha_id} reached SUBMISSION STANDARD!")
                state["success_id"].append(alpha_id)
                state["status"] = "success"
                utils.safe_api_call(ace_lib.set_alpha_properties, session, alpha_id, tags=[tag, "success"])
                success_reached_count += 1
        
        # 3. Record History
        alpha_res = utils.safe_api_call(ace_lib.get_simulation_result_json, session, alpha_id)
        if not alpha_res: 
            filtered_out_count += 1
            continue
        expr = alpha_res.get("regular", {}).get("code", "")
        
        if expr in state["historical_alphas"]:
            state["repeat_historical_alphas"].append(expr)
        else:
            state["historical_alphas"].append(expr)
            
        # 4. Hard Metric Filter
        stats_df = utils.safe_api_call(ace_lib.get_alpha_yearly_stats, session, alpha_id)
        is_stats = alpha_res.get("is", {})
        
        sharpe = is_stats.get("sharpe", 0)
        fitness = is_stats.get("fitness", 0)
        
        # Data >= 8 years
        active_years = (stats_df['sharpe'] != 0.0).sum() if stats_df is not None else 0
        if active_years < 8:
            logger.info(f"Alpha {alpha_id}: Not enough years ({active_years}) - FILTERED")
            filtered_out_count += 1
            continue
            
        # Specific Metrics
        checks_map = {}
        if check_df is not None:
            for _, row in check_df.iterrows():
                checks_map[row['name']] = row.get('value')
                checks_map[f"{row['name']}_RESULT"] = row.get('result')
        
        # Thresholds
        if sharpe < 1.3: 
            logger.info(f"Alpha {alpha_id}: Low Sharpe ({sharpe:.4f} < 1.3) - FILTERED")
            filtered_out_count += 1
            continue
        if fitness < 0.8: 
            logger.info(f"Alpha {alpha_id}: Low Fitness ({fitness:.4f} < 0.8) - FILTERED")
            filtered_out_count += 1
            continue
        
        # Robust (LOW_ROBUST_UNIVERSE_SHARPE)
        if 'LOW_ROBUST_UNIVERSE_SHARPE' in checks_map:
            val = float(checks_map['LOW_ROBUST_UNIVERSE_SHARPE']) if checks_map['LOW_ROBUST_UNIVERSE_SHARPE'] is not None else 0
            if val < 0.8: 
                logger.info(f"Alpha {alpha_id}: Low Robust Sharpe ({val:.4f} < 0.8) - FILTERED")
                filtered_out_count += 1
                continue
            
        # ASI JPN
        if 'LOW_ASI_JPN_SHARPE' in checks_map:
            val = float(checks_map['LOW_ASI_JPN_SHARPE']) if checks_map['LOW_ASI_JPN_SHARPE'] is not None else 0
            if val < 0.8: 
                logger.info(f"Alpha {alpha_id}: Low ASI JPN Sharpe ({val:.4f} < 0.8) - FILTERED")
                filtered_out_count += 1
                continue
            
        # Concentrated Weight
        if checks_map.get('CONCENTRATED_WEIGHT_RESULT') != 'PASS': 
            logger.info(f"Alpha {alpha_id}: Concentrated Weight Fail - FILTERED")
            continue
        
        # Region Sharpes
        region_fail = False
        for reg in ['LOW_GLB_AMER_SHARPE', 'LOW_GLB_EMEA_SHARPE', 'LOW_GLB_APAC_SHARPE']:
            if reg in checks_map:
                val = float(checks_map[reg]) if checks_map[reg] is not None else 0
                if val <= 0.7: 
                    logger.info(f"Alpha {alpha_id}: Low {reg} ({val:.4f} <= 0.7) - FILTERED")
                    region_fail = True
                    break
        if region_fail: 
            filtered_out_count += 1
            continue
                
        # Recent Years Sharpe (Last 2 and 3, excluding partial current)
        if stats_df is not None and len(stats_df) >= 3:
            full_years = stats_df.iloc[:-1] # exclude current year
            if len(full_years) >= 2:
                recent_sharpes = full_years.iloc[-2:]['sharpe']
                if recent_sharpes.mean() <= 0.5:
                    logger.info(f"Alpha {alpha_id}: Low recent sharpe {recent_sharpes.mean():.4f} - FILTERED")
                    filtered_out_count += 1
                    continue
        
        # No year < -5.0
        if stats_df is not None:
            if (stats_df['sharpe'] < -5.0).any():
                logger.info(f"Alpha {alpha_id}: Has year < -5.0 - FILTERED")
                filtered_out_count += 1
                continue
                
        # Correlation with Submitted Alphas (using pre-loaded data)
        try:
            max_corr, _ = calc_self_corr(
                alpha_id=alpha_id, 
                sess=session, 
                os_alpha_rets=os_alpha_rets,
                os_alpha_ids=os_alpha_ids,
                return_alpha_pnls=True
            )
            if max_corr >= 0.7:
                logger.info(f"Alpha {alpha_id}: High correlation {max_corr:.4f} - FILTERED")
                filtered_out_count += 1
                continue
        except Exception as e:
            logger.warning(f"Self corr calc failed for {alpha_id}: {e}")
            filtered_out_count += 1
            continue
            
        # PASS ALL FILTERS
        logger.info(f"✅ Alpha {alpha_id} PASSED all Stage 2 Hard Filters! (S:{sharpe:.4f}, F:{fitness:.4f})")
        passed_alphas.append({"id": alpha_id, "sharpe": sharpe, "fitness": fitness})
        
        # Check Reverse Signal Condition
        if sharpe <= -1.3 and fitness <= -0.8:
            logger.info(f"Alpha {alpha_id}: Trigger Reverse (S:{sharpe:.4f}, F:{fitness:.4f})")
            rev_expr = f"reverse({expr})"
            state["valid_candidates"].append({
                "reasoning": f"Reverse of {alpha_id}",
                "new_expression": rev_expr,
                "new_setting": alpha_res.get("settings", {})
            })

    state["passed_filter_alphas"] = passed_alphas
    logger.info(f"Filter Node Summary: Processed {len(batch_ids)} | Filtered {filtered_out_count} | Passed {len(passed_alphas)} | New Success Standards {success_reached_count}")
    return state

def score_and_rank_candidates(state: WorkflowState) -> WorkflowState:
    """Node 7: Score and Rank"""
    logger.info("--- Node 7: Score & Rank ---")
    session = ace_lib.check_session_and_relogin(state["session"])
    state["session"] = session
    state["iteration_count"] += 1
    
    # 1. Check Success Status
    if state.get("status") == "success":
        logger.info("Workflow success! Stopping iteration.")
        return state
        
    passed_alphas = state.get("passed_filter_alphas", [])
    if not passed_alphas:
        logger.info("No alphas passed hard filters. Skipping ranking.")
        return state
        
    # 2. Score Candidates
    logger.info(f"Scoring and ranking {len(passed_alphas)} passed candidates...")
    best_cand = None
    best_score = -999.0
    
    for cand in passed_alphas:
        score_res = alpha_score.get_alpha_score(session, cand["id"])
        final_score = score_res.get("final_score", -1.0)
        cand["score"] = final_score
        logger.info(f"Alpha {cand['id']} | Score: {final_score:.4f} | Sharpe: {cand['sharpe']:.4f} | Fitness: {cand['fitness']:.4f}")
        
        if final_score > best_score:
            best_score = final_score
            best_cand = cand
            
    # 3. Update Seed if better
    current_best_score = state.get("best_alpha", {}).get("score", -1.0)
    
    if best_cand and best_score > current_best_score:
        logger.info(f"🚀 SUCCESS: Improved Alpha Found! New Seed: {best_cand['id']} (Score: {best_score:.4f}, Prev Best: {current_best_score:.4f})")
        state["best_alpha"] = {
            "alpha_id": best_cand["id"],
            "sharpe": best_cand["sharpe"],
            "fitness": best_cand["fitness"],
            "score": best_score
        }
        state["seed_alpha_id"] = best_cand["id"]
    else:
        logger.info(f"No improvement found in this batch. Max score in batch: {best_score:.4f} | Current global best: {current_best_score:.4f}")
    return state

# --- Conditional Edges ---

def check_loop_logic(state: WorkflowState) -> str:
    """
    Simulate -> Filter -> Check Valid Candidates
    If valid_candidates not empty -> Simulate
    If valid_candidates empty -> Score & Rank
    """
    if state["valid_candidates"]:
        return "simulate_candidates"
    return "score_and_rank_candidates"

def check_stop_conditions(state: WorkflowState) -> str:
    """Check if we should stop the workflow after scoring."""
    # Stop if success found? The prompt says "until found alpha". 
    # But if status is success, we stop.
    
    if state.get("status") == "success":
        return "end"
        
    if len(state["historical_alphas"]) >= 1000:
        return "end"
    if state["no_valid_candidates_counter"] >= 3:
        return "end"
        
    return "initialize_context"

# --- Graph Construction ---

def build_graph():
    workflow = StateGraph(WorkflowState)
    
    workflow.add_node("initialize_context", initialize_context)
    workflow.add_node("generate_improvement_directions", generate_improvement_directions)
    workflow.add_node("generate_alpha_candidates", generate_alpha_candidates)
    workflow.add_node("validate_candidates", validate_candidates)
    workflow.add_node("simulate_candidates", simulate_candidates)
    workflow.add_node("filter_candidates", filter_candidates)
    workflow.add_node("score_and_rank_candidates", score_and_rank_candidates)
    
    workflow.set_entry_point("initialize_context")
    
    workflow.add_edge("initialize_context", "generate_improvement_directions")
    workflow.add_edge("generate_improvement_directions", "generate_alpha_candidates")
    workflow.add_edge("generate_alpha_candidates", "validate_candidates")
    workflow.add_edge("validate_candidates", "simulate_candidates")
    
    # Simulation -> Filter Loop
    workflow.add_edge("simulate_candidates", "filter_candidates")
    
    workflow.add_conditional_edges(
        "filter_candidates",
        check_loop_logic,
        {
            "simulate_candidates": "simulate_candidates",
            "score_and_rank_candidates": "score_and_rank_candidates"
        }
    )
    
    # After ranking, decide to loop back to init or end
    workflow.add_conditional_edges(
        "score_and_rank_candidates",
        check_stop_conditions,
        {
            "initialize_context": "initialize_context",
            "end": END
        }
    )
    
    return workflow.compile()

def run_stage_two_workflow(seed_alpha_id: str):
    session = ace_lib.start_session()
    app = build_graph()
    
    initial_state = {
        "session": session,
        "seed_alpha_id": seed_alpha_id,
        "historical_alphas": [],
        "repeat_historical_alphas": [],
        "success_id": [],
        "error_log": [],
        "no_valid_candidates_counter": 0,
        "iteration_count": 0,
        "status": "running",
        "valid_data_fields": set(),
        "passed_filter_alphas": []
    }
    
    final_state = None
    try:
        # Use invoke to get the final state directly
        final_state = app.invoke(initial_state, config={"recursion_limit": 300})
        logger.info("Workflow Finished Successfully.")
        return final_state
    except Exception as e:
        logger.error(f"Workflow Error: {e}")
        return None

if __name__ == "__main__":
    # Set your seed alpha ID here
    #seed_ids = ['KPxVNoJx', 'd5ejllKE', 'E5oeQ3z9', 'gJ7jRjZM', 'JjR7x2XO', 'leJW7kbe', 'O0Kxl8JJ', '9qg850kd']  # Replace with actual ID if needed
    #seed_ids = ['zqX23kOd', '9qg850kd', 'gJ7jRjZM', 'JjR7x2XO', 'leJW7kbe', 'O0Kxl8JJ']
    seed_ids = ['kqeZd3vz', 'ZYkojRV3']
    for seed_id in seed_ids:
        if not seed_id:
            print("Please set a valid seed_alpha_id in the main function.")
        else:
            print(f"Starting Stage Two Workflow for Seed ID: {seed_id}")
            result_state = run_stage_two_workflow(seed_id)
            
            if result_state:
                # Prepare output directory
                output_dir = "/Users/tianyuan/repos/alpha_simulator_bot/output/workflow_state_result"
                os.makedirs(output_dir, exist_ok=True)
                
                # Construct filename
                date_str = datetime.now().strftime("%Y%m%d")
                filename = f"stage_two_{seed_id}_{date_str}.json"
                file_path = os.path.join(output_dir, filename)
                
                # Prepare state for saving (summarize large lists)
                saved_state = result_state.copy()
                
                # Remove session object as it's not serializable
                if "session" in saved_state:
                    del saved_state["session"]
                    
                # Summarize history lists
                if "historical_alphas" in saved_state:
                    saved_state["historical_alphas_count"] = len(saved_state["historical_alphas"])
                    del saved_state["historical_alphas"]
                    
                if "repeat_historical_alphas" in saved_state:
                    saved_state["repeat_historical_alphas_count"] = len(saved_state["repeat_historical_alphas"])
                    del saved_state["repeat_historical_alphas"]
                    
                # Exclude fields as requested
                for field in ["valid_data_fields", "search_directions", "proposed_alphas", "all_fields_df"]:
                    if field in saved_state:
                        del saved_state[field]

                # Save to file
                try:
                    with open(file_path, "w") as f:
                        json.dump(saved_state, f, indent=4, default=str)
                    print(f"✅ Final state saved to: {file_path}")
                except Exception as e:
                    print(f"❌ Failed to save final state: {e}")
            else:
                print("❌ Workflow failed to return a final state.")
