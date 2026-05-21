import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional
import utils
import ace_lib

# --- 硬过滤常量 ---
MIN_ACTIVE_YEARS = 8             # 最少活跃年数
STRICT_SHARPE_THRESHOLD = 1.3    # 夏普率硬门槛 (必须为正)
STRICT_FITNESS_THRESHOLD = 0.8   # Fitness 硬门槛 (必须为正)
MIN_YEARLY_SHARPE = -5.0         # 单年夏普最低允许值
STRICT_MARGIN_THRESHOLD = 0.0007 # Margin 硬门槛

def _passes_hard_filters(alpha: Dict[str, Any]) -> bool:
    """
    应用硬过滤规则，剔除不符合基本要求的 Alpha。
    本逻辑仅接受正向 Alpha，负向 Alpha 将直接被视为失败。
    """
    details = alpha.get("details", {})
    yearly_stats = alpha.get("yearly_stats")
    is_stats = details.get("is", {})

    if yearly_stats is None or yearly_stats.empty or not is_stats:
        return False

    def safe_get(d, key, default=0.0):
        val = d.get(key)
        return float(val) if val is not None else default

    # 1. 基础绩效检查 (显式拒绝负向 Alpha)
    overall_sharpe = safe_get(is_stats, "sharpe", 0.0)
    overall_fitness = safe_get(is_stats, "fitness", 0.0)
    
    if overall_sharpe < STRICT_SHARPE_THRESHOLD or overall_fitness < STRICT_FITNESS_THRESHOLD:
        return False

    # 2. Margin 检查
    margin = safe_get(is_stats, "margin", 0.0)
    if margin < STRICT_MARGIN_THRESHOLD:
        return False

    # 3. 年度夏普稳定性检查
    filled_yearly_sharpe = yearly_stats["sharpe"].fillna(0.0).astype(float)
    if filled_yearly_sharpe.min() < MIN_YEARLY_SHARPE:
        return False

    # 4. 活跃年数检查
    active_years = (filled_yearly_sharpe != 0.0).sum()
    if active_years < MIN_ACTIVE_YEARS:
        return False

    return True

def calculate_pragmatic_score(alpha: Dict[str, Any]) -> Dict[str, float]:
    """
    更新后的务实评分核心逻辑：
    1. 基础分: 1000
    2. 失败项扣分: 每个 FAIL 扣 100 分
    3. 邻近度惩罚: 每个失败项距离临界值的相对距离 * 10
    4. 性能奖励: Sharpe * 10, Fitness * 5
    """
    check_df = alpha.get("check_df")
    if check_df is None or check_df.empty:
        return {"final_score": -1000.0, "fail_count": 99.0}

    # 1. 统计失败项数量
    fail_count = (check_df["result"] == "FAIL").sum()
    
    # 2. 计算邻近度惩罚 (权重 10.0)
    proximity_penalty = 0.0
    fails = check_df[check_df["result"] == "FAIL"]
    for _, row in fails.iterrows():
        try:
            val = float(str(row.get("value", "0")).split()[0])
            limit = float(str(row.get("limit", "0")).split()[0])
            if limit != 0:
                proximity_penalty += (abs(val - limit) / abs(limit))
            else:
                proximity_penalty += abs(val) 
        except:
            proximity_penalty += 1.0

    # 3. 传统指标奖励 (与邻近度惩罚保持权重平衡)
    is_stats = alpha.get("details", {}).get("is", {})
    sharpe = max(0, float(is_stats.get("sharpe", 0.0)))
    fitness = max(0, float(is_stats.get("fitness", 0.0)))
    
    # 最终得分构造
    final_score = (1000.0 
                   - (fail_count * 100.0) 
                   + (sharpe * 10.0) 
                   + (fitness * 5.0) 
                   - (proximity_penalty * 10.0))
    
    return {
        "final_score": final_score,
        "fail_count": float(fail_count),
        "proximity_penalty": proximity_penalty,
        "sharpe": sharpe,
        "fitness": fitness
    }

def evaluate_and_select_best_alpha(
    session: Any, alpha_ids: List[str]
) -> Optional[Dict[str, Any]]:
    """
    基于平衡权重后的务实评分系统选出最优 Alpha。
    """
    alpha_candidates = []
    print(f"开始评估 {len(alpha_ids)} 个 Alpha 候选者...")
    
    for alpha_id in alpha_ids:
        try:
            alpha_details = utils.safe_api_call(ace_lib.get_simulation_result_json, session, alpha_id)
            check_df = utils.safe_api_call(ace_lib.get_check_submission, session, alpha_id)
            stats_df = utils.safe_api_call(ace_lib.get_alpha_yearly_stats, session, alpha_id)

            if alpha_details and check_df is not None and stats_df is not None:
                candidate = {
                    "id": alpha_id,
                    "details": alpha_details,
                    "check_df": check_df,
                    "yearly_stats": stats_df,
                    "is": alpha_details.get("is", {})
                }
                
                if _passes_hard_filters(candidate):
                    scores = calculate_pragmatic_score(candidate)
                    candidate["scores"] = scores
                    alpha_candidates.append(candidate)
        except Exception as e:
            print(f"获取 Alpha {alpha_id} 数据时出错: {e}")

    if not alpha_candidates:
        print("没有候选者通过硬过滤。")
        return None

    alpha_candidates.sort(key=lambda x: x["scores"]["final_score"], reverse=True)
    best_candidate = alpha_candidates[0]
    
    print(f"🏆 本次迭代最优 Alpha: {best_candidate['id']} | 得分: {best_candidate['scores']['final_score']:.2f} | 失败项: {best_candidate['scores']['fail_count']}")
    
    return {
        "alpha_info": best_candidate,
        "scores": best_candidate["scores"],
        "inversion_needed": False
    }
