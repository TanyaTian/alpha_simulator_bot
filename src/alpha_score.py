import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional
import utils
import ace_lib

# --- Constants ---
# Scoring weights
CORE_PERFORMANCE_WEIGHT = 0.3
STABILITY_WEIGHT = 0.3
HEALTH_WEIGHT = 0.2
PC_SCORE_WEIGHT = 0.2  # Added weight for PC score

# Hard Filter Thresholds
MIN_ACTIVE_YEARS = 8
STRONG_SIGNAL_SHARPE = 0.2

def _calculate_scores(alpha: Dict[str, Any]) -> Dict[str, float]:
    """Calculates all score components for a given alpha."""
    yearly_stats = alpha["yearly_stats"].copy()
    check_df = alpha["check_df"]
    
    scores = {}

    # --- A. Core Performance Score (Modified) ---
    # Focus on the last 2nd and 3rd years (ignoring the very last partial year if it exists)
    # Assuming yearly_stats is sorted by year/date
    
    if len(yearly_stats) >= 3:
        # Exclude the last row (current partial year)
        full_years_stats = yearly_stats.iloc[:-1]
        
        if len(full_years_stats) >= 2:
            # Last 2 full years (indices -1 and -2 of full_years_stats)
            recent_stats = full_years_stats.iloc[-2:]
            older_stats = full_years_stats.iloc[:-2]
            
            avg_recent_sharpe = recent_stats["sharpe"].mean()
            avg_recent_fitness = recent_stats["fitness"].mean()
            
            avg_older_sharpe = older_stats["sharpe"].mean() if not older_stats.empty else 0
            avg_older_fitness = older_stats["fitness"].mean() if not older_stats.empty else 0
            
            # Higher weight for recent 2 full years (e.g., 70% vs 30%)
            sharpe_score = (avg_recent_sharpe * 0.7 + avg_older_sharpe * 0.3)
            fitness_score = (avg_recent_fitness * 0.7 + avg_older_fitness * 0.3)
            
            scores["core_performance"] = sharpe_score + fitness_score
        else:
            # Not enough full years, fallback to simple average
            scores["core_performance"] = full_years_stats["sharpe"].mean() + full_years_stats["fitness"].mean()
    else:
        # Very short history
        scores["core_performance"] = yearly_stats["sharpe"].mean() + yearly_stats["fitness"].mean()

    # --- B. Stability Score (Kept similar) ---
    normalized_sharpe_series = yearly_stats["sharpe"]
    normalized_sharpe_std = normalized_sharpe_series.std()
    strong_signal_ratio = (normalized_sharpe_series > STRONG_SIGNAL_SHARPE).mean()
    stability_from_std = max(0, 1 - normalized_sharpe_std) * 10
    scores["stability"] = stability_from_std + strong_signal_ratio * 10

    # --- C. Generalization Score (Removed) ---
    scores["generalization"] = 0.0

    # --- D. Health Score (Kept similar) ---
    pass_count = (check_df["result"] == "PASS").sum()
    fail_count = (check_df["result"] == "FAIL").sum()
    total_checks = len(check_df)
    
    if total_checks > 0:
        health_score = ((pass_count - fail_count) / total_checks) * 10
    else:
        health_score = 0
    scores["health"] = health_score
    
    # --- E. PC Score (New) ---
    pc_value = 1.0 # Default worst case
    
    # Try to find PC in check_df
    pc_row = check_df[check_df['name'] == 'PROD_CORRELATION']
    if not pc_row.empty and 'value' in pc_row.columns:
         val = pc_row.iloc[0]['value']
         if pd.notna(val):
             pc_value = float(val)

    pc_score = 0.0
    if pc_value > 0.7:
        # Deduct points
        # Requirement: "0.7以上越大扣的越多"
        pc_score = -1 * (pc_value - 0.7) * 50 
    elif pc_value < 0.5:
        # Add points
        # Requirement: "0.5以下越小加的越多"
        pc_score = (0.5 - pc_value) * 20
    else:
        # 0.5 ~ 0.7 -> No score
        pc_score = 0.0
        
    scores["pc_score"] = pc_score

    return scores

def get_alpha_score(session: Any, alpha_id: str) -> Dict[str, Any]:
    """
    Fetches data and calculates the score for a single alpha.
    """
    try:
        alpha_details = utils.safe_api_call(ace_lib.get_simulation_result_json, session, alpha_id)
        check_df = utils.safe_api_call(ace_lib.get_check_submission, session, alpha_id)
        stats_df = utils.safe_api_call(ace_lib.get_alpha_yearly_stats, session, alpha_id)

        if alpha_details is None or check_df is None or stats_df is None:
            return {"final_score": -1.0, "error": "Missing data"}

        # Check for PC in check_df, if missing try to fetch check_prod_corr_test
        has_pc = False
        if not check_df.empty:
            if 'PROD_CORRELATION' in check_df['name'].values:
                has_pc = True
        
        if not has_pc:
             # Try to fetch specific prod corr check
             pc_res = utils.safe_api_call(ace_lib.check_prod_corr_test, session, alpha_id=alpha_id)
             if pc_res is not None and not pc_res.empty:
                 val = pc_res['value'].max()
                 # Add to check_df for consistency
                 new_row = pd.DataFrame([{'name': 'PROD_CORRELATION', 'result': 'PASS' if val < 0.7 else 'FAIL', 'value': val}])
                 check_df = pd.concat([check_df, new_row], ignore_index=True)

        alpha_candidate = {
            "id": alpha_id,
            "details": alpha_details,
            "check_df": check_df,
            "yearly_stats": stats_df,
            "is": alpha_details.get("is", {})
        }

        scores = _calculate_scores(alpha_candidate)

        final_score = (
            scores.get("core_performance", 0) * CORE_PERFORMANCE_WEIGHT +
            scores.get("stability", 0) * STABILITY_WEIGHT +
            scores.get("health", 0) * HEALTH_WEIGHT + 
            scores.get("pc_score", 0) * PC_SCORE_WEIGHT
        )

        scores["final_score"] = final_score
        return scores

    except Exception as e:
        print(f"Error scoring alpha {alpha_id}: {e}")
        return {"final_score": -1.0, "error": str(e)}