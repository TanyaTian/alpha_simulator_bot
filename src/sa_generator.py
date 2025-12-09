import json
import random
from typing import List, Dict, Any, Tuple
from dao import SuperAlphaQueueDAO
import itertools

def generate_sa_combinations(priority: Any = 50, region: str = "USA") -> List[Dict[str, Any]]:
    """
    程序化地创建一批SA任务定义。任务数量由内部定义的参数排列组合决定。

    :param priority: 任务的优先级。
                     - 如果是整数 (e.g., 1)，所有任务都将被分配此优先级。
                     - 如果是元组 (e.g., (1, 10))，任务将被随机平均分配到该范围内的优先级。
    :param region: The region for which to generate tasks.
    :return: 一个字典列表，每个字典都准备好插入数据库。
    """
    colors_to_assign = ["NONE", "RED", "YELLOW", "GREEN", "BLUE", "PURPLE"]
    neutralizations_by_region = {
        "USA": ['MARKET', 'SECTOR', 'INDUSTRY', 'STATISTICAL', 'SUBINDUSTRY', 'CROWDING', 'FAST', 'SLOW', 'REVERSION_AND_MOMENTUM', 'SLOW_AND_FAST'],
        "GLB": ['MARKET', 'SECTOR', 'INDUSTRY', 'STATISTICAL', 'SUBINDUSTRY', 'CROWDING', 'REVERSION_AND_MOMENTUM'],
        "EUR": ['MARKET', 'SECTOR', 'INDUSTRY', 'STATISTICAL', 'SUBINDUSTRY', 'CROWDING', 'FAST', 'SLOW', 'REVERSION_AND_MOMENTUM', 'SLOW_AND_FAST'],
        "ASI": ['MARKET', 'SECTOR', 'INDUSTRY', 'STATISTICAL', 'SUBINDUSTRY', 'CROWDING', 'FAST', 'SLOW', 'REVERSION_AND_MOMENTUM', 'SLOW_AND_FAST'],
        "CHN": ['MARKET', 'SECTOR', 'INDUSTRY', 'STATISTICAL', 'SUBINDUSTRY', 'CROWDING', 'FAST', 'SLOW', 'REVERSION_AND_MOMENTUM', 'SLOW_AND_FAST'],
        "IND": ['MARKET', 'SECTOR', 'INDUSTRY', 'STATISTICAL', 'SUBINDUSTRY', 'CROWDING', 'FAST', 'SLOW', 'REVERSION_AND_MOMENTUM', 'SLOW_AND_FAST']
    }
    datacategories = ["analyst", "broker", "earnings", "fundamental", "imbalance", "insiders", "institutions", "macro", "model", "news", "option", "other", "pv", "risk", "sentiment", "shortinterest", "socialmedia"]
    selections = []
    for color in colors_to_assign:
        selections.append(f"own  && (color != '{color}')&& (prod_correlation <0.6)")
        selections.append(f"own  && (color != '{color}')&& (prod_correlation >0.5)")
        selections.append(f"own  && (color != '{color}')")
        selections.append(f"own  && (color != '{color}')&& ((long_count > 600 && long_count < 800) || (long_count > 1200 && long_count < 1400))")
        selections.append(f"own  && (color != '{color}')&& (turnover > 0.05)")
        selections.append(f"own  && (color != '{color}')&& (long_count > 500)")
        selections.append(f"own  && (color != '{color}')&& ((turnover > 0.05 && turnover < 0.08) || (turnover > 0.15 && turnover < 0.18)) && (prod_correlation < 0.55)")
        selections.append(f"own  && (color != '{color}')&& ((turnover > 0.05 && turnover < 0.08) || (turnover > 0.15 && turnover < 0.18)) && (prod_correlation > 0.45)")
        selections.append(f"own  && (color != '{color}')&& ((turnover > 0.05 && turnover < 0.08) || (turnover > 0.15 && turnover < 0.18))")
        selections.append(f"own  && (color != '{color}')&& ((long_count > 600 && long_count < 800) || (long_count > 1200 && long_count < 1400)) && (self_correlation < 0.5)")
        selections.append(f"own  && (color != '{color}')&& ((long_count > 600 && long_count < 800) || (long_count > 1200 && long_count < 1400)) && (self_correlation > 0.5)")
        selections.append(f"own  && (color != '{color}')&& ((long_count > 600 && long_count < 800) || (long_count > 1200 && long_count < 1400))")
        selections.append(f"own  && (color != '{color}')&& ((operator_count < 5) || (operator_count > 12)) && (prod_correlation < 0.55)")
        selections.append(f"own  && (color != '{color}')&& ((operator_count < 5) || (operator_count > 12)) && (prod_correlation > 0.45)")
        selections.append(f"own  && (color != '{color}')&& ((operator_count < 5) || (operator_count > 12))")
        selections.append(f"own  && (color != '{color}')&& ((short_count < 800 && short_count > 600) || (short_count > 1300)) && (prod_correlation < 0.55)")
        selections.append(f"own  && (color != '{color}')&& ((short_count < 800 && short_count > 600) || (short_count > 1300)) && (prod_correlation > 0.45)")
        selections.append(f"own  && (color != '{color}')&& ((short_count < 800 && short_count > 600) || (short_count > 1300))")
        selections.append(f"own  && (color != '{color}')&& (turnover < 0.25) && (prod_correlation < 0.6)")
        selections.append(f"own  && (color != '{color}')&& (turnover < 0.25) && (prod_correlation > 0.4)")
        selections.append(f"own  && (color != '{color}')&& (operator_count < 6) && (prod_correlation < 0.55)")
        selections.append(f"own  && (color != '{color}')&& (operator_count < 6) && (prod_correlation > 0.45)")
        selections.append(f"own  && (color != '{color}')&& (long_count > 800) && (prod_correlation < 0.6)")
        selections.append(f"own  && (color != '{color}')&& (self_correlation < 0.6)")
        selections.append(f"own  && (color != '{color}')&& (turnover < 0.25)")
        selections.append(f"own  && (color != '{color}')&& (operator_count < 6)")
        selections.append(f"own  && (color != '{color}')&& (long_count > 800)")
        selections.append(f"own  && (color != '{color}')&& (prod_correlation > 0.4)")
        selections.append(f"own  && (color != '{color}')&& (self_correlation > 0.3)")
        selections.append(f"own  && (color != '{color}')&& ((self_correlation <= 0.45) * (prod_correlation < 0.55))")
        for category in datacategories:
            selections.append(f"own && (color != '{color}') && (not(in(datacategories, '{category}')))")
        for neutralization in neutralizations_by_region[region]:
            selections.append(f"own && (color != '{color}') && (neutralization != '{neutralization}')")

    combos = [
        '1',
        "combo_a(alpha)",
        "combo_a(alpha,mode='algo2')",
        "combo_a(alpha,mode='algo3')",
        "stats = generate_stats(alpha); innerCorr = self_corr(stats.returns, 500); ic = if_else(innerCorr == 1.0, nan, innerCorr); maxCorr = reduce_max(ic); 1 - maxCorr",
        "stats = generate_stats(alpha); chang = ts_std_dev(stats.pnl, 252); scale((combo_a(alpha, nlength = 500, mode ="
        "'algo1'))*if_else(rank(chang)>0.5,1,0.1))",
        "stats = generate_stats(alpha); ts_ir(stats.returns, 120)",
        "stats = generate_stats(alpha); ts_rank(stats.pnl, 100)"
    ]
    
    universes = {
        "USA": ["TOP3000", "ILLIQUID_MINVOL1M"],
        "GLB": ["TOP3000", "MINVOL1M", "TOPDIV3000"],
        "EUR": ["TOP2500", "ILLIQUID_MINVOL1M"],
        "ASI": ["MINVOL1M", "ILLIQUID_MINVOL1M"],
        "CHN": ["TOP2000U"],
        "IND": ["TOP500"]
    }
    
    if region not in universes or region not in neutralizations_by_region:
        print(f"Error: Region {region} is not supported.")
        return []

    tasks = []
    
    # Generate all combinations
    product_iter = itertools.product(
        universes[region],
        neutralizations_by_region[region],
        [45, 65, 80], # selectionLimit
        ["POSITIVE"], # selectionHandling
        selections,
        combos,
        [10, 60, 120] # decay
    )

    for universe, neutralization, selection_limit, selection_handling, selection_str, combo_str, decay in product_iter:
        settings = {
            "region": region,
            "universe": universe,
            "delay": 1,
            "decay": decay,
            "neutralization": neutralization,
            "truncation": 0.01,
            "pasteurization": "ON",
            "test_period": "P2Y",
            "unit_handling": "VERIFY",
            "nan_handling": "OFF",
            "max_trade": "ON" if region == "ASI" else "OFF",
            "selection_handling": selection_handling,
            "selection_limit": selection_limit,
            "visualization": False
        }
        
        task_priority = priority
        if isinstance(priority, tuple) and len(priority) == 2:
            task_priority = random.randint(priority[0], priority[1])

        task = {
            'priority': task_priority,
            'status': 'pending',
            'region': region,
            'combo': combo_str,
            'selection': selection_str,
            'settings_json': json.dumps(settings)
        }
        tasks.append(task)
        
    return tasks

def main():
    """
    脚本主入口点。
    """
    print("Initializing SA task generator...")
    dao = SuperAlphaQueueDAO()

    # --- 使用示例 ---
    # 示例1: 为所有任务分配优先级 1
    # new_tasks = generate_sa_combinations(priority=1, region="USA")
    
    # 示例2: 将任务随机分配到 1-10 的优先级
    region = "GLB"
    new_tasks = generate_sa_combinations(priority=(1, 10), region=region)
    
    if new_tasks:
        print(f"Generated {len(new_tasks)} new SA tasks for region {region}.")
        # 直接调用DAO的批量插入，DAO内部会处理分批逻辑
        dao.batch_insert_tasks(new_tasks, batch_size=2000) # 可以选择性覆盖DAO中的默认batch_size
    else:
        print("No new tasks were generated.")
        
    dao.close()
    print("SA task generation finished.")

if __name__ == "__main__":
    main()
