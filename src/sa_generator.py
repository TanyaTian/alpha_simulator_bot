import json
import random
from typing import List, Dict, Any, Tuple
from dao import SuperAlphaQueueDAO
import itertools

def extract_selections_from_file(file_path: str) -> List[str]:
    """
    Extract the 'Selection' field from each JSON line in a file and return unique values.
    """
    selections = set()
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    if "Selection" in data:
                        selections.add(data["Selection"])
                except json.JSONDecodeError:
                    print(f"Warning: Failed to decode JSON from line: {line}")
    except FileNotFoundError:
        print(f"Error: File not found: {file_path}")
    return list(selections)

def generate_sa_combinations(selections: List[str], priority: Any = 50, region: str = "USA") -> List[Dict[str, Any]]:
    """
    程序化地创建一批SA任务定义。任务数量由内部定义的参数排列组合决定。

    :param selections: 从外部传入的Selection列表。
    :param priority: 任务的优先级。
                     - 如果是整数 (e.g., 1)，所有任务都将被分配此优先级。
                     - 如果是元组 (e.g., (1, 10))，任务将被随机平均分配到该范围内的优先级。
    :param region: The region for which to generate tasks.
    :return: 一个字典列表，每个字典都准备好插入数据库。
    """
    neutralizations_by_region = {
        "USA": ['MARKET', 'SECTOR', 'INDUSTRY', 'STATISTICAL', 'SUBINDUSTRY', 'CROWDING', 'FAST', 'SLOW', 'REVERSION_AND_MOMENTUM', 'SLOW_AND_FAST'],
        "GLB": ['MARKET', 'SECTOR', 'INDUSTRY', 'STATISTICAL', 'SUBINDUSTRY', 'CROWDING', 'REVERSION_AND_MOMENTUM'],
        "EUR": ['MARKET', 'SECTOR', 'INDUSTRY', 'STATISTICAL', 'SUBINDUSTRY', 'CROWDING', 'FAST', 'SLOW', 'REVERSION_AND_MOMENTUM', 'SLOW_AND_FAST'],
        "ASI": ['MARKET', 'SECTOR', 'INDUSTRY', 'STATISTICAL', 'SUBINDUSTRY', 'CROWDING', 'FAST', 'SLOW', 'REVERSION_AND_MOMENTUM', 'SLOW_AND_FAST'],
        "CHN": ['MARKET', 'SECTOR', 'INDUSTRY', 'STATISTICAL', 'SUBINDUSTRY', 'CROWDING', 'FAST', 'SLOW', 'REVERSION_AND_MOMENTUM', 'SLOW_AND_FAST'],
        "IND": ['MARKET', 'SECTOR', 'INDUSTRY', 'SUBINDUSTRY', 'CROWDING', 'FAST', 'SLOW', 'REVERSION_AND_MOMENTUM', 'SLOW_AND_FAST']
    }

    combos = [
        '1',
        "combo_a(alpha)",
        "combo_a(alpha,mode='algo2')",
        #"combo_a(alpha,mode='algo3')",
        "stats = generate_stats(alpha); innerCorr = self_corr(stats.returns, 500); ic = if_else(innerCorr == 1.0, nan, innerCorr); maxCorr = reduce_max(ic); 1 - maxCorr",
        "stats = generate_stats(alpha); chang = ts_std_dev(stats.pnl, 252); scale((combo_a(alpha, nlength = 500, mode ='algo1'))*if_else(rank(chang)>0.5,1,0.1))",
        "stats = generate_stats(alpha);mom = ts_ir(stats.returns,60);ts_rank(mom,500)",
        "stats = generate_stats(alpha); ts_rank(stats.pnl, 100)",
        "stats = generate_stats(alpha);tv = ts_mean(stats.trade_value,60);tv_crowd = tv/ts_delay(tv,60);ts_rank(-tv_crowd,500)",
        "stats = generate_stats(alpha);std = ts_std_dev(stats.returns,60);std_crowd = std /ts_delay(std,60);ts_rank(-std_crowd ,500)",
        "stats = generate_stats(alpha);ic = self_corr(stats.returns,60);inneric = if_else(ic==1,nan,ic);ts_rank(-reduce_min(inneric),500)"
    ]
    
    universes = {
        "USA": ["TOP3000"],
        "GLB": ["TOP3000", "MINVOL1M"],
        "EUR": ["TOP2500"],
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
        [1000], # selectionLimit
        ["POSITIVE"], # selectionHandling
        selections,
        combos,
        [10, 60] # decay
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
            "test_period": "P0Y",
            "unit_handling": "VERIFY",
            "nan_handling": "OFF",
            "max_trade": "OFF",
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
    
    # 提取 selections
    file_path = "/Users/tianyuan/repos/consultant/records/ownsa_alpha.txt"
    selections = extract_selections_from_file(file_path)
    
    if not selections:
        print(f"No selections found in {file_path}. Exiting.")
        return
    else:
        print(selections[:10])

    dao = SuperAlphaQueueDAO()

    # --- 使用示例 ---
    # 示例1: 为所有任务分配优先级 1
    # new_tasks = generate_sa_combinations(selections=selections, priority=1, region="USA")
    
    # 示例2: 将任务随机分配到 1-10 的优先级
    region = "EUR"
    new_tasks = generate_sa_combinations(selections=selections, priority=(1, 10), region=region)
    
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
