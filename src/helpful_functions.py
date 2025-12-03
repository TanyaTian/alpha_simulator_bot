import json
import os
from typing import Union

import pandas as pd
from pandas.io.formats.style import Styler

# 从环境变量中获取 BRAIN API 的 URL，如果未设置则使用默认值
brain_api_url = os.environ.get("BRAIN_API_URL", "https://api.worldquantbrain.com")
# 从环境变量中获取 BRAIN 平台的 URL，如果未设置则使用默认值
brain_url = os.environ.get("BRAIN_URL", "https://platform.worldquantbrain.com")


def make_clickable_alpha_id(alpha_id: str) -> str:
    """
    为 Alpha ID 创建一个可点击的 HTML 链接。

    Args:
        alpha_id (str): Alpha 的 ID。

    Returns:
        str: 一个包含指向该 Alpha 平台页面的可点击链接的 HTML 字符串。
    """

    url = brain_url + "/alpha/"
    return f'<a href="{url}{alpha_id}">{alpha_id}</a>'


def prettify_result(
    result: list, detailed_tests_view: bool = False, clickable_alpha_id: bool = False
) -> Union[pd.DataFrame, Styler]:
    """
    将模拟结果合并并格式化为单个 DataFrame 以供分析。

    Args:
        result (list): 包含模拟结果的字典列表。
        detailed_tests_view (bool, optional): 如果为 True，则包含详细的测试结果。默认为 False。
        clickable_alpha_id (bool, optional): 如果为 True，则使 Alpha ID 可点击。默认为 False。

    Returns:
        pandas.DataFrame or pandas.io.formats.style.Styler: 一个包含格式化结果的 DataFrame，
        可选择地使 Alpha ID 变为可点击链接。
    """
    # 提取并合并所有样本内（in-sample）统计数据
    list_of_is_stats = [result[x]["is_stats"] for x in range(len(result)) if result[x]["is_stats"] is not None]
    is_stats_df = pd.concat(list_of_is_stats).reset_index(drop=True)
    is_stats_df = is_stats_df.sort_values("fitness", ascending=False)

    # 提取每个 Alpha 的表达式
    expressions = {
        result[x]["alpha_id"]: (
            {
                "selection": result[x]["simulate_data"]["selection"],
                "combo": result[x]["simulate_data"]["combo"],
            }
            if result[x]["simulate_data"]["type"] == "SUPER"
            else result[x]["simulate_data"]["regular"]
        )
        for x in range(len(result))
        if result[x]["is_stats"] is not None
    }
    expression_df = pd.DataFrame(list(expressions.items()), columns=["alpha_id", "expression"])

    # 提取并合并所有样本内测试结果
    list_of_is_tests = [result[x]["is_tests"] for x in range(len(result)) if result[x]["is_tests"] is not None]
    is_tests_df = pd.concat(list_of_is_tests, sort=True).reset_index(drop=True)
    is_tests_df = is_tests_df[is_tests_df["result"] != "WARNING"]
    if detailed_tests_view:
        # 创建详细视图，将 limit, result, value 合并到 'details' 字典中
        cols = ["limit", "result", "value"]
        is_tests_df["details"] = is_tests_df[cols].to_dict(orient="records")
        is_tests_df = is_tests_df.pivot(index="alpha_id", columns="name", values="details").reset_index()
    else:
        # 创建简略视图，只显示测试结果
        is_tests_df = is_tests_df.pivot(index="alpha_id", columns="name", values="result").reset_index()

    # 合并统计数据、表达式和测试结果
    alpha_stats = pd.merge(is_stats_df, expression_df, on="alpha_id")
    alpha_stats = pd.merge(alpha_stats, is_tests_df, on="alpha_id")
    # 删除所有值为 "PENDING" 的列
    alpha_stats = alpha_stats.drop(columns=alpha_stats.columns[(alpha_stats == "PENDING").any()])
    # 将列名从驼峰式（camelCase）转换为蛇形（snake_case）
    alpha_stats.columns = alpha_stats.columns.str.replace("(?<=[a-z])(?=[A-Z])", "_", regex=True).str.lower()
    if clickable_alpha_id:
        # 如果需要，将 alpha_id 列格式化为可点击链接
        return alpha_stats.style.format({"alpha_id": lambda x: make_clickable_alpha_id(str(x))})
    return alpha_stats


def concat_pnl(result: list) -> pd.DataFrame:
    """
    将多个 Alpha 的盈亏（PnL）结果合并到一个 DataFrame 中。

    Args:
        result (list): 包含模拟结果（内含 PnL 数据）的字典列表。

    Returns:
        pandas.DataFrame: 一个包含所有 Alpha 合并后 PnL 数据的 DataFrame。
    """
    list_of_pnls = [result[x]["pnl"] for x in range(len(result)) if result[x]["pnl"] is not None]
    pnls_df = pd.concat(list_of_pnls).reset_index()

    return pnls_df


def concat_is_tests(result: list) -> pd.DataFrame:
    """
    将多个 Alpha 的样本内测试结果合并到一个 DataFrame 中。

    Args:
        result (list): 包含模拟结果（内含样本内测试数据）的字典列表。

    Returns:
        pandas.DataFrame: 一个包含所有 Alpha 合并后样本内测试结果的 DataFrame。
    """
    is_tests_list = [result[x]["is_tests"] for x in range(len(result)) if result[x]["is_tests"] is not None]
    is_tests_df = pd.concat(is_tests_list, sort=True).reset_index(drop=True)
    return is_tests_df


def save_simulation_result(result: dict) -> None:
    """
    将模拟结果保存到 'simulation_results' 文件夹下的一个 JSON 文件中。

    Args:
        result (dict): 包含单个 Alpha 模拟结果的字典。
    """

    alpha_id = result["id"]
    region = result["settings"]["region"]
    folder_path = "simulation_results/"
    file_path = os.path.join(folder_path, f"{alpha_id}_{region}")

    # 确保目标文件夹存在
    os.makedirs(folder_path, exist_ok=True)

    with open(file_path, "w") as file:
        json.dump(result, file)


def save_pnl(pnl_df: pd.DataFrame, alpha_id: str, region: str) -> None:
    """
    将 Alpha 的 PnL 数据保存到 'alphas_pnl' 文件夹下的一个 CSV 文件中。

    Args:
        pnl_df (pandas.DataFrame): 包含 PnL 数据的 DataFrame。
        alpha_id (str): Alpha 的 ID。
        region (str): 生成 PnL 数据的区域。
    """

    folder_path = "alphas_pnl/"
    file_path = os.path.join(folder_path, f"{alpha_id}_{region}.csv")
    # 确保目标文件夹存在
    os.makedirs(folder_path, exist_ok=True)

    pnl_df.to_csv(file_path)


def save_yearly_stats(yearly_stats: pd.DataFrame, alpha_id: str, region: str):
    """
    将 Alpha 的年度统计数据保存到 'yearly_stats' 文件夹下的一个 CSV 文件中。

    Args:
        yearly_stats (pandas.DataFrame): 包含年度统计数据的 DataFrame。
        alpha_id (str): Alpha 的 ID。
        region (str): 生成统计数据的区域。
    """

    folder_path = "yearly_stats/"
    file_path = os.path.join(folder_path, f"{alpha_id}_{region}.csv")
    # 确保目标文件夹存在
    os.makedirs(folder_path, exist_ok=True)

    yearly_stats.to_csv(file_path, index=False)


def expand_dict_columns(data: pd.DataFrame) -> pd.DataFrame:
    """
    将 DataFrame 中的字典列展开为单独的列。

    Args:
        data (pandas.DataFrame): 包含字典列的输入 DataFrame。

    Returns:
        pandas.DataFrame: 一个列已展开的新 DataFrame。
    """
    # 找出所有值为字典类型的列
    dict_columns = list(filter(lambda x: isinstance(data[x].iloc[0], dict), data.columns))
    # 将每个字典列展开，并为新列重命名（例如：col_key）
    new_columns = pd.concat(
        [data[col].apply(pd.Series).rename(columns=lambda x: f"{col}_{x}") for col in dict_columns],
        axis=1,
    )

    # 将新生成的列与原始 DataFrame 合并
    data = pd.concat([data, new_columns], axis=1)
    return data