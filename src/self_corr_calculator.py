import requests
import pandas as pd
import time
from typing import Optional, Tuple, Dict, List, Union
from concurrent.futures import ThreadPoolExecutor
import pickle
from collections import defaultdict
import numpy as np
from pathlib import Path
from dataclasses import dataclass
from logger import Logger

# 创建 Logger 实例
logger = Logger()

# 定义配置类
@dataclass
class Config:
    data_path: Path = Path("data")

cfg = Config()

def sign_in(username: str, password: str) -> Optional[requests.Session]:
    """
    与 WorldQuant Brain API 进行身份验证，创建并返回一个已验证的会话。
    """
    s = requests.Session()
    s.auth = (username, password)
    try:
        response_sink = s.post('https://api.worldquantbrain.com/authentication')
        response_sink.raise_for_status()
        logger.info("Successfully signed in")
        return s
    except requests.exceptions.RequestException as e:
        logger.error(f"Sign-in failed: {e}")
        return None

def save_obj(obj: object, name: str) -> None:
    """
    将 Python 对象序列化并保存到 pickle 文件中。
    """
    Path(name).parent.mkdir(parents=True, exist_ok=True)
    with open(name + '.pickle', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

def load_obj(name: str) -> object:
    """
    从 pickle 文件中加载并反序列化 Python 对象。
    """
    with open(name + '.pickle', 'rb') as f:
        return pickle.load(f)

def wait_get(url: str, sess: requests.Session, max_retries: int = 10, max_wait: float = 300.0) -> requests.Response:
    """
    执行带有重试逻辑和限流处理的 GET 请求。

    参数:
        url (str): API 地址。
        sess (requests.Session): 已认证的会话。
        max_retries (int): 最大重试次数。
        max_wait (float): 最大等待时间（秒）。
    """
    start_time = time.time()
    retries = 0
    while retries < max_retries:
        if time.time() - start_time > max_wait:
            logger.error(f"Request to {url} timed out, exceeded max wait time {max_wait} seconds")
            raise TimeoutError(f"Request timed out: {url}")
        
        try:
            simulation_progress = sess.get(url, timeout=30)
            retry_after = simulation_progress.headers.get("Retry-After", 0)
            if retry_after:
                logger.info(f"Request to {url} rate-limited, waiting {retry_after} seconds")
                time.sleep(float(retry_after))
                continue
            if simulation_progress.status_code < 400:
                return simulation_progress
            logger.warning(f"Request to {url} failed with status code: {simulation_progress.status_code}")
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request to {url} failed: {e}")
        
        sleep_time = 2 ** retries
        logger.info(f"Retrying request to {url}, attempt {retries + 1}/{max_retries}, waiting {sleep_time} seconds")
        time.sleep(sleep_time)
        retries += 1
    
    logger.error(f"Request to {url} failed after {max_retries} retries")
    raise requests.exceptions.RequestException(f"Failed to fetch {url} after {max_retries} retries")

def _get_alpha_pnl(alpha_id: str, sess: requests.Session) -> pd.DataFrame:
    """
    从 WorldQuant Brain API 获取特定 alpha 的盈亏（PnL）数据。
    """
    logger.debug(f"Fetching PNL data for alpha {alpha_id}")
    try:
        pnl = wait_get(f"https://api.worldquantbrain.com/alphas/{alpha_id}/recordsets/pnl", sess).json()
        df = pd.DataFrame(pnl['records'], columns=[item['name'] for item in pnl['schema']['properties']])
        df = df.rename(columns={'date': 'Date', 'pnl': alpha_id})
        df = df[['Date', alpha_id]]
        logger.debug(f"Successfully fetched PNL data for alpha {alpha_id}, rows: {len(df)}")
        return df
    except (requests.exceptions.RequestException, KeyError, ValueError) as e:
        logger.error(f"Failed to fetch PNL data for alpha {alpha_id}: {e}")
        raise

def get_alpha_pnls(
    alphas: List[Dict],
    sess: requests.Session,
    alpha_pnls: Optional[pd.DataFrame] = None,
    alpha_ids: Optional[Dict[str, List]] = None,
    username = None,
    password = None
) -> Tuple[Dict[str, List], pd.DataFrame]:
    """
    获取多个 alpha 的盈亏数据，并按区域对 alpha ID 进行分类。
    """
    if alpha_ids is None:
        alpha_ids = defaultdict(list)
    if alpha_pnls is None:
        alpha_pnls = pd.DataFrame()

    new_alphas = [item for item in alphas if item['id'] not in alpha_pnls.columns]
    if not new_alphas:
        logger.info("No new alphas require PNL data fetching")
        return alpha_ids, alpha_pnls

    logger.info(f"Fetching PNL data for {len(new_alphas)} new alphas")
    for item_alpha in new_alphas:
        alpha_ids[item_alpha['settings']['region']].append(item_alpha['id'])

    results = []
    total_alphas = len(new_alphas)
    start_time = time.time()

    for idx, item in enumerate(new_alphas, 1):
        alpha_id = item['id']
        
        # 进度日志（每处理10%或至少每10个alpha打印一次）
        if total_alphas > 1 and (idx % max(1, int(total_alphas * 0.1)) == 0 or idx == 1 or idx == total_alphas):
            # 检查 username 和 password 是否为 None
            if username is not None and password is not None:
                new_sess = sign_in(username, password)
                if new_sess is None:
                    logger.error("Failed to re-authenticate session, continuing with existing session")
                else:
                    sess = new_sess
                    logger.info("Session re-authenticated successfully")
            else:
                logger.error("Username or password is None, skipping re-authentication and continuing with existing session")
            elapsed = time.time() - start_time
            avg_time = elapsed / idx
            remaining = avg_time * (total_alphas - idx)
            
            logger.info(
                f"Processing alpha {idx}/{total_alphas} ({idx/total_alphas:.1%}) | "
                f"Elapsed: {elapsed:.1f}s | "
                f"ETA: {remaining:.1f}s | "
                f"Current: {alpha_id}"
            )

        try:
            df = _get_alpha_pnl(alpha_id, sess).set_index('Date')
            results.append(df)
        except Exception as e:
            logger.warning(f"Skipping PNL data for alpha {alpha_id}: {e}")
            continue

    # 最终完成日志
    logger.info(
        f"Finished processing {len(results)}/{total_alphas} alphas. "
        f"Success rate: {len(results)/total_alphas:.1%} | "
        f"Total time: {time.time()-start_time:.1f}s"
    )

    if not results:
        logger.error("Failed to fetch PNL data for all alphas")
        raise ValueError("No valid PNL data retrieved")

    logger.info(f"Successfully fetched PNL data for {len(results)}/{len(new_alphas)} alphas")
    alpha_pnls = pd.concat([alpha_pnls] + results, axis=1)
    alpha_pnls.sort_index(inplace=True)
    logger.info(f"Merged and sorted PNL data, columns: {len(alpha_pnls.columns)}")
    return alpha_ids, alpha_pnls

def get_os_alphas(sess: requests.Session, limit: int = 100, get_first: bool = False) -> List[Dict]:
    """
    从 WorldQuant Brain API 获取处于 OS（开放提交）阶段的 alpha 列表。
    """
    fetched_alphas = []
    offset = 0
    total_alphas = 100
    while len(fetched_alphas) < total_alphas:
        print(f"Fetching alphas from offset {offset} to {offset + limit}")
        url = (f"https://api.worldquantbrain.com/users/self/alphas?"
               f"stage=OS&limit={limit}&offset={offset}&order=-dateSubmitted")
        res = wait_get(url, sess).json()
        if offset == 0:
            total_alphas = res['count']
        alphas = res["results"]
        fetched_alphas.extend(alphas)
        if len(alphas) < limit:
            break
        offset += limit
        if get_first:
            break
    return fetched_alphas[:total_alphas]

def calc_self_corr(
    alpha_id: str,
    sess: requests.Session,
    os_alpha_rets: Optional[pd.DataFrame] = None,
    os_alpha_ids: Optional[Dict[str, List]] = None,
    alpha_result: Optional[Dict] = None,
    return_alpha_pnls: bool = False,
    alpha_pnls: Optional[pd.DataFrame] = None
) -> Union[float, Tuple[float, pd.DataFrame]]:
    """
    计算目标 alpha 与同一区域内其他 alpha 的最大相关性。
    """
    if alpha_result is None:
        alpha_result = wait_get(f"https://api.worldquantbrain.com/alphas/{alpha_id}", sess).json()
    
    if alpha_pnls is not None and len(alpha_pnls) == 0:
        alpha_pnls = None
    
    if alpha_pnls is None:
        _, alpha_pnls = get_alpha_pnls([alpha_result], sess)
        alpha_pnls = alpha_pnls[alpha_id]

    alpha_rets = alpha_pnls - alpha_pnls.ffill().shift(1)
    alpha_rets = alpha_rets[pd.to_datetime(alpha_rets.index) > 
                           pd.to_datetime(alpha_rets.index).max() - pd.DateOffset(years=4)]

    region = alpha_result['settings']['region']
    if os_alpha_rets is None or os_alpha_ids is None or region not in os_alpha_ids or not os_alpha_ids[region]:
        logger.warning(f"Cannot calculate correlation: missing data or no alphas in region {region}")
        return (0.0, alpha_pnls) if return_alpha_pnls else 0.0

    corr_series = os_alpha_rets[os_alpha_ids[region]].corrwith(alpha_rets).sort_values(ascending=False).round(4)
    print(corr_series)
    if return_alpha_pnls:
        cfg.data_path.mkdir(parents=True, exist_ok=True)
        corr_series.to_csv(str(cfg.data_path / 'os_alpha_corr.csv'))

    self_corr = corr_series.max()
    if np.isnan(self_corr):
        self_corr = 0.0

    return (self_corr, alpha_pnls) if return_alpha_pnls else self_corr

def download_data(sess: requests.Session, flag_increment: bool = True) -> None:
    """
    从 WorldQuant Brain API 下载 alpha 数据并保存到磁盘。
    """
    if flag_increment:
        try:
            os_alpha_ids = load_obj(str(cfg.data_path / 'os_alpha_ids'))
            os_alpha_pnls = load_obj(str(cfg.data_path / 'os_alpha_pnls'))
            ppac_alpha_ids = load_obj(str(cfg.data_path / 'ppac_alpha_ids'))
            exist_alpha = [alpha for ids in os_alpha_ids.values() for alpha in ids]
        except Exception as e:
            logger.error(f"Failed to load existing data: {e}")
            os_alpha_ids = None
            os_alpha_pnls = None
            exist_alpha = []
            ppac_alpha_ids = []
    else:
        os_alpha_ids = None
        os_alpha_pnls = None
        exist_alpha = []
        ppac_alpha_ids = []

    if os_alpha_ids is None:
        alphas = get_os_alphas(sess, limit=100, get_first=False)
    else:
        alphas = get_os_alphas(sess, limit=30, get_first=True)

    alphas = [item for item in alphas if item['id'] not in exist_alpha]
    ppac_alpha_ids += [item['id'] for item in alphas 
                      for item_match in item['classifications'] 
                      if item_match['name'] == 'Power Pool Alpha']

    os_alpha_ids, os_alpha_pnls = get_alpha_pnls(alphas, sess, alpha_pnls=os_alpha_pnls, alpha_ids=os_alpha_ids)
    save_obj(os_alpha_ids, str(cfg.data_path / 'os_alpha_ids'))
    save_obj(os_alpha_pnls, str(cfg.data_path / 'os_alpha_pnls'))
    save_obj(ppac_alpha_ids, str(cfg.data_path / 'ppac_alpha_ids'))
    print(f'Newly downloaded alphas: {len(alphas)}, total alphas: {os_alpha_pnls.shape[1]}')

def load_data(tag: Optional[str] = None) -> Tuple[Dict[str, List], pd.DataFrame]:
    """
    从磁盘加载 alpha 数据并根据指定标签进行过滤。
    """
    os_alpha_ids = load_obj(str(cfg.data_path / 'os_alpha_ids'))
    os_alpha_pnls = load_obj(str(cfg.data_path / 'os_alpha_pnls'))
    ppac_alpha_ids = load_obj(str(cfg.data_path / 'ppac_alpha_ids'))

    if tag == 'PPAC':
        for item in os_alpha_ids:
            os_alpha_ids[item] = [alpha for alpha in os_alpha_ids[item] if alpha in ppac_alpha_ids]
    elif tag == 'SelfCorr':
        for item in os_alpha_ids:
            os_alpha_ids[item] = [alpha for alpha in os_alpha_ids[item] if alpha not in ppac_alpha_ids]

    exist_alpha = [alpha for ids in os_alpha_ids.values() for alpha in ids]
    os_alpha_pnls = os_alpha_pnls[exist_alpha]
    os_alpha_rets = os_alpha_pnls - os_alpha_pnls.ffill().shift(1)
    os_alpha_rets = os_alpha_rets[pd.to_datetime(os_alpha_rets.index) > 
                                 pd.to_datetime(os_alpha_rets.index).max() - pd.DateOffset(years=4)]
    return os_alpha_ids, os_alpha_rets