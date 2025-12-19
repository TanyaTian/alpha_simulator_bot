# src/alpha_calculator.py

import asyncio
import time
import pandas as pd
import ast
from datetime import datetime, timedelta
from typing import Optional
from concurrent.futures import ProcessPoolExecutor

import requests
import schedule

from alpha_prune import calculate_correlations, generate_comparison_data
from logger import Logger
from signal_manager import SignalManager
from dao import SimulatedAlphasDAO, StoneGoldBagDAO, PendingAlphaChecksDAO
from config_manager import config_manager


def to_mysql_datetime(dt_str: Optional[str], logger) -> Optional[str]:
    """将ISO格式的日期字符串转换为MySQL的DATETIME格式。"""
    if not dt_str:
        return None
    try:
        if dt_str.endswith('Z'):
            dt_str = dt_str[:-1] + '+00:00'
        dt = datetime.fromisoformat(dt_str)
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        logger.error(f"Failed to parse datetime: {dt_str}, error: {e}")
        return None


def _fetch_alphas_from_api(date_str: str, logger, per_page: int = 100) -> pd.DataFrame:
    """
    根据指定日期从API获取原始alpha数据行（自动分页获取所有数据）。
    """
    dt_obj = datetime.strptime(date_str, '%Y%m%d')
    timezone_offset = "-05:00"  # Per reference implementation

    start_date = dt_obj.strftime(f'%Y-%m-%dT00:00:00{timezone_offset}')
    end_date = (dt_obj + timedelta(days=1)).strftime(f'%Y-%m-%dT00:00:00{timezone_offset}')

    offset = 0
    all_alphas = []

    logger.info(f"[AlphaProcessor] Fetching alphas from API created between {start_date} and {end_date}")

    while True:
        # get session inside loop to avoid expiration
        sess = config_manager.get_session()
        if not sess:
            logger.error("[AlphaProcessor] Failed to sign in, cannot fetch from API.")
            raise ValueError("Failed to get session from config_manager")

        # URL构建参考了 powerpoll_alpha_filter.py
        url = (
            f"https://api.worldquantbrain.com/users/self/alphas?limit={per_page}&offset={offset}"
            f"&status=UNSUBMITTED%1FIS_FAIL"
            f"&dateCreated>={start_date}&dateCreated<{end_date}"
            f"&order=-dateCreated"
            f"&hidden=false"
            f"&type=REGULAR"
        )

        logger.info(f"[AlphaProcessor] Fetching alphas from URL (offset: {offset})")

        try:
            response = sess.get(url, timeout=30)
            response.raise_for_status()

            data = response.json()
            alphas_page = data.get("results", [])

            if not alphas_page:
                logger.info("[AlphaProcessor] No more alphas found on this page. Pagination complete.")
                break

            logger.info(f"[AlphaProcessor] Successfully fetched {len(alphas_page)} alphas from offset {offset}.")
            all_alphas.extend(alphas_page)

            offset += per_page

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.info(
                    f"[AlphaProcessor] API returned 404 for date {date_str}, which might be expected if no alphas were created on a given page.")
                break  # 404 意味着没有数据，分页结束
            logger.error(f"[AlphaProcessor] HTTP error while fetching data: {e}")
            raise
        except Exception as e:
            logger.error(f"[AlphaProcessor] An unexpected error occurred while fetching data from API: {e}")
            logger.error("[AlphaProcessor] Stopping pagination due to error.")
            raise

    logger.info(f"[AlphaProcessor] Total alphas fetched from API for {date_str}: {len(all_alphas)}")
    return pd.DataFrame(all_alphas)


def process_alpha_candidates(calculation_date: str, specified_sharpe: float, specified_fitness: float,
                             corr_threshold: float, username: str, password: str):
    """
    This function runs in a separate process to perform CPU-heavy calculations.
    It encapsulates the logic from read_and_filter_alpha_ids and the alpha_prune calls.

    Args:
        calculation_date (str): The date for which to run the calculation (YYYY-MM-DD).
        specified_sharpe (float): The Sharpe ratio threshold.
        specified_fitness (float): The fitness threshold.
        corr_threshold (float): The correlation threshold.
        username (str): Username for external API.
        password (str): Password for external API.

    Returns:
        A list of alpha_ids that passed the filters and correlation checks.
    """
    logger = Logger()
    logger.info(f"[AlphaProcessor] Starting calculation for date: {calculation_date}")

    stone_gold_bag_dao = StoneGoldBagDAO()

    # 1. Read and filter from API, then save to stone_gold_bag_table
    db_date_str = calculation_date.replace('-', '')

    try:
        df = _fetch_alphas_from_api(db_date_str, logger)
    except Exception as e:
        logger.error(f"[AlphaProcessor] Failed to fetch alphas from API: {e}", exc_info=True)
        return []

    if df.empty:
        logger.info(f"[AlphaProcessor] No alphas found from API for date {db_date_str}")
        return []

    logger.info(f"[AlphaProcessor] Found {len(df)} records for date {db_date_str} from API, processing...")

    alpha_ids_filtered = []
    filtered_records = []

    for _, row in df.iterrows():
        try:
            is_data = row.get('is')
            if not isinstance(is_data, dict):
                logger.warning(f"[AlphaProcessor] Field 'is' for alpha {row.get('id')} is not a dictionary. Skipping.")
                continue

            fitness = float(is_data.get('fitness', 0))
            sharpe = float(is_data.get('sharpe', 0))
            checks = is_data.get('checks', [])
            has_failed_checks = any(check['result'] == 'FAIL' for check in checks)

            if fitness >= specified_fitness and sharpe >= specified_sharpe and not has_failed_checks:
                alpha_ids_filtered.append(row['id'])

                alpha_details = row.to_dict()

                date_created = alpha_details.get("dateCreated")

                settings = alpha_details.get("settings", {})
                region = settings.get("region")

                favorite = 1 if alpha_details.get("favorite", False) else 0
                hidden = 1 if alpha_details.get("hidden", False) else 0

                grade = alpha_details.get("grade")
                if grade is not None:
                    try:
                        grade = round(float(grade), 2)
                    except (ValueError, TypeError):
                        grade = None

                record_to_save = {
                    "id": alpha_details.get("id"),
                    "type": alpha_details.get("type"),
                    "author": alpha_details.get("author"),
                    "settings": str(settings),
                    "regular": str(alpha_details.get("regular", {})),
                    "dateCreated": to_mysql_datetime(date_created, logger),
                    "dateSubmitted": to_mysql_datetime(alpha_details.get("dateSubmitted"), logger),
                    "dateModified": to_mysql_datetime(alpha_details.get("dateModified"), logger),
                    "name": alpha_details.get("name"),
                    "favorite": favorite,
                    "hidden": hidden,
                    "color": alpha_details.get("color"),
                    "category": alpha_details.get("category"),
                    "tags": str(alpha_details.get("tags", [])),
                    "classifications": str(alpha_details.get("classifications", [])),
                    "grade": grade,
                    "stage": alpha_details.get("stage"),
                    "status": alpha_details.get("status"),
                    "is": str(alpha_details.get("is", {})),
                    "os": str(alpha_details.get("os")),
                    "train": str(alpha_details.get("train")),
                    "test": str(alpha_details.get("test")),
                    "prod": str(alpha_details.get("prod")),
                    "competitions": str(alpha_details.get("competitions")),
                    "themes": str(alpha_details.get("themes", [])),
                    "pyramids": str(alpha_details.get("pyramids", [])),
                    "pyramidThemes": str(alpha_details.get("pyramidThemes", [])),
                    "team": str(alpha_details.get("team")),
                    "datetime": db_date_str,
                    "region": region
                }
                filtered_records.append(record_to_save)
        except (ValueError, TypeError) as e:
            logger.warning(f"[AlphaProcessor] Failed to process alpha {row.get('id')}: {e}")
            continue

    logger.info(f"[AlphaProcessor] Filtered {len(alpha_ids_filtered)} qualified alphas based on sharpe/fitness.")
    save_stone_bag(stone_gold_bag_dao, filtered_records, db_date_str, logger)

    # 2. Load from stone_gold_bag_table and run correlation
    alpha_result = []
    if stone_gold_bag_dao.count_by_datetime(db_date_str) > 0:
        records = stone_gold_bag_dao.get_by_datetime(db_date_str)
        for record in records:
            try:
                is_data = ast.literal_eval(record['is']) if isinstance(record['is'], str) else record['is']
                settings_data = ast.literal_eval(record['settings']) if isinstance(record['settings'],
                                                                                    str) else record['settings']
                alpha_result.append({'id': record['id'], 'settings': settings_data, 'is': is_data})
            except Exception as e:
                logger.warning(
                    f"[CPU-Bound] Failed to parse record from stone_gold_bag_table {record.get('id')}: {e}")
                continue
        alpha_result.sort(key=lambda x: x['is'].get('fitness', 0))

    # 3. Call alpha_prune methods
    final_alpha_ids = []
    if alpha_result:
        try:
            os_alpha_ids, os_alpha_rets = generate_comparison_data(alpha_result, username, password)
            filtered_alphas = calculate_correlations(os_alpha_ids, os_alpha_rets, username, password,
                                                     corr_threshold=corr_threshold)

            for region, alpha_ids in filtered_alphas.items():
                final_alpha_ids.extend(alpha_ids)
                logger.info(f"[AlphaProcessor] Region: {region}, {len(alpha_ids)} alphas remaining after correlation.")
            final_alpha_ids = list(set(final_alpha_ids))
            logger.info(f"[AlphaProcessor] Total unique alpha IDs to check after correlation: {len(final_alpha_ids)}")
        except Exception as e:
            logger.error(f"[AlphaProcessor] Failed to filter alphas by correlation: {e}", exc_info=True)

    return final_alpha_ids

def save_stone_bag(dao, filtered_records, db_date_str, logger):
    if not filtered_records: return
    try:
        count = dao.batch_insert(filtered_records)
        logger.info(f"[AlphaProcessor] Saved {count} records to stone_gold_bag_table for date {db_date_str}")
    except Exception as e:
        logger.error(f"[AlphaProcessor] Error saving to stone_gold_bag_table: {e}")

class AlphaCalculator:
    def __init__(self, loop, specified_sharpe: float, specified_fitness: float, corr_threshold: float, signal_manager: SignalManager = None):
        self.loop = loop
        self.logger = Logger()
        self.specified_sharpe = specified_sharpe
        self.specified_fitness = specified_fitness
        self.corr_threshold = corr_threshold
        self.username = config_manager.get('username')
        self.password = config_manager.get('password')
        
        self.pending_alpha_checks_dao = PendingAlphaChecksDAO()
        self._scheduler_running = False
        self.init_date = config_manager.get('init_date_str')

        config_manager.on_config_change(self.on_config_update)
        if signal_manager:
            signal_manager.add_handler(self.handle_exit_signal)
        self.logger.info("[AlphaCalculator] Initialized with specified sharpe/fitness/corr_threshold.")

    async def run_calculation_job(self, calculation_date=None):
        if calculation_date is None:
            calculation_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        self.logger.info(f"Scheduling alpha processing for date: {calculation_date}")

        with ProcessPoolExecutor() as pool:
            alpha_ids = await self.loop.run_in_executor(
                pool, 
                process_alpha_candidates, 
                calculation_date,
                self.specified_sharpe,
                self.specified_fitness,
                self.corr_threshold,
                self.username,
                self.password
            )
        
        if alpha_ids:
            self.logger.info(f"Calculation complete. Found {len(alpha_ids)} alphas to check.")
            self.pending_alpha_checks_dao.add_alphas_to_check(alpha_ids)
        else:
            self.logger.info("Calculation complete. No alphas to check.")

    def start(self):
        """Starts the scheduler in a blocking loop. To be run in a separate thread."""
        self.logger.info("Starting AlphaCalculator scheduler...")
        self._scheduler_running = True

        def job():
            # Since this job is called from a sync scheduler, we need a way to run our async job.
            # The best way is to schedule it on the main event loop.
            asyncio.run_coroutine_threadsafe(self.run_calculation_job(), self.loop)

        schedule.every().day.at("12:30").do(job).tag('daily_calculation_task')
        self.logger.info(f"Next daily calculation scheduled at: {schedule.next_run()}")

        i = 0
        while self._scheduler_running:
            schedule.run_pending()
            if i % 1000 == 0:
                i = 0
                next_run = schedule.next_run()
                if next_run:
                    self.logger.info(f"Next scheduled task in: {(next_run - datetime.now()).total_seconds() / 3600:.2f} hours")
            i += 1
            time.sleep(1)
        
        self.logger.info("AlphaCalculator scheduler stopped.")

    def stop(self):
        self.logger.info("Stopping AlphaCalculator scheduler...")
        self._scheduler_running = False

    def handle_exit_signal(self, signum, frame):
        self.logger.info(f"Received shutdown signal {signum}, stopping AlphaCalculator.")
        self.stop()

    def on_config_update(self, new_config):
        new_init_date = new_config.get('init_date_str')
        if new_init_date and new_init_date != self.init_date:
            self.logger.info(f"[AlphaCalculator] init_date updated to {new_init_date}, triggering reprocessing.")
            self.init_date = new_init_date
            # Schedule the one-off job on the main event loop
            asyncio.run_coroutine_threadsafe(self.run_calculation_job(calculation_date=new_init_date), self.loop)
