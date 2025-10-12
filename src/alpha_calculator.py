# src/alpha_calculator.py

import asyncio
import time
import pandas as pd
import ast
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor

import schedule

from alpha_prune import calculate_correlations, generate_comparison_data
from logger import Logger
from signal_manager import SignalManager
from dao import SimulatedAlphasDAO, StoneGoldBagDAO, PendingAlphaChecksDAO
from config_manager import config_manager


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

    simulated_alphas_dao = SimulatedAlphasDAO()
    stone_gold_bag_dao = StoneGoldBagDAO()

    # 1. Read and filter from simulated_alphas_table, then save to stone_gold_bag_table
    db_date_str = calculation_date.replace('-', '')
    record_count = simulated_alphas_dao.count_by_datetime(db_date_str)
    if record_count == 0:
        logger.error(f"[AlphaProcessor] No records found in simulated_alphas_table for date {db_date_str}")
        return []

    logger.info(f"[AlphaProcessor] Found {record_count} records for date {db_date_str}, loading...")
    df = load_alpha_data_from_db(simulated_alphas_dao, db_date_str, logger)

    alpha_ids_filtered = []
    filtered_records = []

    for _, row in df.iterrows():
        try:
            is_data = ast.literal_eval(row['is'])
            fitness = float(is_data.get('fitness', 0))
            sharpe = float(is_data.get('sharpe', 0))
            checks = is_data.get('checks', [])
            has_failed_checks = any(check['result'] == 'FAIL' for check in checks)

            if fitness >= specified_fitness and sharpe >= specified_sharpe and not has_failed_checks:
                alpha_ids_filtered.append(row['id'])
                filtered_records.append(row.to_dict())
        except (SyntaxError, ValueError, TypeError) as e:
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
                settings_data = ast.literal_eval(record['settings']) if isinstance(record['settings'], str) else record['settings']
                alpha_result.append({'id': record['id'], 'settings': settings_data, 'is': is_data})
            except Exception as e:
                logger.warning(f"[CPU-Bound] Failed to parse record from stone_gold_bag_table {record.get('id')}: {e}")
                continue
        alpha_result.sort(key=lambda x: x['is'].get('fitness', 0))

    # 3. Call alpha_prune methods
    final_alpha_ids = []
    if alpha_result:
        try:
            os_alpha_ids, os_alpha_rets = generate_comparison_data(alpha_result, username, password)
            filtered_alphas = calculate_correlations(os_alpha_ids, os_alpha_rets, username, password, corr_threshold=corr_threshold)
            
            for region, alpha_ids in filtered_alphas.items():
                final_alpha_ids.extend(alpha_ids)
                logger.info(f"[AlphaProcessor] Region: {region}, {len(alpha_ids)} alphas remaining after correlation.")
            final_alpha_ids = list(set(final_alpha_ids))
            logger.info(f"[AlphaProcessor] Total unique alpha IDs to check after correlation: {len(final_alpha_ids)}")
        except Exception as e:
            logger.error(f"[AlphaProcessor] Failed to filter alphas by correlation: {e}", exc_info=True)
    
    return final_alpha_ids

# Helper functions extracted from ProcessSimulatedAlphas, to be used by process_alpha_candidates
def load_alpha_data_from_db(dao, db_date_str, logger) -> pd.DataFrame:
    total_records = dao.count_by_datetime(db_date_str)
    if total_records == 0: return pd.DataFrame()
    page_size = 1000
    records = []
    page_count = (total_records + page_size - 1) // page_size
    for page in range(1, page_count + 1):
        offset = (page - 1) * page_size
        page_records = dao.get_by_datetime_paginated(db_date_str, limit=page_size, offset=offset)
        records.extend(page_records)
    logger.info(f"[AlphaProcessor] Loaded {len(records)} records from DB.")
    return pd.DataFrame(records)

def save_stone_bag(dao, filtered_records, db_date_str, logger):
    if not filtered_records: return
    try:
        count = dao.batch_insert(filtered_records)
        logger.info(f"[AlphaProcessor] Saved {count} records to stone_gold_bag_table for date {db_date_str}")
    except Exception as e:
        logger.error(f"[AlphaProcessor] Error saving to stone_gold_bag_table: {e}")

class AlphaCalculator:
    def __init__(self, specified_sharpe: float, specified_fitness: float, corr_threshold: float, signal_manager: SignalManager = None):
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
        loop = asyncio.get_running_loop()
        
        if calculation_date is None:
            calculation_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        self.logger.info(f"Scheduling alpha processing for date: {calculation_date}")

        with ProcessPoolExecutor() as pool:
            alpha_ids = await loop.run_in_executor(
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
            asyncio.run_coroutine_threadsafe(self.run_calculation_job(), asyncio.get_event_loop())

        schedule.every().day.at("12:30").do(job).tag('daily_calculation_task')
        self.logger.info(f"Next daily calculation scheduled at: {schedule.next_run()}")

        i = 0
        while self._scheduler_running:
            schedule.run_pending()
            if i % 300 == 0:
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
            asyncio.run_coroutine_threadsafe(self.run_calculation_job(calculation_date=new_init_date), asyncio.get_event_loop())
