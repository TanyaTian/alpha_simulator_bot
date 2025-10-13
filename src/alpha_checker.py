# src/alpha_checker.py

import asyncio
import time
import aiohttp
import pandas as pd
import schedule
from datetime import datetime

from logger import Logger
from signal_manager import SignalManager
from dao import PendingAlphaChecksDAO, StoneGoldBagDAO
from config_manager import config_manager

class AlphaChecker:
    def __init__(self, loop, signal_manager: SignalManager = None):
        self.loop = loop
        self.logger = Logger()
        self._load_config()
        
        self.pending_alpha_checks_dao = PendingAlphaChecksDAO()
        self.stone_gold_bag_dao = StoneGoldBagDAO()
        
        self._scheduler_running = False
        self.is_checking = False # A lock to prevent concurrent check runs

        if signal_manager:
            signal_manager.add_handler(self.handle_exit_signal)

    def _load_config(self):
        self.session = config_manager.get_session() # Using the shared session from config_manager
        if not self.session:
            self.logger.error("[AlphaChecker] Failed to get aiohttp session from config_manager. The checker will not run.")
        self.logger.info("[AlphaChecker] Config and session loaded.")

    async def check_alpha(self, session: aiohttp.ClientSession, alpha_id: str, retries: int = 10):
        """Asynchronously checks a single alpha ID with a retry mechanism."""
        url = f"https://api.worldquantbrain.com/alphas/{alpha_id}/check"
        self.logger.debug(f"Checking alpha: {alpha_id} (Retries left: {retries})")
        try:
            async with session.get(url) as response:
                if "retry-after" in response.headers:
                    if retries > 0:
                        wait_time = float(response.headers["Retry-After"])
                        self.logger.warning(f"Rate limit hit for {alpha_id}, sleeping for {wait_time}s. Retrying...")
                        await asyncio.sleep(wait_time)
                        return await self.check_alpha(session, alpha_id, retries - 1) # Retry after waiting
                    else:
                        self.logger.error(f"Retry limit reached for alpha {alpha_id}. Keeping as pending.")
                        return 'pending', alpha_id, None

                response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
                result_json = await response.json()

                if result_json.get("is", 0) == 0:
                    self.logger.warning(f"Alpha {alpha_id} is not ready for check (is=0). Will retry later.")
                    return 'pending', alpha_id, None # Keep status as pending

                checks_df = pd.DataFrame(result_json["is"]["checks"])
                
                if any(checks_df["result"] == "ERROR"):
                    self.logger.error(f"Check returned an error for alpha {alpha_id}.")
                    return 'failed', alpha_id, None

                if any(checks_df["result"] == "FAIL"):
                    self.logger.info(f"Alpha {alpha_id} failed checks.")
                    return 'completed', alpha_id, None # Completed, but not a success

                # All checks passed, extract values
                pc = checks_df[checks_df.name == "PROD_CORRELATION"]["value"].values[0]
                sharpe = checks_df[checks_df.name == "LOW_SHARPE"]["value"].values[0]
                fitness = checks_df[checks_df.name == "LOW_FITNESS"]["value"].values[0]
                
                self.logger.info(f"Alpha {alpha_id} passed all checks.")
                return 'completed', alpha_id, (pc, sharpe, fitness)

        except aiohttp.ClientError as e:
            self.logger.error(f"HTTP error checking alpha {alpha_id}: {e}")
            return 'pending', alpha_id, None # Network error, keep as pending to retry
        except Exception as e:
            self.logger.error(f"Error processing check for alpha {alpha_id}: {e}", exc_info=True)
            return 'failed', alpha_id, None # Mark as failed if there's a parsing error

    async def run_hourly_checks(self):
        """The main async method to fetch and check alphas."""
        if self.is_checking:
            self.logger.info("Check is already in progress. Skipping this run.")
            return

        self.session = config_manager.get_session() # Refresh session each run
        if not self.session:
            self.logger.error("No aiohttp session available. Skipping hourly check.")
            return
        
        self.is_checking = True
        self.logger.info("Starting hourly alpha checks...")
        try:
            pending_alphas = self.pending_alpha_checks_dao.get_pending_checks(limit=500)
            if not pending_alphas:
                self.logger.info("No pending alphas to check. Sleeping for 1 hour.")
                return

            alpha_ids_to_check = [alpha['alpha_id'] for alpha in pending_alphas]
            self.logger.info(f"Found {len(alpha_ids_to_check)} alphas to check.")

            semaphore = asyncio.Semaphore(10) # Limit to 10 concurrent requests

            async def check_alpha_with_semaphore(alpha_id):
                async with semaphore:
                    return await self.check_alpha(self.session, alpha_id)

            tasks = [check_alpha_with_semaphore(alpha_id) for alpha_id in alpha_ids_to_check]
            results = await asyncio.gather(*tasks)

            successful_alphas = []
            for status, alpha_id, data in results:
                if status != 'pending': # Update status unless it needs to be retried without a status change
                    self.pending_alpha_checks_dao.update_check_status(alpha_id, status)
                
                if status == 'completed' and data is not None:
                    pc, sharpe, fitness = data
                    successful_alphas.append({
                        'id': alpha_id,
                        'gold': 'gold',
                        'prodCorrelation': pc,
                        'sharpe': sharpe,
                        'fitness': fitness
                    })
            
            if successful_alphas:
                self.logger.info(f"Saving {len(successful_alphas)} successful alphas to stone_gold_bag.")
                self.stone_gold_bag_dao.batch_insert(successful_alphas)

        finally:
            self.is_checking = False
            self.logger.info("Hourly alpha checks finished.")

    def start(self):
        """Starts the scheduler in a blocking loop. To be run in a separate thread."""
        self.logger.info("Starting AlphaChecker scheduler...")
        self._scheduler_running = True

        def job():
            asyncio.run_coroutine_threadsafe(self.run_hourly_checks(), self.loop)

        schedule.every().hour.at(":01").do(job).tag('hourly_check_task')
        self.logger.info(f"Next hourly check scheduled at: {schedule.next_run()}")

        while self._scheduler_running:
            schedule.run_pending()
            time.sleep(1)
        
        self.logger.info("AlphaChecker scheduler stopped.")

    def stop(self):
        self.logger.info("Stopping AlphaChecker scheduler...")
        self._scheduler_running = False

    def handle_exit_signal(self, signum, frame):
        self.logger.info(f"Received shutdown signal {signum}, stopping AlphaChecker.")
        self.stop()
