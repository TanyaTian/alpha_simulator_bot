# src/alpha_checker.py

import asyncio
import time
import aiohttp
import pandas as pd
import schedule
import datetime
from datetime import datetime

from logger import Logger
from signal_manager import SignalManager
from dao import PendingAlphaChecksDAO, StoneGoldBagDAO
from config_manager import config_manager

class AlphaChecker:
    def __init__(self, loop, signal_manager: SignalManager = None):
        self.loop = loop
        self.logger = Logger()

        self.pending_alpha_checks_dao = PendingAlphaChecksDAO()
        self.stone_gold_bag_dao = StoneGoldBagDAO()

        self._scheduler_running = False
        self.is_checking = False  # A lock to prevent concurrent check runs

        # Get session from config_manager and register for updates
        self.session = config_manager.get_session()
        config_manager.on_config_change(self.handle_config_change)

        if signal_manager:
            signal_manager.add_handler(self.handle_exit_signal)

    def handle_config_change(self, new_config):
        """Callback to handle configuration changes."""
        self.logger.info("Configuration updated, refreshing session for AlphaChecker...")
        self.session = config_manager.get_session()

    async def check_alpha(self, session: aiohttp.ClientSession, alpha_id: str):
        """Asynchronously checks a single alpha ID, handling retries internally."""
        url = f"https://api.worldquantbrain.com/alphas/{alpha_id}/check"
        self.logger.debug(f"Starting check for alpha: {alpha_id} at {url}")

        retry_count = 0
        while True:
            try:
                async with session.get(url) as response:
                    if "retry-after" in response.headers:
                        retry_count += 1
                        if retry_count >= 10:
                            self.logger.error(f"Retry limit (10) reached for alpha {alpha_id}. Giving up.")
                            return 'not_ready', alpha_id, None

                        wait_time = float(response.headers["Retry-After"])
                        self.logger.warning(
                            f"Rate limit for {alpha_id}. Sleeping for {wait_time}s as suggested by server before retrying.But have sleep 60s first.")
                        await asyncio.sleep(
                            60 + wait_time)  # Sleep a fixed 60 seconds instead of the suggested time
                        continue  # Retry the same alpha

                    response.raise_for_status()
                    result_json = await response.json()
                    self.logger.debug(f"JSON response for {alpha_id}: {result_json}")

                    if result_json.get("is", 0) == 0:
                        self.logger.warning(f"Alpha {alpha_id} is not ready (is=0).")
                        return 'not_ready', alpha_id, None

                    checks_df = pd.DataFrame(result_json["is"]["checks"])

                    if any(checks_df["result"] == "ERROR"):
                        self.logger.error(
                            f"Check returned an error for alpha {alpha_id}. Details: {checks_df.to_dict('records')}")
                        return 'failed', alpha_id, None

                    failed_checks = checks_df[checks_df["result"] == "FAIL"]
                    if not failed_checks.empty:
                        self.logger.info(f"Alpha {alpha_id} failed checks.")
                        self.logger.info(f"Fail reasons:\n{failed_checks.to_string(index=False)}")
                        return 'completed', alpha_id, None

                    pc = checks_df[checks_df.name == "PROD_CORRELATION"]["value"].values[0]
                    sharpe = checks_df[checks_df.name == "LOW_SHARPE"]["value"].values[0]
                    fitness = checks_df[checks_df.name == "LOW_FITNESS"]["value"].values[0]

                    self.logger.info(f"Alpha {alpha_id} passed all checks.")
                    return 'completed', alpha_id, (pc, sharpe, fitness)

            except aiohttp.ClientResponseError as e:
                if e.status == 401:
                    self.logger.warning(
                        f"Authentication error (401) for alpha {alpha_id}. A session refresh will be attempted.")
                    return 'auth_error', alpha_id, None
                self.logger.error(f"HTTP error for {alpha_id}: {e}. It will be re-checked in the next run.")
                return 'not_ready', alpha_id, None
            except aiohttp.ClientError as e:
                self.logger.error(f"Client error for {alpha_id}: {e}. It will be re-checked in the next run.")
                return 'not_ready', alpha_id, None
            except Exception as e:
                self.logger.error(f"Unexpected error processing {alpha_id}: {e}", exc_info=True)
                return 'failed', alpha_id, None

    async def run_hourly_checks(self):
        """The main async method to fetch and check alphas using a queue-based approach."""
        if self.is_checking:
            self.logger.info("Check is already in progress. Skipping this run.")
            return

        self.session = config_manager.get_session()
        if not self.session or not self.session.cookies:
            self.logger.error("Session or cookies not found. Skipping check run.")
            return

        self.is_checking = True
        self.logger.info("Starting alpha check run...")

        client_session = None
        try:
            pending_alphas = self.pending_alpha_checks_dao.get_pending_checks(limit=500)
            if not pending_alphas:
                self.logger.info("No pending alphas to check.")
                return

            alpha_queue = [alpha['alpha_id'] for alpha in pending_alphas]
            total_initial_alphas = len(alpha_queue)
            processed_count = 0
            self.logger.info(f"Found {total_initial_alphas} alphas to check.")

            client_session = aiohttp.ClientSession(cookies=self.session.cookies)
            while alpha_queue:
                alpha_id = alpha_queue.pop(0)
                processed_count += 1

                self.logger.info(f"--- [{processed_count}/{total_initial_alphas}] Checking alpha: {alpha_id} ---")

                status, _, data = await self.check_alpha(client_session, alpha_id)

                if status == 'auth_error':
                    self.logger.warning("Authentication error. Refreshing session and retrying alpha.")
                    await client_session.close()

                    self.session = config_manager.get_session()  # Re-login/get new session
                    if not self.session or not self.session.cookies:
                        self.logger.error("Failed to get a new session. Aborting check run.")
                        alpha_queue.insert(0, alpha_id)  # put it back
                        break  # exit while

                    client_session = aiohttp.ClientSession(cookies=self.session.cookies)

                    # Re-add alpha to the front of the queue and continue
                    alpha_queue.insert(0, alpha_id)
                    processed_count -= 1
                    continue

                elif status == 'not_ready':
                    self.logger.warning(
                        f"Alpha {alpha_id} is not ready. It will be checked in the next scheduled run.")
                    # No re-queueing, just continue to the next alpha after a short wait
                    await asyncio.sleep(30)


                elif status == 'failed':
                    self.logger.error(f"Alpha {alpha_id} failed processing, marking as 'failed'.")
                    self.pending_alpha_checks_dao.update_check_status(alpha_id, 'failed')

                elif status == 'completed':
                    self.logger.info(f"Alpha {alpha_id} check completed.")
                    self.pending_alpha_checks_dao.update_check_status(alpha_id, 'completed')

                    if data is not None:
                        pc, sharpe, fitness = data
                        update_data = {
                            'gold': 'gold',
                            'prodCorrelation': pc,
                            'sharpe': sharpe,
                            'fitness': fitness
                        }
                        self.logger.info(f"Updating successful alpha {alpha_id} in stone_gold_bag.")
                        self.stone_gold_bag_dao.update_by_id(alpha_id, update_data)

                # Add a polite wait between checking different alphas
                if alpha_queue:
                    self.logger.info(f"Finished check for {alpha_id}. Waiting 5 seconds before next alpha.")
                    await asyncio.sleep(5)

        except Exception as e:
            self.logger.error(f"An unexpected error occurred during alpha checks: {e}", exc_info=True)
        finally:
            if client_session and not client_session.closed:
                await client_session.close()
            self.is_checking = False
            self.logger.info("Alpha check run finished.")

    def start(self):
        """Starts the scheduler in a blocking loop. To be run in a separate thread."""
        self.logger.info("Starting AlphaChecker scheduler...")
        self._scheduler_running = True

        def job():
            asyncio.run_coroutine_threadsafe(self.run_hourly_checks(), self.loop)

        schedule.every(1).hour.do(job).tag('hourly_check_task')
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
