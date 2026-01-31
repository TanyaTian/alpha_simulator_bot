import os
import time
import pandas as pd
import ace_lib
import re
from typing import Optional
from logger import Logger

# Initialize logger
logger = Logger()

def get_datafields_with_cache(session, region: str, universe: str, delay: int, data_type: str, dataset_id: str) -> Optional[pd.DataFrame]:
    """
    Fetches data fields from the ACE server with a caching mechanism.

    This function wraps `ace_lib.get_datafields` to provide a caching layer that stores results
    on the local filesystem to avoid repeated calls to the server.

    Args:
        session: The authenticated ace_lib session object.
        region (str): The region for the data (e.g., 'USA', 'EUR').
        universe (str): The stock universe (e.g., 'TOP3000').
        delay (int): The data delay (e.g., 1).
        data_type (str): The type of data ('GROUP', 'MATRIX', etc.).
        dataset_id (str): The identifier for the dataset (e.g., 'pv1', 'consensus_v1').

    Returns:
        Optional[pd.DataFrame]: A pandas DataFrame containing the data fields, or None if
                                fetching fails after retries.
    """
    # Sanitize dataset_id to create a valid filename
    sanitized_dataset_id = re.sub(r'[\\/*?:"<>|]', '_', dataset_id)
    
    # 1. Construct cache path
    cache_dir = os.path.join('data', region)
    cache_filename = f"{universe}_{delay}_{data_type}_{sanitized_dataset_id}.csv"
    cache_filepath = os.path.join(cache_dir, cache_filename)

    # 2. Check if cached file exists
    if os.path.exists(cache_filepath):
        logger.debug(f"Cache hit. Loading data from: {cache_filepath}")
        try:
            return pd.read_csv(cache_filepath, index_col=0)
        except Exception as e:
            logger.debug(f"Error reading cache file {cache_filepath}: {e}. Refetching from server.")

    # 3. If cache miss, fetch from server with retry logic
    logger.debug(f"Cache miss. Fetching from server for: region={region}, universe={universe}, dataset_id={dataset_id}")
    max_retries = 5
    df = None
    retry_delay = 2  # Initial delay in seconds

    for attempt in range(max_retries):
        try:
            # Ensure session is fresh before making a call
            session = ace_lib.check_session_and_relogin(session)
            
            df = ace_lib.get_datafields(
                session,
                region=region,
                universe=universe,
                delay=delay,
                data_type=data_type,
                dataset_id=dataset_id
            )

            # If the result is not empty, we are successful
            if df is not None and not df.empty:
                logger.debug("Successfully fetched data from server.")
                break
            
            if attempt < max_retries - 1:
                logger.debug(f"Attempt {attempt + 1}/{max_retries}: Fetched empty data. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff

        except Exception as e:
            if attempt < max_retries - 1:
                logger.debug(f"Attempt {attempt + 1}/{max_retries}: An exception occurred: {e}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logger.debug(f"Final attempt {attempt + 1}/{max_retries} failed: {e}")
    
    # 4. Process and save the result
    if df is not None and not df.empty:
        # Create directory if it doesn't exist
        try:
            os.makedirs(cache_dir, exist_ok=True)
            # Save the dataframe to cache
            df.to_csv(cache_filepath)
            logger.debug(f"Data saved to cache: {cache_filepath}")
        except Exception as e:
            logger.debug(f"Error saving data to cache file {cache_filepath}: {e}")
        return df
    else:
        logger.debug("Failed to fetch data from the server after all retries. No cache file will be created.")
        return None
