import time
import pandas as pd
from ace_lib import SingleSession, brain_api_url, logger

def get_power_pool_corr(s: SingleSession, alpha_id: str) -> pd.DataFrame:
    """
    Retrieve the power-pool correlation data for a specific alpha.

    Args:
        s (SingleSession): An authenticated session object.
        alpha_id (str): The ID of the alpha.

    Returns:
        pandas.DataFrame: A DataFrame containing the power-pool correlation data.

    Raises:
        requests.exceptions.RequestException: If there's an error in the API request.
    """
    while True:
        result = s.get(brain_api_url + "/alphas/" + alpha_id + "/correlations/power-pool")
        if "retry-after" in result.headers:
            time.sleep(float(result.headers["Retry-After"]))
        else:
            break
    if result.json().get("records", 0) == 0:
        # print(f"Failed to get power-pool correlation for alpha_id {alpha_id}. {result.json()}")
        return pd.DataFrame()
    columns = [dct["name"] for dct in result.json()["schema"]["properties"]]
    power_pool_corr_df = pd.DataFrame(result.json()["records"], columns=columns).assign(alpha_id=alpha_id)
    power_pool_corr_df["alpha_max_power_pool_corr"] = result.json()["max"]
    power_pool_corr_df["alpha_min_power_pool_corr"] = result.json()["min"]
    return power_pool_corr_df

def check_power_pool_corr_test(s: SingleSession, alpha_id: str, threshold: float = 0.7) -> pd.DataFrame:
    """
    Check if the alpha's power-pool correlation passes a specified threshold.

    Args:
        s (SingleSession): An authenticated session object.
        alpha_id (str): The ID of the alpha.
        threshold (float, optional): The correlation threshold. Defaults to 0.7.

    Returns:
        pandas.DataFrame: A DataFrame containing the test result.

    Raises:
        requests.exceptions.RequestException: If there's an error in the API request.
    """
    power_pool_corr_df = get_power_pool_corr(s, alpha_id)
    if power_pool_corr_df.empty:
        result = [
            {
                "test": "POWER_POOL_CORRELATION",
                "result": "NONE",
                "limit": threshold,
                "value": None,
                "alpha_id": alpha_id,
            }
        ]
    else:
        value = power_pool_corr_df["alpha_max_power_pool_corr"].values[0]
        result = [
            {
                "test": "POWER_POOL_CORRELATION",
                "result": "PASS" if value <= threshold else "FAIL",
                "limit": threshold,
                "value": value,
                "alpha_id": alpha_id,
            }
        ]
    return pd.DataFrame(result)
