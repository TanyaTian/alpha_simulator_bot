import ast
import re
from typing import List, Optional
import pandas as pd
import requests
import time
import pandas as pd
import ace_lib as ace
from llm_calls import call_llm_free, call_llm_paid
from config_manager import config_manager
from logger import Logger
import os
from datetime import datetime


def fetch_all_categories_data(session, params_combo, categories=None, max_retries=3, delay=5):
    """
    Download dataset data for all specified categories under given parameter combinations, and return merged DataFrame
    
    Args:
        session: requests.Session object for sending HTTP requests
        params_combo (dict): Parameter combinations, e.g., {'region': 'USA', 'universe': 'TOP3000', 'delay': 1}
        categories (list): List of categories, uses default values if None
        max_retries (int): Maximum retry attempts
        delay (int): Retry delay time (seconds)
    
    Returns:
        pd.DataFrame: DataFrame containing dataset data for all categories
    """
    
    def fetch_data(session, params, max_retries=3, delay=5):
        """
        Fetches dataset using session.get method to call API and returns results data in DataFrame format.
        Includes failure retry mechanism.

        Args:
            session: requests.Session object for sending HTTP requests.
            params (dict): API request parameters dictionary.
            max_retries (int): Maximum number of retry attempts.
            delay (int): Delay in seconds between retries.

        Returns:
            pd.DataFrame: API results data converted to DataFrame format. Returns empty DataFrame if request fails or no data.
        """
        base_url = "https://api.worldquantbrain.com/data-sets"
        for attempt in range(max_retries):
            try:
                # Use session.get to send request, params automatically converted to query string
                response = session.get(base_url, params=params)
                response.raise_for_status()  # Check if request successful, raise exception if failed
                data = response.json()
                results = data.get('results', [])
                if not results:
                    print(f"Category '{params.get('category', 'unknown')}' returned no data")
                    return pd.DataFrame()  # Return empty DataFrame
                
                # Add parameter information to results for tracking data source
                df = pd.DataFrame(results)
                df['_region'] = params.get('region', '')
                df['_universe'] = params.get('universe', '')
                df['_delay'] = params.get('delay', '')
                df['_category'] = params.get('category', '')
                
                return df
            except requests.exceptions.RequestException as e:
                print(f"Failed to fetch data (attempt {attempt + 1}/{max_retries}), error: {e}")
                if attempt < max_retries - 1:
                    print(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    print("Maximum retry attempts reached, giving up on data fetch.")
                    return pd.DataFrame()

    # Set default categories
    if categories is None:
        categories = ['analyst', 'broker', 'insiders', 'fundamental', 'macro', 'model', 'news', 
                     'option', 'other', 'pv', 'risk', 'sentiment', 'socialmedia', 'earnings', 
                     'imbalance', 'institutions', 'shortinterest']
    
    # List to store all data
    all_data = []
    
    # Base parameters
    default_params = {
        'delay': params_combo['delay'],
        'instrumentType': 'EQUITY',
        'limit': 30,
        'offset': 0,
        'region': params_combo['region'],
        'universe': params_combo['universe'],
    }
    
    print(f"Processing combination: Region={params_combo['region']}, Universe={params_combo['universe']}, Delay={params_combo['delay']}")
    
    # Iterate through all categories
    for category in categories:
        params = default_params.copy()
        params['category'] = category
        results_df = fetch_data(session, params, max_retries, delay)
        if not results_df.empty:
            all_data.append(results_df)
            print(f"Category '{category}' data fetched successfully, {len(results_df)} records")
        else:
            print(f"Category '{category}' returned no data")
    
    # Merge all data
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        print(f"\nAll categories data fetched successfully! Total records: {len(final_df)}")
        return final_df
    else:
        print("\nNo data was fetched from any category")
        return pd.DataFrame()
    
def extract_python_list_from_string(text_content):
    """
    Extracts a Python list from a multi-line string that is inside a markdown python block.

    Args:
        text_content: The string containing the Python list.

    Returns:
        A Python list object, or None if no list is found.
    """
    try:
        # Find the start of the python code block
        code_block_start = text_content.find("```python")
        if code_block_start == -1:
            # If no python block is found, try to find a list directly
            code_start = text_content.find('[')
            if code_start == -1:
                return None
        else:
            code_start = text_content.find('[', code_block_start)
            if code_start == -1:
                return None

        # Find the matching closing bracket for the list
        code_end = -1
        open_brackets = 0
        for i in range(code_start, len(text_content)):
            if text_content[i] == '[':
                open_brackets += 1
            elif text_content[i] == ']':
                open_brackets -= 1
                if open_brackets == 0:
                    code_end = i + 1
                    break
        
        if code_end == -1:
            return None

        list_string = text_content[code_start:code_end]
        
        # Remove comments from the list string
        list_string = re.sub(r'#.*', '', list_string)

        # Safely evaluate the string as a Python literal
        return ast.literal_eval(list_string)
    except (ValueError, SyntaxError):
        # Handle cases where the string is not a valid Python literal
        return None

def get_familiar_dataset(brain_session, alpha_expression: Optional[str], alpha_description: str, dataset_id: Optional[str], settings_dict: dict, exclude_datasets: Optional[List[str]] = None, filter_requirement: Optional[str] = None):
    """
    Finds similar datasets and their data fields based on an alpha's expression and description.

    This function queries an LLM to find datasets that are thematically similar to a given alpha,
    based on its expression and description. It then fetches all data fields for these newly
    suggested datasets, after filtering out excluded datasets.

    Args:
        brain_session: The authenticated brain session object.
        alpha_expression (Optional[str]): The code/expression of the source alpha. Can be None.
        alpha_description (str): The description of the source alpha.
        dataset_id (Optional[str]): The original dataset ID of the source alpha, to be excluded from suggestions. Can be None.
        settings_dict (dict): A dictionary containing the simulation settings like 'region', 'universe', and 'delay'.
        exclude_datasets (list, optional): A list of strings. Datasets containing any of these strings will be filtered out. Defaults to None.
        filter_requirement (str, optional): An additional requirement to filter the datasets. Defaults to None.

    Returns:
        dict: A dictionary where keys are the suggested dataset IDs and values are pandas.DataFrames
              containing the data fields for each respective dataset. Returns an empty dictionary
              if no similar datasets are found or an error occurs.
    """
    # 1. Construct CSV file path and read the list of available datasets
    try:

        available_datasets_df = fetch_all_categories_data(brain_session, settings_dict)
        
        # Filter out excluded datasets before they are sent to the LLM
        if exclude_datasets and not available_datasets_df.empty:
            # Create a boolean mask for rows to keep
            mask = pd.Series([True] * len(available_datasets_df), index=available_datasets_df.index)
            for keyword in exclude_datasets:
                # Update mask to be False for rows where 'id' contains the keyword
                mask &= ~available_datasets_df['id'].str.contains(keyword, case=False, na=False)
            
            original_count = len(available_datasets_df)
            available_datasets_df = available_datasets_df[mask]
            print(f"Filtered out {original_count - len(available_datasets_df)} datasets based on exclusion rules.")

        required_cols = ['id', 'name', 'description', 'category', 'fieldCount']
        available_cols = [col for col in required_cols if col in available_datasets_df.columns]
        datasets_for_prompt = available_datasets_df[available_cols].reset_index(drop=True).to_json(orient='records')
        
    except Exception as e:
        print(f"Error reading or processing dataset CSV: {e}")
        return {}

    # 2. Formulate the prompt for the LLM
    alpha_expression_section = ""
    if alpha_expression:
        alpha_expression_section = f"""
    **Alpha Expression:**
    ```
    {alpha_expression}
    ```
        """
    else:
        alpha_expression_section = """
    **Alpha Expression:**
    No specific alpha expression is provided. Please rely on the Alpha Description and general financial knowledge.
        """

    dataset_id_exclusion_instruction = ""
    if dataset_id:
        dataset_id_exclusion_instruction = f"5.  **Crucially, you must NOT include the original dataset ID '{dataset_id}' in your suggestions.**"
    else:
        dataset_id_exclusion_instruction = "5.  No original dataset ID is provided, so no specific dataset needs to be excluded from your suggestions based on an original alpha."

    filter_requirement_section = ""
    if filter_requirement:
        filter_requirement_section = f"""
    **Filter Requirement:**
    ```
    {filter_requirement}
    ```
        """

    prompt = f"""
    You are an expert in financial quantitative strategies.
    Based on the provided alpha expression (if available) and its description, please analyze the list of available datasets.
    Your task is to identify and suggest other datasets that follow a similar theme, pattern, or could be used to generate similar alphas.

    **Instructions:**
    1.  Review the Alpha Expression (if provided) and Description to understand its logic (e.g., is it based on sentiment, fundamentals, technicals?).
    2.  Scan the "List of Available Datasets".
    3.  Identify 4~5 datasets from the list that are most thematically similar.(Diversify the dataset from various categories)
    4.  Diversify the dataset selection by picking dataset_ids from various categories.
    {dataset_id_exclusion_instruction}
    6.  Return your answer ONLY as a Python list of the suggested dataset ID strings.
    {alpha_expression_section}
    **Alpha Description:**
    ```
    {alpha_description}
    ```
    {filter_requirement_section}

    **List of Available Datasets:**
    ```
    {datasets_for_prompt}
    ```

    **Output:**
    Return a Python list of strings, for example: `['dataset_id_1', 'dataset_id_2']`
    """

    # 3. Call the LLM
    print("Asking LLM to find familiar datasets...")
    llm_response = call_llm_paid(prompt)

    if not llm_response:
        print("LLM call failed or returned no response.")
        return {}

    # 4. Parse the LLM response
    print(f"LLM response for familiar datasets:\n{llm_response}")
    suggested_dataset_ids = extract_python_list_from_string(llm_response)

    if not suggested_dataset_ids or not isinstance(suggested_dataset_ids, list):
        print("Could not parse a valid list of dataset IDs from the LLM response.")
        return {}

    # 5. Get data fields for each suggested dataset
    all_data_fields_dict = {}
    for new_dataset_id in suggested_dataset_ids:
        if not isinstance(new_dataset_id, str):
            continue # Skip non-string entries
            
        print(f"Fetching data fields for suggested dataset: '{new_dataset_id}'")
        try:
            data_fields_df = ace.get_datafields(
                s=brain_session,
                region=settings_dict['region'],
                universe=settings_dict['universe'],
                delay=settings_dict['delay'],
                dataset_id=new_dataset_id,
                data_type='ALL'
            )
            if not data_fields_df.empty:
                all_data_fields_dict[new_dataset_id] = data_fields_df.sample(n=700) if len(data_fields_df) > 700 else data_fields_df
                print(f"Found {len(data_fields_df)} fields in '{new_dataset_id}'.")
            else:
                print(f"No data fields found for dataset '{new_dataset_id}'.")
        except Exception as e:
            print(f"An error occurred while fetching data fields for dataset '{new_dataset_id}': {e}")

    # 6. Return the dictionary of datasets and their fields
    if not all_data_fields_dict:
        print("No data fields were found for any of the suggested datasets.")
        return {}

    print(f"Returning data fields for {len(all_data_fields_dict)} similar datasets.")
    return all_data_fields_dict


def select_data_fields(alpha_expression: str, alpha_description: str, dataset_id: str, data_fields_df: pd.DataFrame, filter_requirement: str):
    """
    Selects specific data fields from a single dataset based on an alpha's characteristics and a filter requirement.

    This function constructs a prompt for an LLM to select relevant data field IDs from a single provided
    dataset DataFrame (`data_fields_df`). The selection is guided by the alpha's expression, its description, and a specific
    filtering requirement. The raw response from the LLM is then returned.

    Args:
        alpha_expression (str): The code/expression of the source alpha.
        alpha_description (str): The description of the source alpha.
        dataset_id (str): The ID of the dataset being processed.
        data_fields_df (pd.DataFrame): A DataFrame containing the data fields for the dataset.
        filter_requirement (str): A specific instruction or requirement to guide the LLM's data field selection.
        
    Returns:
        str: The raw response from the LLM, or a message indicating failure or no response.
    """
    if data_fields_df.empty:
        message = f"The data_fields_df for dataset '{dataset_id}' is empty. Cannot select fields."
        print(message)
        return message

    # Ensure the required columns are present for the prompt
    required_cols = ['id', 'description', 'dataset', 'coverage', 'type']
    available_cols = [col for col in required_cols if col in data_fields_df.columns]
    
    if not available_cols:
        message = f"DataFrame for dataset '{dataset_id}' contains none of the required columns for the prompt. Skipping."
        print(message)
        return message
        
    data_fields_for_prompt = data_fields_df[available_cols].to_json(orient='records')

    prompt = f"""
    You are an expert in financial quantitative strategies.
    I have an alpha with the following expression and description. Your task is to select specific data field IDs from the provided list of fields from dataset '{dataset_id}' that are suitable for the alpha, based on the provided filter requirement.

    **Alpha Expression:**
    ```
    {alpha_expression}
    ```

    **Alpha Description:**
    ```
    {alpha_description}
    ```

    **Available Data Fields from Dataset '{dataset_id}':**
    ```json
    {data_fields_for_prompt}
    ```

    **Filter Requirement:**
    Please select data field IDs from the list above based on the following instruction:
    ```
    {filter_requirement}
    ```

    **Output:**
    Based on the filter requirement, provide your answer. Please clearly list the ID, type, and reason for selection for each chosen data field.
    The output format should be a Python list:

    [['df1', 'df2'],  # type + reason
    ...
    ['df1n', 'df2n']]  # type + reason
    """

    print(f"\nAsking LLM to select specific data fields for dataset '{dataset_id}'...")
    response = call_llm_paid(prompt)

    if response:
        print(f"LLM response for selected fields in '{dataset_id}':\n", response)
        return response
    else:
        message = f"LLM call failed or returned no response for data field selection in '{dataset_id}'."
        print(message)
        return message


def generate_screening_report(sess, alpha_expression, alpha_description, settings_dict, dataset_filter_req, field_filter_req, dataset_id=None, exclude_datasets=None):
    """
    Generates and saves a screening report for an alpha.

    This function finds familiar datasets, selects relevant data fields from them,
    and compiles all the information into a single report file.

    Args:
        sess: The authenticated brain session object.
        alpha_expression (str): The alpha's expression.
        alpha_description (str): The alpha's description.
        settings_dict (dict): Simulation settings.
        dataset_filter_req (str): The requirement for filtering datasets.
        field_filter_req (str): The requirement for filtering data fields.
        dataset_id (str, optional): The original dataset ID to exclude. Defaults to None.
        exclude_datasets (list, optional): A list of dataset keywords to exclude. Defaults to None.
    """
    # 1. Find familiar datasets
    all_data_fields_dict = get_familiar_dataset(
        sess, alpha_expression, alpha_description, dataset_id, settings_dict, exclude_datasets,
        filter_requirement=dataset_filter_req
    )

    # 2. Prepare the report content
    report_content = f"""
        # Alpha Data Field Screening Report

        **Timestamp:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

        ## 1. Source Alpha Information

        ### Alpha Expression:
        ```
        {alpha_expression}
        ```

        ### Alpha Description:
        ```
        {alpha_description}
        ```

        ## 2. Familiar Datasets Found

        Based on the alpha and description, the following familiar datasets were identified:
        - {', '.join(all_data_fields_dict.keys()) if all_data_fields_dict else 'None'}

        ---

        ## 3. Detailed Field Analysis per Dataset
        """

    if not all_data_fields_dict:
        report_content += "\nNo familiar datasets were found, so no field analysis was performed."
    else:
        # 3. Select fields for each dataset and append to the report
        for ds_id, data_fields_df in all_data_fields_dict.items():
            report_content += f"\n### Dataset: {ds_id}\n\n"
            
            field_selection_result = select_data_fields(
                alpha_expression=alpha_expression,
                alpha_description=alpha_description,
                dataset_id=ds_id,
                data_fields_df=data_fields_df,
                filter_requirement=field_filter_req
            )
            
            report_content += f"**Screening Results for `{ds_id}`:**\n"
            report_content += "```\n"
            report_content += field_selection_result
            report_content += "\n```\n---\n"

    # 4. Save the report to a file
    output_dir = 'output'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_filename = os.path.join(output_dir, f"screening_report_{timestamp}.txt")

    try:
        with open(report_filename, 'w', encoding='utf-8') as f:
            f.write(report_content)
        print(f"\nScreening report successfully saved to: {report_filename}")
    except IOError as e:
        print(f"Error saving report file: {e}")


if __name__ == '__main__':
    sess = config_manager.get_session()
    logger = Logger()
    alpha_description = """This template calculates a generic lag-to-field ratio by dividing any data field 
                        lagged by a specified number of trading periods (e.g., 120, 60, 21, or 252 days) by 
                        another contemporaneous data field. The ts_delay ensures a fixed observation lag to 
                        avoid look-ahead bias while allowing sufficient time for information diffusion. Designed 
                        as a flexible exploratory tool, this template is not limited to traditional financial 
                        metrics. It supports testing diverse combinations of data fields—including alternative, 
                        technical, or behavioral datasets—to uncover novel alpha signals based on how historical 
                        data relates to current market or fundamental indicators. By systematically varying datafield1 
                        and datafield2, researchers can identify predictive relationships where prices or metrics may 
                        underreact to lagged information across different dimensions.
                        """
    alpha_expression = """
                    divide(ts_delay(ts_backfill(datafield1, 120), 60), datafield2)
                    """
    if not sess:
        logger.error("无法获取 session，脚本退出。")
    else:
        settings_dict = {
            'region': 'IND',
            'universe': 'TOP500',
            'delay': 1
        }
        dataset_filter_req = """Please select suitable datasets, where the data fields can be used to replace the datafield1 and datafield2 in the alpha_expression."""
        field_filter_req = """Please screen as many datafields as possible that can be used to replace 
                            the datafield1 and datafield2 in the alpha_expression to generate alphas with signals.
                            """
        
        generate_screening_report(
            sess=sess,
            alpha_expression=alpha_expression,
            alpha_description=alpha_description,
            settings_dict=settings_dict,
            dataset_filter_req=dataset_filter_req,
            field_filter_req=field_filter_req
        )