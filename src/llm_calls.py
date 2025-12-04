from openai import OpenAI
from config_manager import config_manager

def call_llm_free(prompt):
    """
    Interface with the LLM API to process the given prompt using a free API key.
    """
    try:
        # Get LLM configuration from config manager
        base_url = config_manager.get('llm_free_base_url', 'https://api.deepseek.com')
        api_key = config_manager.get('llm_free_api_key', '')
        model = config_manager.get('llm_free_model', 'deepseek-ai/DeepSeek-V3.2-Exp')
        
        if not api_key or api_key == 'your_free_api_key_here':
            print("Warning: Free LLM API key not configured. Please update config.ini")
            return None
            
        client = OpenAI(
            base_url=base_url,
            api_key=api_key
        )
        
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"Error calling free LLM: {e}")
        return None

def call_llm_paid(prompt):
    """
    Interface with the LLM API to process the given prompt using a paid API key.
    """
    try:
        # Get LLM configuration from config manager
        base_url = config_manager.get('llm_paid_base_url', 'https://api.openai.com/v1')
        api_key = config_manager.get('llm_paid_api_key', '')
        model = config_manager.get('llm_paid_model', 'gpt-4')
        
        if not api_key or api_key == 'your_paid_api_key_here':
            print("Warning: Paid LLM API key not configured. Please update config.ini")
            return None
            
        client = OpenAI(
            base_url=base_url,
            api_key=api_key
        )
        
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"Error calling paid LLM: {e}")
        return None


def generate_super_alpha_descriptions(session, alpha_id: str) -> tuple[str, str]:
    """
    Generates descriptions for a super alpha's selection and combo codes using a free LLM.

    Args:
        session: The authenticated brain session object.
        alpha_id: The ID of the super alpha.

    Returns:
        A tuple containing the generated description for the selection code and the combo code.
        Returns (None, None) if details cannot be fetched or an error occurs.
    """
    try:
        # 1. Fetch Super Alpha details
        details_url = f"https://api.worldquantbrain.com/alphas/{alpha_id}"
        details_response = session.get(details_url)
        details_response.raise_for_status()
        details = details_response.json()

        if not details:
            print(f"Could not retrieve details for alpha {alpha_id}.")
            return None, None

        selection_code = None
        if details.get('selection') and details['selection'].get('code'):
            selection_code = details['selection']['code']

        combo_code = None
        if details.get('combo') and details['combo'].get('code'):
            combo_code = details['combo']['code']

        if not selection_code or not combo_code:
            print(f"Alpha {alpha_id} is missing selection or combo code.")
            return None, None
        print("selection_code is:" + selection_code)
        print("combo_code is:" + combo_code)

        # 3. Generate description for selection code
        selection_prompt = f'''
        Please provide a detailed description for the following 'selection' code of a super alpha.
        The description should explain the logic and purpose of the selection criteria in plain English.
        It must be at least 100 characters long.

        Selection Code:
        ```
        {selection_code}
        ```

        Output only the description.
        '''
        print(f"Generating description for selection code of alpha {alpha_id}...")
        selection_description = call_llm_free(selection_prompt)

        # 4. Generate description for combo code
        combo_prompt = f'''
        Please provide a detailed description for the following 'combo' code of a super alpha.
        The description should explain how the alpha signals are combined or weighted.
        It must be at least 100 characters long.

        Combo Code:
        ```
        {combo_code}
        ```

        Output only the description.
        '''
        print(f"Generating description for combo code of alpha {alpha_id}...")
        combo_description = call_llm_free(combo_prompt)

        return selection_description, combo_description

    except Exception as e:
        print(f"An error occurred while generating super alpha descriptions for {alpha_id}: {e}")
        return None, None

def safe_llm_call(messages, llm_config=None):
    """
    Safely call the LLM with the given messages.
    
    Args:
        messages: List of message dictionaries for the LLM
        llm_config: Optional custom LLM configuration dictionary. If None, uses default from config.
    
    Returns:
        LLM response or None if error
    """
    try:
        from langchain_openai import ChatOpenAI
        
        if llm_config is None:
            # Use default configuration from config manager
            llm_config = {
                'base_url': config_manager.get('llm_base_url', 'https://api.deepseek.com'),
                'api_key': config_manager.get('llm_api_key', ''),
                'model': config_manager.get('llm_model', 'deepseek-chat')
            }
        
        if not llm_config.get('api_key') or llm_config['api_key'] == 'your_api_key_here':
            print("Warning: LLM API key not configured. Please update config.ini")
            return None
            
        chat = ChatOpenAI(
            model_name=llm_config.get('model', 'deepseek-chat'),
            openai_api_base=llm_config['base_url'],
            openai_api_key=llm_config['api_key']
        )
        
        response = chat.invoke(messages)
        return response

    except Exception as e:
        print(f"Error calling LLM: {e}")
        return None

def safe_llm_call_with_tool(messages, tools, llm_config=None):
    """
    Safely call the LLM with messages and tools.
    
    Args:
        messages: List of message dictionaries for the LLM
        tools: List of tools to bind to the LLM
        llm_config: Optional custom LLM configuration dictionary. If None, uses default from config.
    
    Returns:
        LLM response or None if error
    """
    try:
        from langchain_openai import ChatOpenAI
        
        if llm_config is None:
            # Use default configuration from config manager
            llm_config = {
                'base_url': config_manager.get('llm_base_url', 'https://api.deepseek.com'),
                'api_key': config_manager.get('llm_api_key', ''),
                'model': config_manager.get('llm_model', 'deepseek-chat')
            }
        
        if not llm_config.get('api_key') or llm_config['api_key'] == 'your_api_key_here':
            print("Warning: LLM API key not configured. Please update config.ini")
            return None
            
        chat = ChatOpenAI(
            model_name=llm_config.get('model', 'deepseek-chat'),
            openai_api_base=llm_config['base_url'],
            openai_api_key=llm_config['api_key']
        )
        
        llm_with_tools = chat.bind_tools(tools)
        
        response = llm_with_tools.invoke(messages)
        return response

    except Exception as e:
        print(f"Error calling LLM with tools: {e}")
        return None

if __name__ == '__main__':
    # This is an example of how to use the LLM functions.
    # You will need a valid brain session to run this.
    
    # Import necessary for main example
    from ace_lib import start_session

    print("--- Example: Testing LLM configuration ---")
    
    # Test free LLM call
    test_prompt = "Hello, how are you?"
    print(f"Testing free LLM with prompt: {test_prompt}")
    response = call_llm_free(test_prompt)
    if response:
        print(f"Free LLM response: {response}")
    else:
        print("Free LLM call failed or not configured")
    
    # Test paid LLM call
    print(f"\nTesting paid LLM with prompt: {test_prompt}")
    response = call_llm_paid(test_prompt)
    if response:
        print(f"Paid LLM response: {response}")
    else:
        print("Paid LLM call failed or not configured")
    
    # Test safe_llm_call
    print("\nTesting safe_llm_call with langchain")
    messages = [{"role": "user", "content": test_prompt}]
    response = safe_llm_call(messages)
    if response:
        print(f"Langchain LLM response: {response}")
    else:
        print("Langchain LLM call failed or not configured")
