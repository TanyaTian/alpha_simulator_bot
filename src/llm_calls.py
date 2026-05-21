from openai import OpenAI
from config_manager import config_manager
from logger import Logger

logger = Logger()

def call_llm_free(prompt):
    """
    使用免费 API 密钥调用 LLM。
    配置来源于 config/config.ini 中的 [LLM] 部分。
    """
    try:
        base_url = config_manager.get('llm_free_base_url', 'https://api-inference.modelscope.cn/v1/')
        api_key = config_manager.get('llm_free_api_key', '')
        model = config_manager.get('llm_free_model', 'deepseek-ai/DeepSeek-V3.2-Exp')
        
        if not api_key or api_key == 'your_free_api_key_here':
            logger.warning("免费 LLM API 密钥未配置。")
            return None
            
        # 增加 timeout=60 避免挂起
        client = OpenAI(base_url=base_url, api_key=api_key, timeout=60.0)
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        logger.error(f"调用免费 LLM 出错: {e}")
        return None

def call_llm_paid(prompt):
    """
    使用付费（或高性能）API 密钥调用 LLM。
    通常用于复杂的优化任务。
    """
    try:
        base_url = config_manager.get('llm_paid_base_url', 'https://api.openai.com/v1')
        api_key = config_manager.get('llm_paid_api_key', '')
        model = config_manager.get('llm_paid_model', 'gpt-4')
        
        if not api_key or api_key == 'your_paid_api_key_here':
            logger.warning("付费 LLM API 密钥未配置。")
            return None
            
        # 增加 timeout=60 避免挂起
        client = OpenAI(base_url=base_url, api_key=api_key, timeout=60.0)
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        logger.error(f"调用付费 LLM 出错: {e}")
        return None

def test_llm_connection(config_name: str = "llm_paid"):
    """
    测试 LLM API 连接是否通畅。
    """
    logger.info(f"正在测试 LLM 连接: {config_name}...")
    if "paid" in config_name or "deepseek" in config_name:
        res = call_llm_paid("Hello")
    else:
        res = call_llm_free("Hello")
    
    if res:
        logger.info(f"LLM 连接测试成功 ({config_name})。")
        return True
    return False

def generate_super_alpha_descriptions(session, alpha_id: str) -> tuple[str, str]:
    """
    自动为 Super Alpha 生成 Selection 和 Combo 代码的文本描述。
    """
    try:
        details_url = f"https://api.worldquantbrain.com/alphas/{alpha_id}"
        details = session.get(details_url).json()

        if not details: return None, None

        selection_code = details.get('selection', {}).get('code')
        combo_code = details.get('combo', {}).get('code')

        if not selection_code or not combo_code: return None, None

        # 构造提示词并调用免费模型生成描述
        sel_desc = call_llm_free(f"请用英文详细描述以下筛选逻辑: {selection_code}")
        com_desc = call_llm_free(f"请用英文详细描述以下组合逻辑: {combo_code}")

        return sel_desc, com_desc
    except Exception as e:
        logger.error(f"为 Alpha {alpha_id} 生成描述时出错: {e}")
        return None, None
