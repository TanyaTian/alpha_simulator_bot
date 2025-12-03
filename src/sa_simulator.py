import time
import json
import random
from typing import List, Dict, Any, Tuple
import pandas as pd
from dao import SuperAlphaQueueDAO
from config_manager import config_manager
from logger import Logger
from ace_lib import simulate_alpha_list, set_alpha_properties, check_prod_corr_test, generate_alpha
from llm_calls import generate_super_alpha_descriptions

class SASimulator:
    def __init__(self):
        """
        Initializes the Super Alpha Simulator.
        """
        self.logger = Logger()
        self.dao = SuperAlphaQueueDAO()
        self.running = True
        
        # 从配置管理器加载初始配置
        self.region = config_manager.get('sa_simulator_region', 'USA')
        self.concurrent_simulations = config_manager.get('sa_simulator_concurrent_simulations', 3)
        self.logger.info(f"SASimulator initialized with region: {self.region}, concurrent_simulations: {self.concurrent_simulations}")
        
        # 注册配置变更回调
        config_manager.on_config_change(self._handle_config_change)

    def _handle_config_change(self, new_config: Dict[str, Any]):
        """当配置被更新时，此方法将被 config_manager 调用。"""
        new_region = new_config.get('sa_simulator_region', self.region)
        new_concurrent = new_config.get('sa_simulator_concurrent_simulations', self.concurrent_simulations)
        
        if self.region != new_region:
            self.region = new_region
            self.logger.info(f"SA Simulator region updated to: {self.region}")
        
        if self.concurrent_simulations != new_concurrent:
            self.concurrent_simulations = new_concurrent
            self.logger.info(f"SA Simulator concurrent_simulations updated to: {self.concurrent_simulations}")

    def process_batch(self, tasks: List[Dict[str, Any]]) -> Tuple[List[int], List[int]]:
        """
        处理一个批次的任务：模拟、筛选、标记。
        基于 super_alpha_simulation_flow.py 中的 batch_simulation_super_alpha 实现真实模拟逻辑。
        """
        self.logger.info(f"Processing batch of {len(tasks)} tasks.")
        completed_ids = []
        failed_ids = []

        # 准备模拟参数 - 使用 generate_alpha 方法生成 SUPER alpha 配置
        alpha_list = []
        task_to_alpha_map = {}  # 映射：任务ID -> alpha配置索引
        
        for task in tasks:
            try:
                # 从 settings_json 中解析模拟配置
                settings = json.loads(task['settings_json'])
                
                # 使用 generate_alpha 方法生成 SUPER alpha 配置
                # 从 settings 中提取参数，如果缺少则使用默认值
                alpha_config = generate_alpha(
                    alpha_type="SUPER",
                    selection=task['selection'],
                    combo=task['combo'],
                    region=settings.get('region', self.region),
                    universe=settings.get('universe', 'TOP3000'),
                    delay=settings.get('delay', 1),
                    decay=settings.get('decay', 0),
                    neutralization=settings.get('neutralization', 'INDUSTRY'),
                    truncation=settings.get('truncation', 0.08),
                    pasteurization=settings.get('pasteurization', 'ON'),
                    test_period=settings.get('testPeriod', 'P0Y0M0D'),
                    unit_handling=settings.get('unitHandling', 'VERIFY'),
                    nan_handling=settings.get('nanHandling', 'OFF'),
                    max_trade=settings.get('maxTrade', 'OFF'),
                    selection_handling=settings.get('selectionHandling', 'POSITIVE'),
                    selection_limit=settings.get('selectionLimit', 100),
                    visualization=settings.get('visualization', False)
                )
                
                if alpha_config:  # 确保 generate_alpha 返回了有效的配置
                    alpha_index = len(alpha_list)
                    alpha_list.append(alpha_config)
                    task_to_alpha_map[task['id']] = alpha_index
                else:
                    self.logger.error(f"Failed to generate alpha config for task {task['id']}")
                    failed_ids.append(task['id'])
                    
            except Exception as e:
                self.logger.error(f"Error generating alpha config for task {task['id']}: {e}")
                failed_ids.append(task['id'])
        
        if not alpha_list:
            self.logger.error("No valid alpha configurations generated for this batch.")
            # 如果所有任务都失败，将剩余的任务标记为失败
            for task in tasks:
                if task['id'] not in failed_ids:
                    failed_ids.append(task['id'])
            return completed_ids, failed_ids

        # 执行真实模拟（带重试机制）
        retries = 0
        max_retries = 5
        batch_succeeded = False
        simulation_results = []
        
        while not batch_succeeded and retries < max_retries:
            try:
                session = config_manager.get_session()
                if not session:
                    self.logger.error("Failed to get valid session for simulation")
                    retries += 1
                    time.sleep(600)  # 等待10分钟后重试
                    continue
                
                self.logger.info(f"Attempting to simulate batch, attempt {retries + 1}/{max_retries}")
                simulation_results = simulate_alpha_list(
                    s=session,
                    alpha_list=alpha_list,
                    limit_of_concurrent_simulations=self.concurrent_simulations,
                    simulation_config={"get_stats": True}
                )
                batch_succeeded = True
                self.logger.info(f"Successfully simulated batch.")
                
            except Exception as e:
                retries += 1
                self.logger.error(f"Error during batch simulation. Retry {retries}/{max_retries} after 600 seconds. Error: {e}")
                time.sleep(600)
                try:
                    self.logger.info("Attempting to re-login and get a new session.")
                    # 尝试重新登录获取新会话
                    config_manager.get_session()
                except Exception as relogin_e:
                    self.logger.error(f"Failed to re-login: {relogin_e}")
        
        if not batch_succeeded:
            self.logger.error(f"Failed to simulate batch after {max_retries} attempts. Marking all tasks as failed.")
            failed_ids.extend([task['id'] for task in tasks])
            return completed_ids, failed_ids
        
        # 处理模拟结果 - 由于返回结果和入参的顺序不一定一致，我们采用批次处理方式
        # 首先检查是否有模拟失败的结果
        batch_failed = False
        for result in simulation_results:
            if not result or 'error' in result:
                batch_failed = True
                break
        
        if batch_failed:
            # 批次中有模拟失败，所有任务都标记为失败
            self.logger.error("Batch simulation failed, marking all tasks as failed.")
            failed_ids.extend([task['id'] for task in tasks])
            return completed_ids, failed_ids
        
        # 批次模拟成功，所有成功生成alpha配置的任务都应该标记为completed
        # 首先，将所有成功生成alpha配置的任务ID添加到completed_ids
        # 这些任务成功完成了模拟，无论alpha质量如何
        for task in tasks:
            if task['id'] in task_to_alpha_map and task['id'] not in completed_ids and task['id'] not in failed_ids:
                completed_ids.append(task['id'])
                self.logger.info(f"Task {task['id']} marked as completed (simulation succeeded)")
        
        # 然后，逐个检查alpha性能，对合格的alpha进行额外处理
        for result_index, result in enumerate(simulation_results):
            alpha_id = result.get("alpha_id")
            is_tests = result.get("is_tests")
            stats = result.get("stats")
            is_stats = result.get("is_stats")
            
            # 检查模拟是否成功 - 与 super_alpha_simulation_flow.py 保持一致
            if is_tests is None or not isinstance(is_tests, pd.DataFrame) or stats is None or not isinstance(stats, pd.DataFrame) or is_stats is None or not isinstance(is_stats, pd.DataFrame):
                self.logger.error(f"Could not retrieve complete test results for alpha {alpha_id}")
                # 这个alpha没有完整的测试结果，但任务已经标记为completed
                continue
            
            # 筛选条件 (基于 super_alpha_simulation_flow.py 的逻辑)
            # 检查 is_stats 是否为 DataFrame 并且包含必要的列
            if is_stats is not None and isinstance(is_stats, pd.DataFrame) and 'sharpe' in is_stats.columns and 'fitness' in is_stats.columns:
                sharpe_value = is_stats['sharpe'].iloc[0] if not is_stats.empty else 0
                fitness_value = is_stats['fitness'].iloc[0] if not is_stats.empty else 0
                
                # 合格条件: Sharpe和Fitness都大于4
                if sharpe_value > 4 and fitness_value > 4:
                    self.logger.info(f"Alpha {alpha_id} qualified with Sharpe: {sharpe_value}, Fitness: {fitness_value}")
                    
                    # 检查 is_tests 中是否包含 FAIL
                    if 'result' in is_tests.columns and (is_tests['result'] == 'FAIL').any():
                        self.logger.info(f"Alpha {alpha_id}: is_tests 中包含 FAIL，不合格。")
                        continue

                    # 检查 sharpe_non_zero_years
                    sharpe_non_zero_years = (stats['sharpe'] != 0.0).sum()
                    if sharpe_non_zero_years < 8:
                        self.logger.info(f"Alpha {alpha_id}: sharpe 值不为 0.0 的年份少于 8 年 ({sharpe_non_zero_years} 年)，不合格。")
                        continue
                    
                    self.logger.info(f"Alpha {alpha_id} passed initial checks (is_tests and sharpe_non_zero_years). Checking prod correlation.")

                    # 检查相关性
                    try:
                        prod_corr_result = check_prod_corr_test(session, alpha_id=alpha_id)
                        pc = prod_corr_result['value'].max() if not prod_corr_result.empty else 1.0
                        
                        if pc < 0.7:
                            self.logger.info(f"Alpha {alpha_id} passed prod correlation check with value {pc}. Qualified!")
                            
                            # 生成alpha描述
                            selection_description = None
                            combo_description = None
                            try:
                                selection_description, combo_description = generate_super_alpha_descriptions(session, alpha_id)
                                if selection_description and combo_description:
                                    self.logger.info(f"Generated descriptions for alpha {alpha_id}")
                                else:
                                    self.logger.warning(f"Failed to generate descriptions for alpha {alpha_id}, using defaults")
                                    selection_description = "None"
                                    combo_description = "None"
                            except Exception as e:
                                self.logger.error(f"Error generating descriptions for alpha {alpha_id}: {e}")
                                selection_description = "None"
                                combo_description = "None"
                            
                            # 标记合格的Alpha，设置name为SA的pc值
                            name = f"SA_{pc:.4f}" if pc is not None else f"SA_unknown"
                            set_alpha_properties(
                                session, 
                                alpha_id, 
                                tags=['qualified_super_alpha'], 
                                name=name,
                                selection_desc=selection_description,
                                combo_desc=combo_description)
                            
                            # 记录这个alpha是合格的（可选，可以记录到日志或数据库）
                            self.logger.info(f"Alpha {alpha_id} is qualified and has been tagged")
                        else:
                            self.logger.info(f"Alpha {alpha_id} failed prod correlation check with value {pc}.")
                    except Exception as e:
                        self.logger.error(f"Error checking prod correlation for alpha {alpha_id}: {e}")
                else:
                    self.logger.info(f"Alpha {alpha_id} did not meet qualification criteria (Sharpe: {sharpe_value}, Fitness: {fitness_value})")
            else:
                self.logger.info(f"Alpha {alpha_id} did not have complete stats")
        
        # 此时，所有成功生成alpha配置的任务都已经标记为completed
        # 只有那些在alpha配置生成阶段就失败的任务才会在failed_ids中
        self.logger.info("Batch processing finished.")
        return completed_ids, failed_ids

    def run_simulation_loop(self, batch_size: int = 10, sleep_interval: int = 60):
        """
        模拟器服务的无限循环主函数。
        
        :param batch_size: 每次从数据库获取的任务数量。
        :param sleep_interval: 当队列为空时，休眠的秒数。
        """
        self.logger.info("SA Simulator loop started.")
        while self.running:
            try:
                # 1. 在每次循环开始时，使用当前内存中的 region 配置
                current_region = self.region
                self.logger.info(f"Starting new loop iteration, using region: {current_region}")
                
                # 2. 获取任务
                pending_tasks = self.dao.fetch_pending_tasks(limit=batch_size, region=current_region)
                
                if not pending_tasks:
                    self.logger.info(f"Task queue is empty for region {current_region}. Sleeping for {sleep_interval} seconds...")
                    time.sleep(sleep_interval)
                    continue
                    
                # 3. 锁定任务
                self.logger.info(f"Fetched {len(pending_tasks)} tasks. Locking them as 'in_progress'.")
                task_ids = [task['id'] for task in pending_tasks]
                self.dao.update_task_status_bulk(task_ids, 'in_progress')
                
                # 4. 处理任务 (此过程耗时较长)
                completed_ids, failed_ids = self.process_batch(pending_tasks)
                
                # 5. 更新最终状态
                if completed_ids:
                    self.logger.info(f"Marking {len(completed_ids)} tasks as 'completed'.")
                    self.dao.update_task_status_bulk(completed_ids, 'completed')
                if failed_ids:
                    self.logger.info(f"Marking {len(failed_ids)} tasks as 'failed'.")
                    self.dao.update_task_status_bulk(failed_ids, 'failed')
                    
                self.logger.info(f"Batch completed: {len(completed_ids)} success, {len(failed_ids)} failed.")
                # 在此批次结束后，下一次循环将自动使用最新的 self.region
                time.sleep(5) 
            except Exception as e:
                self.logger.error(f"An error occurred in the simulation loop: {e}", exc_info=True)
                time.sleep(sleep_interval) # Wait before retrying
                
    def stop(self):
        self.running = False
        self.dao.close()
        self.logger.info("SA Simulator has been stopped.")

def main():
    """
    SA Simulator 模块的独立运行入口。
    可以直接运行此模块来启动SA模拟器服务。
    """
    import signal
    import sys
    from logger import Logger
    
    logger = Logger()
    logger.info("SA Simulator 独立服务启动...")
    
    # 创建SASimulator实例
    simulator = SASimulator()
    
    # 设置信号处理
    def signal_handler(signum, frame):
        logger.info(f"收到信号 {signum}，正在停止SA模拟器...")
        simulator.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # 启动模拟器循环
        logger.info("SA Simulator 服务启动，开始处理任务...")
        simulator.run_simulation_loop(batch_size=2, sleep_interval=60)
    except KeyboardInterrupt:
        logger.info("用户中断，正在停止SA模拟器...")
        simulator.stop()
    except Exception as e:
        logger.error(f"SA Simulator 运行出错: {e}", exc_info=True)
        simulator.stop()
        sys.exit(1)

# 可以在需要时保留简单的测试入口
if __name__ == '__main__':
    main()
