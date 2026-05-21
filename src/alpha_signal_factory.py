from dao.stage_one_signal_dao import StageOneSignalDAO
from dao.alpha_list_pending_simulated_dao import AlphaListPendingSimulatedDAO
from ace_lib import start_session, check_session_and_relogin, get_simulation_result_json
from datetime import datetime
from logger import Logger
import random
import ast

class AlphaSignalFactory:
    def __init__(self):
        self.logger = Logger()
        self.stage_one_dao = StageOneSignalDAO()
        self.pending_dao = AlphaListPendingSimulatedDAO()
        self.session = start_session()
        self.group_ops = ['group_rank', 'group_zscore', 'group_neutralize']
        self.ts_ops = ["ts_rank", "ts_zscore",  "ts_ir", "ts_delta",
            "ts_arg_min", "ts_arg_max", "ts_scale", "ts_quantile",
            "ts_kurtosis", "ts_product", "ts_returns"]
        
    def process_signals(self, date_time, priority=5, mode='normal', operator_type='all', margin_limit=0.0, sharpe_limit=0.0, fitness_limit=0.0):
        """
        处理信号，优化并存储为待模拟的alpha
        
        Args:
            date_time: 日期时间
            priority: 任务优先级，默认为5
            mode: 处理模式，'test'为测试模式，'normal'为正常模式
            operator_type: 算子类型，'ts'为时间序列，'group'为分组，'all'为两者都添加
            margin_limit: margin限制，过滤掉In-Sample margin小于该值的alpha
            sharpe_limit: sharpe限制，过滤掉In-Sample sharpe小于该值的alpha
            fitness_limit: fitness限制，过滤掉In-Sample fitness小于该值的alpha
        """
        self.session = check_session_and_relogin(self.session)
        signal_optimize_set = {}
        settings_dict = {}
        
        # 从 StageOneSignalDAO 获取原始 alpha 信号
        raw_signals_db = self.stage_one_dao.get_all_by_date_time(date_time)
        
        if not raw_signals_db:
            self.logger.info(f"No signals found for date_time: {date_time}")
            return

        self.logger.info(f"Found {len(raw_signals_db)} signals in database for {date_time}")
        
        # 获取详细信息并按 region 分组
        region_signals = {}
        
        for sig in raw_signals_db:
            alpha_id = sig['alpha_id']
            details = get_simulation_result_json(self.session, alpha_id)
            if not details:
                self.logger.warning(f"Failed to get details for alpha {alpha_id}")
                continue
            
            # 提取代码
            regular_data = details.get('regular', {})
            code = regular_data.get('code')
            if not code:
                continue
            
            # 统计数据过滤
            is_stats = details.get('is', {})
            margin = is_stats.get('margin', 0)
            sharpe = is_stats.get('sharpe', 0)
            fitness = is_stats.get('fitness', 0)

            if margin < margin_limit:
                self.logger.info(f"Alpha {alpha_id} filtered: margin {margin} < {margin_limit}")
                continue
            
            if sharpe < sharpe_limit:
                self.logger.info(f"Alpha {alpha_id} filtered: sharpe {sharpe} < {sharpe_limit}")
                continue

            if fitness < fitness_limit:
                self.logger.info(f"Alpha {alpha_id} filtered: fitness {fitness} < {fitness_limit}")
                continue
            
            settings = details.get('settings', {})
            region = settings.get('region', 'USA')
            decay = settings.get('decay', 0)
            
            if region not in region_signals:
                region_signals[region] = []
            
            # 准备 _optimize_expressions 需要的格式 [alpha_id, code, decay]
            region_signals[region].append((alpha_id, code, decay))
            self.logger.info(f"Alpha {alpha_id} accepted for region {region} optimization")
            
            # 保存设置用于 prepare_simulation_data
            settings_dict[alpha_id] = settings

        total_valid_signals = sum(len(sigs) for sigs in region_signals.values())
        self.logger.info(f"Total valid signals for optimization after filtering: {total_valid_signals}")

        if not region_signals:
            self.logger.info("No valid signals after fetching details and filtering")
            return

        for region, alpha_signals in region_signals.items():
            self.logger.info(f"Region {region}: Processing {len(alpha_signals)} alpha signals")
                
            # 按表达式是否包含换行符分组
            multi_line_signals = [rec for rec in alpha_signals if '\n' in rec[1]]
            single_line_signals = [rec for rec in alpha_signals if '\n' not in rec[1]]
            
            # 优化处理
            optimized_multi = self._optimize_expressions(multi_line_signals, region, multi_line=True, operator_type=operator_type)
            self.logger.info(f"Region {region}: Multi-line optimized signals: {len(optimized_multi)}")
            
            optimized_single = self._optimize_expressions(single_line_signals, region, multi_line=False, operator_type=operator_type)
            self.logger.info(f"Region {region}: Single-line optimized signals: {len(optimized_single)}")
            
            # 合并优化结果
            signal_optimize_set[region] = optimized_multi + optimized_single
            self.logger.info(f"Region {region}: Total optimized signals: {len(signal_optimize_set[region])} (from {len(alpha_signals)} originals)")
            
        # 根据模式进行采样（在优化后的表达式上采样）
        sampled_optimize_set = {}
        for region, signals in signal_optimize_set.items():
            if not signals:
                continue
                
            if mode == 'test':
                # 测试模式：从优化后的表达式中随机取10个
                sampled_signals = random.sample(signals, min(10, len(signals)))
                sampled_optimize_set[region] = sampled_signals
            elif mode == 'normal':
                # 正常模式：如果优化后的表达式超过30000个，则随机取30%
                if len(signals) > 30000:
                    sampled_signals = random.sample(signals, int(len(signals)*0.3))
                    sampled_optimize_set[region] = sampled_signals
                else:
                    sampled_optimize_set[region] = signals
            else:
                # 默认模式：保留所有优化后的表达式
                sampled_optimize_set[region] = signals
        
        # 准备模拟数据（按region分组）
        simulation_records_by_region = self.prepare_simulation_data(
            sampled_optimize_set, 
            settings_dict,
            priority=priority
        )
        
        # 合并所有记录用于插入
        all_records = []
        for region, region_records in simulation_records_by_region.items():
            self.logger.info(f"Region {region}: Collected {len(region_records)} simulation records")
            all_records.extend(region_records)
        
        # 将合并后的所有记录进行全局随机打乱，确保不同地区、不同类型的 alpha 混合在一起
        if all_records:
            self.logger.info(f"Performing global shuffle on {len(all_records)} total records...")
            # 使用 random.sample 全量采样是生成全新打乱列表的最稳妥方式
            all_records = random.sample(all_records, len(all_records))
            # 再次使用 shuffle 确保极致打乱
            random.shuffle(all_records)
            
            inserted_count = self.pending_dao.batch_insert(all_records)
            self.logger.info(f"Successfully inserted {inserted_count} shuffled records into database")
        else:
            self.logger.info("No optimized signals to insert")
            
        return simulation_records_by_region

    def _optimize_expressions(self, signals, region, multi_line=False, operator_type='all'):
        """
        优化表达式集合
        
        Args:
            signals: 信号列表 [alpha_id, exp, decay]
            region: 地区代码
            multi_line: 是否为多行表达式
            operator_type: 算子类型
            
        Returns:
            优化后的信号列表
        """
        optimized_signals = []
        for rec in signals:
            alpha_id = rec[0]
            exp = rec[1]
            decay = rec[-1]  # 获取decay值
            
            # 时间序列优化
            if operator_type in ['ts', 'all']:
                new_exps = self.first_order_factory_with_day(
                    [exp], 
                    self.ts_ops, 
                    days=[20, 63, 120, 252], 
                    multi_line=multi_line
                )
                for new_exp in new_exps:
                    # 创建新记录：更新表达式, 添加decay
                    optimized_signals.append((alpha_id, new_exp , decay))
            
            # 分组优化
            if operator_type in ['group', 'all']:
                new_exps = self.get_group_second_order_factory(
                    [exp], 
                    self.group_ops, 
                    region, 
                    multi_line=multi_line
                )
                for new_exp in new_exps:
                    optimized_signals.append((alpha_id, new_exp, decay))
                    
        return optimized_signals

    def _count_operations(self, expression, operations):
        """
        计算表达式中指定操作符的出现次数
        
        Args:
            expression: 要分析的表达式
            operations: 操作符列表
            
        Returns:
            操作符出现次数
        """
        count = 0
        for op in operations:
            # 统计每个运算符出现的实际次数，避免部分匹配
            count += expression.count(op + '(')  # 运算符后面通常跟着括号
        return count

    def prepare_simulation_data(self, signal_optimize_set, settings_dict, priority):
        """
        根据优化信号和设置字典准备模拟数据
        
        Args:
            signal_optimize_set: 按地区组织的优化信号字典 {region: [optimized_signals]}
            settings_dict: 信号设置字典 {signal_id: settings}
            priority: 任务优先级
            
        Returns:
            按地区组织的数据库记录字典 {region: [db_records]}
        """

        db_records_by_region = {}
        
        for region, signals in signal_optimize_set.items():
            region_records = []
            for signal in signals:
                alpha_id, new_exp, decay = signal
                
                # 获取该信号的设置
                signal_settings = settings_dict.get(alpha_id, {})
                
                # 创建数据库记录元组
                simulation_data = {
                    'type': 'REGULAR',
                    'settings': {
                        'instrumentType': 'EQUITY',
                        'region': region,
                        'universe': signal_settings.get('universe', 'TOP3000'),
                        'delay': signal_settings.get('delay', 1),  # 使用提取的delay值
                        'decay': decay,
                        'neutralization': signal_settings.get('neutralization', 'NONE'),
                        'truncation': signal_settings.get('truncation', 0.01),
                        'pasteurization': 'ON',
                        'testPeriod': 'P0Y',
                        'unitHandling': 'VERIFY',
                        'nanHandling': 'OFF',
                        'language': 'FASTEXPR',
                        'visualization': signal_settings.get('visualization', False),
                        'maxTrade': signal_settings.get('maxTrade', 'OFF')
                    },
                    'regular': new_exp
                }
                
                # 创建数据库记录
                region_records.append({
                    'type': simulation_data['type'],
                    'settings': str(simulation_data['settings']),
                    'regular': simulation_data['regular'],
                    'priority': priority,
                    'region': region,
                    'created_at': datetime.now()
                })
            
            # 按region保存记录
            db_records_by_region[region] = region_records
        
        return db_records_by_region

    
    def get_group_second_order_factory(self, first_order, group_ops, region, multi_line=False):
        """
        生成二级信号工厂，支持单行和多行模式
        
        Args:
            first_order: 一级信号列表
            group_ops: 分组操作符列表
            region: 地区代码
            multi_line: 是否为多行表达式模式
            
        Returns:
            生成的二级信号列表
        """
        second_order = []
        for fo in first_order:
            for group_op in group_ops:
                second_order += self.group_factory(group_op, fo, region, multi_line)
        return second_order

    def group_factory(self, op, field, region, multi_line=False):
        """
        根据操作符、字段和地区生成alpha表达式列表，支持单行和多行模式
        
        Args:
            op (str): 操作符，例如 'group_rank'
            field (str): 字段或表达式
            region (str): 地区代码
            multi_line (bool): 是否为多行表达式模式
            
        Returns:
            list: 包含生成的alpha表达式的列表
        """
        output = []
        vectors = ["cap"]
        
        # 分组定义（统一管理避免重复）
        region_groups = {
            "CHN": ['pv13_h_min2_sector', 'pv13_di_6l', 'pv13_rcsed_6l', 'pv13_di_5l', 
                   'pv13_di_4l', 'pv13_di_3l', 'pv13_di_2l', 'pv13_di_1', 'pv13_parent', 'pv13_level',
                   'sta1_top3000c30', 'sta1_top3000c20', 'sta1_top3000c10', 'sta1_top3000c2', 'sta1_top3000c5',
                   'sta2_top3000_fact4_c10', 'sta2_top2000_fact4_c50', 'sta2_top3000_fact3_c20'],
                   
            "HKG": ['pv13_10_f3_g2_minvol_1m_sector', 'pv13_10_minvol_1m_sector', 'pv13_20_minvol_1m_sector', 
                   'pv13_2_minvol_1m_sector', 'pv13_1l_scibr', 'pv13_3l_scibr',
                   'pv13_2l_scibr', 'pv13_4l_scibr', 'pv13_5l_scibr',
                   'sta1_allc50', 'sta1_allc5', 'sta1_allxjp_513_c20', 'sta1_top2000xjp_513_c5',
                   'sta2_all_xjp_513_all_fact4_c10', 'sta2_top2000_xjp_513_top2000_fact3_c10',
                   'sta2_allfactor_xjp_513_13', 'sta2_top2000_xjp_513_top2000_fact3_c20'],
                   
            "TWN": ['pv13_2_minvol_1m_sector', 'pv13_20_minvol_1m_sector', 'pv13_10_minvol_1m_sector',
                   'pv13_5_minvol_1m_sector', 'pv13_10_f3_g2_minvol_1m_sector', 'pv13_5_f3极2_minvol_1m_sector',
                   'pv13_2_f4_g3_minvol_1m_sector',
                   'sta1_allc50', 'sta1_allxjp_513_c50', 'sta1_allxjp_513_c20', 'sta1_allxjp_513_c2',
                   'sta1_allc20', 'sta1_allxjp_513_c5', 'sta1_allxjp_513_c10', 'sta1_allc2', 'sta1_allc5',
                   'sta2_allfactor_xjp_513_0', 'sta2_all_xjp_513_all_fact3_c20',
                   'sta2_all_xjp_513_all_fact4_c20', 'sta2_all_xjp_513_all_fact4_c50'],
                   
            "USA": ['pv13_h_min2_3000_sector', 'pv13_r2_min20_3000_sector', 'pv13_r2_min2_3000_sector',
                   'pv13_r2_min2_3000_sector', 'pv13_h_min2_focused_pureplay_3000_sector',
                   'sta1_top3000c50', 'sta1_allc20', 'sta1_allc10', 'sta1_top3000c20', 'sta1_allc5',
                   'sta2_top3000_fact3_c50', 'sta2_top3000_fact4_c20', 'sta2_top3000_fact4_c10',
                   'mdl10_group_name'],
                   
            "ASI": ['pv13_20_minvol_1m_sector', 'pv13_5_f3_g2_minvol_1m_sector', 'pv13_10_f3_g2_minvol_1m_sector',
                   'pv13_2_f4_g3_minvol_1m_sector', 'pv13_10_minvol_1m_sector', 'pv13_5_minvol_1m_sector',
                   'sta1_allc50', 'sta1_allc10', 'sta1_minvol1mc50', 'sta1_minvol1mc20',
                   'sta1_minvol1m_normc20', 'sta1_minvol1m_normc50'],
                   
            "JPN": ['sta1_alljpn_513_c5', 'sta1_alljpn_513_c50', 'sta1_alljpn_513_c2', 'sta1_alljpn_513_c20',
                   'sta2_top2000_jpn_513_top2000_fact3_c20', 'sta2_all_jpn_513_all_fact1_c5',
                   'sta2_allfactor_jpn_513_9', 'sta2_all_jpn_513_all_fact1_c10',
                   'pv13_2_minvol_1m_sector', 'pv13_2_f4_g3_minvol_1m_sector', 'pv13_10_minvol_1m_sector',
                   'pv13_10_f3_g2_minvol_1m_sector', 'pv13_all_delay_1_parent', 'pv13_all_delay_1_level'],
                   
            "KOR": ['pv13_10_f3_g2_minvol_1m_sector', 'pv13_5_minvol_1m_sector', 'pv13_5_f3_g2_minvol_1m_sector',
                   'pv13_2_minvol_1m_sector', 'pv13_20_minvol_1m_sector', 'pv13_2_f4_g3_minvol_1m_sector',
                   'sta1_allc20', 'sta1_allc50', 'sta1_allc2', 'sta1_allc10', 'sta1_minvol1mc50',
                   'sta1_allxjp_513_c10', 'sta1_top2000xjp_513_c50',
                   'sta2_all_xjp_513_all_fact1_c50', 'sta2_top2000_xjp_513_top2000_fact2_c50',
                   'sta2_all_xjp_513_all_fact4_c50', 'sta2_all_xjp_513_all_fact4_c5'],
                   
            "EUR": ['pv13_5_sector', 'pv13_2_sector', 'pv13_v3_3l_scibr', 'pv13_v3_2l_scibr', 'pv13_2l_scibr',
                   'pv13_52_sector', 'pv13_v3_6l_scibr', 'pv13_v3_4l_scibr', 'pv13_v3_1l_scibr',
                   'sta1_allc10', 'sta1_allc2', 'sta1_top1200c2', 'sta1_allc20', 'sta1_top1200c10',
                   'sta2_top1200_fact3_c50', 'sta2_top1200_fact3_c20', 'sta2_top1200_fact4_c50'],
                   
            "GLB": ['sta1_allc20', 'sta1_allc10', 'sta1_allc50', 'sta1_allc5',
                   'sta3_pvgroup2_sector', 'sta3_pvgroup3_sector',
                   'pv13_2_sector', 'pv13_10_sector', 'pv13_3l_scibr', 'pv13_2l_scibr', 'pv13_1l_scibr',
                   'pv13_52_minvol_1m_all_delay_1_sector', 'pv13_52_minvol_1m_sector'],
                   
            "AMR": ['pv13_4l_scibr', 'pv13_1l_scibr', 'pv13_hierarchy_min51_f1_sector',
                   'pv13_hierarchy_min2_600_sector', 'pv13_r2_min2_sector', 'pv13_h_min20_600_sector']
        }
        
        # 基础分组定义
        cap_group = "bucket(rank(cap), range='0.1, 1, 0.1')"
        asset_group = "bucket(rank(assets),range='0.1, 1, 0.1')"
        sector_cap_group = "bucket(group_rank(cap, sector),range='0.1, 1, 0.1')"
        sector_asset_group = "bucket(group_rank(assets, sector),range='0.1, 1, 0.1')"
        vol_group = "bucket(rank(ts_std_dev(returns,20)),range = '0.1, 1, 0.1')"
        liquidity_group = "bucket(rank(close*volume),range = '0.1, 1, 0.1')"
        country_group = ["country"]
        # 新增 ASI 验证有效的分组
        nlvolcap_group = "bucket(rank(mdl177_nlvolcap), range='0.1, 1, 0.1')"
        mktcappera_group = "bucket(rank(mdl77_mktcappera), range='0.1, 1, 0.1')"
        share_count_group = "bucket(rank(aggregate_share_count_all_owners), range='0.1, 1, 0.1')"

        """
        # 组合分组定义
        combo_group = ["group_cartesian_product(country, market)", 
                     "group_cartesian_product(country, industry)", 
                     "group_cartesian_product(country, subindustry)", 
                     "group_cartesian_product(country, exchange)",
                     "group_cartesian_product(country, sector)"]
        

        
        # 根据地区创建不同的分组
        if region == "GLB" or region == "USA" or region == "EUR":
            # GLB地区没有assets字段，移除相关分组
            # USA/EUR使用asset太多，这季度不能使用了
            groups = ["market", "sector", "industry", "subindustry",
                     cap_group, sector_cap_group, vol_group, liquidity_group]
        else:
            groups = ["market", "sector", "industry", "subindustry",
                     cap_group, asset_group, sector_cap_group, sector_asset_group, 
                     vol_group, liquidity_group]
        
        # 添加地区特定分组
        if region in region_groups:
            groups += region_groups[region]
        
        # 为ASI、EUR、GLB地区添加combo_group和country_group
        if region in ["ASI", "EUR", "GLB"]:
            groups += combo_group + country_group
        """
        # 定义各地区的group集合
        usa_atom_group = ["market", "sector", "industry", "subindustry", "exchange"]
        
        asi_atom_group = ["market", "sector", "industry", "subindustry", "exchange", "country",
                        nlvolcap_group, mktcappera_group, share_count_group,
                        cap_group, sector_cap_group]
        
        eur_atom_group = ["market", "sector", "industry", "subindustry", "exchange", "country",
                        "group_cartesian_product(country, market)", 
                        "group_cartesian_product(country, industry)", 
                        "group_cartesian_product(country, subindustry)", 
                        "group_cartesian_product(country, exchange)",
                        "group_cartesian_product(country, sector)",
                        cap_group, sector_cap_group]
        
        glb_atom_group = eur_atom_group.copy()
        
        chn_atom_group = ["market", "sector", "industry", "subindustry", "exchange"]
        ind_atom_group = ["market", "sector", "industry", "subindustry", "exchange"]
        hkg_atom_group = ["market", "sector", "industry", "subindustry", "exchange"]
        
        # 根据region选择对应的group集合（直接匹配大写）
        if region == 'USA':
            groups = usa_atom_group
        elif region == 'ASI':
            groups = asi_atom_group
        elif region == 'EUR':
            groups = eur_atom_group
        elif region == 'GLB':
            groups = glb_atom_group
        elif region == 'CHN':
            groups = chn_atom_group
        elif region == 'IND':
            groups = ind_atom_group
        elif region == 'HKG':
            groups = hkg_atom_group
        else:
            raise ValueError(f"无效的region: {region}，必须是'USA', 'ASI', 'EUR', 'GLB'或'CHN'（大写）, 'IND'")
        
        # 多行处理模式
        if multi_line:
            field_lines = field.strip().split('\n')
            field_result = field_lines[-1].strip()
            field_body = '\n'.join(field_lines[:-1])
        else:
            field_result = field

        # 生成表达式
        for group in groups:
            if op.startswith("group_vector"):
                for vector in vectors:
                    if multi_line:
                        alpha_op = f"{op}({field_result},{vector},densify({group}))"
                        alpha = f"{field_body}\n            {alpha_op}"
                    else:
                        alpha = f"{op}({field_result},{vector},densify({group}))"
                    output.append(alpha)
            elif op.startswith("group_percentage"):
                if multi_line:
                    alpha_op = f"{op}({field_result},densify({group}),percentage=0.5)"
                    alpha = f"{field_body}\n            {alpha_op}"
                else:
                    alpha = f"{op}({field_result},densify({group}),percentage=0.5)"
                output.append(alpha)
            else:
                if multi_line:
                    alpha_op = f"{op}({field_result},densify({group}))"
                    alpha = f"{field_body}\n            {alpha_op}"
                else:
                    alpha = f"{op}({field_result},densify({group}))"
                output.append(alpha)
            
        return output

    def first_order_factory_with_day(self, fields, ops_set, days=None, multi_line=False):
        """
        根据操作符、字段和天数生成带日期的一级alpha表达式，支持单行和多行模式
        
        Args:
            fields (list): 字段列表
            ops_set (list): 操作符列表
            days (list): 天数列表
            multi_line (bool): 是否为多行表达式模式
            
        Returns:
            list: 包含生成的alpha表达式的列表
        """
        alpha_set = []
        for field in fields:
            for op in ops_set:
                alpha_set += self.ts_factory_with_day(op, field, days, multi_line)
        return alpha_set

    def ts_factory_with_day(self, op, field, days=None, multi_line=False):
        """
        生成带日期的时间序列alpha表达式，支持单行和多行模式
        
        Args:
            op (str): 操作符
            field (str): 字段或表达式
            days (list): 天数列表
            multi_line (bool): 是否为多行表达式模式
            
        Returns:
            list: 包含生成的alpha表达式的列表
        """
        output = []
        if days is None:
            days = [5, 20, 63, 120, 252]
        
        # 多行处理模式
        if multi_line:
            field_lines = field.strip().split('\n')
            field_result = field_lines[-1].strip()
            field_body = '\n'.join(field_lines[:-1])
        else:
            field_result = field

        for day in days:
            if multi_line:
                alpha_op = f"{op}({field_result}, {day})"
                alpha = f"{field_body}\n            {alpha_op}"
            else:
                alpha = f"{op}({field_result}, {day})"
            output.append(alpha)
        
        return output
        

def main():
    """
    主函数,用于从命令行调用生成优化alpha并插入数据库
    """
    dates = ['20260512']  # 示例日期列表
    # 设置日志
    logger = Logger()
    logger.info("Starting Alpha Signal Factory")
    
    for date in dates:
        try:
            # 创建工厂对象并运行
            factory = AlphaSignalFactory()
            # 示例：仅添加 TS 算子，且 margin >= 0.05
            factory.process_signals(date_time=date, priority=1, mode='normal', operator_type='group', margin_limit=0.0015, sharpe_limit=1.45, fitness_limit=0.9)
            logger.info("✅ Alpha signal processing completed successfully")
        except Exception as e:
            logger.error(f"❌ Alpha signal processing failed: {str(e)}")
            raise

# 添加命令行执行入口
if __name__ == "__main__":
    main()
#python src/alpha_signal_factory.py
#已经优化了20250824和20250814
