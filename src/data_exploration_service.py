# data_exploration_service.py

import json
from dao import SimulatedAlphasDAO, DataExplorationDAO
from utils import extract_datafields
import ast
from logger import Logger

class DataExplorationService:
    """
    数据探索功能的业务逻辑层
    """
    def __init__(self):
        self.logger = Logger()
        self.simulated_alphas_dao = SimulatedAlphasDAO()
        self.data_exploration_dao = DataExplorationDAO()

    def _extract_category_from_is(self, is_str: str) -> str | None:
        try:
            is_data = ast.literal_eval(is_str)
            checks = is_data.get('checks', [])
            pyramid_checks = [check for check in checks if check.get('name') == 'MATCHES_PYRAMID']
            if pyramid_checks and 'pyramids' in pyramid_checks[0]:
                pyramids = pyramid_checks[0]['pyramids']
                if pyramids and isinstance(pyramids[0], dict) and 'name' in pyramids[0]:
                    category_name = pyramids[0]['name']
                    category = category_name.split('/')[-1]
                    return category
        except (ValueError, SyntaxError):
            return None
        return None

    def _transform_alpha_record(self, alpha: dict) -> dict | None:
        try:
            settings = ast.literal_eval(alpha['settings'])
            regular = ast.literal_eval(alpha['regular'])

            datafields = extract_datafields(regular.get('code', ''))
            if len(datafields) != 1:
                self.logger.debug(f"Skipping alpha {alpha.get('id')} because it has {len(datafields)} datafields.") # 日志：记录跳过原因
                return None # Skip alphas with more than one datafield

            category = self._extract_category_from_is(alpha['is'])
            if not category:
                self.logger.debug(f"Skipping alpha {alpha.get('id')} because it has no category.") # 日志：记录跳过原因
                return None # Skip if no category

            return {
                'id': alpha['id'],
                'type': alpha['type'],
                'author': alpha['author'],
                'settings': str(alpha['settings']),
                'regular': str(alpha['regular']),
                'classifications': alpha['classifications'],
                'status': alpha['status'],
                'is': str(alpha['is']),
                'datetime': alpha['datetime'],
                'region': settings.get('region'),
                'universe': settings.get('universe'),
                'delay': settings.get('delay'),
                'datafield': datafields[0],
                'category': category,
            }
        except (ValueError, SyntaxError, KeyError) as e:
            self.logger.error(f"Error transforming alpha {alpha.get('id')}: {e}") # 日志：记录转换错误
            return None

    def _load_alpha_data_paginated(self, datetime: str) -> list:
        total_records = self.simulated_alphas_dao.count_by_datetime(datetime)
        if total_records == 0:
            self.logger.warning(f"No source alphas found for {datetime}")
            return []

        page_size = 1000
        records = []
        page_count = (total_records + page_size - 1) // page_size

        self.logger.info(f"Found {total_records} source alphas. Fetching in {page_count} pages.")

        for page in range(1, page_count + 1):
            offset = (page - 1) * page_size
            self.logger.info(f"Fetching page {page}/{page_count}...")
            page_records = self.simulated_alphas_dao.get_by_datetime_paginated(datetime, limit=page_size, offset=offset)
            records.extend(page_records)

        self.logger.info(f"Loaded {len(records)} records from DB.")
        return records

    def process_simulated_alphas_for_day(self, datetime: str, force_process: bool = False) -> dict:
        """
        处理指定日期的 simulated alphas 数据。
        """
        self.logger.info(f"Starting to process simulated alphas for datetime: {datetime}") # 日志：开始处理
        # 1. 前置检查 (Pre-check)
        if not force_process:
            existing_count = self.data_exploration_dao.count_by_datetime(datetime)
            if existing_count > 0:
                self.logger.info(f"Data for {datetime} has already been processed. Found {existing_count} records. Skipping.") # 日志：记录跳过
                return {"status": "skipped", "message": f"数据已于 {datetime} 处理过，共 {existing_count} 条。"}

        # 2. 获取源数据 (Fetch source data)
        self.logger.info(f"Fetching source alphas for {datetime}") # 日志：获取数据
        source_alphas = self._load_alpha_data_paginated(datetime)
        if not source_alphas:
            return {"status": "error", "message": f"在 {datetime} 未找到源数据。"}

        # 3. 数据筛选 (Filter data)
        filtered_alphas = []
        for alpha in source_alphas:
            try:
                settings = ast.literal_eval(alpha['settings'])
                if settings.get('decay') == 0 and settings.get('neutralization') == 'NONE':
                    filtered_alphas.append(alpha)
            except (ValueError, SyntaxError, KeyError):
                continue
        self.logger.info(f"Filtered alphas: {len(filtered_alphas)} records passed the filter.") # 日志：记录筛选后的数据量

        # 4. 数据转换 (Transform data)
        transformed_records = [self._transform_alpha_record(a) for a in filtered_alphas]
        transformed_records = [r for r in transformed_records if r is not None]
        self.logger.info(f"Transformed {len(transformed_records)} records.") # 日志：记录转换后的数据量
        
        if not transformed_records:
            return {"status": "success", "processed_count": 0, "message": "没有符合筛选条件的数据。"}

        # 5. 批量写入数据库 (Batch insert into database)
        self.logger.info(f"Writing {len(transformed_records)} records to the data exploration table.")
        try:
            # The underlying db call returns 0 on failure.
            inserted_count = self.data_exploration_dao.batch_insert_or_update(transformed_records)
            
            # 当有数据需要处理，但返回的条数为0时，我们认为发生了错误
            if inserted_count == 0 and len(transformed_records) > 0:
                self.logger.error(f"Data exploration batch insert failed for {datetime}. DAO returned 0, expected {len(transformed_records)}.")
                return {"status": "error", "message": "数据库批量写入失败，操作未影响任何行。"}

            # 6. 返回处理结果 (Return result)
            self.logger.info(f"Successfully processed and inserted/updated {inserted_count} records for {datetime}.")
            return {
                "status": "success",
                "processed_count": inserted_count,
                "message": f"成功处理并写入 {inserted_count} 条数据。"
            }
        except Exception as e:
            # 捕获在 DAO 层之上可能发生的任何意外错误
            self.logger.error(f"An unexpected error occurred during batch insert for {datetime}: {e}", exc_info=True)
            return {"status": "error", "message": f"数据库批量写入时发生意外错误: {str(e)}"}
