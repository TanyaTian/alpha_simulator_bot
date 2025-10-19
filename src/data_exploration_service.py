# data_exploration_service.py

import json
from dao import SimulatedAlphasDAO, DataExplorationDAO
from utils import extract_datafields
import ast
from logger import Logger

class DataExplorationService:
    """
    Business logic layer for the data exploration feature.
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
                self.logger.debug(f"Skipping alpha {alpha.get('id')} because it has {len(datafields)} datafields.") # Log: Record skip reason
                return None # Skip alphas with more than one datafield

            category = self._extract_category_from_is(alpha['is'])
            if not category:
                self.logger.debug(f"Skipping alpha {alpha.get('id')} because it has no category.") # Log: Record skip reason
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
            self.logger.error(f"Error transforming alpha {alpha.get('id')}: {e}") # Log: Record transformation error
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
        Process simulated alphas data for a specific date.
        """
        self.logger.info(f"Starting to process simulated alphas for datetime: {datetime}") # Log: Start processing
        # 1. 前置检查 (Pre-check)
        if not force_process:
            existing_count = self.data_exploration_dao.count_by_datetime(datetime)
            if existing_count > 0:
                self.logger.info(f"Data for {datetime} has already been processed. Found {existing_count} records. Skipping.") # Log: Record skip
                return {"status": "skipped", "message": f"Data for {datetime} has already been processed. Found {existing_count} records."}

        # 2. 获取源数据 (Fetch source data)
        self.logger.info(f"Fetching source alphas for {datetime}") # Log: Fetch data
        source_alphas = self._load_alpha_data_paginated(datetime)
        if not source_alphas:
            return {"status": "error", "message": f"No source data found for {datetime}."}

        # 3. 数据筛选 (Filter data)
        filtered_alphas = []
        for alpha in source_alphas:
            try:
                settings = ast.literal_eval(alpha['settings'])
                if settings.get('decay') == 0 and settings.get('neutralization') == 'NONE':
                    filtered_alphas.append(alpha)
            except (ValueError, SyntaxError, KeyError):
                continue
        self.logger.info(f"Filtered alphas: {len(filtered_alphas)} records passed the filter.") # Log: Record filtered data count

        # 4. 数据转换 (Transform data)
        transformed_records = [self._transform_alpha_record(a) for a in filtered_alphas]
        transformed_records = [r for r in transformed_records if r is not None]
        self.logger.info(f"Transformed {len(transformed_records)} records.") # Log: Record transformed data count
        
        if not transformed_records:
            return {"status": "success", "processed_count": 0, "message": "No data matching the filter criteria."}

        # 5. 批量写入数据库 (Batch insert into database)
        self.logger.info(f"Writing {len(transformed_records)} records to the data exploration table.")
        try:
            # The underlying db call returns 0 on failure.
            inserted_count = self.data_exploration_dao.batch_insert_or_update(transformed_records)
            
            # When there is data to process, but the returned number of rows is 0, we consider it an error
            if inserted_count == 0 and len(transformed_records) > 0:
                self.logger.error(f"Data exploration batch insert failed for {datetime}. DAO returned 0, expected {len(transformed_records)}.")
                return {"status": "error", "message": "Database batch insert failed, the operation did not affect any rows."}

            # 6. 返回处理结果 (Return result)
            self.logger.info(f"Successfully processed and inserted/updated {inserted_count} records for {datetime}.")
            return {
                "status": "success",
                "processed_count": inserted_count,
                "message": f"Successfully processed and wrote {inserted_count} records."
            }
        except Exception as e:
            # Capture any unexpected errors that may occur above the DAO layer
            self.logger.error(f"An unexpected error occurred during batch insert for {datetime}: {e}", exc_info=True)
            return {"status": "error", "message": f"An unexpected error occurred during batch insert: {str(e)}"}
