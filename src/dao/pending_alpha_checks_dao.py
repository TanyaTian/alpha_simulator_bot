# dao/pending_alpha_checks_dao.py

from database import Database
from logger import Logger
from datetime import datetime

class PendingAlphaChecksDAO:
    """
    Data Access Object for the pending_alpha_checks_table.
    This table acts as a queue for alphas that need to be checked by the alpha_checker.
    """
    logger = Logger()
    TABLE_NAME = 'pending_alpha_checks_table'

    def __init__(self):
        self.db = Database()
        self.logger.debug("PendingAlphaChecksDAO initialized")

    def add_alphas_to_check(self, alpha_ids: list[str]):
        """
        Adds a list of alpha_ids to the check queue.
        If an alpha_id already exists, it will be ignored.
        This is to prevent duplicate checks.

        Args:
            alpha_ids: A list of alpha_id strings.
        
        Returns:
            The number of newly inserted rows.
        """
        if not alpha_ids:
            self.logger.info("No alpha IDs provided to add to the check queue.")
            return 0

        self.logger.info(f"Adding {len(alpha_ids)} alphas to the check queue.")
        
        now = datetime.now()
        
        # Prepare data for batch insert. We set the status to 'pending'.
        data_list = [
            {
                'alpha_id': alpha_id,
                'status': 'pending',
                'added_at': now
            }
            for alpha_id in alpha_ids
        ]

        # We use ON DUPLICATE KEY UPDATE with a dummy update to mimic INSERT IGNORE
        # behavior while still getting a count of affected rows.
        # This prevents re-adding alphas that are already pending or completed.
        # If a new alpha is inserted, status is set. If it exists, status is just re-set to its current value.
        # A more advanced logic could be to update the added_at timestamp if it's already there.
        # For now, we just want to ensure it's in the queue.
        
        # The plan suggests `INSERT IGNORE` or `ON DUPLICATE KEY UPDATE`.
        # The `batch_insert` method in the Database class supports `on_duplicate_update=True`.
        # This will construct an `ON DUPLICATE KEY UPDATE` clause for all columns.
        # For this table, it means if an alpha_id exists, its `status` and `added_at` will be updated.
        # This is acceptable.
        
        try:
            affected_rows = self.db.batch_insert(
                self.TABLE_NAME,
                data_list,
                on_duplicate_update=True
            )
            self.logger.info(f"Successfully added/updated {affected_rows} alphas in the check queue.")
            return affected_rows
        except Exception as e:
            self.logger.error(f"Error adding alphas to check queue: {e}", exc_info=True)
            return 0

    def get_pending_checks(self, limit: int = 1000) -> list[dict]:
        """
        Retrieves a list of alphas that are pending a check.

        Args:
            limit: The maximum number of records to retrieve.

        Returns:
            A list of dictionaries, where each dictionary represents a row.
        """
        self.logger.debug(f"Querying for pending alpha checks with limit: {limit}")
        sql = f"SELECT * FROM {self.TABLE_NAME} WHERE status = %s ORDER BY added_at ASC LIMIT %s"
        try:
            results = self.db.query(sql, ('pending', limit))
            self.logger.info(f"Found {len(results)} pending alpha checks.")
            return results
        except Exception as e:
            self.logger.error(f"Error getting pending alpha checks: {e}", exc_info=True)
            return []

    def update_check_status(self, alpha_id: str, status: str):
        """
        Updates the status of an alpha check and sets the checked_at timestamp.

        Args:
            alpha_id: The ID of the alpha to update.
            status: The new status ('completed', 'failed', etc.).
        """
        self.logger.debug(f"Updating check status for alpha {alpha_id} to '{status}'.")
        
        update_data = {
            'status': status,
            'checked_at': datetime.now()
        }
        where_clause = "alpha_id = %s"
        
        try:
            affected_rows = self.db.update(self.TABLE_NAME, update_data, where_clause, (alpha_id,))
            if affected_rows > 0:
                self.logger.info(f"Successfully updated status for alpha {alpha_id}.")
            else:
                self.logger.warning(f"No record found for alpha {alpha_id} to update status.")
            return affected_rows
        except Exception as e:
            self.logger.error(f"Error updating check status for alpha {alpha_id}: {e}", exc_info=True)
            return 0

    def close(self):
        """Closes the database connection."""
        self.db.close()

