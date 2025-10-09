import os
import csv
import argparse
import ast
import numpy as np
import random
from database import Database
from logger import Logger
from dao import AlphaListPendingSimulatedDAO
from datetime import datetime

# Initialize logger
logger = Logger().logger

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Import alpha CSV into database')
    parser.add_argument('csv_file', help='Path to CSV file containing alphas')
    parser.add_argument('--priority', type=str, required=True,
                        help='Priority assignment: single value (0) or range (0-5)')
    return parser.parse_args()

def validate_priority_input(priority_str):
    """Validate and parse priority input"""
    if '-' in priority_str:
        # Range format: min-max
        try:
            min_pri, max_pri = map(int, priority_str.split('-'))
            if min_pri < 0 or max_pri < min_pri:
                raise ValueError("Invalid priority range")
            return list(range(min_pri, max_pri + 1))
        except (ValueError, TypeError):
            logger.error("Invalid priority range format. Use 'min-max' (e.g., 0-5)")
            return None
    else:
        # Single value
        try:
            priority = int(priority_str)
            if priority < 0:
                raise ValueError("Priority must be >= 0")
            return [priority]
        except (ValueError, TypeError):
            logger.error("Invalid priority value. Must be an integer >= 0")
            return None

def extract_region(settings_str, row_index):
    """
    Extract region from settings string with strict validation
    
    Args:
        settings_str (str): Settings string from CSV
        row_index (int): Current row index for error reporting
        
    Returns:
        str: Extracted region value
        
    Raises:
        ValueError: If region is missing or invalid
    """
    try:
        # Safely evaluate string to dictionary
        settings = ast.literal_eval(settings_str)
        
        # Validate settings structure
        if not isinstance(settings, dict):
            raise ValueError(f"Row {row_index}: Settings is not a dictionary - {settings_str}")
            
        # Check for region field
        if 'region' not in settings:
            raise ValueError(f"Row {row_index}: 'region' field missing in settings")
            
        region = settings['region']
        
        # Validate region value
        if not isinstance(region, str) or not region.strip():
            raise ValueError(f"Row {row_index}: Invalid region value '{region}'")
            
        return region
        
    except (SyntaxError, ValueError) as e:
        raise ValueError(f"Row {row_index}: Failed to parse settings - {str(e)}")

def read_csv_file(file_path):
    """Read and validate CSV file"""
    if not os.path.exists(file_path):
        logger.error(f"CSV file not found: {file_path}")
        return None
    
    alphas = []
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or 'settings' not in reader.fieldnames:
            logger.error("Invalid CSV format. Missing required columns.")
            return None
            
        for row in reader:
            # Validate required fields
            if not all(key in row for key in ['type', 'settings', 'regular']):
                logger.warning(f"Skipping row missing required fields: {row}")
                continue
            alphas.append(row)
    
    # Shuffle alphas to randomize processing order
    random.shuffle(alphas)
    
    return alphas

def assign_priorities(alphas, priority_levels):
    """Assign priorities to alphas"""
    if len(priority_levels) == 1:
        # Single priority for all
        return [priority_levels[0] for _ in alphas]
    
    # Distribute priorities evenly across levels
    chunks = np.array_split(alphas, len(priority_levels))
    priorities = []
    for i, chunk in enumerate(chunks):
        priorities.extend([priority_levels[i]] * len(chunk))
    return priorities

def main():
    """Main import function"""
    args = parse_arguments()
    
    # Validate priority input
    priority_levels = validate_priority_input(args.priority)
    if not priority_levels:
        return
    
    # Read and validate CSV
    alphas = read_csv_file(args.csv_file)
    if not alphas:
        return
    
    logger.info(f"Found {len(alphas)} valid alpha records in {args.csv_file}")
    
    # Assign priorities
    priorities = assign_priorities(alphas, priority_levels)
    
    # Prepare database records
    db_records = []
    for idx, (alpha, priority) in enumerate(zip(alphas, priorities), 1):
        try:
            region = extract_region(alpha['settings'], idx)
            db_records.append({
                'type': alpha['type'],
                'settings': alpha['settings'],
                'regular': alpha['regular'],
                'priority': priority,
                'region': region,
                'created_at': datetime.now()
            })
        except ValueError as e:
            logger.error(f"❌ {str(e)}")
            logger.error("Import aborted. Please fix the CSV file and try again.")
            return
    
    # Initialize database and DAO
    try:
        dao = AlphaListPendingSimulatedDAO()
        
        # Batch insert records
        inserted_count = dao.batch_insert(db_records)
        logger.info(f"Successfully inserted {inserted_count} records into database")
        
    except Exception as e:
        logger.error(f"Database operation failed: {e}")

if __name__ == '__main__':
    main()

# Single priority
#python src/import_alpha_csv.py path/to/alphas.csv --priority 0
#python src/import_alpha_csv.py D:\repos\consultant\consultant\output\alpha_asi_new18_compare.csv --priority 2

# Priority range
#python src\import_alpha_csv.py D:\repos\consultant\consultant\output\alpha_asi_new18.csv --priority 2-15
