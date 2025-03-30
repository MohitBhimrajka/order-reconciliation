"""
Utility functions for the reconciliation application.
"""
import os
import re
import logging
from typing import List, Dict, Set, Optional
import pandas as pd
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define base directory (project root)
BASE_DIR = Path(__file__).parent.parent

# Define required columns for each file type
REQUIRED_COLUMNS = {
    'orders': {
        # Core columns required for status determination & analysis logic
        'order release id',  # Original column name from file
        'is_ship_rel',
        'return creation date',  # Original column name from file
        'final amount',  # Original column name from file
        'total mrp'  # Original column name from file
    },
    'returns': {
        'order_release_id',
        'total_actual_settlement'
    },
    'settlement': {
        'order_release_id',
        'total_actual_settlement'
    }
}

# Define file patterns
ORDERS_PATTERN = "orders-*.csv"
RETURNS_PATTERN = "returns-*.csv"
SETTLEMENT_PATTERN = "settlement-*.csv"

# Define directories
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
REPORT_DIR = OUTPUT_DIR / "reports"
VISUALIZATION_DIR = OUTPUT_DIR / "visualizations"

# Define master file paths
ORDERS_MASTER = OUTPUT_DIR / "master_orders.csv"
RETURNS_MASTER = OUTPUT_DIR / "master_returns.csv"
SETTLEMENT_MASTER = OUTPUT_DIR / "master_settlement.csv"

# Define output file paths
ANALYSIS_OUTPUT = OUTPUT_DIR / "analysis_results.csv"
REPORT_OUTPUT = REPORT_DIR / "reconciliation_report.txt"
ANOMALIES_OUTPUT = OUTPUT_DIR / "anomalies.csv"

# Column renaming mapping for standardization
COLUMN_RENAMES = {
    'orders': {
        'order release id': 'order_release_id',
        'final amount': 'final_amount',
        'total mrp': 'total_mrp',
        'return creation date': 'return_creation_date'
    },
    'returns': {},  # Already using underscores
    'settlement': {}  # Already using underscores
}

def ensure_directories_exist() -> None:
    """Ensure all required directories exist."""
    for directory in [DATA_DIR, OUTPUT_DIR, REPORT_DIR, VISUALIZATION_DIR]:
        directory.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Ensured directory exists: {directory}")

def validate_file_columns(file_path: Path, file_type: str) -> bool:
    """
    Validate that a file has all required columns.
    
    Args:
        file_path: Path to the file to validate
        file_type: Type of file ('orders', 'returns', or 'settlement')
    
    Returns:
        True if file has all required columns, False otherwise
    """
    try:
        # Read just the header row
        df = pd.read_csv(file_path, nrows=0)
        
        # Get required columns for this file type
        required = set(REQUIRED_COLUMNS.get(file_type, []))
        
        # Check for missing required columns
        missing = required - set(df.columns)
        if missing:
            logger.error(f"Missing required columns in {file_path}: {missing}")
            return False
        
        # Log any extra columns found
        extra = set(df.columns) - required
        if extra:
            logger.info(f"Extra columns found in {file_path}: {extra}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error validating columns in {file_path}: {e}")
        return False

def read_file(file_path: Path) -> pd.DataFrame:
    """
    Read a CSV file into a pandas DataFrame.
    
    Args:
        file_path: Path to the file to read
    
    Returns:
        DataFrame containing the file contents
    """
    try:
        df = pd.read_csv(file_path)
        logger.debug(f"Successfully read {file_path}")
        return df
    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")
        raise

def get_processed_files() -> List[Path]:
    """
    Get list of processed files in the data directory.
    
    Returns:
        List of Path objects for processed files
    """
    if not DATA_DIR.exists():
        return []
    
    processed_files = []
    for pattern in [ORDERS_PATTERN, RETURNS_PATTERN, SETTLEMENT_PATTERN]:
        for file in DATA_DIR.glob('*'):
            if re.match(pattern, file.name):
                processed_files.append(file)
    
    return processed_files

def extract_date_from_filename(filename: str) -> Optional[tuple]:
    """
    Extract month and year from filename.
    
    Args:
        filename: Name of the file
    
    Returns:
        Tuple of (month, year) if found, None otherwise
    """
    for pattern in [ORDERS_PATTERN, RETURNS_PATTERN, SETTLEMENT_PATTERN]:
        match = re.match(pattern, filename)
        if match:
            return match.groups()[:2]
    return None

def get_file_identifier(file_type: str, month: str, year: str) -> str:
    """
    Generate standard filename for a given file type, month, and year.
    
    Args:
        file_type: Type of file (orders, returns, settlement)
        month: Month (01-12)
        year: Year (YYYY)
    
    Returns:
        Standardized filename
    """
    return f"{file_type}-{month}-{year}.csv"

def format_currency(value: float) -> str:
    """
    Format a number as currency.
    
    Args:
        value: Number to format
    
    Returns:
        Formatted currency string
    """
    return f"₹{value:,.2f}"

def format_percentage(value: float) -> str:
    """
    Format a number as percentage.
    
    Args:
        value: Number to format
    
    Returns:
        Formatted percentage string
    """
    return f"{value:.2f}%" 