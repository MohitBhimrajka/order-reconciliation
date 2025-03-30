"""
Data ingestion module for reconciliation application.
"""
import os
import logging
from typing import Dict, List
import pandas as pd
from pathlib import Path

from utils import (
    ensure_directories_exist, read_file,
    ORDERS_PATTERN, RETURNS_PATTERN, SETTLEMENT_PATTERN,
    ORDERS_MASTER, RETURNS_MASTER, SETTLEMENT_MASTER,
    validate_file_columns
)

logger = logging.getLogger(__name__)

def scan_directory(directory: str) -> Dict[str, List[str]]:
    """
    Scan a directory for files matching the expected patterns.
    
    Args:
        directory: Directory to scan
        
    Returns:
        Dictionary with file types as keys and lists of file paths as values
    """
    if not os.path.exists(directory):
        logger.error(f"Directory does not exist: {directory}")
        return {"orders": [], "returns": [], "settlement": []}
    
    directory_path = Path(directory)
    
    # Use glob for pattern matching
    orders_files = [str(f) for f in directory_path.glob(ORDERS_PATTERN)]
    returns_files = [str(f) for f in directory_path.glob(RETURNS_PATTERN)]
    settlement_files = [str(f) for f in directory_path.glob(SETTLEMENT_PATTERN)]
    
    # Log found files
    logger.info(f"Found {len(orders_files)} orders files")
    logger.info(f"Found {len(returns_files)} returns files")
    logger.info(f"Found {len(settlement_files)} settlement files")
    
    return {
        "orders": orders_files,
        "returns": returns_files,
        "settlement": settlement_files
    }

def process_orders_file(file_path: Path) -> None:
    """
    Process an orders file and update the orders master file.
    Implements true append/update by source file tracking.
    
    Args:
        file_path: Path to the orders file
    """
    # Validate file columns first
    if not validate_file_columns(file_path, 'orders'):
        raise ValueError(f"Invalid columns in orders file: {file_path}")
    
    # Read the file
    orders_df = read_file(file_path)
    
    # Add source file information
    orders_df['source_file'] = file_path.name
    orders_df['ingestion_timestamp'] = pd.Timestamp.now()
    
    # Standardize column names
    orders_df = orders_df.rename(columns={
        'order release id': 'order_release_id',
        'order line id': 'order_line_id',
        'order status': 'order_status',
        'final amount': 'final_amount',
        'total mrp': 'total_mrp',
        'coupon discount': 'coupon_discount',
        'shipping charge': 'shipping_charge',
        'gift charge': 'gift_charge',
        'tax recovery': 'tax_recovery'
    })
    
    # Convert numeric columns
    numeric_columns = [
        'final_amount', 'total_mrp', 'discount', 'coupon_discount',
        'shipping_charge', 'gift_charge', 'tax_recovery'
    ]
    for col in numeric_columns:
        if col in orders_df.columns:
            orders_df[col] = pd.to_numeric(orders_df[col], errors='coerce')
    
    # Convert date columns
    if 'order_date' in orders_df.columns:
        orders_df['order_date'] = pd.to_datetime(orders_df['order_date'])
    if 'return_creation_date' in orders_df.columns:
        orders_df['return_creation_date'] = pd.to_datetime(orders_df['return_creation_date'])
    
    # Update master file with source file tracking
    if ORDERS_MASTER.exists():
        master_df = read_file(ORDERS_MASTER)
        # Remove only records from the same source file
        master_df = master_df[master_df['source_file'] != file_path.name]
        # Append new data
        master_df = pd.concat([master_df, orders_df], ignore_index=True)
    else:
        master_df = orders_df
    
    # Save updated master file
    master_df.to_csv(ORDERS_MASTER, index=False)
    logger.info(f"Successfully processed orders file: {file_path.name}")
    logger.info(f"Master file now contains {len(master_df)} records")

def process_returns_file(file_path: Path) -> None:
    """
    Process a returns file and update the returns master file.
    Implements true append/update by source file tracking.
    
    Args:
        file_path: Path to the returns file
    """
    # Validate file columns first
    if not validate_file_columns(file_path, 'returns'):
        raise ValueError(f"Invalid columns in returns file: {file_path}")
    
    # Read the file
    returns_df = read_file(file_path)
    
    # Add source file information
    returns_df['source_file'] = file_path.name
    returns_df['ingestion_timestamp'] = pd.Timestamp.now()
    
    # Convert numeric columns
    if 'total_actual_settlement' in returns_df.columns:
        returns_df['total_actual_settlement'] = pd.to_numeric(returns_df['total_actual_settlement'], errors='coerce')
    
    # Update master file with source file tracking
    if RETURNS_MASTER.exists():
        master_df = read_file(RETURNS_MASTER)
        # Remove only records from the same source file
        master_df = master_df[master_df['source_file'] != file_path.name]
        # Append new data
        master_df = pd.concat([master_df, returns_df], ignore_index=True)
    else:
        master_df = returns_df
    
    # Save updated master file
    master_df.to_csv(RETURNS_MASTER, index=False)
    logger.info(f"Successfully processed returns file: {file_path.name}")
    logger.info(f"Master file now contains {len(master_df)} records")

def process_settlement_file(file_path: Path) -> None:
    """
    Process a settlement file and update the settlement master file.
    Implements true append/update by source file tracking.
    
    Args:
        file_path: Path to the settlement file
    """
    # Validate file columns first
    if not validate_file_columns(file_path, 'settlement'):
        raise ValueError(f"Invalid columns in settlement file: {file_path}")
    
    # Read the file
    settlement_df = read_file(file_path)
    
    # Add source file information
    settlement_df['source_file'] = file_path.name
    settlement_df['ingestion_timestamp'] = pd.Timestamp.now()
    
    # Convert numeric columns
    if 'total_actual_settlement' in settlement_df.columns:
        settlement_df['total_actual_settlement'] = pd.to_numeric(settlement_df['total_actual_settlement'], errors='coerce')
    
    # Convert date columns
    if 'settlement_date' in settlement_df.columns:
        settlement_df['settlement_date'] = pd.to_datetime(settlement_df['settlement_date'])
    
    # Update master file with source file tracking
    if SETTLEMENT_MASTER.exists():
        master_df = read_file(SETTLEMENT_MASTER)
        # Remove only records from the same source file
        master_df = master_df[master_df['source_file'] != file_path.name]
        # Append new data
        master_df = pd.concat([master_df, settlement_df], ignore_index=True)
    else:
        master_df = settlement_df
    
    # Save updated master file
    master_df.to_csv(SETTLEMENT_MASTER, index=False)
    logger.info(f"Successfully processed settlement file: {file_path.name}")
    logger.info(f"Master file now contains {len(master_df)} records")

def process_file(file_path: Path, file_type: str) -> bool:
    """
    Process a file based on its type.
    
    Args:
        file_path: Path to the file
        file_type: Type of file (orders, returns, settlement)
    
    Returns:
        True if successful, False otherwise
    """
    try:
        if file_type == 'orders':
            process_orders_file(file_path)
        elif file_type == 'returns':
            process_returns_file(file_path)
        elif file_type == 'settlement':
            process_settlement_file(file_path)
        else:
            logger.error(f"Invalid file type: {file_type}")
            return False
        return True
    except Exception as e:
        logger.error(f"Error processing {file_type} file {file_path}: {e}")
        return False 