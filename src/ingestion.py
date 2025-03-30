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
    validate_file_columns, COLUMN_RENAMES
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
    orders_df = orders_df.rename(columns=COLUMN_RENAMES['orders'])
    
    # Convert numeric columns
    numeric_columns = [
        'final_amount', 'total_mrp', 'discount', 'coupon_discount',
        'shipping_charge', 'gift_charge', 'tax_recovery'
    ]
    for col in numeric_columns:
        if col in orders_df.columns:
            orders_df[col] = pd.to_numeric(orders_df[col], errors='coerce')
    
    # Convert date columns
    date_columns = [
        'created_on', 'packed_on', 'fmpu_date', 'inscanned_on',
        'shipped_on', 'delivered_on', 'cancelled_on',
        'rto_creation_date', 'lost_date', 'return_creation_date'
    ]
    for col in date_columns:
        if col in orders_df.columns:
            orders_df[col] = pd.to_datetime(orders_df[col], errors='coerce')
    
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
    
    # Standardize column names
    returns_df = returns_df.rename(columns=COLUMN_RENAMES['returns'])
    
    # Convert numeric columns
    numeric_columns = [
        'customer_paid_amount', 'postpaid_amount', 'prepaid_amount',
        'mrp', 'total_discount_amount', 'total_tax_rate',
        'igst_amount', 'cgst_amount', 'sgst_amount',
        'tcs_amount', 'tds_amount', 'commission_percentage',
        'minimum_commission', 'platform_fees', 'total_commission',
        'total_commission_plus_tcs_tds_deduction', 'total_logistics_deduction',
        'shipping_fee', 'fixed_fee', 'pick_and_pack_fee',
        'payment_gateway_fee', 'total_tax_on_logistics',
        'customer_paid_amt', 'total_settlement', 'total_actual_settlement',
        'amount_pending_settlement', 'prepaid_commission_deduction',
        'prepaid_logistics_deduction', 'prepaid_payment',
        'postpaid_commission_deduction', 'postpaid_logistics_deduction',
        'postpaid_payment', 'postpaid_amount_other', 'prepaid_amount_other',
        'shipping_amount', 'gift_amount', 'additional_amount',
        'cess_amount', 'taxable_amount', 'igst_rate', 'cgst_rate',
        'sgst_rate', 'cess_rate', 'tcs_igst_rate', 'tcs_sgst_rate',
        'tcs_cgst_rate', 'tds_rate', 'prepaid_commission_percentage',
        'prepaid_minimum_commission', 'prepaid_platform_fees',
        'prepaid_total_commission', 'prepaid_ship_commission_charge',
        'prepaid_gift_commission_charge', 'prepaid_cod_commission_charge',
        'prepaid_cart_discount', 'prepaid_coupon_discount',
        'postpaid_commission_percentage', 'postpaid_minimum_commission',
        'postpaid_platform_fees', 'postpaid_total_commission',
        'postpaid_ship_commission_charge', 'postpaid_gift_commission_charge',
        'postpaid_cod_commission_charge', 'postpaid_cart_discount',
        'postpaid_coupon_discount', 'tcs_amount_prepaid',
        'tcs_amount_postpaid', 'tds_amount_prepaid', 'tds_amount_postpaid',
        'royaltyCharges_prepaid', 'royaltyCharges_postpaid',
        'royaltyPercent_prepaid', 'royaltyPercent_postpaid',
        'marketingCharges_prepaid', 'marketingCharges_postpaid',
        'marketingPercent_prepaid', 'marketingPercent_postpaid',
        'marketingContribution_prepaid', 'marketingContribution_postpaid',
        'reverseAdditionalCharges_prepaid', 'reverseAdditionalCharges_postpaid'
    ]
    for col in numeric_columns:
        if col in returns_df.columns:
            returns_df[col] = pd.to_numeric(returns_df[col], errors='coerce')
    
    # Convert date columns
    date_columns = [
        'return_date', 'packing_date', 'delivery_date',
        'settlement_date_prepaid_comm_deduction',
        'settlement_date_prepaid_logistics_deduction',
        'settlement_date_prepaid_payment',
        'settlement_date_postpaid_comm_deduction',
        'settlement_date_postpaid_logistics_deduction',
        'settlement_date_postpaid_payment'
    ]
    for col in date_columns:
        if col in returns_df.columns:
            returns_df[col] = pd.to_datetime(returns_df[col], errors='coerce')
    
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
    
    # Standardize column names
    settlement_df = settlement_df.rename(columns=COLUMN_RENAMES['settlement'])
    
    # Convert numeric columns
    numeric_columns = [
        'customer_paid_amount', 'postpaid_amount', 'prepaid_amount',
        'mrp', 'total_discount_amount', 'total_tax_rate',
        'igst_amount', 'cgst_amount', 'sgst_amount',
        'tcs_amount', 'tds_amount', 'commission_percentage',
        'minimum_commission', 'platform_fees', 'total_commission',
        'total_commission_plus_tcs_tds_deduction', 'total_logistics_deduction',
        'shipping_fee', 'fixed_fee', 'pick_and_pack_fee',
        'payment_gateway_fee', 'total_tax_on_logistics',
        'customer_paid_amt', 'total_expected_settlement',
        'total_actual_settlement', 'amount_pending_settlement',
        'prepaid_commission_deduction', 'prepaid_logistics_deduction',
        'prepaid_payment', 'postpaid_commission_deduction',
        'postpaid_logistics_deduction', 'postpaid_payment',
        'postpaid_amount_other', 'prepaid_amount_other',
        'shipping_amount', 'gift_amount', 'additional_amount',
        'cess_amount', 'taxable_amount', 'igst_rate', 'cgst_rate',
        'sgst_rate', 'cess_rate', 'tcs_igst_rate', 'tcs_sgst_rate',
        'tcs_cgst_rate', 'tds_rate', 'prepaid_commission_percentage',
        'prepaid_minimum_commission', 'prepaid_platform_fees',
        'prepaid_total_commission', 'prepaid_ship_commission_charge',
        'prepaid_gift_commission_charge', 'prepaid_cod_commission_charge',
        'prepaid_cart_discount', 'prepaid_coupon_discount',
        'postpaid_commission_percentage', 'postpaid_minimum_commission',
        'postpaid_platform_fees', 'postpaid_total_commission',
        'postpaid_ship_commission_charge', 'postpaid_gift_commission_charge',
        'postpaid_cod_commission_charge', 'postpaid_cart_discount',
        'postpaid_coupon_discount', 'tcs_amount_prepaid',
        'tcs_amount_postpaid', 'tds_amount_prepaid', 'tds_amount_postpaid',
        'royaltyCharges_prepaid', 'royaltyCharges_postpaid',
        'royaltyPercent_prepaid', 'royaltyPercent_postpaid',
        'marketingCharges_prepaid', 'marketingCharges_postpaid',
        'marketingPercent_prepaid', 'marketingPercent_postpaid',
        'marketingContribution_prepaid', 'marketingContribution_postpaid',
        'forwardAdditionalCharges_prepaid', 'forwardAdditionalCharges_postpaid'
    ]
    for col in numeric_columns:
        if col in settlement_df.columns:
            settlement_df[col] = pd.to_numeric(settlement_df[col], errors='coerce')
    
    # Convert date columns
    date_columns = [
        'return_date', 'packing_date', 'delivery_date',
        'settlement_date_prepaid_comm_deduction',
        'settlement_date_prepaid_logistics_deduction',
        'settlement_date_prepaid_payment',
        'settlement_date_postpaid_comm_deduction',
        'settlement_date_postpaid_logistics_deduction',
        'settlement_date_postpaid_payment'
    ]
    for col in date_columns:
        if col in settlement_df.columns:
            settlement_df[col] = pd.to_datetime(settlement_df[col], errors='coerce')
    
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