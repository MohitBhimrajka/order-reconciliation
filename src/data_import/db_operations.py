"""
Database operations for data import.
"""
import logging
from datetime import datetime
from typing import Dict, List, Optional
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

from src.database.models import Order, Return, Settlement, OrderStatusHistory, AuditLog
from src.database.config import get_db

logger = logging.getLogger(__name__)

def create_audit_log(
    session: Session,
    action: str,
    entity_type: str,
    entity_id: str,
    details: Dict
) -> None:
    """Create an audit log entry."""
    audit = AuditLog(
        action=action,
        entity_type=entity_type,
        entity_id=entity_id,
        details=details
    )
    session.add(audit)

def upsert_orders(df: pd.DataFrame, session: Session) -> List[str]:
    """
    Upsert orders into the database.
    
    Args:
        df: DataFrame containing order data
        session: Database session
    
    Returns:
        List of order IDs that were processed
    """
    processed_ids = []
    
    for _, row in df.iterrows():
        # Create order dict
        order_data = {
            'order_release_id': row['order_release_id'],
            'seller_id': row.get('seller_id'),
            'warehouse_id': row.get('warehouse_id'),
            'created_on': pd.to_datetime(row.get('created_on')),
            'packed_on': pd.to_datetime(row.get('packed_on')),
            'fmpu_date': pd.to_datetime(row.get('fmpu_date')),
            'inscanned_on': pd.to_datetime(row.get('inscanned_on')),
            'shipped_on': pd.to_datetime(row.get('shipped_on')),
            'delivered_on': pd.to_datetime(row.get('delivered_on')),
            'cancelled_on': pd.to_datetime(row.get('cancelled_on')),
            'rto_creation_date': pd.to_datetime(row.get('rto_creation_date')),
            'lost_date': pd.to_datetime(row.get('lost_date')),
            'return_creation_date': pd.to_datetime(row.get('return_creation_date')),
            'final_amount': row.get('final_amount', 0),
            'total_mrp': row.get('total_mrp', 0),
            'discount': row.get('discount', 0),
            'coupon_discount': row.get('coupon_discount', 0),
            'shipping_charge': row.get('shipping_charge', 0),
            'gift_charge': row.get('gift_charge', 0),
            'tax_recovery': row.get('tax_recovery', 0),
            'city': row.get('city', ''),
            'state': row.get('state', ''),
            'zipcode': row.get('zipcode', ''),
            'is_ship_rel': row.get('is_ship_rel', True),
            'source_file': row['source_file']
        }
        
        # Create upsert statement
        stmt = insert(Order).values(order_data)
        stmt = stmt.on_conflict_do_update(
            index_elements=['order_release_id'],
            set_=order_data
        )
        
        try:
            result = session.execute(stmt)
            session.flush()
            
            # Create audit log
            create_audit_log(
                session,
                'upsert',
                'order',
                order_data['order_release_id'],
                {'source_file': order_data['source_file']}
            )
            
            processed_ids.append(order_data['order_release_id'])
            
        except Exception as e:
            logger.error(f"Error upserting order {order_data['order_release_id']}: {e}")
            session.rollback()
            continue
    
    try:
        session.commit()
    except Exception as e:
        logger.error(f"Error committing orders batch: {e}")
        session.rollback()
    
    return processed_ids

def upsert_returns(df: pd.DataFrame, session: Session) -> List[str]:
    """
    Upsert returns into the database.
    
    Args:
        df: DataFrame containing return data
        session: Database session
    
    Returns:
        List of return IDs that were processed
    """
    processed_ids = []
    
    for _, row in df.iterrows():
        # Get the associated order
        order = session.query(Order).filter_by(
            order_release_id=row['order_release_id']
        ).first()
        
        if not order:
            logger.warning(
                f"Order not found for return: {row['order_release_id']}"
            )
            continue
        
        # Create return dict
        return_data = {
            'order_id': order.id,
            'order_release_id': row['order_release_id'],
            'order_line_id': row.get('order_line_id', ''),
            'return_type': row.get('return_type', ''),
            'return_date': pd.to_datetime(row.get('return_date')),
            'packing_date': pd.to_datetime(row.get('packing_date')),
            'delivery_date': pd.to_datetime(row.get('delivery_date')),
            'ecommerce_portal_name': row.get('ecommerce_portal_name', ''),
            'sku_code': row.get('sku_code', ''),
            'invoice_number': row.get('invoice_number', ''),
            'packet_id': row.get('packet_id', ''),
            'hsn_code': row.get('hsn_code', ''),
            'product_tax_category': row.get('product_tax_category', ''),
            'currency': row.get('currency', 'INR'),
            'customer_paid_amount': row.get('customer_paid_amount', 0),
            'postpaid_amount': row.get('postpaid_amount', 0),
            'prepaid_amount': row.get('prepaid_amount', 0),
            'mrp': row.get('mrp', 0),
            'total_discount_amount': row.get('total_discount_amount', 0),
            'shipping_case': row.get('shipping_case', ''),
            'total_tax_rate': row.get('total_tax_rate', 0),
            'igst_amount': row.get('igst_amount', 0),
            'cgst_amount': row.get('cgst_amount', 0),
            'sgst_amount': row.get('sgst_amount', 0),
            'tcs_amount': row.get('tcs_amount', 0),
            'tds_amount': row.get('tds_amount', 0),
            'commission_percentage': row.get('commission_percentage', 0),
            'minimum_commission': row.get('minimum_commission', 0),
            'platform_fees': row.get('platform_fees', 0),
            'total_commission': row.get('total_commission', 0),
            'total_commission_plus_tcs_tds_deduction': row.get('total_commission_plus_tcs_tds_deduction', 0),
            'total_logistics_deduction': row.get('total_logistics_deduction', 0),
            'shipping_fee': row.get('shipping_fee', 0),
            'fixed_fee': row.get('fixed_fee', 0),
            'pick_and_pack_fee': row.get('pick_and_pack_fee', 0),
            'payment_gateway_fee': row.get('payment_gateway_fee', 0),
            'total_tax_on_logistics': row.get('total_tax_on_logistics', 0),
            'article_level': row.get('article_level', ''),
            'shipment_zone_classification': row.get('shipment_zone_classification', ''),
            'customer_paid_amt': row.get('customer_paid_amt', 0),
            'total_settlement': row.get('total_settlement', 0),
            'total_actual_settlement': row.get('total_actual_settlement', 0),
            'amount_pending_settlement': row.get('amount_pending_settlement', 0),
            'source_file': row['source_file']
        }
        
        # Create upsert statement
        stmt = insert(Return).values(return_data)
        stmt = stmt.on_conflict_do_update(
            index_elements=['order_release_id', 'order_line_id'],
            set_=return_data
        )
        
        try:
            result = session.execute(stmt)
            session.flush()
            
            # Create audit log
            create_audit_log(
                session,
                'upsert',
                'return',
                f"{return_data['order_release_id']}_{return_data['order_line_id']}",
                {'source_file': return_data['source_file']}
            )
            
            processed_ids.append(f"{return_data['order_release_id']}_{return_data['order_line_id']}")
            
        except Exception as e:
            logger.error(f"Error upserting return {return_data['order_release_id']}: {e}")
            session.rollback()
            continue
    
    try:
        session.commit()
    except Exception as e:
        logger.error(f"Error committing returns batch: {e}")
        session.rollback()
    
    return processed_ids

def upsert_settlements(df: pd.DataFrame, session: Session) -> List[str]:
    """
    Upsert settlements into the database.
    
    Args:
        df: DataFrame containing settlement data
        session: Database session
    
    Returns:
        List of settlement IDs that were processed
    """
    processed_ids = []
    
    for _, row in df.iterrows():
        # Get the associated order
        order = session.query(Order).filter_by(
            order_release_id=row['order_release_id']
        ).first()
        
        if not order:
            logger.warning(
                f"Order not found for settlement: {row['order_release_id']}"
            )
            continue
        
        # Get the associated return if it exists
        return_record = None
        if row.get('return_type'):
            return_record = session.query(Return).filter_by(
                order_release_id=row['order_release_id'],
                order_line_id=row['order_line_id']
            ).first()
        
        # Create settlement dict
        settlement_data = {
            'order_id': order.id,
            'return_id': return_record.id if return_record else None,
            'order_release_id': row['order_release_id'],
            'order_line_id': row.get('order_line_id', ''),
            'return_type': row.get('return_type'),
            'return_date': pd.to_datetime(row.get('return_date')),
            'packing_date': pd.to_datetime(row.get('packing_date')),
            'delivery_date': pd.to_datetime(row.get('delivery_date')),
            'ecommerce_portal_name': row.get('ecommerce_portal_name', ''),
            'sku_code': row.get('sku_code', ''),
            'invoice_number': row.get('invoice_number', ''),
            'packet_id': row.get('packet_id', ''),
            'hsn_code': row.get('hsn_code', ''),
            'product_tax_category': row.get('product_tax_category', ''),
            'currency': row.get('currency', 'INR'),
            'customer_paid_amount': row.get('customer_paid_amount', 0),
            'postpaid_amount': row.get('postpaid_amount', 0),
            'prepaid_amount': row.get('prepaid_amount', 0),
            'mrp': row.get('mrp', 0),
            'total_discount_amount': row.get('total_discount_amount', 0),
            'shipping_case': row.get('shipping_case', ''),
            'total_tax_rate': row.get('total_tax_rate', 0),
            'igst_amount': row.get('igst_amount', 0),
            'cgst_amount': row.get('cgst_amount', 0),
            'sgst_amount': row.get('sgst_amount', 0),
            'tcs_amount': row.get('tcs_amount', 0),
            'tds_amount': row.get('tds_amount', 0),
            'commission_percentage': row.get('commission_percentage', 0),
            'minimum_commission': row.get('minimum_commission', 0),
            'platform_fees': row.get('platform_fees', 0),
            'total_commission': row.get('total_commission', 0),
            'total_commission_plus_tcs_tds_deduction': row.get('total_commission_plus_tcs_tds_deduction', 0),
            'total_logistics_deduction': row.get('total_logistics_deduction', 0),
            'shipping_fee': row.get('shipping_fee', 0),
            'fixed_fee': row.get('fixed_fee', 0),
            'pick_and_pack_fee': row.get('pick_and_pack_fee', 0),
            'payment_gateway_fee': row.get('payment_gateway_fee', 0),
            'total_tax_on_logistics': row.get('total_tax_on_logistics', 0),
            'article_level': row.get('article_level', ''),
            'shipment_zone_classification': row.get('shipment_zone_classification', ''),
            'customer_paid_amt': row.get('customer_paid_amt', 0),
            'total_expected_settlement': row.get('total_expected_settlement', 0),
            'total_actual_settlement': row.get('total_actual_settlement', 0),
            'amount_pending_settlement': row.get('amount_pending_settlement', 0),
            'source_file': row['source_file']
        }
        
        # Create upsert statement
        stmt = insert(Settlement).values(settlement_data)
        stmt = stmt.on_conflict_do_update(
            index_elements=['order_release_id', 'order_line_id'],
            set_=settlement_data
        )
        
        try:
            result = session.execute(stmt)
            session.flush()
            
            # Create audit log
            create_audit_log(
                session,
                'upsert',
                'settlement',
                f"{settlement_data['order_release_id']}_{settlement_data['order_line_id']}",
                {'source_file': settlement_data['source_file']}
            )
            
            processed_ids.append(f"{settlement_data['order_release_id']}_{settlement_data['order_line_id']}")
            
        except Exception as e:
            logger.error(f"Error upserting settlement {settlement_data['order_release_id']}: {e}")
            session.rollback()
            continue
    
    try:
        session.commit()
    except Exception as e:
        logger.error(f"Error committing settlements batch: {e}")
        session.rollback()
    
    return processed_ids

def update_order_status(
    order_id: str,
    new_status: str,
    session: Session,
    details: Optional[Dict] = None
) -> bool:
    """
    Update an order's status and create a status history entry.
    
    Args:
        order_id: Order ID to update
        new_status: New status to set
        session: Database session
        details: Optional details about the status change
    
    Returns:
        True if successful, False otherwise
    """
    try:
        # Get the order
        order = session.query(Order).filter_by(order_release_id=order_id).first()
        if not order:
            logger.error(f"Order not found: {order_id}")
            return False
        
        # Create status history entry
        status_history = OrderStatusHistory(
            order_id=order.id,
            status=new_status,
            details=details or {}
        )
        session.add(status_history)
        
        # Create audit log
        create_audit_log(
            session,
            'status_change',
            'order',
            order_id,
            {'new_status': new_status, **(details or {})}
        )
        
        session.commit()
        return True
        
    except Exception as e:
        logger.error(f"Error updating order status for {order_id}: {e}")
        session.rollback()
        return False 