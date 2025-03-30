"""
Database models for the reconciliation application.
"""
from datetime import datetime
from typing import Optional
from sqlalchemy import (
    Column, Integer, String, Float, DateTime, Boolean, ForeignKey,
    Enum, Text, Numeric, JSON
)
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy.dialects.postgresql import UUID
import uuid

Base = declarative_base()

class Order(Base):
    """Model for orders."""
    __tablename__ = 'orders'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    seller_id = Column(String, nullable=False)
    warehouse_id = Column(String, nullable=False)
    store_order_id = Column(String, nullable=False)
    order_release_id = Column(String, unique=True, nullable=False)
    order_line_id = Column(String, nullable=False)
    seller_order_id = Column(String, nullable=False)
    order_id_fk = Column(String, nullable=False)
    core_item_id = Column(String, nullable=False)
    created_on = Column(DateTime, nullable=False)
    style_id = Column(String, nullable=False)
    seller_sku_code = Column(String, nullable=False)
    sku_id = Column(String, nullable=False)
    myntra_sku_code = Column(String, nullable=False)
    size = Column(String, nullable=False)
    vendor_article_number = Column(String, nullable=False)
    brand = Column(String, nullable=False)
    style_name = Column(String, nullable=False)
    article_type = Column(String, nullable=False)
    article_type_id = Column(String, nullable=False)
    order_status = Column(String, nullable=False)
    packet_id = Column(String, nullable=False)
    seller_pack_id = Column(String, nullable=False)
    courier_code = Column(String, nullable=False)
    order_tracking_number = Column(String, nullable=False)
    seller_warehouse_id = Column(String, nullable=False)
    cancellation_reason_id_fk = Column(String, nullable=True)
    cancellation_reason = Column(String, nullable=True)
    packed_on = Column(DateTime, nullable=True)
    fmpu_date = Column(DateTime, nullable=True)
    inscanned_on = Column(DateTime, nullable=True)
    shipped_on = Column(DateTime, nullable=True)
    delivered_on = Column(DateTime, nullable=True)
    cancelled_on = Column(DateTime, nullable=True)
    rto_creation_date = Column(DateTime, nullable=True)
    lost_date = Column(DateTime, nullable=True)
    return_creation_date = Column(DateTime, nullable=True)
    final_amount = Column(Numeric(10, 2), nullable=False)
    total_mrp = Column(Numeric(10, 2), nullable=False)
    discount = Column(Numeric(10, 2), nullable=False)
    coupon_discount = Column(Numeric(10, 2), nullable=False)
    shipping_charge = Column(Numeric(10, 2), nullable=False)
    gift_charge = Column(Numeric(10, 2), nullable=False)
    tax_recovery = Column(Numeric(10, 2), nullable=False)
    city = Column(String, nullable=False)
    state = Column(String, nullable=False)
    zipcode = Column(String, nullable=False)
    is_ship_rel = Column(Boolean, nullable=False)
    source_file = Column(String, nullable=False)
    ingestion_timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    # Relationships
    returns = relationship("Return", back_populates="order")
    settlements = relationship("Settlement", back_populates="order")
    status_history = relationship("OrderStatusHistory", back_populates="order")

class Return(Base):
    """Model for returns."""
    __tablename__ = 'returns'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    order_id = Column(UUID(as_uuid=True), ForeignKey('orders.id'), nullable=False)
    order_release_id = Column(String, nullable=False)
    order_line_id = Column(String, nullable=False)
    return_type = Column(String, nullable=False)  # refund, exchange
    return_date = Column(DateTime, nullable=True)
    packing_date = Column(DateTime, nullable=True)
    delivery_date = Column(DateTime, nullable=True)
    ecommerce_portal_name = Column(String, nullable=False)
    sku_code = Column(String, nullable=False)
    invoice_number = Column(String, nullable=False)
    packet_id = Column(String, nullable=False)
    hsn_code = Column(String, nullable=False)
    product_tax_category = Column(String, nullable=False)
    currency = Column(String, nullable=False)
    customer_paid_amount = Column(Numeric(10, 2), nullable=False)
    postpaid_amount = Column(Numeric(10, 2), nullable=False)
    prepaid_amount = Column(Numeric(10, 2), nullable=False)
    mrp = Column(Numeric(10, 2), nullable=False)
    total_discount_amount = Column(Numeric(10, 2), nullable=False)
    shipping_case = Column(String, nullable=False)
    total_tax_rate = Column(Numeric(5, 2), nullable=False)
    igst_amount = Column(Numeric(10, 2), nullable=False)
    cgst_amount = Column(Numeric(10, 2), nullable=False)
    sgst_amount = Column(Numeric(10, 2), nullable=False)
    tcs_amount = Column(Numeric(10, 2), nullable=False)
    tds_amount = Column(Numeric(10, 2), nullable=False)
    commission_percentage = Column(Numeric(5, 2), nullable=False)
    minimum_commission = Column(Numeric(10, 2), nullable=False)
    platform_fees = Column(Numeric(10, 2), nullable=False)
    total_commission = Column(Numeric(10, 2), nullable=False)
    total_commission_plus_tcs_tds_deduction = Column(Numeric(10, 2), nullable=False)
    total_logistics_deduction = Column(Numeric(10, 2), nullable=False)
    shipping_fee = Column(Numeric(10, 2), nullable=False)
    fixed_fee = Column(Numeric(10, 2), nullable=False)
    pick_and_pack_fee = Column(Numeric(10, 2), nullable=False)
    payment_gateway_fee = Column(Numeric(10, 2), nullable=False)
    total_tax_on_logistics = Column(Numeric(10, 2), nullable=False)
    article_level = Column(String, nullable=False)
    shipment_zone_classification = Column(String, nullable=False)
    customer_paid_amt = Column(Numeric(10, 2), nullable=False)
    total_settlement = Column(Numeric(10, 2), nullable=False)
    total_actual_settlement = Column(Numeric(10, 2), nullable=False)
    amount_pending_settlement = Column(Numeric(10, 2), nullable=False)
    source_file = Column(String, nullable=False)
    ingestion_timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    # Relationships
    order = relationship("Order", back_populates="returns")
    settlements = relationship("Settlement", back_populates="return")

class Settlement(Base):
    """Model for settlements."""
    __tablename__ = 'settlements'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    order_id = Column(UUID(as_uuid=True), ForeignKey('orders.id'), nullable=False)
    return_id = Column(UUID(as_uuid=True), ForeignKey('returns.id'), nullable=True)
    order_release_id = Column(String, nullable=False)
    order_line_id = Column(String, nullable=False)
    return_type = Column(String, nullable=True)
    return_date = Column(DateTime, nullable=True)
    packing_date = Column(DateTime, nullable=True)
    delivery_date = Column(DateTime, nullable=True)
    ecommerce_portal_name = Column(String, nullable=False)
    sku_code = Column(String, nullable=False)
    invoice_number = Column(String, nullable=False)
    packet_id = Column(String, nullable=False)
    hsn_code = Column(String, nullable=False)
    product_tax_category = Column(String, nullable=False)
    currency = Column(String, nullable=False)
    customer_paid_amount = Column(Numeric(10, 2), nullable=False)
    postpaid_amount = Column(Numeric(10, 2), nullable=False)
    prepaid_amount = Column(Numeric(10, 2), nullable=False)
    mrp = Column(Numeric(10, 2), nullable=False)
    total_discount_amount = Column(Numeric(10, 2), nullable=False)
    shipping_case = Column(String, nullable=False)
    total_tax_rate = Column(Numeric(5, 2), nullable=False)
    igst_amount = Column(Numeric(10, 2), nullable=False)
    cgst_amount = Column(Numeric(10, 2), nullable=False)
    sgst_amount = Column(Numeric(10, 2), nullable=False)
    tcs_amount = Column(Numeric(10, 2), nullable=False)
    tds_amount = Column(Numeric(10, 2), nullable=False)
    commission_percentage = Column(Numeric(5, 2), nullable=False)
    minimum_commission = Column(Numeric(10, 2), nullable=False)
    platform_fees = Column(Numeric(10, 2), nullable=False)
    total_commission = Column(Numeric(10, 2), nullable=False)
    total_commission_plus_tcs_tds_deduction = Column(Numeric(10, 2), nullable=False)
    total_logistics_deduction = Column(Numeric(10, 2), nullable=False)
    shipping_fee = Column(Numeric(10, 2), nullable=False)
    fixed_fee = Column(Numeric(10, 2), nullable=False)
    pick_and_pack_fee = Column(Numeric(10, 2), nullable=False)
    payment_gateway_fee = Column(Numeric(10, 2), nullable=False)
    total_tax_on_logistics = Column(Numeric(10, 2), nullable=False)
    article_level = Column(String, nullable=False)
    shipment_zone_classification = Column(String, nullable=False)
    customer_paid_amt = Column(Numeric(10, 2), nullable=False)
    total_expected_settlement = Column(Numeric(10, 2), nullable=False)
    total_actual_settlement = Column(Numeric(10, 2), nullable=False)
    amount_pending_settlement = Column(Numeric(10, 2), nullable=False)
    source_file = Column(String, nullable=False)
    ingestion_timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    # Relationships
    order = relationship("Order", back_populates="settlements")
    return_ = relationship("Return", back_populates="settlements")

class OrderStatusHistory(Base):
    """Model for tracking order status changes."""
    __tablename__ = 'order_status_history'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    order_id = Column(UUID(as_uuid=True), ForeignKey('orders.id'), nullable=False)
    status = Column(String, nullable=False)
    changed_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    changed_by = Column(String, nullable=True)  # Could be system or user
    details = Column(Text, nullable=True)
    
    # Relationships
    order = relationship("Order", back_populates="status_history")

class AuditLog(Base):
    """Model for tracking system changes."""
    __tablename__ = 'audit_logs'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    action = Column(String, nullable=False)  # create, update, delete
    entity_type = Column(String, nullable=False)  # order, return, settlement
    entity_id = Column(UUID(as_uuid=True), nullable=False)
    changes = Column(JSON, nullable=True)
    user = Column(String, nullable=True)
    ip_address = Column(String, nullable=True)
    user_agent = Column(String, nullable=True) 