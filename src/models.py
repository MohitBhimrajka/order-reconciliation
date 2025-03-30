from sqlalchemy import Column, Integer, String, Float, DateTime, Date, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class Order(Base):
    __tablename__ = 'orders'

    id = Column(Integer, primary_key=True)
    order_release_id = Column(String, unique=True, nullable=False)
    order_line_id = Column(String, nullable=False)
    seller_order_id = Column(String, nullable=True)
    created_on = Column(DateTime, nullable=False)
    delivered_on = Column(DateTime, nullable=True)
    cancelled_on = Column(DateTime, nullable=True)
    final_amount = Column(Float, nullable=False)
    total_mrp = Column(Float, nullable=False)
    discount = Column(Float, nullable=False)
    shipping_charge = Column(Float, nullable=False)
    order_status = Column(String, nullable=False)
    payment_type = Column(String, nullable=False)  # prepaid/postpaid
    city = Column(String, nullable=True)
    state = Column(String, nullable=True)
    zipcode = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    returns = relationship("Return", back_populates="order")
    settlements = relationship("Settlement", back_populates="order")

class Return(Base):
    __tablename__ = 'returns'

    id = Column(Integer, primary_key=True)
    order_release_id = Column(String, ForeignKey('orders.order_release_id'), nullable=False)
    order_line_id = Column(String, nullable=False)
    return_type = Column(String, nullable=False)  # return_refund/exchange
    return_date = Column(DateTime, nullable=False)
    packing_date = Column(DateTime, nullable=True)
    delivery_date = Column(DateTime, nullable=True)
    customer_paid_amount = Column(Float, nullable=False)
    prepaid_amount = Column(Float, nullable=False)
    postpaid_amount = Column(Float, nullable=False)
    mrp = Column(Float, nullable=False)
    total_discount_amount = Column(Float, nullable=False)
    total_settlement = Column(Float, nullable=False)
    total_actual_settlement = Column(Float, nullable=False)
    amount_pending_settlement = Column(Float, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    order = relationship("Order", back_populates="returns")

class Settlement(Base):
    __tablename__ = 'settlements'

    id = Column(Integer, primary_key=True)
    order_release_id = Column(String, ForeignKey('orders.order_release_id'), nullable=False)
    order_line_id = Column(String, nullable=False)
    total_expected_settlement = Column(Float, nullable=False)
    total_actual_settlement = Column(Float, nullable=False)
    amount_pending_settlement = Column(Float, nullable=False)
    prepaid_commission_deduction = Column(Float, nullable=False)
    prepaid_logistics_deduction = Column(Float, nullable=False)
    prepaid_payment = Column(Float, nullable=False)
    postpaid_commission_deduction = Column(Float, nullable=False)
    postpaid_logistics_deduction = Column(Float, nullable=False)
    postpaid_payment = Column(Float, nullable=False)
    settlement_status = Column(String, nullable=False)  # completed/partial/pending
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    order = relationship("Order", back_populates="settlements")
    settlement_dates = relationship("SettlementDate", back_populates="settlement")

class SettlementDate(Base):
    __tablename__ = 'settlement_dates'

    id = Column(Integer, primary_key=True)
    settlement_id = Column(Integer, ForeignKey('settlements.id'), nullable=False)
    settlement_date = Column(DateTime, nullable=False)
    settlement_amount = Column(Float, nullable=False)
    bank_utr_no = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    settlement = relationship("Settlement", back_populates="settlement_dates")

class MonthlyReconciliation(Base):
    __tablename__ = 'monthly_reconciliation'

    id = Column(Integer, primary_key=True)
    month = Column(Date, unique=True, nullable=False)
    total_orders = Column(Integer, nullable=False)
    total_returns = Column(Integer, nullable=False)
    total_settlements = Column(Float, nullable=False)
    pending_settlements = Column(Float, nullable=False)
    completed_settlements = Column(Float, nullable=False)
    return_losses = Column(Float, nullable=False)
    net_profit = Column(Float, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow) 