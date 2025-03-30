from sqlalchemy import Column, Integer, String, Float, DateTime, Date, ForeignKey, UniqueConstraint, Numeric, func
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
    order_release_id = Column(String(255), ForeignKey('orders.order_release_id', ondelete='CASCADE'), nullable=False)
    settlement_date = Column(Date, nullable=False)
    settlement_status = Column(String(50), nullable=False)
    amount_settled = Column(Numeric(10, 2), nullable=False)
    amount_pending = Column(Numeric(10, 2), nullable=False)
    month = Column(String(7), nullable=False)  # Format: YYYY-MM
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    order = relationship("Order", back_populates="settlements")
    history = relationship("SettlementHistory", back_populates="settlement", cascade="all, delete-orphan")
    
    def update_settlement(self, amount_settled: float, status: str):
        """Update settlement and create history record."""
        self.amount_settled = amount_settled
        self.amount_pending = self.order.final_amount - amount_settled
        self.settlement_status = status
        self.updated_at = datetime.utcnow()
        
        # Create history record
        history = SettlementHistory(
            order_release_id=self.order_release_id,
            settlement_date=self.settlement_date,
            settlement_status=status,
            amount_settled=amount_settled,
            amount_pending=self.amount_pending,
            month=self.month
        )
        self.history.append(history)
    
    @classmethod
    def get_pending_settlements(cls, session, month: str = None):
        """Get all pending settlements, optionally filtered by month."""
        query = session.query(cls).filter(cls.settlement_status == 'pending')
        if month:
            query = query.filter(cls.month == month)
        return query.all()
    
    @classmethod
    def get_settlement_history(cls, session, order_release_id: str):
        """Get settlement history for a specific order."""
        return session.query(SettlementHistory).filter(
            SettlementHistory.order_release_id == order_release_id
        ).order_by(SettlementHistory.created_at.desc()).all()
    
    @classmethod
    def get_settlement_stats(cls, session, month: str):
        """Get settlement statistics for a specific month."""
        return session.query(
            func.count(cls.id).label('total_settlements'),
            func.sum(cls.amount_settled).label('total_settled'),
            func.sum(cls.amount_pending).label('total_pending'),
            func.count(cls.id).filter(cls.settlement_status == 'completed').label('completed_settlements'),
            func.count(cls.id).filter(cls.settlement_status == 'partial').label('partial_settlements'),
            func.count(cls.id).filter(cls.settlement_status == 'pending').label('pending_settlements')
        ).filter(cls.month == month).first()

class SettlementHistory(Base):
    __tablename__ = 'settlement_history'
    
    id = Column(Integer, primary_key=True)
    order_release_id = Column(String(255), ForeignKey('orders.order_release_id', ondelete='CASCADE'), nullable=False)
    settlement_date = Column(Date, nullable=False)
    settlement_status = Column(String(50), nullable=False)
    amount_settled = Column(Numeric(10, 2), nullable=False)
    amount_pending = Column(Numeric(10, 2), nullable=False)
    month = Column(String(7), nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    settlement = relationship("Settlement", back_populates="history")
    
    @classmethod
    def get_settlement_trends(cls, session, start_month: str, end_month: str):
        """Get settlement trends between two months."""
        return session.query(
            cls.month,
            func.count(cls.id).label('total_settlements'),
            func.sum(cls.amount_settled).label('total_settled'),
            func.sum(cls.amount_pending).label('total_pending'),
            func.count(cls.id).filter(cls.settlement_status == 'completed').label('completed_settlements'),
            func.count(cls.id).filter(cls.settlement_status == 'partial').label('partial_settlements'),
            func.count(cls.id).filter(cls.settlement_status == 'pending').label('pending_settlements')
        ).filter(
            cls.month >= start_month,
            cls.month <= end_month
        ).group_by(cls.month).order_by(cls.month).all()

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