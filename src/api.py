from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from datetime import datetime, date
from typing import List, Optional
from .database import get_db
from .models import Order, Return, Settlement, MonthlyReconciliation
from pydantic import BaseModel
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Reconciliation API")

class OrderBase(BaseModel):
    order_release_id: str
    order_line_id: str
    final_amount: float
    order_status: str
    payment_type: str

class ReturnBase(BaseModel):
    order_release_id: str
    return_type: str
    customer_paid_amount: float
    total_settlement: float
    total_actual_settlement: float

class SettlementBase(BaseModel):
    order_release_id: str
    total_expected_settlement: float
    total_actual_settlement: float
    settlement_status: str

class MonthlyReconciliationBase(BaseModel):
    month: date
    total_orders: int
    total_returns: int
    total_settlements: float
    pending_settlements: float
    completed_settlements: float
    return_losses: float
    net_profit: float

@app.get("/orders/", response_model=List[OrderBase])
def get_orders(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    payment_type: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Get orders with optional filters."""
    query = db.query(Order)
    
    if start_date:
        query = query.filter(Order.created_on >= start_date)
    if end_date:
        query = query.filter(Order.created_on <= end_date)
    if payment_type:
        query = query.filter(Order.payment_type == payment_type)
    
    return query.all()

@app.get("/returns/", response_model=List[ReturnBase])
def get_returns(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    return_type: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Get returns with optional filters."""
    query = db.query(Return)
    
    if start_date:
        query = query.filter(Return.return_date >= start_date)
    if end_date:
        query = query.filter(Return.return_date <= end_date)
    if return_type:
        query = query.filter(Return.return_type == return_type)
    
    return query.all()

@app.get("/settlements/", response_model=List[SettlementBase])
def get_settlements(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    status: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Get settlements with optional filters."""
    query = db.query(Settlement)
    
    if start_date:
        query = query.filter(Settlement.created_at >= start_date)
    if end_date:
        query = query.filter(Settlement.created_at <= end_date)
    if status:
        query = query.filter(Settlement.settlement_status == status)
    
    return query.all()

@app.get("/monthly-reconciliation/", response_model=List[MonthlyReconciliationBase])
def get_monthly_reconciliation(
    start_month: Optional[date] = None,
    end_month: Optional[date] = None,
    db: Session = Depends(get_db)
):
    """Get monthly reconciliation data with optional date range."""
    query = db.query(MonthlyReconciliation)
    
    if start_month:
        query = query.filter(MonthlyReconciliation.month >= start_month)
    if end_month:
        query = query.filter(MonthlyReconciliation.month <= end_month)
    
    return query.all()

@app.get("/reconciliation-summary/")
def get_reconciliation_summary(
    month: Optional[date] = None,
    db: Session = Depends(get_db)
):
    """Get detailed reconciliation summary for a specific month."""
    try:
        if not month:
            month = datetime.now().replace(day=1).date()
        
        # Get monthly reconciliation data
        monthly_data = db.query(MonthlyReconciliation).filter(
            MonthlyReconciliation.month == month
        ).first()
        
        if not monthly_data:
            raise HTTPException(status_code=404, detail="No data found for the specified month")
        
        # Get pending settlements
        pending_settlements = db.query(Settlement).filter(
            Settlement.created_at >= month,
            Settlement.created_at < (month.replace(day=1) + pd.DateOffset(months=1)),
            Settlement.settlement_status != 'completed'
        ).all()
        
        # Get returns with losses
        return_losses = db.query(Return).filter(
            Return.return_date >= month,
            Return.return_date < (month.replace(day=1) + pd.DateOffset(months=1)),
            Return.total_actual_settlement < 0
        ).all()
        
        return {
            "month": monthly_data.month,
            "total_orders": monthly_data.total_orders,
            "total_returns": monthly_data.total_returns,
            "total_settlements": monthly_data.total_settlements,
            "pending_settlements": monthly_data.pending_settlements,
            "completed_settlements": monthly_data.completed_settlements,
            "return_losses": monthly_data.return_losses,
            "net_profit": monthly_data.net_profit,
            "pending_settlements_details": [
                {
                    "order_release_id": s.order_release_id,
                    "amount": s.amount_pending_settlement,
                    "status": s.settlement_status
                }
                for s in pending_settlements
            ],
            "return_losses_details": [
                {
                    "order_release_id": r.order_release_id,
                    "amount": abs(r.total_actual_settlement),
                    "return_type": r.return_type
                }
                for r in return_losses
            ]
        }
    except Exception as e:
        logger.error(f"Error getting reconciliation summary: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) 