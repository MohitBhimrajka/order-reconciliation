from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from datetime import datetime, date
from typing import List, Optional, Dict, Any
from .database import get_db
from .models import Order, Return, Settlement, SettlementHistory, MonthlyReconciliation
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

class SettlementHistoryBase(BaseModel):
    order_release_id: str
    settlement_date: date
    settlement_status: str
    amount_settled: float
    amount_pending: float
    month: str
    created_at: datetime
    updated_at: datetime

class SettlementTrend(BaseModel):
    month: str
    total_settlements: int
    total_settled: float
    total_pending: float
    completed_settlements: int
    partial_settlements: int
    pending_settlements: int

class SettlementAnalysis(BaseModel):
    total_settlements: int
    completed_settlements: int
    partial_settlements: int
    pending_settlements: int
    total_amount_settled: float
    total_amount_pending: float
    completion_rate: float
    amount_completion_rate: float
    avg_settlement_time: float
    pending_from_previous: int
    trends: List[SettlementTrend]

class PendingSettlement(BaseModel):
    order_release_id: str
    settlement_date: date
    expected_amount: float
    amount_settled: float
    amount_pending: float
    days_pending: int
    status: str

class OrderMetrics(BaseModel):
    total_orders: int
    total_amount: float
    average_order_value: float
    orders_by_status: Dict[str, int]
    orders_by_payment_type: Dict[str, int]
    orders_by_month: Dict[str, int]

class OrderSearchParams(BaseModel):
    search_term: Optional[str] = None
    status: Optional[str] = None
    payment_type: Optional[str] = None
    date_range: Optional[Dict[str, date]] = None
    amount_range: Optional[Dict[str, float]] = None
    warehouse_id: Optional[str] = None
    seller_id: Optional[str] = None

class ReturnMetrics(BaseModel):
    total_returns: int
    total_amount: float
    returns_by_type: Dict[str, int]
    returns_by_month: Dict[str, int]
    average_return_value: float
    return_rate: float

class ReturnAnalysis(BaseModel):
    month: str
    total_returns: int
    total_amount: float
    returns_by_type: Dict[str, int]
    return_rate: float
    average_processing_time: float

class DataQualityMetrics(BaseModel):
    total_records: int
    valid_records: int
    invalid_records: int
    validation_errors: Dict[str, int]
    completeness_score: float
    accuracy_score: float

class DataQualityIssue(BaseModel):
    id: str
    type: str
    severity: str
    description: str
    affected_records: int
    created_at: datetime
    status: str

class ValidationRule(BaseModel):
    id: str
    name: str
    description: str
    field: str
    rule_type: str
    parameters: Dict[str, Any]
    is_active: bool

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

@app.get("/settlements/history/{order_release_id}", response_model=List[SettlementHistoryBase])
def get_settlement_history(
    order_release_id: str,
    db: Session = Depends(get_db)
):
    """Get settlement history for a specific order."""
    try:
        history = Settlement.get_settlement_history(db, order_release_id)
        return history
    except Exception as e:
        logger.error(f"Error getting settlement history: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/settlements/trends", response_model=List[SettlementTrend])
def get_settlement_trends(
    start_month: str,
    end_month: str,
    db: Session = Depends(get_db)
):
    """Get settlement trends between two months."""
    try:
        trends = SettlementHistory.get_settlement_trends(db, start_month, end_month)
        return trends
    except Exception as e:
        logger.error(f"Error getting settlement trends: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/settlements/analysis/{month}", response_model=SettlementAnalysis)
def get_settlement_analysis(
    month: str,
    db: Session = Depends(get_db)
):
    """Get detailed settlement analysis for a specific month."""
    try:
        analysis = analyze_settlements(db, month)
        return analysis
    except Exception as e:
        logger.error(f"Error getting settlement analysis: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/settlements/pending", response_model=List[PendingSettlement])
def get_pending_settlements(
    month: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Get all pending settlements, optionally filtered by month."""
    try:
        pending = Settlement.get_pending_settlements(db, month)
        return [{
            'order_release_id': s.order_release_id,
            'settlement_date': s.settlement_date,
            'expected_amount': float(s.order.final_amount),
            'amount_settled': float(s.amount_settled),
            'amount_pending': float(s.amount_pending),
            'days_pending': (datetime.now().date() - s.settlement_date).days,
            'status': s.settlement_status
        } for s in pending]
    except Exception as e:
        logger.error(f"Error getting pending settlements: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/settlements/stats/{month}")
def get_settlement_stats(
    month: str,
    db: Session = Depends(get_db)
):
    """Get settlement statistics for a specific month."""
    try:
        stats = Settlement.get_settlement_stats(db, month)
        return {
            'total_settlements': stats.total_settlements,
            'total_settled': float(stats.total_settled),
            'total_pending': float(stats.total_pending),
            'completed_settlements': stats.completed_settlements,
            'partial_settlements': stats.partial_settlements,
            'pending_settlements': stats.pending_settlements
        }
    except Exception as e:
        logger.error(f"Error getting settlement stats: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/orders/{order_release_id}", response_model=OrderBase)
def get_order(
    order_release_id: str,
    db: Session = Depends(get_db)
):
    """Get details of a specific order."""
    try:
        order = db.query(Order).filter(Order.order_release_id == order_release_id).first()
        if not order:
            raise HTTPException(status_code=404, detail="Order not found")
        return order
    except Exception as e:
        logger.error(f"Error getting order details: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/orders/metrics", response_model=OrderMetrics)
def get_order_metrics(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: Session = Depends(get_db)
):
    """Get order metrics for dashboard."""
    try:
        query = db.query(Order)
        if start_date:
            query = query.filter(Order.created_on >= start_date)
        if end_date:
            query = query.filter(Order.created_on <= end_date)
        
        orders = query.all()
        
        # Calculate metrics
        total_orders = len(orders)
        total_amount = sum(o.final_amount for o in orders)
        average_order_value = total_amount / total_orders if total_orders > 0 else 0
        
        # Status distribution
        orders_by_status = {}
        for order in orders:
            orders_by_status[order.order_status] = orders_by_status.get(order.order_status, 0) + 1
        
        # Payment type distribution
        orders_by_payment_type = {}
        for order in orders:
            orders_by_payment_type[order.payment_type] = orders_by_payment_type.get(order.payment_type, 0) + 1
        
        # Monthly distribution
        orders_by_month = {}
        for order in orders:
            month = order.created_on.strftime('%Y-%m')
            orders_by_month[month] = orders_by_month.get(month, 0) + 1
        
        return {
            'total_orders': total_orders,
            'total_amount': total_amount,
            'average_order_value': average_order_value,
            'orders_by_status': orders_by_status,
            'orders_by_payment_type': orders_by_payment_type,
            'orders_by_month': orders_by_month
        }
    except Exception as e:
        logger.error(f"Error getting order metrics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/orders/search", response_model=List[OrderBase])
def search_orders(
    params: OrderSearchParams,
    db: Session = Depends(get_db)
):
    """Advanced order search with multiple filters."""
    try:
        query = db.query(Order)
        
        if params.search_term:
            query = query.filter(
                (Order.order_release_id.ilike(f"%{params.search_term}%")) |
                (Order.customer_name.ilike(f"%{params.search_term}%")) |
                (Order.item_name.ilike(f"%{params.search_term}%"))
            )
        
        if params.status:
            query = query.filter(Order.order_status == params.status)
        
        if params.payment_type:
            query = query.filter(Order.payment_type == params.payment_type)
        
        if params.date_range:
            if 'start' in params.date_range:
                query = query.filter(Order.created_on >= params.date_range['start'])
            if 'end' in params.date_range:
                query = query.filter(Order.created_on <= params.date_range['end'])
        
        if params.amount_range:
            if 'min' in params.amount_range:
                query = query.filter(Order.final_amount >= params.amount_range['min'])
            if 'max' in params.amount_range:
                query = query.filter(Order.final_amount <= params.amount_range['max'])
        
        if params.warehouse_id:
            query = query.filter(Order.warehouse_id == params.warehouse_id)
        
        if params.seller_id:
            query = query.filter(Order.seller_id == params.seller_id)
        
        return query.all()
    except Exception as e:
        logger.error(f"Error searching orders: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/returns/{order_release_id}", response_model=ReturnBase)
def get_return(
    order_release_id: str,
    db: Session = Depends(get_db)
):
    """Get details of a specific return."""
    try:
        return_record = db.query(Return).filter(Return.order_release_id == order_release_id).first()
        if not return_record:
            raise HTTPException(status_code=404, detail="Return not found")
        return return_record
    except Exception as e:
        logger.error(f"Error getting return details: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/returns/metrics", response_model=ReturnMetrics)
def get_return_metrics(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: Session = Depends(get_db)
):
    """Get return metrics for dashboard."""
    try:
        query = db.query(Return)
        if start_date:
            query = query.filter(Return.return_date >= start_date)
        if end_date:
            query = query.filter(Return.return_date <= end_date)
        
        returns = query.all()
        
        # Calculate metrics
        total_returns = len(returns)
        total_amount = sum(r.total_actual_settlement for r in returns)
        average_return_value = total_amount / total_returns if total_returns > 0 else 0
        
        # Type distribution
        returns_by_type = {}
        for r in returns:
            returns_by_type[r.return_type] = returns_by_type.get(r.return_type, 0) + 1
        
        # Monthly distribution
        returns_by_month = {}
        for r in returns:
            month = r.return_date.strftime('%Y-%m')
            returns_by_month[month] = returns_by_month.get(month, 0) + 1
        
        # Calculate return rate
        total_orders = db.query(Order).count()
        return_rate = (total_returns / total_orders * 100) if total_orders > 0 else 0
        
        return {
            'total_returns': total_returns,
            'total_amount': total_amount,
            'returns_by_type': returns_by_type,
            'returns_by_month': returns_by_month,
            'average_return_value': average_return_value,
            'return_rate': return_rate
        }
    except Exception as e:
        logger.error(f"Error getting return metrics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/returns/analysis/{month}", response_model=ReturnAnalysis)
def get_return_analysis(
    month: str,
    db: Session = Depends(get_db)
):
    """Get return analysis for a specific month."""
    try:
        # Get returns for the month
        returns = db.query(Return).filter(
            Return.return_date >= datetime.strptime(month, '%Y-%m').date(),
            Return.return_date < (datetime.strptime(month, '%Y-%m') + pd.DateOffset(months=1)).date()
        ).all()
        
        # Calculate metrics
        total_returns = len(returns)
        total_amount = sum(r.total_actual_settlement for r in returns)
        
        # Type distribution
        returns_by_type = {}
        for r in returns:
            returns_by_type[r.return_type] = returns_by_type.get(r.return_type, 0) + 1
        
        # Calculate return rate
        total_orders = db.query(Order).filter(
            Order.created_on >= datetime.strptime(month, '%Y-%m').date(),
            Order.created_on < (datetime.strptime(month, '%Y-%m') + pd.DateOffset(months=1)).date()
        ).count()
        return_rate = (total_returns / total_orders * 100) if total_orders > 0 else 0
        
        # Calculate average processing time
        processing_times = []
        for r in returns:
            if r.return_date and r.created_at:
                processing_times.append((r.return_date - r.created_at.date()).days)
        avg_processing_time = sum(processing_times) / len(processing_times) if processing_times else 0
        
        return {
            'month': month,
            'total_returns': total_returns,
            'total_amount': total_amount,
            'returns_by_type': returns_by_type,
            'return_rate': return_rate,
            'average_processing_time': avg_processing_time
        }
    except Exception as e:
        logger.error(f"Error getting return analysis: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/data-quality/metrics", response_model=DataQualityMetrics)
def get_data_quality_metrics(
    db: Session = Depends(get_db)
):
    """Get data quality metrics."""
    try:
        # Get total records
        total_orders = db.query(Order).count()
        total_returns = db.query(Return).count()
        total_settlements = db.query(Settlement).count()
        total_records = total_orders + total_returns + total_settlements
        
        # Get validation results
        validation_errors = {
            'missing_required_fields': 0,
            'invalid_data_types': 0,
            'out_of_range_values': 0,
            'duplicate_records': 0
        }
        
        # Calculate completeness score
        valid_records = total_records - sum(validation_errors.values())
        completeness_score = (valid_records / total_records * 100) if total_records > 0 else 0
        
        # Calculate accuracy score (placeholder)
        accuracy_score = 95.0  # This should be calculated based on actual validation rules
        
        return {
            'total_records': total_records,
            'valid_records': valid_records,
            'invalid_records': sum(validation_errors.values()),
            'validation_errors': validation_errors,
            'completeness_score': completeness_score,
            'accuracy_score': accuracy_score
        }
    except Exception as e:
        logger.error(f"Error getting data quality metrics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/data-quality/issues", response_model=List[DataQualityIssue])
def get_data_quality_issues(
    severity: Optional[str] = None,
    status: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Get data quality issues with optional filters."""
    try:
        # This is a placeholder - in a real implementation, you would query a data_quality_issues table
        issues = []
        return issues
    except Exception as e:
        logger.error(f"Error getting data quality issues: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/data-quality/validation-rules", response_model=List[ValidationRule])
def get_validation_rules(
    is_active: Optional[bool] = None,
    db: Session = Depends(get_db)
):
    """Get validation rules with optional active status filter."""
    try:
        # This is a placeholder - in a real implementation, you would query a validation_rules table
        rules = []
        return rules
    except Exception as e:
        logger.error(f"Error getting validation rules: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) 