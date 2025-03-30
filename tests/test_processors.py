"""
Tests for data processing functions.
"""
import pytest
from datetime import datetime
from sqlalchemy.orm import Session

from src.processors import (
    process_orders,
    process_returns,
    process_settlements,
    process_files,
    calculate_monthly_reconciliation,
    analyze_settlements
)

@pytest.mark.db
def test_process_orders(db_session: Session):
    """Test processing orders data."""
    # Test data
    orders_data = [
        {
            "order_release_id": "TEST-001",
            "order_status": "Completed",
            "created_on": datetime.now(),
            "final_amount": 1000.00,
            "source_file": "orders-03-2024.csv"
        }
    ]
    
    # Process orders
    process_orders(db_session, orders_data)
    
    # Verify
    order = db_session.query(Order).filter_by(order_release_id="TEST-001").first()
    assert order is not None
    assert order.order_status == "Completed"
    assert order.final_amount == 1000.00

@pytest.mark.db
def test_process_returns(db_session: Session):
    """Test processing returns data."""
    # Test data
    returns_data = [
        {
            "order_release_id": "TEST-001",
            "return_date": datetime.now(),
            "total_settlement": 500.00,
            "source_file": "returns-03-2024.csv"
        }
    ]
    
    # Process returns
    process_returns(db_session, returns_data)
    
    # Verify
    return_record = db_session.query(Return).filter_by(order_release_id="TEST-001").first()
    assert return_record is not None
    assert return_record.total_settlement == 500.00

@pytest.mark.db
def test_process_settlements(db_session: Session):
    """Test processing settlements data."""
    # Test data
    settlements_data = [
        {
            "order_release_id": "TEST-001",
            "status": "settled",
            "total_actual_settlement": 1000.00,
            "created_at": datetime.now(),
            "source_file": "settlements-03-2024.csv"
        }
    ]
    
    # Process settlements
    process_settlements(db_session, settlements_data)
    
    # Verify
    settlement = db_session.query(Settlement).filter_by(order_release_id="TEST-001").first()
    assert settlement is not None
    assert settlement.status == "settled"
    assert settlement.total_actual_settlement == 1000.00

@pytest.mark.db
def test_process_files(db_session: Session):
    """Test processing all files together."""
    # Test data
    orders_data = [
        {
            "order_release_id": "TEST-001",
            "order_status": "Completed",
            "created_on": datetime.now(),
            "final_amount": 1000.00,
            "source_file": "orders-03-2024.csv"
        }
    ]
    
    returns_data = [
        {
            "order_release_id": "TEST-001",
            "return_date": datetime.now(),
            "total_settlement": 500.00,
            "source_file": "returns-03-2024.csv"
        }
    ]
    
    settlements_data = [
        {
            "order_release_id": "TEST-001",
            "status": "settled",
            "total_actual_settlement": 1000.00,
            "created_at": datetime.now(),
            "source_file": "settlements-03-2024.csv"
        }
    ]
    
    # Process all files
    process_files(db_session, orders_data, returns_data, settlements_data)
    
    # Verify
    order = db_session.query(Order).filter_by(order_release_id="TEST-001").first()
    assert order is not None
    
    return_record = db_session.query(Return).filter_by(order_release_id="TEST-001").first()
    assert return_record is not None
    
    settlement = db_session.query(Settlement).filter_by(order_release_id="TEST-001").first()
    assert settlement is not None

@pytest.mark.db
def test_calculate_monthly_reconciliation(db_session: Session):
    """Test calculating monthly reconciliation."""
    # Create test data
    order = Order(
        order_release_id="TEST-001",
        order_status="Completed",
        created_on=datetime.now(),
        final_amount=1000.00
    )
    db_session.add(order)
    
    settlement = Settlement(
        order_release_id="TEST-001",
        status="settled",
        total_actual_settlement=1000.00,
        created_at=datetime.now()
    )
    db_session.add(settlement)
    
    db_session.commit()
    
    # Calculate reconciliation
    result = calculate_monthly_reconciliation(db_session)
    assert len(result) > 0
    assert "total_orders" in result[0]
    assert "total_settled" in result[0]
    assert "net_profit_loss" in result[0]

@pytest.mark.db
def test_analyze_settlements(db_session: Session):
    """Test analyzing settlements."""
    # Create test data
    order = Order(
        order_release_id="TEST-001",
        order_status="Completed",
        created_on=datetime.now(),
        final_amount=1000.00
    )
    db_session.add(order)
    
    settlement = Settlement(
        order_release_id="TEST-001",
        status="settled",
        total_actual_settlement=1000.00,
        created_at=datetime.now()
    )
    db_session.add(settlement)
    
    db_session.commit()
    
    # Analyze settlements
    result = analyze_settlements(db_session)
    assert "total_settlements" in result
    assert "settlement_rate" in result
    assert "avg_settlement_time" in result 