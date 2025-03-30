"""
API endpoint tests.
"""
import pytest
from datetime import datetime, timedelta
from fastapi.testclient import TestClient

from src.database.models import Order, Return, Settlement

@pytest.mark.api
def test_get_orders(client: TestClient, db_session):
    """Test getting orders endpoint."""
    # Create test order
    order = Order(
        order_release_id="TEST-001",
        order_status="Completed",
        created_on=datetime.now(),
        final_amount=1000.00
    )
    db_session.add(order)
    db_session.commit()
    
    # Test endpoint
    response = client.get("/api/orders/")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["order_release_id"] == "TEST-001"

@pytest.mark.api
def test_get_returns(client: TestClient, db_session):
    """Test getting returns endpoint."""
    # Create test return
    return_record = Return(
        order_release_id="TEST-001",
        return_date=datetime.now(),
        total_settlement=500.00
    )
    db_session.add(return_record)
    db_session.commit()
    
    # Test endpoint
    response = client.get("/api/returns/")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["order_release_id"] == "TEST-001"

@pytest.mark.api
def test_get_settlements(client: TestClient, db_session):
    """Test getting settlements endpoint."""
    # Create test settlement
    settlement = Settlement(
        order_release_id="TEST-001",
        status="settled",
        total_actual_settlement=1000.00,
        created_at=datetime.now()
    )
    db_session.add(settlement)
    db_session.commit()
    
    # Test endpoint
    response = client.get("/api/settlements/")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["order_release_id"] == "TEST-001"

@pytest.mark.api
def test_get_monthly_reconciliation(client: TestClient, db_session):
    """Test getting monthly reconciliation endpoint."""
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
    
    # Test endpoint
    response = client.get("/api/monthly-reconciliation/")
    assert response.status_code == 200
    data = response.json()
    assert len(data) > 0
    assert "total_orders" in data[0]
    assert "total_settled" in data[0]

@pytest.mark.api
def test_get_reconciliation_summary(client: TestClient, db_session):
    """Test getting reconciliation summary endpoint."""
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
    
    # Test endpoint
    response = client.get("/api/reconciliation-summary/")
    assert response.status_code == 200
    data = response.json()
    assert "total_orders" in data
    assert "total_settled" in data
    assert "net_profit_loss" in data 