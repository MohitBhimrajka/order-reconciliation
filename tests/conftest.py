"""
Test configuration and fixtures.
"""
import os
import pytest
from typing import Generator
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from fastapi.testclient import TestClient

from src.database.models import Base
from src.api import app
from src.database.config import get_db

# Test database URL
TEST_DATABASE_URL = "postgresql://test_user:test_password@localhost:5432/test_reconciliation_db"

@pytest.fixture(scope="session")
def test_db_engine():
    """Create test database engine."""
    engine = create_engine(TEST_DATABASE_URL)
    Base.metadata.create_all(bind=engine)
    yield engine
    Base.metadata.drop_all(bind=engine)

@pytest.fixture(scope="function")
def db_session(test_db_engine) -> Generator[Session, None, None]:
    """Create a fresh database session for each test."""
    connection = test_db_engine.connect()
    transaction = connection.begin()
    session = sessionmaker(bind=connection)()
    
    yield session
    
    session.close()
    transaction.rollback()
    connection.close()

@pytest.fixture(scope="function")
def client(db_session) -> Generator[TestClient, None, None]:
    """Create a test client with a test database session."""
    def override_get_db():
        try:
            yield db_session
        finally:
            pass
    
    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()

@pytest.fixture(scope="function")
def test_env():
    """Set up test environment variables."""
    os.environ["APP_ENV"] = "test"
    os.environ["APP_DEBUG"] = "True"
    os.environ["DB_URL"] = TEST_DATABASE_URL
    yield
    # Clean up environment variables if needed
    os.environ.pop("APP_ENV", None)
    os.environ.pop("APP_DEBUG", None)
    os.environ.pop("DB_URL", None) 