"""
Database initialization script.
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.database.config import DATABASE_URL
from src.database.models import Base

def init_db():
    """Initialize the database by creating all tables."""
    # Create engine
    engine = create_engine(DATABASE_URL)
    
    # Create all tables
    Base.metadata.create_all(engine)
    
    # Create session factory
    Session = sessionmaker(bind=engine)
    
    return Session()

if __name__ == "__main__":
    print("Initializing database...")
    init_db()
    print("Database initialization complete!") 