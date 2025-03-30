from contextlib import contextmanager
from typing import List, Any, Callable
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class RollbackManager:
    def __init__(self, session: Session):
        self.session = session
        self.operations: List[Callable] = []
        self.timestamp = datetime.now().isoformat()

    def add_operation(self, operation: Callable, *args, **kwargs):
        """Add an operation to be executed in case of rollback."""
        self.operations.append((operation, args, kwargs))

    @contextmanager
    def transaction(self):
        """Context manager for handling database transactions with rollback capability."""
        try:
            yield self
        except Exception as e:
            logger.error(f"Transaction failed at {self.timestamp}: {str(e)}")
            self.rollback()
            raise
        finally:
            self.operations.clear()

    def rollback(self):
        """Execute rollback operations in reverse order."""
        logger.info(f"Starting rollback for transaction at {self.timestamp}")
        for operation, args, kwargs in reversed(self.operations):
            try:
                operation(*args, **kwargs)
                logger.info(f"Successfully executed rollback operation: {operation.__name__}")
            except Exception as e:
                logger.error(f"Failed to execute rollback operation {operation.__name__}: {str(e)}")
                # Continue with other rollback operations even if one fails

class DatabaseOperation:
    def __init__(self, session: Session):
        self.session = session
        self.rollback_manager = RollbackManager(session)

    def execute_with_rollback(self, operation: Callable, *args, **kwargs) -> Any:
        """Execute a database operation with rollback capability."""
        with self.rollback_manager.transaction():
            try:
                result = operation(*args, **kwargs)
                self.session.commit()
                return result
            except SQLAlchemyError as e:
                self.session.rollback()
                logger.error(f"Database operation failed: {str(e)}")
                raise

    def batch_operation(self, operations: List[Callable], *args, **kwargs) -> List[Any]:
        """Execute multiple database operations with rollback capability."""
        results = []
        with self.rollback_manager.transaction():
            try:
                for operation in operations:
                    result = operation(*args, **kwargs)
                    results.append(result)
                self.session.commit()
                return results
            except SQLAlchemyError as e:
                self.session.rollback()
                logger.error(f"Batch operation failed: {str(e)}")
                raise

    def safe_delete(self, model: Any, id: Any) -> bool:
        """Safely delete a record with rollback capability."""
        def delete_operation():
            record = self.session.query(model).get(id)
            if record:
                self.session.delete(record)
                return True
            return False

        return self.execute_with_rollback(delete_operation)

    def safe_update(self, model: Any, id: Any, update_data: dict) -> Any:
        """Safely update a record with rollback capability."""
        def update_operation():
            record = self.session.query(model).get(id)
            if record:
                for key, value in update_data.items():
                    setattr(record, key, value)
                return record
            return None

        return self.execute_with_rollback(update_operation)

    def safe_bulk_insert(self, model: Any, records: List[dict]) -> List[Any]:
        """Safely insert multiple records with rollback capability."""
        def insert_operation():
            new_records = []
            for record_data in records:
                record = model(**record_data)
                self.session.add(record)
                new_records.append(record)
            return new_records

        return self.execute_with_rollback(insert_operation) 