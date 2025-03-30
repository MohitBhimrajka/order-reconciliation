from sqlalchemy import Index, text
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
import logging
from typing import List, Dict, Any
from datetime import datetime, timedelta
from src.cache import cache

logger = logging.getLogger(__name__)

class QueryOptimizer:
    def __init__(self, session: Session):
        self.session = session
        self.cache = cache

    def create_indexes(self):
        """Create necessary indexes for performance optimization."""
        try:
            # Create indexes for frequently queried columns
            indexes = [
                # Orders table indexes
                Index('idx_orders_order_release_id', 'order_release_id'),
                Index('idx_orders_created_on', 'created_on'),
                Index('idx_orders_order_status', 'order_status'),
                Index('idx_orders_payment_type', 'payment_type'),
                
                # Returns table indexes
                Index('idx_returns_order_release_id', 'order_release_id'),
                Index('idx_returns_return_date', 'return_date'),
                Index('idx_returns_return_type', 'return_type'),
                
                # Settlements table indexes
                Index('idx_settlements_order_release_id', 'order_release_id'),
                Index('idx_settlements_settlement_status', 'settlement_status'),
                
                # Monthly reconciliation indexes
                Index('idx_monthly_reconciliation_month', 'month'),
            ]
            
            for index in indexes:
                index.create(self.session.bind)
            
            logger.info("Successfully created database indexes")
        except SQLAlchemyError as e:
            logger.error(f"Error creating indexes: {str(e)}")
            raise

    def optimize_query(self, query: str, params: Dict[str, Any] = None) -> str:
        """Optimize SQL query for better performance."""
        try:
            # Add query hints for better execution plan
            optimized_query = query.replace(
                "SELECT",
                "SELECT /*+ INDEX(table_name index_name) */"
            )
            return optimized_query
        except Exception as e:
            logger.error(f"Error optimizing query: {str(e)}")
            return query

    def get_cached_query(self, cache_key: str, query_func: callable, expire: int = 300) -> Any:
        """Get query result from cache or execute query."""
        return self.cache.get_or_set(
            cache_key,
            lambda: query_func(),
            expire
        )

    def batch_process(self, query: str, batch_size: int = 1000) -> List[Dict[str, Any]]:
        """Process large queries in batches."""
        try:
            offset = 0
            results = []
            
            while True:
                batch_query = f"{query} LIMIT {batch_size} OFFSET {offset}"
                batch_results = self.session.execute(text(batch_query)).fetchall()
                
                if not batch_results:
                    break
                    
                results.extend([dict(row) for row in batch_results])
                offset += batch_size
                
            return results
        except SQLAlchemyError as e:
            logger.error(f"Error in batch processing: {str(e)}")
            raise

    def analyze_table(self, table_name: str) -> Dict[str, Any]:
        """Analyze table statistics for optimization."""
        try:
            query = f"""
            SELECT 
                relname as table_name,
                n_live_tup as row_count,
                n_dead_tup as dead_tuples,
                last_vacuum as last_vacuum,
                last_autovacuum as last_autovacuum,
                last_analyze as last_analyze,
                last_autoanalyze as last_autoanalyze
            FROM pg_stat_user_tables 
            WHERE relname = :table_name
            """
            
            result = self.session.execute(text(query), {"table_name": table_name}).first()
            return dict(result) if result else {}
        except SQLAlchemyError as e:
            logger.error(f"Error analyzing table: {str(e)}")
            raise

    def vacuum_analyze(self, table_name: str):
        """Perform VACUUM ANALYZE on table."""
        try:
            query = f"VACUUM ANALYZE {table_name}"
            self.session.execute(text(query))
            self.session.commit()
            logger.info(f"Successfully performed VACUUM ANALYZE on {table_name}")
        except SQLAlchemyError as e:
            logger.error(f"Error performing VACUUM ANALYZE: {str(e)}")
            raise

    def get_query_stats(self, query: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Get query execution statistics."""
        try:
            # Enable query statistics
            self.session.execute(text("SET track_activity_query_size = 0"))
            
            # Execute query with EXPLAIN ANALYZE
            explain_query = f"EXPLAIN (ANALYZE, BUFFERS) {query}"
            result = self.session.execute(text(explain_query), params or {}).fetchall()
            
            return {
                "plan": "\n".join(row[0] for row in result),
                "execution_time": float(result[-1][0].split("Execution time: ")[1].split(" ms")[0])
            }
        except SQLAlchemyError as e:
            logger.error(f"Error getting query stats: {str(e)}")
            raise

    def optimize_table(self, table_name: str):
        """Optimize table for better performance."""
        try:
            # Analyze table statistics
            self.session.execute(text(f"ANALYZE {table_name}"))
            
            # Reindex table
            self.session.execute(text(f"REINDEX TABLE {table_name}"))
            
            # Update table statistics
            self.session.execute(text(f"UPDATE pg_stat_user_tables SET last_analyze = NOW() WHERE relname = :table_name"),
                               {"table_name": table_name})
            
            self.session.commit()
            logger.info(f"Successfully optimized table {table_name}")
        except SQLAlchemyError as e:
            logger.error(f"Error optimizing table: {str(e)}")
            raise 