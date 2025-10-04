import frappe
import time
import threading
from contextlib import contextmanager
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from enum import Enum

class ConnectionState(Enum):
    """Database connection states"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FAILED = "failed"
    RECONNECTING = "reconnecting"

@dataclass
class ConnectionMetrics:
    """Connection performance metrics"""
    active_connections: int = 0
    failed_attempts: int = 0
    last_error: Optional[str] = None
    last_error_time: Optional[float] = None
    reconnect_attempts: int = 0
    total_queries: int = 0
    slow_queries: int = 0
    connection_pool_size: int = 0

class DatabaseConnectionManager:
    """Enhanced database connection management with monitoring and recovery"""
    
    def __init__(self, max_retries: int = 3, retry_delay: float = 1.0, 
                 slow_query_threshold: float = 5.0):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.slow_query_threshold = slow_query_threshold
        self.metrics = ConnectionMetrics()
        self._lock = threading.Lock()
        self._connection_state = ConnectionState.HEALTHY
        self.logger = frappe.logger()
    
    @contextmanager
    def managed_connection(self, auto_retry: bool = True):
        """Context manager for database connections with automatic retry"""
        connection = None
        start_time = time.time()
        
        try:
            connection = self._get_connection_with_retry() if auto_retry else self._get_connection()
            
            with self._lock:
                self.metrics.active_connections += 1
            
            yield connection
            
        except Exception as e:
            self._handle_connection_error(e)
            raise
        
        finally:
            query_time = time.time() - start_time
            
            with self._lock:
                self.metrics.active_connections = max(0, self.metrics.active_connections - 1)
                self.metrics.total_queries += 1
                
                if query_time > self.slow_query_threshold:
                    self.metrics.slow_queries += 1
                    self.logger.warning(f"Slow query detected: {query_time:.2f}s")
            
            if connection:
                self._close_connection_safely(connection)
    
    def _get_connection_with_retry(self):
        """Get database connection with retry logic"""
        for attempt in range(self.max_retries + 1):
            try:
                return self._get_connection()
            
            except Exception as e:
                with self._lock:
                    self.metrics.failed_attempts += 1
                    self.metrics.last_error = str(e)
                    self.metrics.last_error_time = time.time()
                
                if attempt == self.max_retries:
                    self._connection_state = ConnectionState.FAILED
                    self.logger.error(f"Failed to connect to database after {self.max_retries + 1} attempts: {str(e)}")
                    raise
                
                self._connection_state = ConnectionState.RECONNECTING
                self.logger.warning(f"Database connection attempt {attempt + 1} failed, retrying in {self.retry_delay}s: {str(e)}")
                
                with self._lock:
                    self.metrics.reconnect_attempts += 1
                
                time.sleep(self.retry_delay * (attempt + 1))  # Exponential backoff
        
        self._connection_state = ConnectionState.HEALTHY
        return self._get_connection()
    
    def _get_connection(self):
        """Get a database connection"""
        try:
            # Use Frappe's database connection
            if not frappe.db:
                frappe.connect()
            
            # Test the connection
            frappe.db.sql("SELECT 1", as_dict=True)
            
            return frappe.db
        
        except Exception as e:
            self.logger.error(f"Failed to establish database connection: {str(e)}")
            raise
    
    def _close_connection_safely(self, connection):
        """Safely close database connection"""
        try:
            if hasattr(connection, 'commit'):
                connection.commit()
        except Exception as e:
            self.logger.warning(f"Error during connection cleanup: {str(e)}")
    
    def _handle_connection_error(self, error: Exception):
        """Handle connection errors and update state"""
        with self._lock:
            self.metrics.failed_attempts += 1
            self.metrics.last_error = str(error)
            self.metrics.last_error_time = time.time()
        
        # Determine connection state based on error
        error_str = str(error).lower()
        if any(keyword in error_str for keyword in ['timeout', 'connection lost', 'server has gone away']):
            self._connection_state = ConnectionState.DEGRADED
        else:
            self._connection_state = ConnectionState.FAILED
        
        self.logger.error(f"Database connection error: {str(error)}")
    
    def check_connection_health(self) -> Dict[str, Any]:
        """Check database connection health and return metrics"""
        try:
            start_time = time.time()
            
            with self.managed_connection(auto_retry=False) as db:
                # Test basic connectivity
                db.sql("SELECT 1 as test", as_dict=True)
                
                # Test write capability
                db.sql("SELECT NOW() as current_time", as_dict=True)
            
            response_time = time.time() - start_time
            
            # Update connection state based on response time
            if response_time < 1.0:
                self._connection_state = ConnectionState.HEALTHY
            elif response_time < 5.0:
                self._connection_state = ConnectionState.DEGRADED
            else:
                self._connection_state = ConnectionState.FAILED
            
            return {
                'status': self._connection_state.value,
                'response_time': response_time,
                'metrics': {
                    'active_connections': self.metrics.active_connections,
                    'failed_attempts': self.metrics.failed_attempts,
                    'reconnect_attempts': self.metrics.reconnect_attempts,
                    'total_queries': self.metrics.total_queries,
                    'slow_queries': self.metrics.slow_queries,
                    'last_error': self.metrics.last_error,
                    'last_error_time': self.metrics.last_error_time
                }
            }
        
        except Exception as e:
            self._handle_connection_error(e)
            return {
                'status': ConnectionState.FAILED.value,
                'error': str(e),
                'metrics': {
                    'active_connections': self.metrics.active_connections,
                    'failed_attempts': self.metrics.failed_attempts,
                    'reconnect_attempts': self.metrics.reconnect_attempts,
                    'total_queries': self.metrics.total_queries,
                    'slow_queries': self.metrics.slow_queries,
                    'last_error': self.metrics.last_error,
                    'last_error_time': self.metrics.last_error_time
                }
            }
    
    def execute_safe_query(self, query: str, values: tuple = None, as_dict: bool = True, 
                          auto_retry: bool = True) -> List[Dict[str, Any]]:
        """Execute query with connection management and error handling"""
        with self.managed_connection(auto_retry=auto_retry) as db:
            return db.sql(query, values, as_dict=as_dict)
    
    def execute_safe_insert(self, doctype: str, doc_data: Dict[str, Any], 
                           auto_retry: bool = True) -> str:
        """Execute insert with connection management"""
        with self.managed_connection(auto_retry=auto_retry) as db:
            doc = frappe.get_doc(doc_data)
            doc.insert()
            return doc.name
    
    def execute_safe_update(self, doctype: str, name: str, update_data: Dict[str, Any],
                           auto_retry: bool = True):
        """Execute update with connection management"""
        with self.managed_connection(auto_retry=auto_retry) as db:
            doc = frappe.get_doc(doctype, name)
            for field, value in update_data.items():
                setattr(doc, field, value)
            doc.save()
    
    def get_connection_metrics(self) -> ConnectionMetrics:
        """Get current connection metrics"""
        return self.metrics
    
    def reset_metrics(self):
        """Reset connection metrics"""
        with self._lock:
            self.metrics = ConnectionMetrics()
            self._connection_state = ConnectionState.HEALTHY

class DatabaseTransactionManager:
    """Transaction management with rollback support"""
    
    def __init__(self, connection_manager: DatabaseConnectionManager):
        self.connection_manager = connection_manager
        self.logger = frappe.logger()
    
    @contextmanager
    def transaction(self, auto_retry: bool = True):
        """Context manager for database transactions"""
        with self.connection_manager.managed_connection(auto_retry=auto_retry) as db:
            savepoint_name = f"migration_sp_{int(time.time())}"
            
            try:
                # Create savepoint
                db.sql(f"SAVEPOINT {savepoint_name}")
                
                yield db
                
                # Commit transaction
                db.commit()
                
            except Exception as e:
                self.logger.error(f"Transaction failed, rolling back to savepoint {savepoint_name}: {str(e)}")
                try:
                    db.sql(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                except Exception as rollback_error:
                    self.logger.error(f"Rollback failed: {str(rollback_error)}")
                    db.rollback()  # Full rollback as fallback
                raise
            
            finally:
                try:
                    db.sql(f"RELEASE SAVEPOINT {savepoint_name}")
                except Exception as cleanup_error:
                    self.logger.warning(f"Failed to release savepoint: {str(cleanup_error)}")

# Global connection manager instance
connection_manager = DatabaseConnectionManager()
transaction_manager = DatabaseTransactionManager(connection_manager)

# Convenience functions
def get_connection_manager() -> DatabaseConnectionManager:
    """Get the global connection manager"""
    return connection_manager

def execute_with_retry(query: str, values: tuple = None, as_dict: bool = True) -> List[Dict[str, Any]]:
    """Execute query with automatic retry"""
    return connection_manager.execute_safe_query(query, values, as_dict)

def check_database_health() -> Dict[str, Any]:
    """Check database health"""
    return connection_manager.check_connection_health()