import os
import psutil
import time
import threading
from contextlib import contextmanager
from typing import Optional, Dict, Any

class ResourceManager:
    """Manages system resources and prevents resource exhaustion"""
    
    def __init__(self):
        self.max_memory_mb = int(os.environ.get('MIGRATION_MAX_MEMORY_MB', '512'))
        self.max_file_size_mb = int(os.environ.get('MIGRATION_MAX_FILE_MB', '100'))
        self.max_operation_time = int(os.environ.get('MIGRATION_MAX_TIME_SECONDS', '3600'))
    
    def check_system_resources(self) -> Dict[str, Any]:
        """Check current system resource availability"""
        try:
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            cpu = psutil.cpu_percent(interval=1)
            
            return {
                'memory_available_mb': memory.available / 1024 / 1024,
                'memory_percent': memory.percent,
                'disk_free_gb': disk.free / 1024 / 1024 / 1024,
                'disk_percent': (disk.used / disk.total) * 100,
                'cpu_percent': cpu,
                'healthy': (
                    memory.percent < 85 and 
                    (disk.used / disk.total) * 100 < 90 and 
                    cpu < 90
                )
            }
        except Exception as e:
            import frappe
            frappe.log_error(f"Failed to check system resources: {str(e)}")
            return {'healthy': False, 'error': str(e)}
    
    def validate_file_size(self, file_path: str) -> bool:
        """Validate file size is within limits"""
        try:
            size_mb = os.path.getsize(file_path) / 1024 / 1024
            if size_mb > self.max_file_size_mb:
                import frappe
                frappe.log_error(f"File {file_path} ({size_mb:.1f}MB) exceeds limit ({self.max_file_size_mb}MB)")
                return False
            return True
        except Exception as e:
            import frappe
            frappe.log_error(f"Failed to check file size for {file_path}: {str(e)}")
            return False
    
    @contextmanager
    def resource_monitor(self, operation_name: str):
        """Monitor resource usage during operation"""
        import frappe
        
        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss / 1024 / 1024
        
        # Setup timeout
        timeout_occurred = threading.Event()
        
        def timeout_handler():
            timeout_occurred.set()
            frappe.log_error(f"Operation '{operation_name}' timed out after {self.max_operation_time} seconds")
        
        timer = threading.Timer(self.max_operation_time, timeout_handler)
        timer.start()
        
        try:
            # Check initial resources
            resources = self.check_system_resources()
            if not resources.get('healthy', False):
                raise RuntimeError(f"System resources insufficient for operation '{operation_name}': {resources}")
            
            yield
            
            if timeout_occurred.is_set():
                raise TimeoutError(f"Operation '{operation_name}' exceeded maximum time limit")
                
        finally:
            timer.cancel()
            
            # Log final metrics
            end_time = time.time()
            end_memory = psutil.Process().memory_info().rss / 1024 / 1024
            duration = end_time - start_time
            memory_delta = end_memory - start_memory
            
            frappe.logger().info(f"Operation '{operation_name}' completed", extra={
                'duration_seconds': round(duration, 2),
                'memory_delta_mb': round(memory_delta, 2),
                'peak_memory_mb': round(end_memory, 2)
            })
    
    def check_available_memory(self, required_mb: int) -> bool:
        """Check if enough memory is available for operation"""
        try:
            available_mb = psutil.virtual_memory().available / 1024 / 1024
            return available_mb >= required_mb
        except Exception:
            return False
    
    def get_optimal_batch_size(self, record_size_bytes: int = 1024) -> int:
        """Calculate optimal batch size based on available memory"""
        try:
            available_mb = psutil.virtual_memory().available / 1024 / 1024
            # Use 10% of available memory for batching
            usable_mb = available_mb * 0.1
            usable_bytes = usable_mb * 1024 * 1024
            
            batch_size = int(usable_bytes / record_size_bytes)
            
            # Ensure reasonable bounds
            min_batch = 10
            max_batch = 10000
            
            return max(min_batch, min(batch_size, max_batch))
        except Exception:
            return 1000  # Safe default