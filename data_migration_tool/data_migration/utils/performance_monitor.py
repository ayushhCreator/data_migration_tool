import frappe
import time
from contextlib import contextmanager
from typing import Dict, Any, Optional
import psutil
import os

class PerformanceMonitor:
    def __init__(self, logger):
        self.logger = logger
        self.metrics = {}
        # Resource limits - configurable via environment variables
        self.min_memory_mb = int(os.environ.get('MIGRATION_MIN_MEMORY_MB', '100'))
        self.max_memory_percent = int(os.environ.get('MIGRATION_MAX_MEMORY_PERCENT', '85'))
        self.max_cpu_percent = int(os.environ.get('MIGRATION_MAX_CPU_PERCENT', '90'))
        self.max_operation_time = int(os.environ.get('MIGRATION_MAX_OPERATION_TIME', '3600'))
    
    def check_resource_availability(self, required_memory_mb: Optional[int] = None) -> Dict[str, Any]:
        """Check if system has sufficient resources for operation"""
        memory = psutil.virtual_memory()
        cpu_percent = psutil.cpu_percent(interval=1)
        disk = psutil.disk_usage('/')
        
        available_memory_mb = memory.available / 1024 / 1024
        required_mb = required_memory_mb or self.min_memory_mb
        
        status = {
            'available_memory_mb': available_memory_mb,
            'required_memory_mb': required_mb,
            'memory_percent': memory.percent,
            'cpu_percent': cpu_percent,
            'disk_free_gb': disk.free / 1024 / 1024 / 1024,
            'has_sufficient_memory': available_memory_mb >= required_mb,
            'memory_usage_ok': memory.percent <= self.max_memory_percent,
            'cpu_usage_ok': cpu_percent <= self.max_cpu_percent,
            'system_healthy': True
        }
        
        # Determine overall system health
        status['system_healthy'] = (
            status['has_sufficient_memory'] and 
            status['memory_usage_ok'] and 
            status['cpu_usage_ok']
        )
        
        return status
    
    @contextmanager
    def measure_operation(self, operation_name: str, required_memory_mb: Optional[int] = None):
        """Enhanced context manager with resource validation and timeout"""
        import threading
        
        # Pre-flight resource check
        resource_status = self.check_resource_availability(required_memory_mb)
        
        if not resource_status['system_healthy']:
            error_msg = f"Insufficient resources for operation '{operation_name}': {resource_status}"
            self.logger.logger.error(error_msg)
            raise RuntimeError(error_msg)
        
        start_time = time.time()
        start_memory = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024  # MB
        
        # Setup operation timeout
        timeout_occurred = threading.Event()
        
        def timeout_handler():
            timeout_occurred.set()
            self.logger.logger.error(f"⏰ Operation '{operation_name}' timed out after {self.max_operation_time} seconds")
        
        timer = threading.Timer(self.max_operation_time, timeout_handler)
        timer.start()
        
        try:
            self.logger.logger.info(f"🚀 Starting operation: {operation_name}", extra={
                'available_memory_mb': resource_status['available_memory_mb'],
                'start_memory_mb': start_memory
            })
            
            yield
            
            # Check if timeout occurred during operation
            if timeout_occurred.is_set():
                raise TimeoutError(f"Operation '{operation_name}' exceeded maximum time limit")
                
        finally:
            timer.cancel()
            
            end_time = time.time()
            end_memory = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024  # MB
            
            duration = end_time - start_time
            memory_delta = end_memory - start_memory
            
            self.metrics[operation_name] = {
                'duration': duration,
                'memory_usage': memory_delta,
                'start_memory': start_memory,
                'end_memory': end_memory,
                'resource_status': resource_status
            }
            
            # Log completion with performance metrics
            log_extra = {
                'duration_seconds': round(duration, 2),
                'memory_delta_mb': round(memory_delta, 2),
                'peak_memory_mb': round(end_memory, 2)
            }
            
            if duration > 60:  # Warn for operations > 1 minute
                self.logger.logger.warning(f"⏱️ Slow operation: {operation_name}", extra=log_extra)
            else:
                self.logger.logger.info(f"✅ Operation completed: {operation_name}", extra=log_extra)
    
    def get_system_metrics(self) -> Dict[str, Any]:
        """Get comprehensive system metrics"""
        try:
            memory = psutil.virtual_memory()
            cpu_percent = psutil.cpu_percent()
            disk = psutil.disk_usage('/')
            
            return {
                'cpu_percent': cpu_percent,
                'memory_percent': memory.percent,
                'memory_available_mb': memory.available / 1024 / 1024,
                'memory_total_gb': memory.total / 1024 / 1024 / 1024,
                'disk_usage_percent': (disk.used / disk.total) * 100,
                'disk_free_gb': disk.free / 1024 / 1024 / 1024,
                'process_count': len(psutil.pids()),
                'load_average': os.getloadavg() if hasattr(os, 'getloadavg') else 'N/A'
            }
        except Exception as e:
            self.logger.logger.error(f"Failed to get system metrics: {str(e)}")
            return {'error': str(e)}
    
    def log_system_health(self):
        """Log current system health with alerts"""
        metrics = self.get_system_metrics()
        
        if 'error' in metrics:
            self.logger.logger.error("❌ Failed to get system health metrics")
            return
        
        self.logger.logger.info("🖥️ System Health", extra=metrics)
        
        # Alert if resources are running low
        alerts = []
        if metrics['memory_percent'] > self.max_memory_percent:
            alerts.append(f"High memory usage: {metrics['memory_percent']:.1f}%")
        
        if metrics['cpu_percent'] > self.max_cpu_percent:
            alerts.append(f"High CPU usage: {metrics['cpu_percent']:.1f}%")
        
        if metrics['disk_usage_percent'] > 90:
            alerts.append(f"Low disk space: {metrics['disk_free_gb']:.1f}GB free")
        
        if alerts:
            self.logger.logger.warning("⚠️ Resource alerts: " + "; ".join(alerts))
    
    def validate_file_size(self, file_path: str, max_size_mb: Optional[int] = None) -> bool:
        """Validate file size against limits"""
        try:
            if not os.path.exists(file_path):
                self.logger.logger.error(f"File not found: {file_path}")
                return False
            
            file_size_mb = os.path.getsize(file_path) / 1024 / 1024
            max_allowed = max_size_mb or int(os.environ.get('MIGRATION_MAX_FILE_MB', '100'))
            
            if file_size_mb > max_allowed:
                self.logger.logger.error(f"File too large: {file_path} ({file_size_mb:.1f}MB > {max_allowed}MB)")
                return False
            
            return True
            
        except Exception as e:
            self.logger.logger.error(f"Failed to validate file size for {file_path}: {str(e)}")
            return False
