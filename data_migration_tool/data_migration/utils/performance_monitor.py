import frappe
import time
from contextlib import contextmanager
from typing import Dict, Any
import psutil
import os

class PerformanceMonitor:
    def __init__(self, logger):
        self.logger = logger
        self.metrics = {}
    
    @contextmanager
    def measure_operation(self, operation_name: str):
        """Context manager to measure operation performance"""
        start_time = time.time()
        start_memory = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024  # MB
        
        try:
            yield
        finally:
            end_time = time.time()
            end_memory = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024  # MB
            
            duration = end_time - start_time
            memory_delta = end_memory - start_memory
            
            self.metrics[operation_name] = {
                'duration': duration,
                'memory_usage': memory_delta,
                'start_memory': start_memory,
                'end_memory': end_memory
            }
            
            self.logger.logger.info(f"⏱️ Performance: {operation_name}", extra={
                'duration_seconds': round(duration, 2),
                'memory_delta_mb': round(memory_delta, 2),
                'peak_memory_mb': round(end_memory, 2)
            })
    
    def get_system_metrics(self) -> Dict[str, Any]:
        """Get current system metrics"""
        return {
            'cpu_percent': psutil.cpu_percent(),
            'memory_percent': psutil.virtual_memory().percent,
            'disk_usage_percent': psutil.disk_usage('/').percent,
            'process_count': len(psutil.pids())
        }
    
    def log_system_health(self):
        """Log current system health"""
        metrics = self.get_system_metrics()
        self.logger.logger.info("🖥️ System Health", extra=metrics)
        
        # Alert if resources are running low
        if metrics['memory_percent'] > 85:
            self.logger.logger.warning("⚠️ High memory usage detected")
        
        if metrics['cpu_percent'] > 90:
            self.logger.logger.warning("⚠️ High CPU usage detected")
