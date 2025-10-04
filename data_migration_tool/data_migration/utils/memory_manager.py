import gc
import psutil
import threading
import time
import weakref
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass, field
from contextlib import contextmanager
import frappe

@dataclass
class MemoryMetrics:
    """Memory usage metrics"""
    current_usage_mb: float = 0.0
    peak_usage_mb: float = 0.0
    available_mb: float = 0.0
    threshold_mb: float = 0.0
    gc_collections: int = 0
    active_objects: Dict[str, int] = field(default_factory=dict)
    weak_references: int = 0

class MemoryLeakDetector:
    """Detects and prevents memory leaks during migration operations"""
    
    def __init__(self, warning_threshold_mb: float = 500.0, critical_threshold_mb: float = 1000.0,
                 monitoring_interval: float = 30.0):
        self.warning_threshold_mb = warning_threshold_mb
        self.critical_threshold_mb = critical_threshold_mb
        self.monitoring_interval = monitoring_interval
        self.metrics = MemoryMetrics(threshold_mb=critical_threshold_mb)
        self.logger = frappe.logger()
        
        # Object tracking
        self._tracked_objects = weakref.WeakSet()
        self._object_counts = {}
        self._lock = threading.Lock()
        
        # Monitoring thread
        self._monitoring_active = False
        self._monitor_thread = None
        
        # Cleanup callbacks
        self._cleanup_callbacks = []
    
    def start_monitoring(self):
        """Start memory monitoring in background thread"""
        if self._monitoring_active:
            return
        
        self._monitoring_active = True
        self._monitor_thread = threading.Thread(target=self._monitor_memory, daemon=True)
        self._monitor_thread.start()
        self.logger.info("Memory leak detector started")
    
    def stop_monitoring(self):
        """Stop memory monitoring"""
        self._monitoring_active = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5.0)
        self.logger.info("Memory leak detector stopped")
    
    def _monitor_memory(self):
        """Background memory monitoring"""
        while self._monitoring_active:
            try:
                self._update_metrics()
                self._check_thresholds()
                time.sleep(self.monitoring_interval)
            except Exception as e:
                self.logger.error(f"Memory monitoring error: {str(e)}")
                time.sleep(self.monitoring_interval)
    
    def _update_metrics(self):
        """Update memory usage metrics"""
        process = psutil.Process()
        memory_info = process.memory_info()
        system_memory = psutil.virtual_memory()
        
        current_mb = memory_info.rss / 1024 / 1024
        available_mb = system_memory.available / 1024 / 1024
        
        with self._lock:
            self.metrics.current_usage_mb = current_mb
            self.metrics.available_mb = available_mb
            
            if current_mb > self.metrics.peak_usage_mb:
                self.metrics.peak_usage_mb = current_mb
            
            # Update object counts
            self.metrics.weak_references = len(self._tracked_objects)
            self.metrics.active_objects = dict(self._object_counts)
    
    def _check_thresholds(self):
        """Check memory thresholds and take action"""
        current_usage = self.metrics.current_usage_mb
        
        if current_usage > self.critical_threshold_mb:
            self.logger.critical(f"💥 CRITICAL: Memory usage {current_usage:.1f}MB exceeds critical threshold {self.critical_threshold_mb:.1f}MB")
            self._emergency_cleanup()
            
        elif current_usage > self.warning_threshold_mb:
            self.logger.warning(f"⚠️ WARNING: Memory usage {current_usage:.1f}MB exceeds warning threshold {self.warning_threshold_mb:.1f}MB")
            self._perform_cleanup()
    
    def _perform_cleanup(self):
        """Perform standard memory cleanup"""
        self.logger.info("Performing memory cleanup...")
        
        # Run garbage collection
        collected = gc.collect()
        self.metrics.gc_collections += 1
        
        # Execute cleanup callbacks
        for callback in self._cleanup_callbacks:
            try:
                callback()
            except Exception as e:
                self.logger.error(f"Cleanup callback failed: {str(e)}")
        
        self.logger.info(f"Cleanup completed, collected {collected} objects")
    
    def _emergency_cleanup(self):
        """Emergency memory cleanup for critical situations"""
        self.logger.critical("Performing emergency memory cleanup...")
        
        # Force aggressive garbage collection
        for generation in range(3):
            gc.collect(generation)
        
        # Clear weak references to dead objects
        with self._lock:
            self._tracked_objects.clear()
            self._object_counts.clear()
        
        # Execute emergency cleanup callbacks
        for callback in self._cleanup_callbacks:
            try:
                callback()
            except Exception as e:
                self.logger.error(f"Emergency cleanup callback failed: {str(e)}")
        
        # Force Python to release memory back to OS
        if hasattr(gc, 'set_threshold'):
            old_thresholds = gc.get_threshold()
            gc.set_threshold(0, 0, 0)  # Disable automatic GC temporarily
            gc.collect()
            gc.set_threshold(*old_thresholds)  # Restore thresholds
        
        self.logger.critical("Emergency cleanup completed")
    
    def track_object(self, obj, object_type: str = None):
        """Track an object for memory leak detection"""
        if object_type is None:
            object_type = type(obj).__name__
        
        with self._lock:
            self._tracked_objects.add(obj)
            self._object_counts[object_type] = self._object_counts.get(object_type, 0) + 1
    
    def add_cleanup_callback(self, callback: Callable):
        """Add a cleanup callback function"""
        self._cleanup_callbacks.append(callback)
    
    def remove_cleanup_callback(self, callback: Callable):
        """Remove a cleanup callback function"""
        if callback in self._cleanup_callbacks:
            self._cleanup_callbacks.remove(callback)
    
    @contextmanager
    def memory_monitor(self, operation_name: str = "operation"):
        """Context manager for monitoring memory during operations"""
        start_memory = self.get_current_memory_usage()
        start_time = time.time()
        
        self.logger.info(f"Starting {operation_name} - Memory: {start_memory:.1f}MB")
        
        try:
            yield self
        finally:
            end_memory = self.get_current_memory_usage()
            end_time = time.time()
            duration = end_time - start_time
            memory_diff = end_memory - start_memory
            
            if memory_diff > 50:  # More than 50MB increase
                self.logger.warning(f"⚠️ Memory increase detected in {operation_name}: +{memory_diff:.1f}MB (Duration: {duration:.1f}s)")
                self._perform_cleanup()
            else:
                self.logger.info(f"Completed {operation_name} - Memory: {end_memory:.1f}MB ({memory_diff:+.1f}MB, Duration: {duration:.1f}s)")
    
    def get_current_memory_usage(self) -> float:
        """Get current memory usage in MB"""
        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024
    
    def get_memory_metrics(self) -> MemoryMetrics:
        """Get current memory metrics"""
        self._update_metrics()
        return self.metrics
    
    def force_cleanup(self):
        """Force immediate memory cleanup"""
        self._perform_cleanup()
    
    def reset_metrics(self):
        """Reset memory metrics"""
        with self._lock:
            self.metrics = MemoryMetrics(threshold_mb=self.critical_threshold_mb)
            self._object_counts.clear()

class MemoryEfficientProcessor:
    """Memory-efficient data processing utilities"""
    
    def __init__(self, memory_detector: MemoryLeakDetector):
        self.memory_detector = memory_detector
        self.logger = frappe.logger()
    
    def process_large_dataset(self, data_source, processor_func, batch_size: int = 1000,
                            memory_check_interval: int = 10):
        """Process large datasets in memory-efficient batches"""
        batch_count = 0
        processed_count = 0
        
        try:
            for batch in self._chunk_data(data_source, batch_size):
                with self.memory_detector.memory_monitor(f"batch_{batch_count}"):
                    # Process batch
                    results = processor_func(batch)
                    processed_count += len(batch)
                    batch_count += 1
                    
                    # Track batch for memory monitoring
                    self.memory_detector.track_object(batch, "processing_batch")
                    
                    # Periodic memory check
                    if batch_count % memory_check_interval == 0:
                        current_memory = self.memory_detector.get_current_memory_usage()
                        self.logger.info(f"Processed {processed_count} records in {batch_count} batches - Memory: {current_memory:.1f}MB")
                        
                        # Force cleanup if memory is high
                        if current_memory > self.memory_detector.warning_threshold_mb:
                            self.memory_detector.force_cleanup()
                    
                    # Explicit cleanup of batch data
                    del batch
                    if 'results' in locals():
                        del results
                    
                    # Force garbage collection every 50 batches
                    if batch_count % 50 == 0:
                        gc.collect()
        
        except Exception as e:
            self.logger.error(f"Error in memory-efficient processing: {str(e)}")
            raise
        
        finally:
            # Final cleanup
            gc.collect()
            self.logger.info(f"Completed processing {processed_count} records in {batch_count} batches")
    
    def _chunk_data(self, data_source, chunk_size: int):
        """Chunk data source into manageable pieces"""
        if hasattr(data_source, '__len__'):
            # List-like data source
            for i in range(0, len(data_source), chunk_size):
                yield data_source[i:i + chunk_size]
        else:
            # Iterator-like data source
            chunk = []
            for item in data_source:
                chunk.append(item)
                if len(chunk) >= chunk_size:
                    yield chunk
                    chunk = []
            
            if chunk:  # Yield remaining items
                yield chunk
    
    @contextmanager
    def memory_limited_operation(self, max_memory_mb: float = 100.0):
        """Context manager for memory-limited operations"""
        initial_memory = self.memory_detector.get_current_memory_usage()
        
        try:
            yield
            
            # Check if memory limit was exceeded
            current_memory = self.memory_detector.get_current_memory_usage()
            memory_used = current_memory - initial_memory
            
            if memory_used > max_memory_mb:
                self.logger.warning(f"Memory limit exceeded: {memory_used:.1f}MB > {max_memory_mb:.1f}MB")
                self.memory_detector.force_cleanup()
        
        except Exception as e:
            self.logger.error(f"Error in memory-limited operation: {str(e)}")
            raise
        
        finally:
            # Ensure cleanup
            gc.collect()

# Global memory detector instance
memory_detector = MemoryLeakDetector()
memory_processor = MemoryEfficientProcessor(memory_detector)

# Convenience functions
def start_memory_monitoring():
    """Start global memory monitoring"""
    memory_detector.start_monitoring()

def stop_memory_monitoring():
    """Stop global memory monitoring"""
    memory_detector.stop_monitoring()

def get_memory_status() -> Dict[str, Any]:
    """Get current memory status"""
    metrics = memory_detector.get_memory_metrics()
    return {
        'current_usage_mb': metrics.current_usage_mb,
        'peak_usage_mb': metrics.peak_usage_mb,
        'available_mb': metrics.available_mb,
        'threshold_mb': metrics.threshold_mb,
        'gc_collections': metrics.gc_collections,
        'active_objects': metrics.active_objects,
        'weak_references': metrics.weak_references
    }

def force_memory_cleanup():
    """Force immediate memory cleanup"""
    memory_detector.force_cleanup()