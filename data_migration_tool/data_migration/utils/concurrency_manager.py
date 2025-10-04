import os
import time
import fcntl
import frappe
from contextlib import contextmanager
from typing import Optional

class FileLockManager:
    """Manages file locks to prevent race conditions in concurrent processing"""
    
    def __init__(self, logger=None):
        self.logger = logger or frappe.logger()
    
    @contextmanager
    def acquire_file_lock(self, file_path: str, timeout: int = 300):
        """
        Acquire exclusive lock on a file to prevent concurrent processing
        
        Args:
            file_path: Path to the file to lock
            timeout: Maximum time to wait for lock in seconds
            
        Raises:
            TimeoutError: If lock cannot be acquired within timeout
            OSError: If file operations fail
        """
        lock_file_path = f"{file_path}.lock"
        lock_fd = None
        lock_acquired = False
        start_time = time.time()
        
        try:
            # Create lock file
            lock_fd = os.open(lock_file_path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
            
            # Try to acquire lock with timeout
            while time.time() - start_time < timeout:
                try:
                    # Try to acquire exclusive lock (non-blocking)
                    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    lock_acquired = True
                    
                    # Write process info to lock file
                    lock_info = f"PID:{os.getpid()}\nTIME:{time.time()}\nFILE:{file_path}\n"
                    os.write(lock_fd, lock_info.encode())
                    os.fsync(lock_fd)
                    
                    self.logger.info(f"🔒 Acquired lock for file: {file_path}")
                    break
                    
                except (OSError, IOError):
                    # Lock is held by another process, wait and retry
                    time.sleep(1)
            
            if not lock_acquired:
                raise TimeoutError(f"Could not acquire lock for {file_path} within {timeout} seconds")
            
            yield lock_fd
            
        finally:
            # Release lock and cleanup
            if lock_fd is not None:
                try:
                    if lock_acquired:
                        fcntl.flock(lock_fd, fcntl.LOCK_UN)
                    os.close(lock_fd)
                except (OSError, IOError) as e:
                    self.logger.error(f"Error releasing lock: {str(e)}")
                
                # Remove lock file
                try:
                    if os.path.exists(lock_file_path):
                        os.unlink(lock_file_path)
                except (OSError, IOError) as e:
                    self.logger.error(f"Error removing lock file: {str(e)}")
            
            if lock_acquired:
                self.logger.info(f"🔓 Released lock for file: {file_path}")
    
    def is_file_locked(self, file_path: str) -> bool:
        """Check if a file is currently locked"""
        lock_file_path = f"{file_path}.lock"
        
        if not os.path.exists(lock_file_path):
            return False
        
        try:
            # Try to read lock file to get process info
            with open(lock_file_path, 'r') as f:
                lock_info = f.read()
            
            # Extract PID from lock info
            for line in lock_info.split('\n'):
                if line.startswith('PID:'):
                    pid = int(line.split(':')[1])
                    
                    # Check if process is still running
                    try:
                        os.kill(pid, 0)  # Send signal 0 to check if process exists
                        return True  # Process exists, file is locked
                    except (OSError, ProcessLookupError):
                        # Process doesn't exist, remove stale lock
                        try:
                            os.unlink(lock_file_path)
                            self.logger.warning(f"Removed stale lock file: {lock_file_path}")
                        except OSError:
                            pass
                        return False
            
        except (OSError, IOError, ValueError) as e:
            self.logger.error(f"Error checking lock status: {str(e)}")
            return False
        
        return False
    
    def cleanup_stale_locks(self, directory: str, max_age_hours: int = 24):
        """Clean up stale lock files older than specified hours"""
        try:
            current_time = time.time()
            max_age_seconds = max_age_hours * 3600
            
            for filename in os.listdir(directory):
                if filename.endswith('.lock'):
                    lock_path = os.path.join(directory, filename)
                    
                    try:
                        # Check file age
                        file_age = current_time - os.path.getmtime(lock_path)
                        
                        if file_age > max_age_seconds:
                            # Check if lock is still valid
                            original_file = lock_path[:-5]  # Remove .lock extension
                            
                            if not self.is_file_locked(original_file):
                                os.unlink(lock_path)
                                self.logger.info(f"Cleaned up stale lock: {lock_path}")
                    
                    except (OSError, IOError) as e:
                        self.logger.error(f"Error cleaning up lock {lock_path}: {str(e)}")
        
        except (OSError, IOError) as e:
            self.logger.error(f"Error cleaning up stale locks in {directory}: {str(e)}")


class ConcurrencyManager:
    """Manages concurrent operations and prevents race conditions"""
    
    def __init__(self, logger=None):
        self.logger = logger or frappe.logger()
        self.file_lock_manager = FileLockManager(logger)
        self.active_operations = {}
    
    @contextmanager
    def exclusive_operation(self, operation_id: str, timeout: int = 300):
        """
        Ensure only one instance of an operation runs at a time
        
        Args:
            operation_id: Unique identifier for the operation
            timeout: Maximum time to wait for operation to complete
        """
        if operation_id in self.active_operations:
            start_time = self.active_operations[operation_id]
            elapsed = time.time() - start_time
            
            if elapsed < timeout:
                raise RuntimeError(f"Operation '{operation_id}' is already running (started {elapsed:.1f}s ago)")
            else:
                self.logger.warning(f"Forcibly taking over stale operation: {operation_id}")
                del self.active_operations[operation_id]
        
        self.active_operations[operation_id] = time.time()
        
        try:
            self.logger.info(f"🚀 Starting exclusive operation: {operation_id}")
            yield
            self.logger.info(f"✅ Completed exclusive operation: {operation_id}")
            
        finally:
            if operation_id in self.active_operations:
                del self.active_operations[operation_id]
    
    @contextmanager
    def process_file_exclusively(self, file_path: str, timeout: int = 300):
        """
        Process a file exclusively to prevent concurrent processing
        
        Args:
            file_path: Path to file being processed
            timeout: Maximum time to wait for file lock
        """
        with self.file_lock_manager.acquire_file_lock(file_path, timeout):
            operation_id = f"file_processing_{os.path.basename(file_path)}"
            
            with self.exclusive_operation(operation_id, timeout):
                yield