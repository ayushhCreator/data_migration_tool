import os
import re
import frappe
from pathlib import Path
from typing import Optional

class SecurePathManager:
    """Manages secure file paths and prevents path traversal attacks"""
    
    @staticmethod
    def get_migration_base_directory() -> str:
        """Get secure base directory for migration files"""
        # Use site-specific private directory
        return frappe.get_site_path('private', 'files', 'migration')
    
    @staticmethod
    def get_watch_directory() -> str:
        """Get CSV watch directory with fallback"""
        try:
            settings = frappe.get_single('Migration Settings')
            custom_dir = getattr(settings, 'csv_watch_directory', None)
            
            if custom_dir and os.path.isabs(custom_dir):
                # Validate custom directory
                if SecurePathManager.validate_directory_security(custom_dir):
                    return custom_dir
                else:
                    frappe.log_error(f"Custom directory {custom_dir} failed security validation")
            
        except Exception as e:
            frappe.log_error(f"Error getting custom watch directory: {str(e)}")
        
        # Use secure default
        default_dir = SecurePathManager.get_migration_base_directory()
        os.makedirs(default_dir, exist_ok=True)
        return default_dir
    
    @staticmethod
    def sanitize_filename(filename: str) -> str:
        """Sanitize filename to prevent path traversal"""
        if not filename:
            raise ValueError("Filename cannot be empty")
        
        # Remove path components
        filename = os.path.basename(filename)
        
        # Remove dangerous characters
        filename = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', filename)
        
        # Ensure not empty after sanitization
        if not filename.strip():
            raise ValueError("Filename is invalid after sanitization")
        
        # Limit length (keeping extension)
        if len(filename) > 255:
            name, ext = os.path.splitext(filename)
            filename = name[:255-len(ext)] + ext
        
        return filename
    
    @staticmethod
    def validate_directory_security(directory: str) -> bool:
        """Validate directory is secure for use"""
        try:
            # Check it's not a system directory
            system_dirs = ['/etc', '/usr', '/var', '/sys', '/proc', '/dev', '/boot']
            abs_path = os.path.abspath(directory)
            
            for sys_dir in system_dirs:
                if abs_path.startswith(sys_dir):
                    return False
            
            # Check permissions
            if os.path.exists(directory):
                stat_info = os.stat(directory)
                # Check it's not world-writable (security risk)
                if stat_info.st_mode & 0o002:
                    return False
            
            return True
            
        except Exception:
            return False
    
    @staticmethod
    def get_secure_file_path(directory: str, filename: str) -> str:
        """Get secure file path with validation"""
        safe_filename = SecurePathManager.sanitize_filename(filename)
        secure_dir = os.path.abspath(directory)
        file_path = os.path.join(secure_dir, safe_filename)
        
        # Ensure the resulting path is within the intended directory
        if not os.path.abspath(file_path).startswith(secure_dir):
            raise ValueError("File path escapes intended directory")
        
        return file_path
    
    @staticmethod
    def create_migration_directories() -> dict:
        """Create all required migration directories"""
        base_dir = SecurePathManager.get_migration_base_directory()
        
        directories = {
            'base': base_dir,
            'processed': os.path.join(base_dir, 'processed'),
            'errors': os.path.join(base_dir, 'errors'),
            'pending': os.path.join(base_dir, 'pending'),
            'backup': os.path.join(base_dir, 'backup')
        }
        
        for name, path in directories.items():
            try:
                os.makedirs(path, mode=0o755, exist_ok=True)
                # Create .gitkeep file
                gitkeep_path = os.path.join(path, '.gitkeep')
                if not os.path.exists(gitkeep_path):
                    with open(gitkeep_path, 'w') as f:
                        f.write(f"# Keep {name} directory in git\n")
            except Exception as e:
                frappe.log_error(f"Failed to create directory {path}: {str(e)}")
        
        return directories