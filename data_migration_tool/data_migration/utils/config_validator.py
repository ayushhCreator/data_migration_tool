import frappe
import os
import re
from typing import List, Dict, Any

class ConfigValidator:
    """Validates migration configuration and system requirements"""
    
    @staticmethod
    def validate_migration_settings() -> List[str]:
        """Validate migration settings and return list of errors"""
        errors = []
        
        try:
            settings = frappe.get_single('Migration Settings')
        except Exception as e:
            return [f"Cannot load Migration Settings: {str(e)}"]
        
        # Validate CSV processing settings
        if settings.enable_csv_processing:
            if not settings.csv_watch_directory:
                errors.append("CSV watch directory is required when CSV processing is enabled")
            else:
                if not os.path.isabs(settings.csv_watch_directory):
                    errors.append("CSV watch directory must be an absolute path")
                
                # Check directory permissions
                try:
                    test_dir = settings.csv_watch_directory
                    if not os.path.exists(test_dir):
                        os.makedirs(test_dir, exist_ok=True)
                    
                    # Test write permission
                    test_file = os.path.join(test_dir, '.permission_test')
                    with open(test_file, 'w') as f:
                        f.write('test')
                    os.unlink(test_file)
                    
                except Exception as e:
                    errors.append(f"CSV watch directory is not writable: {str(e)}")
            
            # Validate chunk size
            if settings.csv_chunk_size:
                chunk_size = int(settings.csv_chunk_size or 0)
                if chunk_size < 10 or chunk_size > 50000:
                    errors.append("CSV chunk size must be between 10 and 50000")
        
        # # Validate Zoho settings
        # if settings.enable_zoho_sync:
        #     required_zoho_fields = ['zoho_client_id', 'zoho_client_secret', 'zoho_refresh_token']
        #     for field in required_zoho_fields:
        #         if not getattr(settings, field, None):
        #             errors.append(f"Zoho {field.replace('_', ' ').title()} is required when Zoho sync is enabled")
        
        # # Validate Odoo settings
        # if settings.enable_odoo_sync:
        #     required_odoo_fields = ['odoo_url', 'odoo_database', 'odoo_username', 'odoo_password']
        #     for field in required_odoo_fields:
        #         if not getattr(settings, field, None):
        #             errors.append(f"Odoo {field.replace('_', ' ').title()} is required when Odoo sync is enabled")
            
        #     # Validate URL format
        #     if settings.odoo_url:
        #         url_pattern = r'^https?://[^\s/$.?#].[^\s]*$'
        #         if not re.match(url_pattern, settings.odoo_url):
        #             errors.append("Odoo URL format is invalid")
        
        # Validate performance settings
        if settings.max_concurrent_jobs:
            max_jobs = int(settings.max_concurrent_jobs or 0)
            if max_jobs < 1 or max_jobs > 20:
                errors.append("Max concurrent jobs must be between 1 and 20")
        
        return errors
    
    @staticmethod
    def validate_system_requirements() -> List[str]:
        """Validate system meets minimum requirements"""
        errors = []
        
        try:
            import psutil
            
            # Check memory
            memory = psutil.virtual_memory()
            if memory.total < 1 * 1024 * 1024 * 1024:  # 1GB minimum
                errors.append("Minimum 1GB RAM required")
            
            # Check disk space
            disk = psutil.disk_usage('/')
            if disk.free < 5 * 1024 * 1024 * 1024:  # 5GB minimum
                errors.append("Minimum 5GB free disk space required")
            
            # Check if we can import required packages
            required_packages = ['pandas', 'openpyxl', 'requests']
            for package in required_packages:
                try:
                    __import__(package)
                except ImportError:
                    errors.append(f"Required package '{package}' is not installed")
            
        except ImportError:
            errors.append("psutil package is required but not installed")
        except Exception as e:
            errors.append(f"System requirement check failed: {str(e)}")
        
        return errors
    
    @staticmethod
    def validate_email_format(email: str) -> bool:
        """Validate email format"""
        if not email:
            return False
        
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))
    
    @staticmethod
    def validate_url_format(url: str) -> bool:
        """Validate URL format"""
        if not url:
            return False
        
        from urllib.parse import urlparse
        try:
            parsed = urlparse(url)
            return bool(parsed.scheme and parsed.netloc)
        except Exception:
            return False
    
    @staticmethod
    def validate_file_extension(filename: str, allowed_extensions: List[str]) -> bool:
        """Validate file has allowed extension"""
        if not filename or not allowed_extensions:
            return False
        
        file_ext = os.path.splitext(filename)[1].lower()
        return file_ext in [ext.lower() for ext in allowed_extensions]

# Hook this into migration settings validation
def validate_on_settings_save(doc, method):
    """Hook to validate settings on save"""
    if doc.doctype == 'Migration Settings':
        config_errors = ConfigValidator.validate_migration_settings()
        system_errors = ConfigValidator.validate_system_requirements()
        
        all_errors = config_errors + system_errors
        if all_errors:
            frappe.throw("<br>".join(all_errors))