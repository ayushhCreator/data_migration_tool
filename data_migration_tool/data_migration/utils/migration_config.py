import os
import frappe
from typing import Dict, Any, Optional, Union

class MigrationConfig:
    """Centralized configuration management for data migration"""
    
    # Default configuration values
    DEFAULTS = {
        # File Processing
        'MAX_FILE_SIZE_MB': 100,
        'CHUNK_SIZE': 1000,
        'SUPPORTED_EXTENSIONS': ['.csv', '.xlsx', '.xls'],
        'MAX_FILENAME_LENGTH': 255,
        
        # Database Operations
        'MAX_NAME_GENERATION_ATTEMPTS': 10000,
        'BATCH_SIZE': 100,
        'MAX_RETRIES': 3,
        'DB_TIMEOUT_SECONDS': 30,
        
        # Performance Limits
        'MAX_MEMORY_MB': 512,
        'MIN_AVAILABLE_MEMORY_MB': 100,
        'MAX_MEMORY_PERCENT': 85,
        'MAX_CPU_PERCENT': 90,
        'MAX_OPERATION_TIME_SECONDS': 3600,
        
        # Field Type Detection
        'DATA_FIELD_MAX_LENGTH': 140,
        'SMALL_TEXT_MAX_LENGTH': 1000,
        'MEDIUM_TEXT_MAX_LENGTH': 5000,
        'PHONE_MIN_LENGTH': 10,
        'EMAIL_MAX_LENGTH': 254,
        
        # Concurrency
        'MAX_CONCURRENT_JOBS': 5,
        'FILE_LOCK_TIMEOUT_SECONDS': 300,
        'OPERATION_TIMEOUT_SECONDS': 300,
        
        # Cleanup
        'LOG_RETENTION_DAYS': 30,
        'TEMP_FILE_RETENTION_HOURS': 24,
        'LOCK_FILE_MAX_AGE_HOURS': 24,
        
        # Validation
        'URL_TIMEOUT_SECONDS': 10,
        'MIN_DISK_SPACE_GB': 5,
        'MIN_SYSTEM_MEMORY_GB': 1,
        
        # Security
        'ALLOWED_MIME_TYPES': [
            'text/csv',
            'application/vnd.ms-excel',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        ],
        'MAX_PATH_DEPTH': 10,
        'FORBIDDEN_PATH_COMPONENTS': ['..', '.', '~', '$'],
        
        # Logging
        'LOG_FILE_COUNT': 20,
        'LOG_FILE_SIZE_MB': 1,
        'LOG_LEVEL': 'INFO'
    }
    
    def __init__(self):
        self._config_cache = {}
        self._load_config()
    
    def _load_config(self):
        """Load configuration from various sources with priority order"""
        # Priority: Environment Variables > Site Config > Frappe Settings > Defaults
        
        for key, default_value in self.DEFAULTS.items():
            value = default_value
            
            # 1. Check environment variables
            env_key = f"MIGRATION_{key}"
            if env_key in os.environ:
                value = self._convert_env_value(os.environ[env_key], type(default_value))
            
            # 2. Check site config
            elif hasattr(frappe, 'conf') and frappe.conf:
                site_key = f"migration_{key.lower()}"
                if site_key in frappe.conf:
                    value = frappe.conf[site_key]
            
            # 3. Check Migration Settings DocType
            else:
                try:
                    settings = frappe.get_single('Migration Settings')
                    settings_key = key.lower()
                    if hasattr(settings, settings_key) and getattr(settings, settings_key) is not None:
                        value = getattr(settings, settings_key)
                except Exception:
                    pass  # Use default if settings not available
            
            self._config_cache[key] = value
    
    def _convert_env_value(self, env_value: str, target_type: type) -> Any:
        """Convert environment variable string to appropriate type"""
        try:
            if target_type == bool:
                return env_value.lower() in ('true', '1', 'yes', 'on')
            elif target_type == int:
                return int(env_value)
            elif target_type == float:
                return float(env_value)
            elif target_type == list:
                # Assume comma-separated values
                return [item.strip() for item in env_value.split(',') if item.strip()]
            else:
                return env_value
        except (ValueError, TypeError):
            return self.DEFAULTS.get(env_value, env_value)
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key"""
        return self._config_cache.get(key, default or self.DEFAULTS.get(key))
    
    def set(self, key: str, value: Any):
        """Set configuration value (runtime only)"""
        self._config_cache[key] = value
    
    def get_file_limits(self) -> Dict[str, Any]:
        """Get file processing limits"""
        return {
            'max_size_mb': self.get('MAX_FILE_SIZE_MB'),
            'chunk_size': self.get('CHUNK_SIZE'),
            'supported_extensions': self.get('SUPPORTED_EXTENSIONS'),
            'max_filename_length': self.get('MAX_FILENAME_LENGTH')
        }
    
    def get_performance_limits(self) -> Dict[str, Any]:
        """Get performance and resource limits"""
        return {
            'max_memory_mb': self.get('MAX_MEMORY_MB'),
            'min_available_memory_mb': self.get('MIN_AVAILABLE_MEMORY_MB'),
            'max_memory_percent': self.get('MAX_MEMORY_PERCENT'),
            'max_cpu_percent': self.get('MAX_CPU_PERCENT'),
            'max_operation_time_seconds': self.get('MAX_OPERATION_TIME_SECONDS')
        }
    
    def get_database_limits(self) -> Dict[str, Any]:
        """Get database operation limits"""
        return {
            'max_name_attempts': self.get('MAX_NAME_GENERATION_ATTEMPTS'),
            'batch_size': self.get('BATCH_SIZE'),
            'max_retries': self.get('MAX_RETRIES'),
            'timeout_seconds': self.get('DB_TIMEOUT_SECONDS')
        }
    
    def get_field_type_config(self) -> Dict[str, Any]:
        """Get field type detection configuration"""
        return {
            'data_field_max_length': self.get('DATA_FIELD_MAX_LENGTH'),
            'small_text_max_length': self.get('SMALL_TEXT_MAX_LENGTH'),
            'medium_text_max_length': self.get('MEDIUM_TEXT_MAX_LENGTH'),
            'phone_min_length': self.get('PHONE_MIN_LENGTH'),
            'email_max_length': self.get('EMAIL_MAX_LENGTH')
        }
    
    def get_security_config(self) -> Dict[str, Any]:
        """Get security configuration"""
        return {
            'allowed_mime_types': self.get('ALLOWED_MIME_TYPES'),
            'max_path_depth': self.get('MAX_PATH_DEPTH'),
            'forbidden_path_components': self.get('FORBIDDEN_PATH_COMPONENTS')
        }
    
    def validate_config(self) -> list:
        """Validate current configuration and return list of errors"""
        errors = []
        
        # Validate file limits
        if self.get('MAX_FILE_SIZE_MB') <= 0:
            errors.append("MAX_FILE_SIZE_MB must be greater than 0")
        
        if self.get('CHUNK_SIZE') <= 0:
            errors.append("CHUNK_SIZE must be greater than 0")
        
        # Validate performance limits
        if not (0 < self.get('MAX_MEMORY_PERCENT') <= 100):
            errors.append("MAX_MEMORY_PERCENT must be between 1 and 100")
        
        if not (0 < self.get('MAX_CPU_PERCENT') <= 100):
            errors.append("MAX_CPU_PERCENT must be between 1 and 100")
        
        # Validate database limits
        if self.get('MAX_NAME_GENERATION_ATTEMPTS') <= 0:
            errors.append("MAX_NAME_GENERATION_ATTEMPTS must be greater than 0")
        
        if self.get('BATCH_SIZE') <= 0:
            errors.append("BATCH_SIZE must be greater than 0")
        
        # Validate field type config
        field_config = self.get_field_type_config()
        if not (0 < field_config['data_field_max_length'] <= 1000):
            errors.append("DATA_FIELD_MAX_LENGTH must be between 1 and 1000")
        
        return errors
    
    def get_environment_summary(self) -> Dict[str, Any]:
        """Get summary of current environment configuration"""
        return {
            'config_source': {
                'environment_vars': {k: v for k, v in os.environ.items() if k.startswith('MIGRATION_')},
                'site_config_keys': [k for k in (frappe.conf or {}).keys() if k.startswith('migration_')],
                'defaults_used': list(self.DEFAULTS.keys())
            },
            'current_values': dict(self._config_cache),
            'validation_errors': self.validate_config()
        }
    
    def reload(self):
        """Reload configuration from all sources"""
        self._config_cache.clear()
        self._load_config()


# Global configuration instance
migration_config = MigrationConfig()

# Convenience functions for common configurations
def get_file_limits():
    return migration_config.get_file_limits()

def get_performance_limits():
    return migration_config.get_performance_limits()

def get_database_limits():
    return migration_config.get_database_limits()

def get_field_type_config():
    return migration_config.get_field_type_config()

def get_security_config():
    return migration_config.get_security_config()