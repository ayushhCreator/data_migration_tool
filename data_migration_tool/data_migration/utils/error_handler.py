import frappe
import traceback
from typing import Dict, Any, Optional, List
from enum import Enum

class ErrorSeverity(Enum):
    """Error severity levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class ErrorCategory(Enum):
    """Error categories for better organization"""
    VALIDATION = "validation"
    SECURITY = "security"
    RESOURCE = "resource"
    DATABASE = "database"
    FILE_PROCESSING = "file_processing"
    CONFIGURATION = "configuration"
    SYSTEM = "system"
    NETWORK = "network"

class MigrationError(Exception):
    """Custom exception for migration operations"""
    
    def __init__(self, message: str, category: ErrorCategory = ErrorCategory.SYSTEM, 
                 severity: ErrorSeverity = ErrorSeverity.MEDIUM, context: Dict = None,
                 suggestions: List[str] = None):
        super().__init__(message)
        self.category = category
        self.severity = severity
        self.context = context or {}
        self.suggestions = suggestions or []

class UserFriendlyErrorHandler:
    """Handles errors with user-friendly messages and actionable suggestions"""
    
    def __init__(self, logger=None):
        self.logger = logger or frappe.logger()
    
    def format_error_message(self, error: Exception, context: Dict = None) -> Dict[str, Any]:
        """Format error message with context and suggestions"""
        context = context or {}
        
        # Determine error category and severity
        if isinstance(error, MigrationError):
            category = error.category
            severity = error.severity
            suggestions = error.suggestions
        else:
            category, severity, suggestions = self._categorize_error(error, context)
        
        # Create user-friendly message
        user_message = self._create_user_message(error, category, context)
        
        # Get actionable suggestions
        if not suggestions:
            suggestions = self._get_suggestions(error, category, context)
        
        return {
            'error_type': type(error).__name__,
            'category': category.value,
            'severity': severity.value,
            'user_message': user_message,
            'technical_message': str(error),
            'suggestions': suggestions,
            'context': context,
            'timestamp': frappe.utils.now(),
            'traceback': traceback.format_exc() if frappe.conf.get('developer_mode') else None
        }
    
    def _categorize_error(self, error: Exception, context: Dict) -> tuple:
        """Categorize error and determine severity"""
        error_str = str(error).lower()
        error_type = type(error).__name__
        
        # Security errors
        if any(keyword in error_str for keyword in ['permission', 'unauthorized', 'forbidden', 'security']):
            return ErrorCategory.SECURITY, ErrorSeverity.HIGH, []
        
        # Validation errors
        if any(keyword in error_str for keyword in ['invalid', 'required', 'missing', 'format']):
            return ErrorCategory.VALIDATION, ErrorSeverity.MEDIUM, []
        
        # Resource errors
        if any(keyword in error_str for keyword in ['memory', 'disk', 'space', 'timeout', 'resource']):
            return ErrorCategory.RESOURCE, ErrorSeverity.HIGH, []
        
        # Database errors
        if any(keyword in error_str for keyword in ['database', 'sql', 'connection', 'duplicate']):
            return ErrorCategory.DATABASE, ErrorSeverity.HIGH, []
        
        # File processing errors
        if any(keyword in error_str for keyword in ['file', 'csv', 'excel', 'path', 'encoding']):
            return ErrorCategory.FILE_PROCESSING, ErrorSeverity.MEDIUM, []
        
        # Network errors
        if any(keyword in error_str for keyword in ['network', 'connection', 'timeout', 'url']):
            return ErrorCategory.NETWORK, ErrorSeverity.MEDIUM, []
        
        # Configuration errors
        if any(keyword in error_str for keyword in ['config', 'setting', 'parameter']):
            return ErrorCategory.CONFIGURATION, ErrorSeverity.MEDIUM, []
        
        # Default to system error
        return ErrorCategory.SYSTEM, ErrorSeverity.MEDIUM, []
    
    def _create_user_message(self, error: Exception, category: ErrorCategory, context: Dict) -> str:
        """Create user-friendly error message"""
        base_message = str(error)
        
        # Add context-specific information
        if category == ErrorCategory.FILE_PROCESSING:
            file_name = context.get('filename', 'Unknown file')
            line_number = context.get('line_number')
            if line_number:
                return f"Error processing file '{file_name}' at line {line_number}: {base_message}"
            else:
                return f"Error processing file '{file_name}': {base_message}"
        
        elif category == ErrorCategory.VALIDATION:
            field_name = context.get('field_name', 'Unknown field')
            return f"Validation error in field '{field_name}': {base_message}"
        
        elif category == ErrorCategory.DATABASE:
            doctype = context.get('doctype', 'Unknown DocType')
            return f"Database error while working with '{doctype}': {base_message}"
        
        elif category == ErrorCategory.RESOURCE:
            operation = context.get('operation', 'unknown operation')
            return f"Resource limit exceeded during {operation}: {base_message}"
        
        elif category == ErrorCategory.SECURITY:
            return f"Security validation failed: {base_message}"
        
        elif category == ErrorCategory.CONFIGURATION:
            setting_name = context.get('setting_name', 'configuration')
            return f"Configuration error in {setting_name}: {base_message}"
        
        else:
            return base_message
    
    def _get_suggestions(self, error: Exception, category: ErrorCategory, context: Dict) -> List[str]:
        """Get actionable suggestions based on error category"""
        suggestions = []
        
        if category == ErrorCategory.FILE_PROCESSING:
            suggestions.extend([
                "Check if the file format is supported (.csv, .xlsx, .xls)",
                "Verify the file is not corrupted or empty",
                "Ensure the file encoding is UTF-8",
                "Try reducing the file size by splitting into smaller files"
            ])
        
        elif category == ErrorCategory.VALIDATION:
            suggestions.extend([
                "Check the data format matches the expected field type",
                "Verify all required fields have values",
                "Remove any special characters that might be causing issues",
                "Check the Migration Settings configuration"
            ])
        
        elif category == ErrorCategory.DATABASE:
            suggestions.extend([
                "Check database connectivity",
                "Verify the DocType exists and is accessible",
                "Check user permissions for the target DocType",
                "Ensure the database has sufficient space"
            ])
        
        elif category == ErrorCategory.RESOURCE:
            suggestions.extend([
                "Reduce the file size or process in smaller batches",
                "Free up system memory by closing other applications",
                "Check available disk space",
                "Consider increasing system resources"
            ])
        
        elif category == ErrorCategory.SECURITY:
            suggestions.extend([
                "Verify user has necessary permissions",
                "Check file path for invalid characters",
                "Ensure file is from a trusted source",
                "Contact system administrator if issue persists"
            ])
        
        elif category == ErrorCategory.CONFIGURATION:
            suggestions.extend([
                "Check Migration Settings configuration",
                "Verify all required fields are filled",
                "Check environment variables and site configuration",
                "Contact administrator to review settings"
            ])
        
        elif category == ErrorCategory.NETWORK:
            suggestions.extend([
                "Check internet connection",
                "Verify API credentials are correct",
                "Check if external services are accessible",
                "Retry the operation after some time"
            ])
        
        else:
            suggestions.extend([
                "Check the system logs for more details",
                "Retry the operation",
                "Contact support if the issue persists"
            ])
        
        return suggestions
    
    def handle_error(self, error: Exception, context: Dict = None, notify_user: bool = True) -> Dict[str, Any]:
        """Handle error with logging and user notification"""
        error_info = self.format_error_message(error, context)
        
        # Log error with appropriate level
        if error_info['severity'] == ErrorSeverity.CRITICAL.value:
            self.logger.critical(f"💥 CRITICAL ERROR: {error_info['user_message']}", extra=error_info)
        elif error_info['severity'] == ErrorSeverity.HIGH.value:
            self.logger.error(f"❌ ERROR: {error_info['user_message']}", extra=error_info)
        elif error_info['severity'] == ErrorSeverity.MEDIUM.value:
            self.logger.warning(f"⚠️ WARNING: {error_info['user_message']}", extra=error_info)
        else:
            self.logger.info(f"ℹ️ INFO: {error_info['user_message']}", extra=error_info)
        
        # Notify user if requested
        if notify_user and frappe.local.request:
            self._notify_user(error_info)
        
        return error_info
    
    def _notify_user(self, error_info: Dict[str, Any]):
        """Send user notification about the error"""
        try:
            # Determine message indicator based on severity
            if error_info['severity'] in [ErrorSeverity.CRITICAL.value, ErrorSeverity.HIGH.value]:
                indicator = 'red'
            elif error_info['severity'] == ErrorSeverity.MEDIUM.value:
                indicator = 'orange'
            else:
                indicator = 'blue'
            
            # Create notification message
            message = error_info['user_message']
            if error_info['suggestions']:
                message += f"<br><br><strong>Suggestions:</strong><ul>"
                for suggestion in error_info['suggestions'][:3]:  # Limit to 3 suggestions
                    message += f"<li>{suggestion}</li>"
                message += "</ul>"
            
            frappe.msgprint(message, indicator=indicator, title=f"{error_info['category'].title()} Error")
            
        except Exception as e:
            self.logger.error(f"Failed to notify user about error: {str(e)}")

# Global error handler instance
error_handler = UserFriendlyErrorHandler()

# Convenience functions
def handle_migration_error(error: Exception, context: Dict = None, notify_user: bool = True) -> Dict[str, Any]:
    """Handle migration error with user-friendly messaging"""
    return error_handler.handle_error(error, context, notify_user)

def create_migration_error(message: str, category: ErrorCategory = ErrorCategory.SYSTEM,
                         severity: ErrorSeverity = ErrorSeverity.MEDIUM, context: Dict = None,
                         suggestions: List[str] = None) -> MigrationError:
    """Create a migration error with proper categorization"""
    return MigrationError(message, category, severity, context, suggestions)