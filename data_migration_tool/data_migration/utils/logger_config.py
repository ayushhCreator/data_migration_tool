import frappe
import logging
from frappe.utils.logger import set_log_level
from typing import Dict, Any
import json

class MigrationLogger:
    def __init__(self, module_name: str = "data_migration"):
        self.module_name = module_name
        self.logger = self._setup_logger()
    
    def _setup_logger(self):
        """Setup custom logger with multiple log levels"""
        set_log_level("DEBUG")
        return frappe.logger(
            self.module_name,
            allow_site=True,
            file_count=20,  # Keep 20 log files
            max_size=1024*1024  # 1MB per file
        )
    
    def log_migration_start(self, source: str, record_count: int):
        """Log migration start with context"""
        self.logger.info(f"🚀 Migration started", extra={
            "source": source,
            "record_count": record_count,
            "user": frappe.session.user,
            "timestamp": frappe.utils.now()
        })
    
    def log_record_processing(self, record_id: str, action: str, status: str):
        """Log individual record processing"""
        if status == "success":
            self.logger.info(f"✅ Record processed successfully", extra={
                "record_id": record_id,
                "action": action,
                "status": status
            })
        elif status == "warning":
            self.logger.warning(f"⚠️ Record processed with warnings", extra={
                "record_id": record_id, 
                "action": action,
                "status": status
            })
        else:
            self.logger.error(f"❌ Record processing failed", extra={
                "record_id": record_id,
                "action": action, 
                "status": status
            })
    
    def log_doctype_creation(self, doctype_name: str, fields: list):
        """Log new DocType creation"""
        self.logger.info(f"🏗️ New DocType created", extra={
            "doctype_name": doctype_name,
            "field_count": len(fields),
            "fields": json.dumps(fields)
        })
    
    def log_field_mapping(self, source_field: str, target_field: str, doctype: str):
        """Log field mapping decisions"""
        self.logger.debug(f"🔗 Field mapped", extra={
            "source_field": source_field,
            "target_field": target_field,
            "doctype": doctype
        })
    
    def log_error(self, error: Exception, context: Dict[str, Any]):
        """Log errors with full context"""
        self.logger.error(f"💥 Migration error: {str(error)}", extra={
            "error_type": type(error).__name__,
            "error_message": str(error),
            "context": json.dumps(context),
            "traceback": frappe.get_traceback()
        })

# Global logger instance
migration_logger = MigrationLogger()
