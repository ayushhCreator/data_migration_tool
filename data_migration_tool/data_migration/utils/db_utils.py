import frappe
import re
from typing import Any, Dict, List, Optional, Union

class SafeDBOperations:
    """Safe database operations with proper error handling and validation"""
    
    @staticmethod
    def safe_exists(doctype: str, name: str) -> bool:
        """Safely check if document exists"""
        try:
            if not doctype or not name:
                return False
            return bool(frappe.db.exists(doctype, name))
        except Exception as e:
            frappe.log_error(f"Database exists check failed for {doctype}:{name} - {str(e)}")
            return False
    
    @staticmethod
    def safe_get_value(doctype: str, filters: Union[str, Dict], fieldname: Union[str, List[str]] = "name") -> Any:
        """Safely get value from database"""
        try:
            return frappe.db.get_value(doctype, filters, fieldname)
        except Exception as e:
            frappe.log_error(f"Database get_value failed for {doctype} - {str(e)}")
            return None
    
    @staticmethod
    def generate_unique_name(doctype: str, base_name: str, max_attempts: int = 10000) -> str:
        """Generate unique name with reasonable limits"""
        if not base_name:
            base_name = "Unnamed"
        
        # Clean base name
        clean_base = re.sub(r'[^a-zA-Z0-9\-_\s]', '', base_name)[:50]
        if not clean_base:
            clean_base = "Record"
        
        # Try original name first
        if not SafeDBOperations.safe_exists(doctype, clean_base):
            return clean_base
        
        # Try with counters
        for counter in range(1, max_attempts + 1):
            test_name = f"{clean_base}-{counter}"
            if not SafeDBOperations.safe_exists(doctype, test_name):
                return test_name
        
        # Last resort - use UUID
        unique_suffix = frappe.utils.generate_hash()[:8]
        return f"{clean_base}-{unique_suffix}"
    
    @staticmethod
    def batch_insert(doctype: str, records: List[Dict], batch_size: int = 100) -> Dict[str, int]:
        """Safely insert records in batches"""
        results = {"success": 0, "failed": 0, "errors": []}
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            
            try:
                for record in batch:
                    try:
                        doc = frappe.get_doc(record)
                        doc.insert()
                        results["success"] += 1
                    except Exception as e:
                        results["failed"] += 1
                        error_msg = f"Failed to insert record: {str(e)}"
                        results["errors"].append(error_msg)
                        frappe.log_error(error_msg, record)
                
                # Commit after each batch
                frappe.db.commit()
                
            except Exception as e:
                frappe.db.rollback()
                results["failed"] += len(batch)
                error_msg = f"Batch insert failed: {str(e)}"
                results["errors"].append(error_msg)
                frappe.log_error(error_msg)
        
        return results
    
    @staticmethod
    def safe_sql_query(query: str, values: tuple = None, as_dict: bool = False) -> Optional[Any]:
        """Execute SQL query safely with parameterized values"""
        try:
            if values:
                return frappe.db.sql(query, values, as_dict=as_dict)
            else:
                return frappe.db.sql(query, as_dict=as_dict)
        except Exception as e:
            frappe.log_error(f"SQL query failed: {query} - {str(e)}")
            return None if not as_dict else []
    
    @staticmethod
    def validate_doctype_name(doctype_name: str) -> bool:
        """Validate if doctype name is safe and valid"""
        if not doctype_name or not isinstance(doctype_name, str):
            return False
        
        # Check length (MySQL table name limit)
        if len(doctype_name) > 61:
            return False
        
        # Check format - must start with letter, only alphanumeric, spaces, hyphens, underscores
        if not re.match(r'^[A-Za-z][A-Za-z0-9 _-]*$', doctype_name):
            return False
        
        # Check for reserved words
        reserved_words = ['user', 'select', 'insert', 'delete', 'update', 'drop', 'create', 'alter']
        if doctype_name.lower() in reserved_words:
            return False
        
        return True
    
    @staticmethod
    def escape_like_pattern(pattern: str) -> str:
        """Escape special characters in LIKE patterns"""
        if not pattern:
            return ""
        
        # Escape special LIKE characters
        pattern = pattern.replace('\\', '\\\\')
        pattern = pattern.replace('%', '\\%')
        pattern = pattern.replace('_', '\\_')
        
        return pattern