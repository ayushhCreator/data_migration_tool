import frappe
from frappe.model.document import Document

class CSVSchemaRegistry(Document):
    def validate(self):
        """Validate schema registry entry"""
        if not self.source_file:
            frappe.throw("Source file is required")
        
        if not self.schema_fingerprint:
            frappe.throw("Schema fingerprint is required")
        
        if not self.target_doctype:
            frappe.throw("Target DocType is required")
        
        # Ensure target DocType exists
        if not frappe.db.exists('DocType', self.target_doctype):
            frappe.throw(f"DocType '{self.target_doctype}' does not exist")
