# Copyright (c) 2025, Ayush and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document

class ImportLog(Document):
    def validate(self):
        """Validate import log entry"""
        if not self.import_timestamp:
            self.import_timestamp = frappe.utils.now()
        
        # Truncate raw data preview to reasonable size
        if self.raw_data_preview and len(self.raw_data_preview) > 1000:
            self.raw_data_preview = self.raw_data_preview[:1000] + "..."
    
    def before_save(self):
        """Set timestamps"""
        if not self.import_timestamp:
            self.import_timestamp = frappe.utils.now()
