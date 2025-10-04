# Copyright (c) 2025, Ayush and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document

class MigrationDataBuffer(Document):
    """Migration Data Buffer for JIT processing"""
    
    def validate(self):
        """Validate buffer record"""
        if not self.created_at:
            self.created_at = frappe.utils.now()
    
    def on_update(self):
        """Update processed timestamp when status changes"""
        if self.processing_status in ['Processed', 'Failed', 'Skipped']:
            if not self.processed_at:
                self.processed_at = frappe.utils.now()
