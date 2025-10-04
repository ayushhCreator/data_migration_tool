# Copyright (c) 2025, Ayush and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document

class ServiceCategory(Document):
    def validate(self):
        if not self.category_name:
            frappe.throw("Category Name is required")
        
        # Ensure unique category name
        if self.is_new():
            existing = frappe.db.exists("Service Category", {"category_name": self.category_name})
            if existing:
                frappe.throw(f"Service Category '{self.category_name}' already exists")
    
    def before_save(self):
        # Clean and format category name
        if self.category_name:
            self.category_name = self.category_name.strip().title()
