# Copyright (c) 2025, Ayush and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document

class Addon(Document):
    def validate(self):
        if not self.addon_name:
            frappe.throw("Add-on Name is required")
        
        if self.default_price and float(self.default_price) < 0:
            frappe.throw("Default Price cannot be negative")
    
    def before_save(self):
        if self.addon_name:
            self.addon_name = self.addon_name.strip().title()
