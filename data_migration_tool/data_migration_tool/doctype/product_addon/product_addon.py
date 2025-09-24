# Copyright (c) 2025, Ayush and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document

class ProductAddon(Document):
    def validate(self):
        if not self.addon:
            frappe.throw("Add-on is required")
        
        if self.quantity and int(self.quantity) <= 0:
            frappe.throw("Quantity must be greater than 0")
        
        if self.custom_price and float(self.custom_price) < 0:
            frappe.throw("Custom Price cannot be negative")
