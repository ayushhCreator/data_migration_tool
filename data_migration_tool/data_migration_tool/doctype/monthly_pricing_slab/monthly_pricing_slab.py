# Copyright (c) 2025, Ayush and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document

class MonthlyPricingSlab(Document):
    def validate(self):
        if not self.duration:
            frappe.throw("Duration is required")
        
        if not self.price or float(self.price) <= 0:
            frappe.throw("Price must be greater than 0")
        
        if self.discount_pct:
            discount = float(self.discount_pct)
            if discount < 0 or discount > 100:
                frappe.throw("Discount percentage must be between 0 and 100")
        
        # Calculate effective rate
        self._calculate_effective_rate()
    
    def _calculate_effective_rate(self):
        if self.price and self.discount_pct:
            price = float(self.price)
            discount = float(self.discount_pct) / 100
            self.effective_rate = price * (1 - discount)
        else:
            self.effective_rate = self.price or 0
