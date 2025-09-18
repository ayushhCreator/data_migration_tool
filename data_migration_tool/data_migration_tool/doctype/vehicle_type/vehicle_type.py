# Copyright (c) 2025, Ayush and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document

class VehicleType(Document):
    def validate(self):
        if not self.vehicle_name:
            frappe.throw("Vehicle Type Name is required")
    
    def before_save(self):
        if self.vehicle_name:
            self.vehicle_name = self.vehicle_name.strip().title()
