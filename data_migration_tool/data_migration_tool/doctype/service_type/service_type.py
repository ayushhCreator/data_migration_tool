# Copyright (c) 2025, Ayush and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document

class ServiceType(Document):
    def validate(self):
        if not self.service_name:
            frappe.throw("Service Type Name is required")
    
    def before_save(self):
        if self.service_name:
            self.service_name = self.service_name.strip().title()
