import frappe
from frappe.model.document import Document

class ZohoModuleConfig(Document):
    def validate(self):
        """Validate Zoho module configuration"""
        if self.enabled and not self.module_name:
            frappe.throw("Module Name is required when enabled")
        
        # Validate module name format
        if self.module_name:
            allowed_modules = [
                'Leads', 'Contacts', 'Accounts', 'Deals', 'Tasks', 
                'Events', 'Calls', 'Products', 'Quotes', 'Sales_Orders',
                'Purchase_Orders', 'Invoices', 'Campaigns', 'Suppliers'
            ]
            
            if self.module_name not in allowed_modules:
                frappe.msgprint(f"Warning: '{self.module_name}' may not be a standard Zoho module. "
                              f"Standard modules: {', '.join(allowed_modules)}", 
                              indicator='yellow')
    
    def before_save(self):
        """Actions before saving"""
        if self.target_doctype and not frappe.db.exists('DocType', self.target_doctype):
            frappe.throw(f"Target DocType '{self.target_doctype}' does not exist")
