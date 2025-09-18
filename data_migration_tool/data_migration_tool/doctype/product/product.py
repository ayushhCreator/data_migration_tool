# Copyright (c) 2025, Ayush and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
from frappe.utils import cint, flt

class Product(Document):
    def validate(self):
        self._validate_core_fields()
        self._validate_service_logic()
        self._validate_addons()
        self._compute_pricing()
    
    def _validate_core_fields(self):
        """Validate required core fields"""
        required_fields = ["product_name", "service_category", "vehicle_type", "service_type"]
        for field in required_fields:
            if not self.get(field):
                frappe.throw(f"{field.replace('_', ' ').title()} is required")
        
        # Set default UOM if not provided
        if not self.stock_uom:
            self.stock_uom = "Nos"
    
    def _validate_service_logic(self):
        """Validate service type logic and pricing requirements"""
        if not self.service_type:
            return
            
        # Get service type details
        service_type_doc = frappe.get_doc("Service Type", self.service_type)
        is_subscription = cint(service_type_doc.is_subscription)
        
        if is_subscription:
            # Subscription services must have monthly slabs
            if not self.monthly_slabs:
                frappe.throw("Monthly pricing slabs are required for subscription services")
        else:
            # Non-subscription services must have one-time price
            if not self.one_time_price or flt(self.one_time_price) <= 0:
                frappe.throw("One-time price is required and must be greater than 0 for non-subscription services")
    
    def _validate_addons(self):
        """Validate addon configurations"""
        if not self.addons:
            return
            
        seen_addons = set()
        for addon_row in self.addons:
            if not addon_row.addon:
                frappe.throw("Add-on is required in add-on table")
            
            # Check for duplicates
            addon_key = (addon_row.addon, addon_row.quantity or 1)
            if addon_key in seen_addons:
                frappe.throw(f"Duplicate add-on '{addon_row.addon}' with same quantity")
            seen_addons.add(addon_key)
            
            # Validate quantity
            if addon_row.quantity and cint(addon_row.quantity) <= 0:
                frappe.throw("Add-on quantity must be greater than 0")
            
            # Validate custom price
            if addon_row.custom_price and flt(addon_row.custom_price) < 0:
                frappe.throw("Custom price cannot be negative")
    
    def _compute_pricing(self):
        """Compute effective rates for monthly slabs"""
        if not self.monthly_slabs:
            return
            
        for slab in self.monthly_slabs:
            if not slab.price or flt(slab.price) <= 0:
                frappe.throw("Monthly slab price must be greater than 0")
            
            discount_pct = flt(slab.discount_pct)
            if discount_pct < 0 or discount_pct > 100:
                frappe.throw("Discount percentage must be between 0 and 100")
            
            # Calculate effective rate
            price = flt(slab.price)
            discount_multiplier = 1 - (discount_pct / 100)
            slab.effective_rate = price * discount_multiplier
    
    def before_save(self):
        """Clean and format data before saving"""
        if self.product_name:
            self.product_name = self.product_name.strip()
    
    def after_insert(self):
        """Post-creation actions"""
        frappe.logger().info(f"Product {self.name} created successfully")
    
    def get_total_addon_price(self):
        """Calculate total addon price for this product"""
        total_addon_price = 0
        
        for addon_row in self.addons:
            # Use custom price if available, otherwise get default price from addon
            if addon_row.custom_price:
                price = flt(addon_row.custom_price)
            else:
                addon_doc = frappe.get_doc("Addon", addon_row.addon)
                price = flt(addon_doc.default_price)
            
            quantity = cint(addon_row.quantity) or 1
            total_addon_price += price * quantity
        
        return total_addon_price
