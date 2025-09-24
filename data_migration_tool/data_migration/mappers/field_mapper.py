import frappe
import re
from typing import Dict, List


class EnhancedFieldMapper:
    def __init__(self):
        # Business-specific intelligent mappings
        self.universal_mappings = {
            # Customer/Supplier fields
            'customer_id': 'name', 'cust_id': 'name', 'id': 'name',
            'customer_name': 'customer_name', 'company_name': 'customer_name',
            'supplier_name': 'supplier_name', 'vendor_name': 'supplier_name',
            
            # Contact fields
            'email': 'email_id', 'e_mail': 'email_id', 'emailid': 'email_id',
            'phone': 'mobile_no', 'mobile': 'mobile_no', 'contact': 'mobile_no',
            'first_name': 'first_name', 'fname': 'first_name',
            'last_name': 'last_name', 'lname': 'last_name',
            
            # Address fields
            'address': 'address_line1', 'addr1': 'address_line1',
            'address2': 'address_line2', 'addr2': 'address_line2',
            
            # Financial fields
            'amount': 'amount', 'total': 'grand_total', 'value': 'amount',
            'rate': 'rate', 'price': 'rate', 'unit_price': 'rate',
            
            # Item fields
            'item_id': 'item_code', 'sku': 'item_code', 'product_code': 'item_code',
            'item_name': 'item_name', 'product_name': 'item_name',
            'description': 'description', 'desc': 'description'
        }
        
        # DocType-specific identifier priorities
        self.identifier_priorities = {
            'Customer': ['customer_id', 'id', 'email_id', 'customer_name'],
            'Supplier': ['supplier_id', 'id', 'email_id', 'supplier_name'],  
            'Contact': ['contact_id', 'id', 'email_id', 'mobile_no'],
            'Item': ['item_code', 'sku', 'item_id', 'item_name'],
            'Lead': ['lead_id', 'id', 'email_id', 'mobile_no']
        }
    
    def get_comprehensive_mapping(self, headers: List[str], target_doctype: str) -> Dict[str, Any]:
        """Get complete field mapping with identifiers and validation"""
        mappings = {}
        identifier_fields = []
        
        # Get DocType metadata
        try:
            meta = frappe.get_meta(target_doctype)
            available_fields = [f.fieldname for f in meta.fields]
        except:
            available_fields = []
        
        normalized_headers = [h.lower().replace(' ', '_').replace('-', '_') for h in headers]
        
        for i, original_header in enumerate(headers):
            clean_header = normalized_headers[i]
            
            # Direct mapping first
            if clean_header in self.universal_mappings:
                target_field = self.universal_mappings[clean_header]
                if target_field in available_fields or target_field == 'name':
                    mappings[original_header] = target_field
                    
                    # Mark as identifier if it's in the priority list
                    if target_doctype in self.identifier_priorities:
                        if target_field in self.identifier_priorities[target_doctype]:
                            identifier_fields.append(target_field)
            else:
                # Fuzzy matching for unmapped fields
                similar_field = self.find_similar_field(clean_header, available_fields)
                if similar_field:
                    mappings[original_header] = similar_field
        
        return {
            'field_mappings': mappings,
            'identifier_fields': identifier_fields or ['name'],  # fallback
            'unmapped_headers': [h for h in headers if h not in mappings],
            'confidence': len(mappings) / len(headers) * 100
        }
    
    def find_similar_field(self, target: str, available: List[str]) -> str:
        """Enhanced fuzzy matching"""
        import difflib
        
        # Exact match
        if target in available:
            return target
            
        # Substring matching
        for field in available:
            if target in field.lower() or field.lower() in target:
                return field
        
        # Fuzzy matching with higher threshold
        matches = difflib.get_close_matches(target, available, n=1, cutoff=0.7)
        return matches[0] if matches else None
