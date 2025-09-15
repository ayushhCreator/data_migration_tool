import frappe
import re
from typing import Dict, List

class FieldMapper:
    """Simple field mapper for CSV processing"""
    
    def __init__(self):
        pass
    
    def get_field_mappings(self, csv_headers: List[str], target_doctype: str) -> Dict[str, str]:
        """Get field mappings for CSV headers"""
        mappings = {}
        
        # Your business-specific mappings
        universal_mappings = {
            # Item fields
            'item_id': 'item_code',
            'item_name': 'item_name',
            'rate': 'standard_rate',
            'unit_name': 'stock_uom',
            'product_type': 'item_group',
            'description': 'description',
            
            # Contact fields  
            'display_name': 'full_name',
            'emailid': 'email_id',
            'phone': 'mobile_no',
            'contact_id': 'name',
            'first_name': 'first_name',
            'last_name': 'last_name',
            
            # Address fields
            'address': 'address_line1',
            'city': 'city',
            'state': 'state',
            'country': 'country',
            
            # Invoice fields
            'invoice_id': 'name',
            'customer': 'customer',
            'total': 'grand_total',
            
            # Common fields
            'name': 'item_name',
            'email': 'email_id',
            'mobile': 'mobile_no',
            'amount': 'total_amount'
        }
        
        for header in csv_headers:
            clean_header = header.lower().replace(' ', '_').replace('-', '_')
            if clean_header in universal_mappings:
                mappings[header] = universal_mappings[clean_header]
        
        return mappings
