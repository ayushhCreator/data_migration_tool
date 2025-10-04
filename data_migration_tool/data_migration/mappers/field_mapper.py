# import frappe
# import re
# from typing import Dict, List

# class FieldMapper:
#     """Simple field mapper for CSV processing"""
    
#     def __init__(self):
#         pass
    
#     def get_field_mappings(self, csv_headers: List[str], target_doctype: str) -> Dict[str, str]:
#         """Get field mappings for CSV headers"""
#         mappings = {}
        
#         # Your business-specific mappings
#         universal_mappings = {
#             # Item fields
#             'item_id': 'item_code',
#             'item_name': 'item_name',
#             'rate': 'standard_rate',
#             'unit_name': 'stock_uom',
#             'product_type': 'item_group',
#             'description': 'description',
            
#             # Contact fields  
#             'display_name': 'full_name',
#             'emailid': 'email_id',
#             'phone': 'mobile_no',
#             'contact_id': 'name',
#             'first_name': 'first_name',
#             'last_name': 'last_name',
            
#             # Address fields
#             'address': 'address_line1',
#             'city': 'city',
#             'state': 'state',
#             'country': 'country',
            
#             # Invoice fields
#             'invoice_id': 'name',
#             'customer': 'customer',
#             'total': 'grand_total',
            
#             # Common fields
#             'name': 'item_name',
#             'email': 'email_id',
#             'mobile': 'mobile_no',
#             'amount': 'total_amount'
#         }
        
#         for header in csv_headers:
#             clean_header = header.lower().replace(' ', '_').replace('-', '_')
#             if clean_header in universal_mappings:
#                 mappings[header] = universal_mappings[clean_header]
        
#         return mappings



import frappe
import re
from typing import Dict, List, Optional, Tuple


class FieldMapper:
    """Enhanced field mapper for CSV processing with smart field detection"""

    def __init__(self):
        # Load DocType field cache for faster lookups
        self._doctype_field_cache = {}

    def get_field_mappings(self, csv_headers: List[str], target_doctype: str) -> Dict[str, str]:
        """Get field mappings for CSV headers with smart detection"""
        mappings = {}

        # Get your existing universal mappings
        universal_mappings = self._get_universal_mappings()

        # Get your custom DocType mappings (NEW - handles your current DocTypes)
        custom_mappings = self._get_custom_doctype_mappings(target_doctype)

        # Get smart pattern-based mappings (NEW - prevents future issues)
        smart_mappings = self._get_smart_pattern_mappings(csv_headers, target_doctype)

        # Merge all mappings (priority: custom > smart > universal)
        all_mappings = {**universal_mappings, **smart_mappings, **custom_mappings}

        for header in csv_headers:
            clean_header = header.lower().replace(' ', '_').replace('-', '_')

            # Direct mapping
            if clean_header in all_mappings:
                mappings[header] = all_mappings[clean_header]
            # Fuzzy matching for similar names
            elif self._find_fuzzy_match(clean_header, all_mappings):
                mappings[header] = self._find_fuzzy_match(clean_header, all_mappings)
            # Smart DocType field detection
            else:
                smart_match = self._detect_doctype_field(header, target_doctype)
                if smart_match:
                    mappings[header] = smart_match

        return mappings

    def _get_universal_mappings(self) -> Dict[str, str]:
        """Your existing universal mappings"""
        return {
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

    # def _get_custom_doctype_mappings(self, target_doctype: str) -> Dict[str, str]:
    #     """Custom mappings for your specific DocTypes - SOLVES YOUR CURRENT ISSUE"""

    #     doctype_mappings = {
    #         'Service Category': {
    #             'category_id': 'category_id',
    #             'category_name': 'category_name', 
    #             'description': 'description'
    #         },
    #         'Service Type': {
    #             'service_type_id': 'service_type_id',
    #             'service_type': 'service_type',
    #             'description': 'description'
    #         },
    #         'Vehicle Type': {
    #             'vehicle_type_id': 'vehicle_type_id', 
    #             'vehicle_type': 'vehicle_type',
    #             'description': 'description'
    #         },
    #         'Product': {
    #             'product_id': 'product_id',
    #             'product_name': 'product_name',
    #             'category_id': 'category_id',
    #             'vehicle_type_id': 'vehicle_type_id',
    #             'service_type_id': 'service_type_id',
    #             'base_price': 'base_price',
    #             'frequency': 'frequency',
    #             'description': 'description'
    #         },
    #         'Addon': {
    #             'addon_id': 'addon_id',
    #             'addon_name': 'addon_name',
    #             'price': 'price',
    #             'default_price': 'default_price',
    #             'description': 'description'
    #         }
    #     }

    #     return doctype_mappings.get(target_doctype, {})
    def _get_pattern_based_mappings(self, csv_headers: List[str], target_doctype: str) -> Dict[str, str]:
        """Get field mappings using generic patterns"""
        mappings = {}
        # Get target DocType fields
        if frappe.db.exists('DocType', target_doctype):
            meta = frappe.get_meta(target_doctype)
            target_fields = [f.fieldname for f in meta.fields]
        else:
            target_fields = []
        for header in csv_headers:
            clean_header = self._clean_field_name(header)
            # Try exact match first
            if clean_header in target_fields:
                mappings[header] = clean_header
                continue
            # Try pattern matching
            best_match = self._find_best_field_match(clean_header, target_fields)
            if best_match:
                mappings[header] = best_match
            else:
                mappings[header] = clean_header
        return mappings

    def _find_best_field_match(self, source_field: str, target_fields: List[str]) -> Optional[str]:
        """Find best matching target field using patterns"""
        source_lower = source_field.lower()
        # Direct substring matches
        for target in target_fields:
            if source_lower in target.lower() or target.lower() in source_lower:
                return target
        # Pattern-based matches
        if 'email' in source_lower:
            for f in target_fields:
                if 'email' in f.lower():
                    return f
        if 'phone' in source_lower or 'mobile' in source_lower:
            for f in target_fields:
                if any(p in f.lower() for p in ['phone', 'mobile', 'contact']):
                    return f
        # Fuzzy matching fallback
        import difflib
        matches = difflib.get_close_matches(source_field, target_fields, n=1, cutoff=0.6)
        if matches:
            return matches[0]
        return None


    def _get_smart_pattern_mappings(self, csv_headers: List[str], target_doctype: str) -> Dict[str, str]:
        """Smart pattern-based mappings for future CSVs"""

        mappings = {}
        doctype_base = target_doctype.lower().replace(' ', '_')

        # Pattern-based mappings
        patterns = {
            # ID patterns
            rf'^{doctype_base}_id$': f'{doctype_base}_id',
            rf'^{doctype_base}id$': f'{doctype_base}_id',
            r'^.*_id$': lambda match: match.group(0),
            r'^id$': 'name',

            # Name patterns  
            rf'^{doctype_base}_name$': f'{doctype_base}_name',
            rf'^{doctype_base}name$': f'{doctype_base}_name',
            r'^.*_name$': lambda match: match.group(0),
            r'^name$': 'name',
            r'^title$': 'name',

            # Price patterns
            r'^price$': 'price',
            r'^cost$': 'price', 
            r'^amount$': 'price',
            r'^rate$': 'price',
            r'^base_price$': 'base_price',
            r'^unit_price$': 'price',
            r'^default_price$': 'default_price',
            r'^one_time_price$': 'one_time_price',

            # Description patterns
            r'^description$': 'description',
            r'^desc$': 'description',
            r'^details$': 'description',
            r'^notes$': 'description',
            r'^remarks$': 'description',

            # Common business fields
            r'^frequency$': 'frequency',
            r'^status$': 'status',
            r'^active$': 'is_active',
            r'^is_active$': 'is_active',
            r'^enabled$': 'is_active'
        }

        for header in csv_headers:
            clean_header = header.lower().replace(' ', '_').replace('-', '_')

            for pattern, target_field in patterns.items():
                match = re.match(pattern, clean_header)
                if match:
                    if callable(target_field):
                        mappings[clean_header] = target_field(match)
                    else:
                        mappings[clean_header] = target_field
                    break

        return mappings

    def _find_fuzzy_match(self, csv_field: str, mappings: Dict[str, str]) -> Optional[str]:
        """Find fuzzy matches for field names"""

        # Check if any mapping key contains the csv_field or vice versa
        for mapping_key, mapping_value in mappings.items():
            if (csv_field in mapping_key or mapping_key in csv_field or
                self._fields_similar(csv_field, mapping_key)):
                return mapping_value

        return None

    def _fields_similar(self, field1: str, field2: str) -> bool:
        """Check if two fields are similar using various criteria"""

        # Remove common suffixes/prefixes
        clean1 = re.sub(r'_(id|name|type|code)$', '', field1)
        clean2 = re.sub(r'_(id|name|type|code)$', '', field2)

        if clean1 == clean2:
            return True

        # Check for common synonyms
        synonyms = {
            'name': ['title', 'label', 'display_name'],
            'description': ['desc', 'details', 'info', 'notes'],
            'price': ['cost', 'amount', 'value', 'rate'],
            'type': ['category', 'kind', 'class', 'group'],
            'id': ['identifier', 'key', 'code', 'uid']
        }

        for key, values in synonyms.items():
            if ((clean1 == key and clean2 in values) or 
                (clean2 == key and clean1 in values) or
                (clean1 in values and clean2 in values)):
                return True

        return False

    def _detect_doctype_field(self, csv_field: str, target_doctype: str) -> Optional[str]:
        """Detect DocType field by analyzing existing DocType structure"""

        try:
            if target_doctype not in self._doctype_field_cache:
                meta = frappe.get_meta(target_doctype)
                self._doctype_field_cache[target_doctype] = [
                    field.fieldname for field in meta.fields 
                    if field.fieldtype not in ['Section Break', 'Column Break', 'Tab Break']
                ]

            doctype_fields = self._doctype_field_cache[target_doctype]
            csv_clean = csv_field.lower().replace(' ', '_').replace('-', '_')

            # Direct match
            if csv_clean in doctype_fields:
                return csv_clean

            # Fuzzy match with existing fields
            for doctype_field in doctype_fields:
                if self._fields_similar(csv_clean, doctype_field):
                    return doctype_field

        except Exception as e:
            frappe.log_error(f"Error detecting DocType field: {str(e)}", "Field Detection Error")

        return None

    # NEW METHODS FOR ENHANCED FUNCTIONALITY

    def get_missing_fields_analysis(self, csv_headers: List[str], target_doctype: str) -> Dict:
        """Analyze which CSV fields are missing from target DocType"""

        mappings = self.get_field_mappings(csv_headers, target_doctype)

        missing_fields = []
        mapped_fields = []

        for header in csv_headers:
            clean_header = header.lower().replace(' ', '_').replace('-', '_')
            if clean_header in mappings:
                mapped_fields.append({
                    'csv_field': header,
                    'doctype_field': mappings[clean_header],
                    'status': 'mapped'
                })
            else:
                missing_fields.append({
                    'csv_field': header,
                    'suggested_field': self._suggest_field_for_doctype(header, target_doctype),
                    'status': 'missing'
                })

        return {
            'target_doctype': target_doctype,
            'total_csv_fields': len(csv_headers),
            'mapped_fields': mapped_fields,
            'missing_fields': missing_fields,
            'mapping_success_rate': len(mapped_fields) / len(csv_headers) * 100 if csv_headers else 0
        }

    def _suggest_field_for_doctype(self, csv_field: str, target_doctype: str) -> Dict:
        """Suggest how to add missing field to DocType"""

        clean_field = csv_field.lower().replace(' ', '_').replace('-', '_')

        # Determine field type
        if '_id' in clean_field or clean_field == 'id':
            if clean_field.replace('_id', '').replace('_', ' ').title() in ['Service Category', 'Service Type', 'Vehicle Type', 'Product', 'Addon']:
                fieldtype = 'Link'
                options = clean_field.replace('_id', '').replace('_', ' ').title()
            else:
                fieldtype = 'Data'
                options = None
        elif 'price' in clean_field or 'cost' in clean_field or 'amount' in clean_field:
            fieldtype = 'Currency'
            options = None
        elif 'date' in clean_field:
            fieldtype = 'Date'
            options = None  
        elif 'email' in clean_field:
            fieldtype = 'Data'
            options = None
        elif clean_field in ['description', 'desc', 'details', 'notes']:
            fieldtype = 'Text'
            options = None
        else:
            fieldtype = 'Data'
            options = None

        suggestion = {
            'fieldname': clean_field,
            'fieldtype': fieldtype,
            'label': csv_field.replace('_', ' ').title(),
            'reqd': 1 if '_id' in clean_field else 0,
            'unique': 1 if '_id' in clean_field else 0
        }

        if options:
            suggestion['options'] = options

        return suggestion

    def generate_field_addition_script(self, analysis: Dict) -> str:
        """Generate script to add missing fields to DocType"""

        if not analysis.get('missing_fields'):
            return "# No missing fields to add"

        script_lines = [
            f"# Field addition script for {analysis['target_doctype']}",
            "import frappe",
            "",
            f"def add_missing_fields_to_{analysis['target_doctype'].replace(' ', '_').lower()}():",
            f'    doctype = frappe.get_doc("DocType", "{analysis["target_doctype"]}")',
            ""
        ]

        for missing in analysis['missing_fields']:
            field_def = missing['suggested_field']
            script_lines.extend([
                "    doctype.append('fields', {",
                f'        "fieldname": "{field_def["fieldname"]}",',
                f'        "fieldtype": "{field_def["fieldtype"]}",',
                f'        "label": "{field_def["label"]}",',
                f'        "reqd": {field_def["reqd"]},',
                f'        "unique": {field_def["unique"]},'
            ])

            if field_def.get('options'):
                script_lines.append(f'        "options": "{field_def["options"]}",')

            script_lines.extend([
                "    })",
                ""
            ])

        script_lines.extend([
            "    doctype.save()",
            "    frappe.db.commit()",
            f'    print("Added {len(analysis["missing_fields"])} fields to {analysis["target_doctype"]}")',
            "",
            "# Run the function",
            f"add_missing_fields_to_{analysis['target_doctype'].replace(' ', '_').lower()}()"
        ])

        return "\n".join(script_lines)
    
    def get_yawlit_specific_mappings(self, target_doctype: str) -> Dict[str, str]:
        """Yawlit-specific field mappings"""
        mappings = {
            'Service Category': {
                'category_id': 'name',
                'category_name': 'category_name',
                'description': 'description'
            },
            'Service Type': {
                'service_type_id': 'name',
                'service_type': 'service_name',  # ⚠️ YOUR CSV USES service_type BUT DOCTYPE USES service_name
                'description': 'description'
            },
            'Vehicle Type': {
                'vehicle_type_id': 'name',
                'vehicle_type': 'vehicle_name',  # ⚠️ YOUR CSV USES vehicle_type BUT DOCTYPE USES vehicle_name
                'description': 'description'
            },
            'Addon': {
                'addon_id': 'name',
                'addon_name': 'addon_name',
                'price': 'default_price',  # ⚠️ YOUR CSV USES price BUT DOCTYPE USES default_price
                'description': 'description'
            },
            'Product': {
                'product_id': 'name',
                'product_name': 'product_name',
                'category_id': 'service_category',  # Link field
                'service_type_id': 'service_type',  # Link field  
                'vehicle_type_id': 'vehicle_type',  # Link field
                'base_price': 'one_time_price',  # ⚠️ YOUR CSV USES base_price BUT DOCTYPE USES one_time_price
                'frequency': 'frequency'
            }
        }
        return mappings.get(target_doctype, {})


# API Integration Methods
@frappe.whitelist()
def analyze_csv_field_compatibility(csv_filename: str, target_doctype: str) -> Dict:
    """API endpoint to analyze CSV field compatibility"""

    try:
        import pandas as pd
        import os

        settings = frappe.get_single('Migration Settings')
        csv_path = os.path.join(settings.csv_watch_directory, csv_filename)

        if not os.path.exists(csv_path):
            return {"error": f"CSV file not found: {csv_filename}"}

        # Read CSV headers
        df = pd.read_csv(csv_path, nrows=0)
        headers = df.columns.tolist()

        # Analyze compatibility
        field_mapper = FieldMapper()
        analysis = field_mapper.get_missing_fields_analysis(headers, target_doctype)

        # Add generation script
        analysis['field_addition_script'] = field_mapper.generate_field_addition_script(analysis)

        return analysis

    except Exception as e:
        return {"error": str(e)}


@frappe.whitelist()
def get_smart_field_mappings(csv_headers_json: str, target_doctype: str) -> Dict:
    """API endpoint to get smart field mappings"""

    try:
        import json
        csv_headers = json.loads(csv_headers_json)

        field_mapper = FieldMapper()
        mappings = field_mapper.get_field_mappings(csv_headers, target_doctype)

        return {
            "mappings": mappings,
            "total_headers": len(csv_headers),
            "mapped_count": len(mappings),
            "success_rate": len(mappings) / len(csv_headers) * 100 if csv_headers else 0
        }

    except Exception as e:
        return {"error": str(e)}
