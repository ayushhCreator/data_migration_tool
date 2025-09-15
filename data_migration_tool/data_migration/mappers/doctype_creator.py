from fileinput import filename
from pathlib import Path
import frappe
import pandas as pd
from typing import Dict, Any, List
import re
from datetime import datetime


class DynamicDocTypeCreator:
    """Enhanced DocType creator with JIT support and flexible field definitions"""
    
    def __init__(self, logger):
        self.logger = logger
    
    def analyze_csv_structure(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze CSV structure with enhanced data type detection"""
        field_analysis = {}
        
        for column in df.columns:
            # Get sample values (non-empty strings only)
            sample_values = df[column].dropna().astype(str).head(20).tolist()
            sample_values = [v.strip() for v in sample_values if v.strip()]
            
            field_analysis[column] = {
                'original_name': column,
                'clean_name': self._clean_field_name(column),
                'suggested_type': self._determine_field_type(sample_values),
                'sample_values': sample_values[:5],
                'null_count': df[column].isna().sum(),
                'unique_count': df[column].nunique(),
                'max_length': max([len(str(v)) for v in sample_values] or [0]),
                'has_currency_prefix': self._has_currency_prefix(sample_values),
                'is_likely_phone': self._is_likely_phone(sample_values),
                'is_likely_email': self._is_likely_email(sample_values)
            }
        
        return {
            'fields': field_analysis,
            'total_records': len(df),
            'suggested_doctype_name': 'Custom Import Data',
            'analysis_timestamp': frappe.utils.now()
        }
    
    def create_doctype_from_analysis(self, analysis: Dict[str, Any], doctype_name: str) -> str:
        """Create DocType with JIT-friendly field definitions"""
        try:
            clean_name = self._clean_doctype_name(doctype_name)
            
            # Check if DocType already exists
            if frappe.db.exists('DocType', clean_name):
                self.logger.logger.info(f"📋 DocType {clean_name} already exists")
                return clean_name
            
            # Create DocType definition with JIT support
            doctype_dict = {
                "doctype": "DocType",
                "name": clean_name,
                "module": "Data Migration Tool",
                "custom": 1,
                "is_submittable": 0,
                "track_changes": 1,
                "allow_rename": 1,
                "fields": []
            }
            
            # Add JIT migration tracking fields
            doctype_dict["fields"].extend([
                {
                    "fieldname": "migration_source",
                    "fieldtype": "Data",
                    "label": "Migration Source",
                    "read_only": 1,
                    "default": "JIT CSV Import"
                },
                {
                    "fieldname": "migration_id",
                    "fieldtype": "Data", 
                    "label": "Migration ID",
                    "unique": 1,
                    "read_only": 1
                },
                {
                    "fieldname": "migration_timestamp",
                    "fieldtype": "Datetime",
                    "label": "Migration Timestamp",
                    "read_only": 1
                }
            ])
            
            # Add CSV fields with JIT-optimized types
            for original_name, field_info in analysis['fields'].items():
                field_dict = self._create_jit_field_definition(original_name, field_info)
                doctype_dict["fields"].append(field_dict)
            
            # Add permissions
            doctype_dict["permissions"] = [
                {
                    "role": "System Manager",
                    "read": 1,
                    "write": 1,
                    "create": 1,
                    "delete": 1
                },
                {
                    "role": "All",
                    "read": 1
                }
            ]
            
            # Create DocType
            doc = frappe.get_doc(doctype_dict)
            doc.insert(ignore_permissions=True)
            frappe.db.commit()
            
            self.logger.logger.info(f"✅ Created JIT-optimized DocType: {clean_name}")
            return clean_name
            
        except Exception as e:
            self.logger.logger.error(f"❌ Failed to create DocType {doctype_name}: {str(e)}")
            raise e
    
    def _create_jit_field_definition(self, original_name: str, field_info: Dict[str, Any]) -> Dict[str, Any]:
        """Enhanced field definition with large ID support"""
        field_dict = {
            "fieldname": field_info['clean_name'],
            "fieldtype": field_info['suggested_type'],
            "label": self._clean_label(original_name),
            "description": f"JIT field from CSV column: {original_name}"
        }

        # Special handling for ID fields
        field_name_lower = original_name.lower()
        if 'id' in field_name_lower:
            # Always use Data type for ID fields to avoid truncation
            field_dict['fieldtype'] = 'Data'
            field_dict['length'] = 255  # Increased length for IDs
            field_dict['description'] += ' (Large IDs auto-shortened)'
            return field_dict

        # Regular field handling
        suggested_type = field_info['suggested_type']
        max_length = field_info.get('max_length', 0)

        if suggested_type == 'Data':
            if max_length > 140:
                field_dict['fieldtype'] = 'Text'
            else:
                field_dict['length'] = min(max(max_length + 20, 50), 255)  # Increased max to 255

        elif suggested_type in ['Float', 'Currency']:
            field_dict['precision'] = 2

        return field_dict








    def _determine_field_type(self, sample_values: List[str]) -> str:
        """Enhanced field type detection for JIT processing"""
        if not sample_values:
            return 'Data'
        
        # Remove empty values
        values = [v for v in sample_values if v and v.strip()]
        if not values:
            return 'Data'
        
        total_values = len(values)
        
        # Check for specific patterns
        currency_count = sum(1 for v in values if self._has_currency_prefix([v]))
        numeric_count = sum(1 for v in values if self._looks_like_number(v))
        date_count = sum(1 for v in values if self._looks_like_date(v))
        email_count = sum(1 for v in values if self._is_likely_email([v]))
        phone_count = sum(1 for v in values if self._is_likely_phone([v]))
        boolean_count = sum(1 for v in values if self._looks_like_boolean(v))
        
        # Determine field type based on patterns (80% threshold)
        threshold = total_values * 0.8
        
        if currency_count >= threshold:
            return 'Currency'
        elif numeric_count >= threshold:
            # Check if values have decimal points
            decimal_count = sum(1 for v in values if '.' in str(v))
            return 'Float' if decimal_count > 0 else 'Int'
        elif date_count >= threshold:
            return 'Date'
        elif email_count >= threshold:
            return 'Email'
        elif phone_count >= threshold:
            return 'Phone'
        elif boolean_count >= threshold:
            return 'Check'
        else:
            # Check text length to decide between Data and Text
            max_length = max(len(str(v)) for v in values)
            return 'Text' if max_length > 140 else 'Data'
    
    def _has_currency_prefix(self, values: List[str]) -> bool:
        """Check if values have currency prefixes"""
        if not values:
            return False
        
        currency_patterns = ['INR', 'USD', 'EUR', 'GBP', '$', '€', '£', '₹', '¥']
        
        for value in values:
            value_str = str(value).strip()
            for pattern in currency_patterns:
                if value_str.startswith(pattern):
                    return True
        return False
    
    def _looks_like_number(self, value: str) -> bool:
        """Check if value looks like a number (including currency)"""
        try:
            # Remove common currency symbols and formatting
            clean_value = str(value).strip()
            for symbol in ['INR', 'USD', 'EUR', 'GBP', '$', '€', '£', '₹', '¥', ',', ' ']:
                clean_value = clean_value.replace(symbol, '')
            
            # Handle negative numbers in parentheses
            if clean_value.startswith('(') and clean_value.endswith(')'):
                clean_value = clean_value[1:-1]
            
            # Handle percentages
            if clean_value.endswith('%'):
                clean_value = clean_value[:-1]
            
            float(clean_value)
            return True
        except (ValueError, TypeError):
            return False
    
    def _looks_like_date(self, value: str) -> bool:
        """Check if value looks like a date"""
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',      # YYYY-MM-DD
            r'\d{2}/\d{2}/\d{4}',       # MM/DD/YYYY or DD/MM/YYYY
            r'\d{2}-\d{2}-\d{4}',       # MM-DD-YYYY or DD-MM-YYYY
            r'\d{4}/\d{2}/\d{2}',       # YYYY/MM/DD
            r'\d{1,2}/\d{1,2}/\d{4}',   # M/D/YYYY variations
            r'\d{1,2}-\d{1,2}-\d{4}',   # M-D-YYYY variations
        ]
        
        value_str = str(value).strip()
        for pattern in date_patterns:
            if re.match(pattern, value_str):
                return True
        return False
    
    def _is_likely_email(self, values: List[str]) -> bool:
        """Check if values look like email addresses"""
        if not values:
            return False
        
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        for value in values:
            value_str = str(value).strip().lower()
            if re.match(email_pattern, value_str):
                return True
        return False
    
    def _is_likely_phone(self, values: List[str]) -> bool:
        """Check if values look like phone numbers"""
        if not values:
            return False
        
        phone_patterns = [
            r'^\+\d{10,15}$',          # +1234567890
            r'^\d{10,15}$',            # 1234567890
            r'^(\+\d{1,3}[\s-]?)?\d{10,15}$',  # +1 234567890 or +1-234567890
            r'^\(\d{3,4}\)\s?\d{6,10}$',       # (123) 4567890
        ]
        
        for value in values:
            # Clean the value
            clean_value = re.sub(r'[^\d+()-]', '', str(value).strip())
            
            for pattern in phone_patterns:
                if re.match(pattern, clean_value):
                    return True
        return False
    
    def _looks_like_boolean(self, value: str) -> bool:
        """Check if value looks like a boolean"""
        boolean_values = [
            '1', '0', 'true', 'false', 'yes', 'no', 'y', 'n',
            'on', 'off', 'enabled', 'disabled', 'active', 'inactive'
        ]
        
        return str(value).strip().lower() in boolean_values
    
    def _clean_field_name(self, name: str) -> str:
        """Clean field name for Frappe compatibility"""
        if not name:
            return 'field'
        
        # Convert to string and clean
        clean = str(name).strip()
        
        # Replace spaces and special characters with underscores
        clean = re.sub(r'[^a-zA-Z0-9_]', '_', clean.lower())
        
        # Replace multiple underscores with single
        clean = re.sub(r'_+', '_', clean)
        
        # Remove leading/trailing underscores
        clean = clean.strip('_')
        
        # Ensure it doesn't start with a number
        if clean and clean[0].isdigit():
            clean = f"field_{clean}"
        
        # Ensure minimum length and valid name
        if len(clean) < 1:
            clean = "field"
        elif len(clean) == 1:
            clean = f"field_{clean}"
        
        # Ensure it's not a Python keyword
        python_keywords = [
            'class', 'def', 'return', 'if', 'else', 'for', 'while', 
            'try', 'except', 'import', 'from', 'pass', 'break', 'continue'
        ]
        if clean in python_keywords:
            clean = f"{clean}_field"
        
        # Ensure it's not a Frappe reserved field
        frappe_reserved = [
            'name', 'owner', 'creation', 'modified', 'modified_by', 
            'docstatus', 'idx', 'parent', 'parentfield', 'parenttype'
        ]
        if clean in frappe_reserved:
            clean = f"custom_{clean}"
        
        return clean
    
    def _clean_doctype_name(self, filename: str) -> str:
        """FIXED: Enhanced DocType name mapping that triggers user approval for unknown types"""

        # Remove file extension
        base_name = Path(filename).stem

        # Remove all special characters and spaces, keep only alphanumeric
        clean_name = re.sub(r'[^a-zA-Z0-9]', '', base_name)

        # Convert to title case
        clean_name = ''.join(word.capitalize() for word in re.findall(r'[a-zA-Z0-9]+', clean_name))

        # ONLY map if it's an exact match to common patterns
        exact_mappings = {
            'Vendor': 'Supplier',
            'Vendors': 'Supplier',
            'Supplier': 'Supplier',
            'Suppliers': 'Supplier',
            'Contact': 'Contact',
            'Contacts': 'Contact',
            'Customer': 'Customer',
            'Customers': 'Customer',
            'Address': 'Address',
            'Addresses': 'Address'
        }

        # Check if it exactly matches a known pattern
        mapped_doctype = exact_mappings.get(clean_name)

        if mapped_doctype and frappe.db.exists('DocType', mapped_doctype):
            self.logger.logger.info(f"✅ Mapped {filename} to existing DocType: {mapped_doctype}")
            return mapped_doctype

        # For unknown files, return the cleaned name to trigger user approval
        self.logger.logger.info(f"🔄 Unknown DocType pattern: {clean_name} - will request user approval")
        return clean_name


    def _clean_label(self, name: str) -> str:
        """Clean field label for display"""
        if not name:
            return 'Field'
        
        # Convert underscores to spaces and title case
        clean = str(name).replace('_', ' ').replace('-', ' ')
        clean = ' '.join(word.capitalize() for word in clean.split())
        
        return clean
    
    def map_external_fields(self, record: Dict, target_doctype: str) -> Dict[str, str]:
        """Map external fields to DocType fields with enhanced matching"""
        field_mapping = {}
        
        # Get DocType fields
        try:
            meta = frappe.get_meta(target_doctype)
            doctype_fields = [f.fieldname for f in meta.fields]
        except Exception as e:
            self.logger.logger.error(f"❌ Cannot get metadata for {target_doctype}: {str(e)}")
            return field_mapping
        
        for external_field in record.keys():
            clean_field = self._clean_field_name(external_field)
            
            # Direct match
            if clean_field in doctype_fields:
                field_mapping[external_field] = clean_field
            else:
                # Find similar field with fuzzy matching
                similar = self._find_similar_field(clean_field, doctype_fields)
                if similar:
                    field_mapping[external_field] = similar
        
        return field_mapping
    
    def _find_similar_field(self, target_field: str, available_fields: List[str]) -> str:
        """Find similar field name with enhanced matching"""
        if not target_field:
            return None
        
        target_lower = target_field.lower()
        
        # Exact match (case insensitive)
        for field in available_fields:
            if field.lower() == target_lower:
                return field
        
        # Partial match (contains)
        for field in available_fields:
            if target_lower in field.lower() or field.lower() in target_lower:
                return field
        
        # Fuzzy match with similarity scoring
        best_match = None
        best_score = 0
        
        for field in available_fields:
            score = self._calculate_similarity(target_field, field)
            if score > best_score and score > 0.7:  # 70% similarity threshold
                best_match = field
                best_score = score
        
        return best_match
    
    def _calculate_similarity(self, field1: str, field2: str) -> float:
        """Calculate similarity score between two strings"""
        if not field1 or not field2:
            return 0
        
        field1_lower = field1.lower()
        field2_lower = field2.lower()
        
        # Simple character overlap ratio
        common_chars = sum(1 for a, b in zip(field1_lower, field2_lower) if a == b)
        max_length = max(len(field1_lower), len(field2_lower))
        
        if max_length == 0:
            return 0
        
        return common_chars / max_length
    
    def analyze_record_structure(self, record: Dict, suggested_name: str) -> Dict[str, Any]:
        """Analyze single record structure (for API data)"""
        field_analysis = {}
        
        for field_name, value in record.items():
            field_analysis[field_name] = {
                'original_name': field_name,
                'clean_name': self._clean_field_name(field_name),
                'suggested_type': self._infer_type_from_value(value),
                'sample_value': str(value)[:100] if value else '',
                'is_empty': value is None or str(value).strip() == ''
            }
        
        return {
            'fields': field_analysis,
            'suggested_doctype_name': suggested_name,
            'record_count': 1,
            'analysis_timestamp': frappe.utils.now()
        }
    
    def _infer_type_from_value(self, value) -> str:
        """Infer field type from a single value"""
        if value is None:
            return 'Data'
        
        value_str = str(value).strip()
        
        # Check various patterns
        if self._looks_like_number(value_str):
            return 'Float' if '.' in value_str else 'Int'
        elif self._looks_like_date(value_str):
            return 'Date'
        elif self._is_likely_email([value_str]):
            return 'Email'
        elif self._is_likely_phone([value_str]):
            return 'Phone'
        elif self._looks_like_boolean(value_str):
            return 'Check'
        elif len(value_str) > 140:
            return 'Text'
        else:
            return 'Data'
