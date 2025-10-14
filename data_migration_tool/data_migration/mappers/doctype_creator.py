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
        """
        ENHANCED: Create DocType with:
        1. Auto-detected unique ID fields
        2. Built-in row_hash field for deduplication
        3. Proper cache management
        """
        try:
            clean_name = self.clean_doctype_name(doctype_name)
            if frappe.db.exists('DocType', clean_name):
                self.logger.logger.info(f"📋 DocType '{clean_name}' already exists")
                # ENHANCEMENT: Add hash field to existing DocType if missing
                self.ensure_hash_field_exists(clean_name)
                return clean_name
            total_fields = len(analysis['fields'])
            self.logger.logger.info(f"📊 Creating DocType '{clean_name}' with {total_fields} fields + hash field")
            doctype_dict = {
                "doctype": "DocType",
                "name": clean_name,
                "module": "Data Migration Tool",
                "custom": 1,
                "is_submittable": 0,
                "track_changes": 0,
                "allow_rename": 1,
                "fields": []
            }
            # CRITICAL FIX 1: Add row_hash field for intelligent deduplication
            doctype_dict["fields"].append({
                "fieldname": "row_hash",
                "fieldtype": "Data",
                "label": "Row Hash",
                "length": 32,
                "unique": 1,  # Make hash unique
                "read_only": 1,
                "description": "SHA-256 hash for duplicate detection",
                "hidden": 1  # Hide from UI
            })
            # Add migration tracking fields
            doctype_dict["fields"].extend([
                {
                    "fieldname": "migration_source",
                    "fieldtype": "Data",
                    "label": "Migration Source",
                    "length": 50,
                    "read_only": 1,
                    "default": "CSV Import",
                    "hidden": 1
                },
                {
                    "fieldname": "migration_batch",
                    "fieldtype": "Data",
                    "label": "Migration Batch",
                    "length": 20,
                    "read_only": 1,
                    "hidden": 1
                },
                {
                    "fieldname": "last_import_date",
                    "fieldtype": "Datetime",
                    "label": "Last Import Date",
                    "read_only": 1,
                    "hidden": 1
                }
            ])
            # CRITICAL FIX 2: Auto-detect and mark ID fields as unique
            id_field_found = False
            field_count = 0
            data_field_count = 0
            for original_name, field_info in analysis['fields'].items():
                field_dict = self.create_jit_field_definition(original_name, field_info)
                # ENHANCEMENT: Detect ID fields and mark as unique
                field_lower = original_name.lower()
                is_id_field = any(pattern in field_lower for pattern in ['id', '_id', 'code', 'reference', 'key'])
                if is_id_field and not id_field_found and field_info.get('unique_count', 0) > 0:
                    # Check if values are unique enough (>80% unique)
                    total_records = analysis.get('total_records', 0)
                    unique_ratio = field_info['unique_count'] / total_records if total_records > 0 else 0
                    if unique_ratio > 0.8:  # 80% unique threshold
                        field_dict['unique'] = 1
                        field_dict['reqd'] = 0  # Don't make required to avoid issues
                        id_field_found = True
                        self.logger.logger.info(f"🔑 Marking '{original_name}' as unique identifier (uniqueness: {unique_ratio*100:.1f}%)")
                # MySQL row size management
                if field_dict['fieldtype'] == 'Data':
                    data_field_count += 1
                    if data_field_count > 30:
                        field_dict['fieldtype'] = 'Long Text'
                        field_dict.pop('length', None)
                doctype_dict["fields"].append(field_dict)
                field_count += 1
            # Set safe permissions
            doctype_dict["permissions"] = [
                {"role": "System Manager", "read": 1, "write": 1, "create": 1, "delete": 1},
                {"role": "All", "read": 1}
            ]
            # Create DocType
            original_user = frappe.session.user
            frappe.set_user('Administrator')
            try:
                doc = frappe.get_doc(doctype_dict)
                doc.insert(ignore_permissions=True)
                frappe.db.commit()
                # Comprehensive cache clearing
                self.clear_doctype_cache_comprehensive(clean_name)
                import time
                time.sleep(0.5)
                self.logger.logger.info(f"✅ Created DocType '{clean_name}' with {field_count} fields + hash field")
                if id_field_found:
                    self.logger.logger.info(f"🔑 ID field configured for unique constraint")
                return clean_name
            finally:
                frappe.set_user(original_user)
        except Exception as e:
            error_msg = str(e)
            if "Row size too large" in error_msg:
                self.logger.logger.error(f"❌ MySQL row size limit exceeded for '{doctype_name}'")
                return self.create_minimal_doctype(clean_name, analysis)
            else:
                self.logger.logger.error(f"❌ Failed to create DocType '{doctype_name}': {error_msg}")
                raise e

    def ensure_hash_field_exists(self, doctype_name: str):
        """Add row_hash field to existing DocType if it doesn't exist"""
        try:
            meta = frappe.get_meta(doctype_name)
            # Check if row_hash field already exists
            if any(f.fieldname == 'row_hash' for f in meta.fields):
                self.logger.logger.info(f"✅ row_hash field already exists in {doctype_name}")
                return
            # Get the DocType document
            doc = frappe.get_doc('DocType', doctype_name)
            # Add row_hash field
            doc.append('fields', {
                "fieldname": "row_hash",
                "fieldtype": "Data",
                "label": "Row Hash",
                "length": 32,
                "unique": 1,
                "read_only": 1,
                "description": "SHA-256 hash for duplicate detection",
                "hidden": 1
            })
            doc.save(ignore_permissions=True)
            frappe.db.commit()
            self.clear_doctype_cache_comprehensive(doctype_name)
            self.logger.logger.info(f"✅ Added row_hash field to existing DocType: {doctype_name}")
        except Exception as e:
            self.logger.logger.warning(f"⚠️ Could not add hash field to {doctype_name}: {str(e)}")


    def create_minimal_doctype(self, clean_name: str, analysis: Dict[str, Any]) -> str:
        """Fallback: Create DocType with only essential fields to avoid row size limits"""
        try:
            self.logger.logger.info(f"🔧 Creating minimal DocType {clean_name} due to size constraints")
            # Take only first 20 most important fields
            important_fields = {}
            field_priority = ['id', 'name', 'email', 'phone', 'code', 'number', 'amount', 'date', 'description']
            # First, get fields that match priority patterns
            for pattern in field_priority:
                for original_name, field_info in analysis['fields'].items():
                    if pattern in original_name.lower() and len(important_fields) < 15:
                        important_fields[original_name] = field_info
            # Fill remaining slots with other fields
            for original_name, field_info in analysis['fields'].items():
                if original_name not in important_fields and len(important_fields) < 20:
                    important_fields[original_name] = field_info
            # Create minimal DocType
            minimal_analysis = {
                'fields': important_fields,
                'total_records': analysis.get('total_records', 0)
            }
            doctype_dict = {
                "doctype": "DocType",
                "name": clean_name,
                "module": "Data Migration Tool",
                "custom": 1,
                "is_submittable": 0,
                "track_changes": 0,
                "fields": []
            }
            # Add only essential tracking
            doctype_dict["fields"].append({
                "fieldname": "migration_source",
                "fieldtype": "Data",
                "label": "Source",
                "length": 30,
                "default": "CSV"
            })
            # Add optimized fields (all as Long Text to avoid row size issues)
            for original_name, field_info in important_fields.items():
                field_dict = {
                    'fieldname': field_info['clean_name'],
                    'fieldtype': 'Long Text',  # Use Long Text for everything to avoid row size
                    'label': self.clean_label(original_name),
                    'description': f"CSV: {original_name}"
                }
                doctype_dict["fields"].append(field_dict)
            doctype_dict["permissions"] = [{"role": "System Manager", "read": 1, "write": 1, "create": 1}]
            # Create minimal DocType
            frappe.set_user('Administrator')
            doc = frappe.get_doc(doctype_dict)
            doc.insert(ignore_permissions=True)
            frappe.db.commit()
            self.logger.logger.info(f"✅ Created minimal DocType {clean_name} with {len(important_fields)} key fields")
            self.logger.logger.warning(f"⚠️ Reduced from {len(analysis['fields'])} to {len(important_fields)} fields due to MySQL limits")
            return clean_name
        except Exception as e:
            self.logger.logger.error(f"❌ Failed to create minimal DocType: {str(e)}")
            raise e

    def create_jit_field_definition(self, original_name: str, field_info: Dict[str, Any]) -> Dict[str, Any]:
        """Enhanced field definition with MySQL row size limits and intelligent field optimization"""
        field_dict = {
            'fieldname': field_info['clean_name'],
            'fieldtype': field_info['suggested_type'],
            'label': self.clean_label(original_name),
            'description': f"JIT field from CSV column: {original_name}"
        }
        # CRITICAL: Handle MySQL row size limits (8126 bytes per row)
        suggested_type = field_info['suggested_type']
        max_length = field_info.get('max_length', 0)
        # Optimize field types to avoid MySQL row size limit
        if suggested_type == 'Data':
            if max_length > 255:
                field_dict['fieldtype'] = 'Long Text'  # Uses LONGTEXT, stored off-page
            elif max_length > 140:
                field_dict['fieldtype'] = 'Text'       # Uses TEXT, stored off-page  
            else:
                # Keep Data fields short to stay within row size limits
                field_dict['length'] = min(max(max_length + 10, 30), 140)
        elif suggested_type == 'Text':
            field_dict['fieldtype'] = 'Long Text'  # Always use LONGTEXT for large text
        # Special handling for common field types
        fieldname_lower = original_name.lower()
        if 'id' in fieldname_lower:
            field_dict['fieldtype'] = 'Data'
            field_dict['length'] = 140  # Standard length for IDs
            field_dict['description'] = f"ID field: {original_name}"
        elif 'email' in fieldname_lower:
            field_dict['fieldtype'] = 'Data'
            field_dict['length'] = 140
        elif 'phone' in fieldname_lower or 'mobile' in fieldname_lower:
            field_dict['fieldtype'] = 'Data'
            field_dict['length'] = 20
        elif 'description' in fieldname_lower or 'details' in fieldname_lower or 'notes' in fieldname_lower:
            field_dict['fieldtype'] = 'Long Text'  # Always use LONGTEXT for descriptions
        elif 'address' in fieldname_lower:
            field_dict['fieldtype'] = 'Long Text'  # Use LONGTEXT for addresses
        # For very wide CSV files (>50 columns), be more aggressive with Text fields
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
    
    def clean_doctype_name(self, filename: str) -> str:
        """
        ✅ FIXED: Clean filename to create VALID Frappe DocType name WITH SPACES
        
        Frappe DocType Rules:
        - Must have Title Case WITH spaces (e.g., "Yawlit Customers")
        - Max 61 characters
        - Only alphanumeric and spaces
        - Cannot start with number
        - Must be URL-safe when converted to lowercase with hyphens
        
        Examples:
        - "Yawlit Customers.csv" → "Yawlit Customers" (URL: yawlit-customers)
        - "customers_updted.csv" → "Customers" (removing "updted" suffix)
        - "test_products.csv" → "Test Products" (URL: test-products)
        - "customer_data.csv" → "Customer Data" (URL: customer-data)
        """
        if not filename:
            return "Custom Import Data"
        
        # Remove file extension
        base_name = Path(filename).stem
        
        # Replace underscores and hyphens with spaces
        base_name = base_name.replace('_', ' ').replace('-', ' ')
        
        # Remove special characters but KEEP SPACES
        clean_name = re.sub(r'[^a-zA-Z0-9\s]', ' ', base_name)
        
        # Convert to Title Case and clean up multiple spaces
        clean_name = ' '.join(word.capitalize() for word in clean_name.split())
        
        # ✅ CRITICAL FIX: Remove problematic suffixes that cause confusion
        clean_name = re.sub(r'\s+(updated?|updted|new|final|latest|copy|data|import)\s*$', '', clean_name, flags=re.IGNORECASE)
        
        # ✅ VALIDATION: Ensure resulting name is URL-safe
        url_safe_name = clean_name.lower().replace(' ', '-')
        if not re.match(r'^[a-z][a-z0-9-]*[a-z0-9]$', url_safe_name) and len(url_safe_name) > 1:
            # If not URL-safe, make it safe
            url_safe_name = re.sub(r'[^a-z0-9-]', '', url_safe_name)
            if url_safe_name.startswith('-'):
                url_safe_name = url_safe_name.lstrip('-')
            if url_safe_name.endswith('-'):
                url_safe_name = url_safe_name.rstrip('-')
            # Convert back to Title Case with spaces
            clean_name = ' '.join(word.capitalize() for word in url_safe_name.split('-'))
        
        # Exact match mapping for standard DocTypes
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
            'Addresses': 'Address',
            'Lead': 'Lead',
            'Leads': 'Lead'
        }
        
        # Check exact mapping
        if clean_name in exact_mappings:
            mapped_doctype = exact_mappings[clean_name]
            if frappe.db.exists('DocType', mapped_doctype):
                self.logger.logger.info(f"✅ Mapped '{filename}' to existing DocType: '{mapped_doctype}'")
                return mapped_doctype
        
        # Ensure not too long (Frappe limit is 61 characters)
        if len(clean_name) > 61:
            clean_name = clean_name[:61].strip()
        
        # Ensure doesn't start with number
        if clean_name and clean_name[0].isdigit():
            clean_name = f"Import {clean_name}"
        
        # Fallback if empty
        if not clean_name or clean_name.isspace():
            clean_name = "Custom Import Data"
        
        self.logger.logger.info(f"📝 Cleaned DocType name: '{filename}' → '{clean_name}' (URL: {clean_name.lower().replace(' ', '-')})")
        return clean_name

   
    # ENHANCED: Add cache clearing method 
    def clear_doctype_cache_comprehensive(self, doctype_name: str):
        """Comprehensive cache clearing for new DocType"""
        try:
            # Clear multiple cache levels
            frappe.clear_cache(doctype=doctype_name)
            frappe.clear_document_cache(doctype_name, doctype_name)
            # Clear metadata cache
            if hasattr(frappe.local, 'form_dict'):
                frappe.local.form_dict.pop(doctype_name, None)
            # Clear route cache
            frappe.cache().delete_value('app_include_js')
            frappe.cache().delete_value('app_include_css')
            frappe.cache().delete_value('website_generators')
            # Force metadata reload
            frappe.get_meta(doctype_name, cached=False)
            self.logger.logger.info(f"🧹 Comprehensive cache cleared for DocType: {doctype_name}")
        except Exception as e:
            self.logger.logger.warning(f"⚠️ Cache clearing failed: {str(e)}")
    
    def register_doctype_route(self, doctype_name: str):
        """✅ CRITICAL FIX: Register proper URL routing for DocType with spaces"""
        try:
            # Clear routing cache first
            frappe.clear_cache()
            
            # Force metadata reload to register routes
            frappe.get_meta(doctype_name, cached=False)
            
            # ✅ IMPORTANT: Ensure DocType is accessible via both space and hyphen URLs
            # Frappe automatically handles "Yawlit Customers" → "yawlit-customers" URL conversion
            # But we need to ensure the metadata is properly loaded
            
            # Test if DocType is accessible
            try:
                test_records = frappe.get_all(doctype_name, limit=1)
                self.logger.logger.info(f"🔗 DocType '{doctype_name}' is accessible via API")
                
                # Log the expected URL for user reference
                url_version = doctype_name.lower().replace(' ', '-')
                self.logger.logger.info(f"🌐 DocType '{doctype_name}' should be accessible at: /app/List/{url_version}")
                
            except Exception as access_error:
                self.logger.logger.warning(f"⚠️ DocType access test failed: {str(access_error)}")
            
            # Clear all routing-related caches
            frappe.cache().delete_value('app_include_js')
            frappe.cache().delete_value('app_include_css')
            frappe.cache().delete_value('website_generators')
            frappe.cache().delete_value('website_route_rules')
            
            # ✅ NEW: Force browser cache invalidation by updating build version
            try:
                frappe.cache().delete_value('app_build_version')
                frappe.cache().delete_value('desk_assets')
            except:
                pass
            
            self.logger.logger.info(f"🔗 Registered routing for DocType: {doctype_name}")
            
        except Exception as e:
            self.logger.logger.warning(f"⚠️ Route registration failed: {str(e)}")
    
    def register_doctype_route(self, doctype_name: str):
        """✅ CRITICAL FIX: Register proper URL routing for DocType with spaces"""
        try:
            # Clear routing cache first
            frappe.clear_cache()
            
            # Force metadata reload to register routes
            frappe.get_meta(doctype_name, cached=False)
            
            # ✅ IMPORTANT: Ensure DocType is accessible via both space and hyphen URLs
            # Frappe automatically handles "Yawlit Customers" → "yawlit-customers" URL conversion
            # But we need to ensure the metadata is properly loaded
            
            # Test if DocType is accessible
            try:
                test_records = frappe.get_all(doctype_name, limit=1)
                self.logger.logger.info(f"🔗 DocType '{doctype_name}' is accessible via API")
            except Exception as access_error:
                self.logger.logger.warning(f"⚠️ DocType access test failed: {str(access_error)}")
            
            # Clear all routing-related caches
            frappe.cache().delete_value('app_include_js')
            frappe.cache().delete_value('app_include_css')
            frappe.cache().delete_value('website_generators')
            frappe.cache().delete_value('website_route_rules')
            
            self.logger.logger.info(f"🔗 Registered routing for DocType: {doctype_name}")
            
        except Exception as e:
            self.logger.logger.warning(f"⚠️ Route registration failed: {str(e)}")

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
        
    def analyze_csv_with_confidence_scoring(self, df: pd.DataFrame) -> Dict[str, Any]:
        """FIXED: Intelligent header analysis with confidence scoring"""

        headers = list(df.columns)
        sample_data = df.head(10).to_dict('records')

        # DocType patterns with weights
        doctype_patterns = {
            'Customer': {
                'required_indicators': ['customer_name', 'name', 'customer_id'],
                'optional_indicators': ['email', 'phone', 'address', 'company'],
                'keywords': ['customer', 'client', 'buyer'],
                'exclude_keywords': ['supplier', 'vendor', 'employee'],
                'field_weight': 15  # Points for each matching field
            },
            'Supplier': {
                'required_indicators': ['supplier_name', 'vendor_name', 'name'],
                'optional_indicators': ['email', 'phone', 'address', 'company'],
                'keywords': ['supplier', 'vendor', 'provider'],
                'exclude_keywords': ['customer', 'client', 'employee'],
                'field_weight': 15
            },
            'Contact': {
                'required_indicators': ['first_name', 'email', 'contact_id'],
                'optional_indicators': ['last_name', 'phone', 'mobile', 'company'],
                'keywords': ['contact', 'person', 'individual'],
                'exclude_keywords': ['company', 'organization'],
                'field_weight': 15
            },
            'Item': {
                'required_indicators': ['item_name', 'item_code', 'product_name'],
                'optional_indicators': ['price', 'description', 'category', 'sku'],
                'keywords': ['item', 'product', 'inventory', 'stock'],
                'exclude_keywords': ['customer', 'supplier', 'employee'],
                'field_weight': 15
            }
        }

        # Normalize headers
        normalized_headers = [h.lower().replace(' ', '_') for h in headers]
        header_text = ' '.join(normalized_headers)

        # Calculate confidence scores
        scores = {}
        for doctype, pattern in doctype_patterns.items():
            score = 0

            # Check required indicators (high weight)
            required_matches = 0
            for indicator in pattern['required_indicators']:
                if any(indicator in h for h in normalized_headers):
                    score += pattern['field_weight']
                    required_matches += 1

            # Check optional indicators (medium weight)
            for indicator in pattern['optional_indicators']:
                if any(indicator in h for h in normalized_headers):
                    score += 5

            # Check keywords (low weight)
            for keyword in pattern['keywords']:
                if keyword in header_text:
                    score += 3

            # Penalize exclude keywords
            for exclude in pattern['exclude_keywords']:
                if exclude in header_text:
                    score -= 10

            # Bonus for having required matches
            if required_matches > 0:
                score += 20

            scores[doctype] = max(score, 0)

        # Determine best match
        best_doctype = max(scores, key=scores.get) if scores else None
        max_score = scores.get(best_doctype, 0) if best_doctype else 0
        confidence = min((max_score / (len(headers) * 10)) * 100, 100)

        return {
            'suggested_doctype': best_doctype,
            'confidence': round(confidence, 1),
            'all_scores': scores,
            'requires_approval': confidence < 75,
            'field_analysis': self._analyze_field_types(df),
            'sample_data': sample_data[:3]
        }
    
    def detect_doctype_with_confidence(self, headers: List[str], sample_data: Dict) -> Dict[str, Any]:
            """Replace analyze_csv_with_confidence_scoring method"""
            patterns = {
                'Customer': {
                    'required': ['customer_name', 'customer_id', 'company_name'],
                    'optional': ['email', 'phone', 'address', 'gst_number'],
                    'keywords': ['customer', 'client', 'buyer'],
                    'anti_keywords': ['supplier', 'vendor', 'employee']
                },
                'Supplier': {
                    'required': ['supplier_name', 'vendor_name', 'company_name'],
                    'optional': ['email', 'phone', 'address', 'tax_id'],
                    'keywords': ['supplier', 'vendor', 'provider'],
                    'anti_keywords': ['customer', 'client', 'employee']
                },
                'Contact': {
                    'required': ['first_name', 'email', 'phone'],
                    'optional': ['last_name', 'designation', 'company'],
                    'keywords': ['contact', 'person', 'individual'],
                    'anti_keywords': ['company', 'organization']
                },
                'Item': {
                    'required': ['item_name', 'item_code', 'sku'],
                    'optional': ['price', 'category', 'description', 'uom'],
                    'keywords': ['item', 'product', 'inventory', 'sku'],
                    'anti_keywords': ['customer', 'supplier']
                }
            }
            normalized_headers = [h.lower().replace(' ', '_') for h in headers]
            scores = {}
            for doctype, pattern in patterns.items():
                score = 0
                # Required field matches (high weight)
                required_matches = sum(1 for req in pattern['required'] if any(req in h for h in normalized_headers))
                score += required_matches * 20
                # Optional field matches (medium weight)
                optional_matches = sum(1 for opt in pattern['optional'] if any(opt in h for h in normalized_headers))
                score += optional_matches * 10
                # Keyword presence (low weight)
                header_text = ' '.join(normalized_headers)
                for keyword in pattern['keywords']:
                    if keyword in header_text:
                        score += 5
                # Anti-keyword penalty
                for anti in pattern['anti_keywords']:
                    if anti in header_text:
                        score -= 15
                scores[doctype] = max(score, 0)
            best_match = max(scores, key=scores.get) if scores else None
            confidence = (scores.get(best_match, 0) / (len(headers) * 15)) * 100
            return {
                'suggested_doctype': best_match,
                'confidence_score': min(confidence, 100),
                'all_scores': scores,
                'requires_approval': confidence < 80,
                'reasoning': f"Matched {scores.get(best_match, 0)} points from {len(headers)} headers"
            }
    

    # ...existing code...


    def clean_label(self, name: str) -> str:
        """Clean field name to create proper label"""
        if not name:
            return "Field"
        # Convert to readable label
        clean = str(name).replace('_', ' ').replace('-', ' ')
        clean = ' '.join(word.capitalize() for word in clean.split())
        return clean

    def create_minimal_doctype(self, clean_name: str, analysis: Dict[str, Any]) -> str:
        """Fallback: Create DocType with only essential fields to avoid row size limits"""
        try:
            self.logger.logger.info(f"🔧 Creating minimal DocType {clean_name} due to size constraints")
            # Take only first 20 most important fields
            important_fields = {}
            field_priority = ['id', 'name', 'email', 'phone', 'code', 'number', 'amount', 'date', 'description']
            # First, get fields that match priority patterns
            for pattern in field_priority:
                for original_name, field_info in analysis['fields'].items():
                    if pattern in original_name.lower() and len(important_fields) < 15:
                        important_fields[original_name] = field_info
            # Fill remaining slots with other fields
            for original_name, field_info in analysis['fields'].items():
                if original_name not in important_fields and len(important_fields) < 20:
                    important_fields[original_name] = field_info
            # Create minimal DocType
            doctype_dict = {
                "doctype": "DocType",
                "name": clean_name,
                "module": "Data Migration Tool",
                "custom": 1,
                "is_submittable": 0,
                "track_changes": 0,
                "fields": []
            }
            # Add only essential tracking
            doctype_dict["fields"].append({
                "fieldname": "migration_source",
                "fieldtype": "Data",
                "label": "Source",
                "length": 30,
                "default": "CSV"
            })
            # Add optimized fields (all as Long Text to avoid row size issues)
            for original_name, field_info in important_fields.items():
                field_dict = {
                    'fieldname': field_info['clean_name'],
                    'fieldtype': 'Long Text',  # Use Long Text for everything to avoid row size
                    'label': self.clean_label(original_name),
                    'description': f"CSV: {original_name}"
                }
                doctype_dict["fields"].append(field_dict)
            doctype_dict["permissions"] = [{"role": "System Manager", "read": 1, "write": 1, "create": 1}]
            # Create minimal DocType
            frappe.set_user('Administrator')
            doc = frappe.get_doc(doctype_dict)
            doc.insert(ignore_permissions=True)
            frappe.db.commit()
            self.logger.logger.info(f"✅ Created minimal DocType {clean_name} with {len(important_fields)} key fields")
            self.logger.logger.warning(f"⚠️ Reduced from {len(analysis['fields'])} to {len(important_fields)} fields due to MySQL limits")
            return clean_name
        except Exception as e:
            self.logger.logger.error(f"❌ Failed to create minimal DocType: {str(e)}")
            raise e
    
    def find_existing_doctype_by_headers(self, headers: List[str], sample_data: Dict[str, Any] = None, confidence_threshold: float = 0.8) -> Dict[str, Any]:
        """
        🔍 DYNAMIC DocType detection based on CSV headers with configurable confidence threshold
        
        Returns:
            {
                'doctype': str or None,  # Best matching DocType name
                'confidence': float,     # Confidence score (0.0 to 1.0)
                'match_details': dict,   # Details about the match
                'should_create_new': bool # Whether to create new DocType
            }
        """
        try:
            self.logger.logger.info(f"🔍 Analyzing {len(headers)} headers for existing DocType match")

            # Normalize headers for comparison
            normalized_headers = [h.lower().strip().replace(' ', '_').replace('-', '_') for h in headers]
            header_set = set(normalized_headers)

            # Get all relevant DocTypes for comparison
            # Include both custom DocTypes from migration tool and standard DocTypes
            existing_doctypes = []
            
            # Get custom DocTypes created by the migration tool
            custom_doctypes = frappe.get_all(
                "DocType",
                filters={
                    "custom": 1,
                    "module": "Data Migration Tool"
                },
                fields=["name"]
            )
            existing_doctypes.extend(custom_doctypes)
            
            # Also check standard DocTypes that commonly match CSV imports
            standard_doctypes_to_check = [
                'Customer', 'Supplier', 'Contact', 'Lead', 'Item', 'Address',
                'Sales Order', 'Purchase Order', 'Sales Invoice', 'Purchase Invoice',
                'Quotation', 'Employee', 'User', 'Company'
            ]
            
            for doctype_name in standard_doctypes_to_check:
                if frappe.db.exists('DocType', doctype_name):
                    existing_doctypes.append({'name': doctype_name})
            
            self.logger.logger.info(f"🔍 Checking {len(existing_doctypes)} DocTypes for header matches ({len(custom_doctypes)} custom + {len(standard_doctypes_to_check)} standard)")

            best_match = None
            best_score = 0.0
            match_details = {}

            for doctype_info in existing_doctypes:
                doctype_name = doctype_info['name'] if isinstance(doctype_info, dict) else doctype_info.name

                try:
                    # Get DocType meta to analyze fields
                    meta = frappe.get_meta(doctype_name)
                    existing_fields = [f.fieldname for f in meta.fields if not f.fieldname.startswith('_')]

                    # Remove system fields for fair comparison
                    system_fields = ['name', 'owner', 'creation', 'modified', 'modified_by', 'docstatus',
                                     'idx', 'row_hash', 'migration_source', 'migration_batch', 'last_import_date']
                    content_fields = [f for f in existing_fields if f not in system_fields]
                    content_field_set = set(content_fields)

                    if not content_fields:
                        continue

                    # Calculate similarity scores
                    common_fields = header_set.intersection(content_field_set)

                    # Jaccard similarity: intersection / union
                    union_size = len(header_set.union(content_field_set))
                    jaccard_score = len(common_fields) / union_size if union_size > 0 else 0

                    # Field coverage: how many CSV headers are covered
                    coverage_score = len(common_fields) / len(header_set) if len(header_set) > 0 else 0

                    # Exact match bonus
                    exact_matches = sum(1 for h in normalized_headers if h in content_field_set)
                    exact_match_score = exact_matches / len(normalized_headers) if normalized_headers else 0

                    # Combined weighted score
                    final_score = (jaccard_score * 0.3) + (coverage_score * 0.5) + (exact_match_score * 0.2)

                    self.logger.logger.info(f"📊 {doctype_name}: Jaccard={jaccard_score:.2f}, Coverage={coverage_score:.2f}, Exact={exact_match_score:.2f}, Final={final_score:.2f}")

                    if final_score > best_score:
                        best_score = final_score
                        best_match = doctype_name
                        match_details = {
                            'jaccard_similarity': jaccard_score,
                            'field_coverage': coverage_score,
                            'exact_matches': exact_matches,
                            'common_fields': list(common_fields),
                            'total_csv_headers': len(headers),
                            'total_doctype_fields': len(content_fields),
                            'missing_in_doctype': list(header_set - content_field_set),
                            'extra_in_doctype': list(content_field_set - header_set),
                            'is_standard_doctype': doctype_name in standard_doctypes_to_check
                        }

                except Exception as field_error:
                    self.logger.logger.warning(f"⚠️ Error analyzing {doctype_name}: {str(field_error)}")
                    continue

            # Determine if match is good enough
            is_good_match = best_score >= confidence_threshold

            result = {
                'doctype': best_match if is_good_match else None,
                'confidence': best_score,
                'match_details': match_details,
                'should_create_new': not is_good_match,
                'all_candidates': [(dt['name'] if isinstance(dt, dict) else dt.name, 'analyzed') for dt in existing_doctypes]
            }

            if is_good_match:
                self.logger.logger.info(f"✅ MATCH FOUND: {best_match} (confidence: {best_score:.1%})")
            else:
                self.logger.logger.info(f"❌ NO MATCH: Best was {best_match or 'None'} (confidence: {best_score:.1%} < {confidence_threshold:.1%})")

            return result

        except Exception as e:
            self.logger.logger.error(f"❌ Error in DocType detection: {str(e)}")
            return {
                'doctype': None,
                'confidence': 0.0,
                'match_details': {},
                'should_create_new': True,
                'error': str(e)
            }

    def get_dynamic_confidence_threshold(self, settings=None) -> float:
            """Get configurable confidence threshold from settings"""
            try:
                if settings and hasattr(settings, 'doctype_match_threshold'):
                    threshold = float(settings.doctype_match_threshold or 80) / 100
                else:
                    # Try to get from Migration Settings
                    migration_settings = frappe.get_single("Migration Settings")
                    threshold = float(getattr(migration_settings, 'doctype_match_threshold', 80)) / 100

                # Ensure threshold is within reasonable bounds
                return max(0.5, min(0.95, threshold))
            except:
                return 0.8  # Default 80%
