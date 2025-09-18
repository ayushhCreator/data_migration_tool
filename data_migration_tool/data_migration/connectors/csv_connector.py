# Enhanced CSV Connector - Phase 1 Improvements
import os
import json
import frappe
from frappe.utils import now, get_datetime
import pandas as pd
import numpy as np
from pathlib import Path
import re
from datetime import datetime
from typing import Dict, List, Any, Optional

class CSVConnector:
    """Enhanced CSV Connector with universal duplicate detection and improved error handling"""

    def __init__(self, logger):
        self.logger = logger
        self.supported_formats = ['.csv', '.xlsx', '.xls']
        self.current_field_name = ''

    def convert_numpy_types(self, obj):
        """Convert numpy types to Python native types for JSON serialization"""
        if isinstance(obj, dict):
            return {k: self.convert_numpy_types(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.convert_numpy_types(i) for i in obj]
        elif isinstance(obj, (np.integer, np.int64, np.int32)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64, np.float32)):
            return float(obj)
        elif isinstance(obj, np.bool_):
            return bool(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return obj

    def profile_data(self, df: pd.DataFrame, filename: str):
        """FIXED: Enhanced data profiling with numpy type conversion"""
        try:
            profile = {}
            for col in df.columns:
                non_empty_values = df[col][df[col] != '']
                profile[col] = {
                    "total_rows": int(len(df[col])),  # Convert to int
                    "non_empty_rows": int(len(non_empty_values)),  # Convert to int
                    "empty_rows": int(len(df[col]) - len(non_empty_values)),  # Convert to int
                    "unique_values": int(non_empty_values.nunique()) if len(non_empty_values) > 0 else 0,
                    "sample_values": self.convert_numpy_types(non_empty_values.head(3).tolist()) if len(non_empty_values) > 0 else [],
                    "max_length": int(non_empty_values.str.len().max()) if len(non_empty_values) > 0 else 0,
                    "data_completeness": f"{(len(non_empty_values) / len(df[col]) * 100):.1f}%"
                }
            
            # Use convert_numpy_types before JSON serialization
            safe_profile = self.convert_numpy_types(profile)
            self.logger.logger.info(f"📈 Data Profile for {filename}: {json.dumps(safe_profile, indent=2)}")
            
        except Exception as e:
            self.logger.logger.warning(f"⚠️ Data profiling failed: {str(e)}")

    def read_file_as_strings(self, file_path: str) -> pd.DataFrame:
        """Read CSV/Excel with enhanced encoding detection and error handling"""
        file_ext = Path(file_path).suffix.lower()
        
        try:
            if file_ext == '.csv':
                # Try multiple encodings with better error handling
                encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252', 'iso-8859-1']
                df = None
                encoding_used = None
                
                for encoding in encodings:
                    try:
                        df = pd.read_csv(
                            file_path, 
                            dtype=str, 
                            keep_default_na=False, 
                            encoding=encoding,
                            on_bad_lines='skip'  # Skip bad lines instead of failing
                        )
                        encoding_used = encoding
                        self.logger.logger.info(f"📄 Successfully read CSV with {encoding} encoding: {Path(file_path).name}")
                        break
                    except UnicodeDecodeError:
                        continue
                    except Exception as e:
                        self.logger.logger.warning(f"⚠️ Failed to read with {encoding}: {str(e)}")
                        continue
                
                if df is None:
                    # Last resort: read with error handling
                    try:
                        df = pd.read_csv(
                            file_path, 
                            dtype=str, 
                            keep_default_na=False, 
                            encoding='utf-8', 
                            errors='replace',
                            on_bad_lines='skip'
                        )
                        self.logger.logger.warning("⚠️ Used error handling for encoding issues")
                        encoding_used = 'utf-8 (with error handling)'
                    except Exception as final_error:
                        raise Exception(f"Failed to read CSV file with all encoding attempts: {str(final_error)}")
                
            elif file_ext in ['.xlsx', '.xls']:
                try:
                    df = pd.read_excel(file_path, dtype=str, keep_default_na=False)
                    encoding_used = 'Excel format'
                    self.logger.logger.info(f"📄 Successfully read Excel file: {Path(file_path).name}")
                except Exception as excel_error:
                    raise Exception(f"Failed to read Excel file: {str(excel_error)}")
            else:
                raise Exception(f"Unsupported file format: {file_ext}")

            # Clean and validate DataFrame
            if df is None or df.empty:
                raise Exception("File is empty or could not be read")
            
            # Clean column names
            df.columns = df.columns.astype(str).str.strip()
            
            # Remove completely empty rows
            df = df.dropna(how='all')
            
            # Fill NaN values with empty strings
            df = df.fillna('')
            
            # Remove rows where all values are empty strings
            df = df[~(df == '').all(axis=1)]
            
            if df.empty:
                raise Exception("No valid data found in file after cleaning")
            
            self.logger.logger.info(
                f"📊 Successfully loaded {len(df)} rows and {len(df.columns)} columns from {Path(file_path).name} "
                f"using {encoding_used}"
            )

            # Data profiling for better insights
            self.profile_data(df, Path(file_path).name)

            return df

        except Exception as e:
            error_msg = f"Failed to read file {Path(file_path).name}: {str(e)}"
            self.logger.logger.error(f"❌ {error_msg}")
            raise Exception(error_msg)


    def store_raw_data(self, df: pd.DataFrame, source_file: str, target_doctype: str) -> int:
        """Store raw data with proper user context"""
        stored_count = 0
        total_rows = len(df)

        try:
            # Set valid system user
            valid_user = "Administrator"  # Use your admin email

            self.logger.logger.info(f"📦 Starting to store {total_rows} rows in buffer for {target_doctype}")

            batch_size = 50
            for batch_start in range(0, total_rows, batch_size):
                batch_end = min(batch_start + batch_size, total_rows)
                batch_df = df.iloc[batch_start:batch_end]

                for index, row in batch_df.iterrows():
                    try:
                        raw_data = row.to_dict()
                        cleaned_data = {k: str(v).strip() if v else '' for k, v in raw_data.items()}

                        # Create buffer document with valid user
                        buffer_doc = frappe.get_doc({
                            "doctype": "Migration Data Buffer",
                            "source_file": source_file,
                            "target_doctype": target_doctype,
                            "row_index": int(index),
                            "raw_data": json.dumps(cleaned_data),
                            "processing_status": "Pending",
                            "created_at": now(),
                            "created_by": valid_user,  # Set valid user
                            "owner": valid_user,
                            "modified_by": valid_user
                        })

                        buffer_doc.insert(ignore_permissions=True)
                        stored_count += 1

                    except Exception as row_error:
                        self.logger.logger.error(f"❌ Failed to store row {index}: {str(row_error)}")
                        continue

                frappe.db.commit()
                progress = (batch_end / total_rows) * 100
                self.logger.logger.info(f"📦 Stored batch {batch_start}-{batch_end}: {progress:.1f}% complete")

            self.logger.logger.info(f"✅ Successfully stored {stored_count}/{total_rows} raw records in buffer")
            return stored_count

        except Exception as e:
            frappe.db.rollback()
            error_msg = f"Failed to store raw data: {str(e)}"
            self.logger.logger.error(f"❌ {error_msg}")
            raise Exception(error_msg)



    def _get_system_user(self):
        """Get a valid system user for background operations"""
        try:
            # Try to get current session user
            if hasattr(frappe, 'session') and frappe.session.user:
                return frappe.session.user

            # Fallback to Administrator
            if frappe.db.exists('User', 'Administrator'):
                return 'Administrator'

            # Last resort: get first System Manager
            system_managers = frappe.get_system_managers()
            if system_managers:
                return system_managers[0]

            # Create or use a system user
            return self._ensure_system_user()

        except Exception as e:
            self.logger.logger.warning(f"⚠️ Error getting system user: {str(e)}")
            return 'Administrator'  # Fallback

    def _ensure_system_user(self):
        """Ensure a system user exists for background operations"""
        system_user_email = 'migration.system@example.com'

        if not frappe.db.exists('User', system_user_email):
            try:
                # Create system user for migrations
                user_doc = frappe.get_doc({
                    'doctype': 'User',
                    'email': system_user_email,
                    'first_name': 'Migration',
                    'last_name': 'System',
                    'user_type': 'System User',
                    'roles': [{'role': 'System Manager'}]
                })
                user_doc.insert(ignore_permissions=True)
                frappe.db.commit()
                self.logger.logger.info(f"✅ Created system user: {system_user_email}")
            except Exception as e:
                self.logger.logger.error(f"❌ Failed to create system user: {str(e)}")
                return 'Administrator'

        return system_user_email



    def process_buffered_data(self, target_doctype: str, batch_size: int = 100) -> Dict[str, int]:
        """ENHANCED: Batch processing with universal duplicate detection and improved validation"""
        results = {"success": 0, "failed": 0, "skipped": 0}
        
        try:
            # Get pending records for this DocType
            pending_records = frappe.db.sql("""
                SELECT name, raw_data, row_index, source_file
                FROM `tabMigration Data Buffer`
                WHERE target_doctype = %s AND processing_status = 'Pending'
                ORDER BY row_index
                LIMIT %s
            """, (target_doctype, batch_size), as_dict=True)

            if not pending_records:
                self.logger.logger.debug("📭 No pending records to process")
                return results

            self.logger.logger.info(f"🔄 Processing {len(pending_records)} buffered records for {target_doctype}")

            # Get DocType metadata
            try:
                meta = frappe.get_meta(target_doctype)
            except Exception as e:
                self.logger.logger.error(f"❌ Cannot get metadata for {target_doctype}: {str(e)}")
                return results

            # Process each record
            for record in pending_records:
                try:
                    # Parse raw data
                    raw_data = json.loads(record.raw_data)
                    
                    # Apply JIT conversion
                    converted_data = self._apply_jit_conversion(raw_data, meta)
                    self.logger.logger.debug(f"🔍 Row {record.row_index} converted data: {converted_data}")

                    # Validate data
                    validation_errors = self.validate_and_clean_data(converted_data, meta)
                    if validation_errors:
                        error_msg = "; ".join(validation_errors)
                        self._update_buffer_status(record.name, "Failed", error_msg)
                        results["failed"] += 1
                        self.logger.logger.warning(f"⚠️ Row {record.row_index}: {error_msg}")
                        continue

                    # ENHANCED: Universal duplicate check
                    if self._check_duplicate_record_universal(converted_data, target_doctype):
                        self._update_buffer_status(record.name, "Skipped", "Duplicate record found")
                        results["skipped"] += 1
                        self.logger.logger.info(f"⚠️ Row {record.row_index}: Skipped duplicate")
                        continue

                    # Create document
                    doc_data = {"doctype": target_doctype}
                    doc_data.update(converted_data)
                    
                    doc = frappe.get_doc(doc_data)
                    doc.insert(ignore_permissions=True, ignore_mandatory=False)

                    self._update_buffer_status(record.name, "Processed", f"Created {doc.name}")
                    results["success"] += 1
                    self.logger.logger.info(f"✅ Row {record.row_index}: Created {doc.name}")

                except frappe.DuplicateEntryError as e:
                    self._update_buffer_status(record.name, "Skipped", f"Duplicate: {str(e)}")
                    results["skipped"] += 1
                    self.logger.logger.info(f"⚠️ Row {record.row_index}: Skipped duplicate entry")

                except frappe.ValidationError as e:
                    error_msg = str(e)[:500]
                    self._update_buffer_status(record.name, "Failed", f"Validation: {error_msg}")
                    results["failed"] += 1
                    self.logger.logger.warning(f"⚠️ Row {record.row_index}: Validation failed - {error_msg}")

                except Exception as e:
                    error_msg = str(e)[:500]
                    self._update_buffer_status(record.name, "Failed", error_msg)
                    results["failed"] += 1
                    self.logger.logger.error(f"❌ Row {record.row_index}: Processing failed - {error_msg}")

            # Commit all changes
            frappe.db.commit()
            self.logger.logger.info(f"✅ Batch processing completed: {results}")
            
            return results

        except Exception as e:
            frappe.db.rollback()
            error_msg = f"Batch processing failed completely: {str(e)}"
            self.logger.logger.error(f"💥 {error_msg}")
            return results

    def _check_duplicate_record_universal(self, converted_data: Dict[str, Any], doctype: str) -> bool:
        """ENHANCED: Universal duplicate detection for all DocTypes"""
        try:
            # Get DocType metadata
            meta = frappe.get_meta(doctype)
            
            # Strategy 1: Check unique fields
            unique_fields = [f.fieldname for f in meta.fields if getattr(f, 'unique', False)]
            
            for field in unique_fields:
                if field in converted_data and converted_data[field]:
                    existing = frappe.db.exists(doctype, {field: converted_data[field]})
                    if existing:
                        self.logger.logger.debug(f"🔍 Duplicate found by unique field {field}: {converted_data[field]}")
                        return True
            
            # Strategy 2: DocType-specific duplicate detection rules
            duplicate_rules = self._get_doctype_duplicate_rules(doctype)
            
            for rule in duplicate_rules:
                filters = {}
                rule_matched = True
                
                for field in rule:
                    if field in converted_data and converted_data[field]:
                        filters[field] = converted_data[field]
                    else:
                        rule_matched = False
                        break
                
                if rule_matched and filters:
                    existing = frappe.db.exists(doctype, filters)
                    if existing:
                        self.logger.logger.debug(f"🔍 Duplicate found by rule {rule}: {filters}")
                        return True
            
            # Strategy 3: Fallback to name field
            if 'name' in converted_data and converted_data['name']:
                existing = frappe.db.exists(doctype, converted_data['name'])
                if existing:
                    self.logger.logger.debug(f"🔍 Duplicate found by name: {converted_data['name']}")
                    return True
            
            return False

        except Exception as e:
            self.logger.logger.warning(f"⚠️ Duplicate check failed for {doctype}: {str(e)}")
            return False  # If duplicate check fails, proceed with insert

    def _get_doctype_duplicate_rules(self, doctype: str) -> List[List[str]]:
        """Get DocType-specific duplicate detection rules"""
        rules_map = {
            'Supplier': [
                ['supplier_name'],
                ['email_id'],
                ['tax_id']
            ],
            'Customer': [
                ['customer_name'],
                ['email_id'],
                ['tax_id']
            ],
            'Item': [
                ['item_code'],
                ['item_name', 'item_group']
            ],
            'Contact': [
                ['email_id'],
                ['mobile_no'],
                ['first_name', 'last_name', 'company_name']
            ],
            'Address': [
                ['address_line1', 'city', 'pincode'],
                ['address_title']
            ],
            'Lead': [
                ['email_id'],
                ['mobile_no'],
                ['lead_name', 'company_name']
            ]
        }
        
        return rules_map.get(doctype, [])

    def validate_and_clean_data(self, data: Dict[str, Any], meta) -> List[str]:
        """ENHANCED: Comprehensive data validation with better error messages"""
        errors = []
        
        try:
            for field in meta.fields:
                field_name = field.fieldname
                field_type = field.fieldtype
                field_value = data.get(field_name, '')
                
                # Check required fields
                if getattr(field, 'reqd', False) and not field_value:
                    errors.append(f"Missing required field: {field_name}")
                    continue
                
                # Skip validation if field is empty and not required
                if not field_value:
                    continue
                
                # Field-type specific validation
                if field_type == "Email" and field_value:
                    if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', str(field_value)):
                        errors.append(f"Invalid email format in {field_name}: {field_value}")
                
                elif field_type == "Phone" and field_value:
                    # Clean phone number and validate
                    clean_phone = re.sub(r'[^\d+\-\(\)\s]', '', str(field_value))
                    if len(clean_phone) < 10:
                        errors.append(f"Invalid phone number in {field_name}: {field_value}")
                
                elif field_type in ["Int", "Float", "Currency"] and field_value:
                    try:
                        if field_type == "Int":
                            int(field_value)
                        else:
                            float(field_value)
                    except (ValueError, TypeError):
                        errors.append(f"Invalid {field_type.lower()} value in {field_name}: {field_value}")
                
                elif field_type == "Date" and field_value:
                    try:
                        get_datetime(field_value)
                    except:
                        errors.append(f"Invalid date format in {field_name}: {field_value}")
                
                elif field_type == "Link" and field_value:
                    # Validate link field exists
                    link_doctype = field.options
                    if link_doctype and not frappe.db.exists(link_doctype, field_value):
                        errors.append(f"Invalid {link_doctype} reference in {field_name}: {field_value}")
                
                # Check field length limits
                if hasattr(field, 'length') and field.length and field_value:
                    if len(str(field_value)) > field.length:
                        errors.append(f"Value too long for {field_name} (max {field.length}): {len(str(field_value))} characters")
        
        except Exception as e:
            errors.append(f"Validation error: {str(e)}")
        
        return errors

    def _apply_jit_conversion(self, raw_data: Dict[str, str], meta) -> Dict[str, Any]:
        """ENHANCED: Field mapping and smart conversion with better defaults"""
        converted_data = {}
        available_fields = [f.fieldname for f in meta.fields]
        
        # Enhanced field mappings
        field_mappings = {
            # Universal mappings
            'id': 'name',
            'email': 'email_id',
            'phone': 'phone',
            'mobile': 'mobile_no',
            'fax': 'fax',
            'website': 'website',
            'notes': 'notes',
            'description': 'description',
            'status': 'disabled',  # Will be processed specially
            
            # Supplier mappings
            'contact_id': 'supplier_name',
            'contact_name': 'supplier_name', 
            'company_name': 'supplier_name',
            'display_name': 'supplier_name',
            'first_name': 'supplier_name',
            'last_name': 'supplier_name',
            'emailid': 'email_id',
            'mobilephone': 'mobile_no',
            'currency_code': 'default_currency',
            'supplier_details': 'supplier_details',
            
            # Customer mappings
            'customer_name': 'customer_name',
            'customer_type': 'customer_type',
            'customer_group': 'customer_group',
            
            # Item mappings
            'item_id': 'item_code',
            'item_name': 'item_name',
            'rate': 'standard_rate',
            'unit_name': 'stock_uom',
            'product_type': 'item_group',
            
            # Contact mappings
            'full_name': 'first_name',
            'company': 'company_name',
            
            # Address mappings
            'address': 'address_line1',
            'address1': 'address_line1',
            'address2': 'address_line2',
            'city': 'city',
            'state': 'state',
            'country': 'country',
            'zip': 'pincode',
            'postal_code': 'pincode',
            'pincode': 'pincode',
            
            # Account mappings
            'account_id': 'account_number',
            'account_name': 'account_name',
            'account_code': 'account_number',
            'account_type': 'account_type',
        }

        # Process each field in raw data
        for csv_field, raw_value in raw_data.items():
            if not raw_value or str(raw_value).strip() == '':
                continue

            # Clean field name for mapping
            clean_field = csv_field.lower().replace(' ', '_').replace('-', '_')
            target_field = field_mappings.get(clean_field, clean_field)

            # Check if target field exists in DocType
            if target_field not in available_fields:
                # Try to find similar field
                similar_field = self._find_similar_field(target_field, available_fields)
                if similar_field:
                    target_field = similar_field
                else:
                    continue  # Skip fields that don't exist

            # Get field metadata
            field_meta = None
            for f in meta.fields:
                if f.fieldname == target_field:
                    field_meta = f
                    break

            if not field_meta:
                continue

            # Convert value based on field type
            self.current_field_name = clean_field
            converted_value = self._smart_type_conversion(raw_value, field_meta.fieldtype, field_meta)

            if converted_value is not None:
                converted_data[target_field] = converted_value

        # Apply DocType-specific defaults and processing
        converted_data = self._apply_doctype_specific_processing(converted_data, raw_data, meta)

        return converted_data

    def _find_similar_field(self, target_field: str, available_fields: List[str]) -> Optional[str]:
        """Enhanced field similarity matching"""
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

        # Common field mappings
        common_mappings = {
            'email': 'email_id',
            'phone': 'mobile_no',
            'mobile': 'mobile_no',
            'name': 'supplier_name',  # For Supplier
            'company': 'supplier_name',  # For Supplier
            'title': 'supplier_name',  # For Supplier
        }

        if target_lower in common_mappings and common_mappings[target_lower] in available_fields:
            return common_mappings[target_lower]

        return None

    def _smart_type_conversion(self, raw_value: str, field_type: str, field_meta=None) -> Any:
        """ENHANCED: Smart type conversion with better handling of edge cases"""
        try:
            if raw_value is None or str(raw_value).strip() == '':
                return None

            raw_str = str(raw_value).strip()

            # Handle ID fields specially
            if 'id' in self.current_field_name.lower() and len(raw_str) > 10:
                if raw_str.isdigit():
                    return f"ID-{raw_str[-8:]}"
                else:
                    return raw_str[:50]

            if field_type in ['Float', 'Currency']:
                clean_value = raw_str
                
                # Remove currency symbols
                currency_patterns = ['INR', 'USD', 'EUR', 'GBP', '$', '€', '£', '₹', '¥']
                for pattern in currency_patterns:
                    clean_value = clean_value.replace(pattern, '')
                
                # Remove commas, spaces, and other formatting
                clean_value = re.sub(r'[,\s\u00A0]', '', clean_value)  # Including non-breaking spaces
                
                # Handle negative numbers in parentheses
                if clean_value.startswith('(') and clean_value.endswith(')'):
                    clean_value = '-' + clean_value[1:-1]
                
                # Handle percentages
                if clean_value.endswith('%'):
                    clean_value = clean_value[:-1]
                    if clean_value:
                        return float(clean_value) / 100  # Convert percentage to decimal
                
                if not clean_value or clean_value in ['-', 'N/A', 'NULL', '0', 'nil', 'none']:
                    return 0.0
                
                try:
                    return float(clean_value)
                except ValueError:
                    self.logger.logger.warning(f"⚠️ Could not convert '{raw_str}' to float")
                    return 0.0

            elif field_type == 'Int':
                clean_value = re.sub(r'[^\d\-]', '', raw_str)
                if not clean_value or clean_value == '-':
                    return 0
                try:
                    return int(clean_value)
                except ValueError:
                    return 0

            elif field_type == 'Check':
                lower_val = raw_str.lower()
                true_values = ['1', 'true', 'yes', 'y', 'active', 'enabled', 'on']
                false_values = ['0', 'false', 'no', 'n', 'inactive', 'disabled', 'off']
                
                if lower_val in true_values:
                    return 1
                elif lower_val in false_values:
                    return 0
                else:
                    # Default to 0 for unknown values
                    return 0

            elif field_type in ['Date', 'Datetime']:
                if raw_str in ['', '0', 'NULL', 'N/A', 'nil', 'none']:
                    return None
                
                try:
                    return get_datetime(raw_str).strftime('%Y-%m-%d')
                except:
                    # Try common date formats
                    date_formats = [
                        '%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%m-%d-%Y', '%d-%m-%Y',
                        '%Y/%m/%d', '%d.%m.%Y', '%m.%d.%Y'
                    ]
                    
                    for fmt in date_formats:
                        try:
                            return datetime.strptime(raw_str, fmt).strftime('%Y-%m-%d')
                        except ValueError:
                            continue
                    
                    self.logger.logger.warning(f"⚠️ Could not parse date '{raw_str}'")
                    return None

            elif field_type in ['Data', 'Small Text']:
                max_length = getattr(field_meta, 'length', 140) if field_meta else 140
                if len(raw_str) > max_length:
                    truncated = raw_str[:max_length-3] + "..."
                    self.logger.logger.warning(f"⚠️ Truncated long value for field {self.current_field_name}")
                    return truncated
                return raw_str

            elif field_type in ['Text', 'Long Text', 'HTML Editor']:
                return raw_str

            elif field_type == 'Link':
                # For link fields, return the value as-is for now
                # Validation will happen in validate_and_clean_data
                return raw_str[:140]  # Limit length for link fields

            else:
                # Default: return as string with length limit
                max_length = 140
                if len(raw_str) > max_length:
                    return raw_str[:max_length-3] + "..."
                return raw_str

        except Exception as e:
            self.logger.logger.warning(f"⚠️ Type conversion failed for '{raw_value}' to {field_type}: {str(e)}")
            return str(raw_value)[:50] if raw_value else None

    def _apply_doctype_specific_processing(self, converted_data: Dict[str, Any], raw_data: Dict[str, str], meta) -> Dict[str, Any]:
        """Apply DocType-specific processing and defaults"""
        doctype = meta.name

        if doctype == 'Supplier':
            # Supplier name fallback
            if 'supplier_name' not in converted_data or not converted_data['supplier_name']:
                supplier_name = (
                    raw_data.get('Display Name') or
                    raw_data.get('Company Name') or
                    raw_data.get('Contact Name') or
                    raw_data.get('Name') or
                    f"Supplier-{frappe.generate_hash()[:8]}"
                )
                converted_data['supplier_name'] = str(supplier_name).strip()[:140]

            # Supplier type & group defaults
            converted_data.setdefault('supplier_type', 
                'Company' if raw_data.get('Company Name') else 'Individual')
            converted_data.setdefault('supplier_group', 'All Supplier Groups')

            # Handle status field
            status = raw_data.get('Status', 'Active').strip().lower()
            converted_data['disabled'] = 0 if status in ['active', 'enabled', '1'] else 1

        elif doctype == 'Customer':
            # Customer name fallback
            if 'customer_name' not in converted_data or not converted_data['customer_name']:
                customer_name = (
                    raw_data.get('Customer Name') or
                    raw_data.get('Company Name') or
                    raw_data.get('Display Name') or
                    f"Customer-{frappe.generate_hash()[:8]}"
                )
                converted_data['customer_name'] = str(customer_name).strip()[:140]

            # Customer defaults
            converted_data.setdefault('customer_type', 'Company')
            converted_data.setdefault('customer_group', 'All Customer Groups')

        elif doctype == 'Item':
            # Item code fallback
            if 'item_code' not in converted_data or not converted_data['item_code']:
                item_code = (
                    raw_data.get('Item Code') or
                    raw_data.get('SKU') or
                    raw_data.get('Product Code') or
                    f"ITEM-{frappe.generate_hash()[:8]}"
                )
                converted_data['item_code'] = str(item_code).strip()[:140]

            # Item defaults
            converted_data.setdefault('item_group', 'All Item Groups')
            converted_data.setdefault('stock_uom', 'Nos')
            converted_data.setdefault('is_stock_item', 1)

        elif doctype == 'Contact':
            # Contact name construction
            first_name = converted_data.get('first_name', '') or raw_data.get('First Name', '')
            last_name = converted_data.get('last_name', '') or raw_data.get('Last Name', '')
            
            if first_name or last_name:
                full_name = f"{first_name} {last_name}".strip()
                if not converted_data.get('first_name'):
                    converted_data['first_name'] = full_name

        elif doctype == 'Address':
            # Address title fallback
            if 'address_title' not in converted_data or not converted_data['address_title']:
                address_title = (
                    raw_data.get('Address Title') or
                    raw_data.get('Title') or
                    f"Address-{frappe.generate_hash()[:6]}"
                )
                converted_data['address_title'] = str(address_title).strip()[:140]

            # Address type default
            converted_data.setdefault('address_type', 'Other')

        elif doctype == 'Account':
            # Account defaults
            converted_data.setdefault('company', 'Your Company')  # Should be configurable
            
            if 'account_name' not in converted_data or not converted_data['account_name']:
                account_name = (
                    raw_data.get('Account Name') or
                    f"Account-{frappe.generate_hash()[:8]}"
                )
                converted_data['account_name'] = str(account_name)[:140]

        return converted_data

    def _update_buffer_status(self, buffer_name: str, status: str, error_log: str):
        """Update buffer record status with enhanced logging"""
        try:
            frappe.db.sql("""
                UPDATE `tabMigration Data Buffer`
                SET processing_status = %s,
                    error_log = %s,
                    processed_at = %s
                WHERE name = %s
            """, (
                status,
                error_log[:1000] if error_log else '',  # Limit error log length
                now() if status in ['Processed', 'Failed', 'Skipped'] else None,
                buffer_name
            ))
            
        except Exception as e:
            self.logger.logger.error(f"❌ Failed to update buffer status: {str(e)}")

    def get_buffer_statistics(self, target_doctype: str = None) -> Dict[str, Any]:
        """ENHANCED: Get comprehensive buffer statistics"""
        try:
            conditions = ["1=1"]
            params = []

            if target_doctype:
                conditions.append("target_doctype = %s")
                params.append(target_doctype)

            where_clause = " AND ".join(conditions)

            # Get detailed statistics
            stats = frappe.db.sql(f"""
                SELECT
                    processing_status,
                    target_doctype,
                    COUNT(*) as count,
                    MIN(created_at) as first_created,
                    MAX(processed_at) as last_processed
                FROM `tabMigration Data Buffer`
                WHERE {where_clause}
                GROUP BY processing_status, target_doctype
                ORDER BY target_doctype, processing_status
            """, params, as_dict=True)

            # Get error summary
            error_summary = frappe.db.sql(f"""
                SELECT 
                    LEFT(error_log, 100) as error_type,
                    COUNT(*) as error_count
                FROM `tabMigration Data Buffer`
                WHERE {where_clause} AND processing_status = 'Failed'
                    AND error_log IS NOT NULL AND error_log != ''
                GROUP BY LEFT(error_log, 100)
                ORDER BY error_count DESC
                LIMIT 10
            """, params, as_dict=True)

            result = {
                "total_records": sum(s['count'] for s in stats),
                "by_status": {},
                "by_doctype": {},
                "processing_summary": stats,
                "error_summary": error_summary,
                "last_updated": now()
            }

            for stat in stats:
                status = stat['processing_status']
                doctype = stat['target_doctype']
                count = stat['count']

                if status not in result["by_status"]:
                    result["by_status"][status] = 0
                result["by_status"][status] += count

                if doctype not in result["by_doctype"]:
                    result["by_doctype"][doctype] = {}
                result["by_doctype"][doctype][status] = count

            return result

        except Exception as e:
            self.logger.logger.error(f"❌ Failed to get buffer statistics: {str(e)}")
            return {"error": str(e), "total_records": 0}

    def cleanup_processed_buffer(self, days_old: int = 7) -> int:
        """ENHANCED: Clean up old processed records with better logging"""
        try:
            cutoff_date = frappe.utils.add_days(now(), -days_old)
            
            # Get count before deletion for reporting
            count_query = """
                SELECT COUNT(*) as count
                FROM `tabMigration Data Buffer`
                WHERE processing_status IN ('Processed', 'Skipped')
                AND processed_at < %s
            """
            
            count_result = frappe.db.sql(count_query, cutoff_date, as_dict=True)
            records_to_delete = count_result[0]['count'] if count_result else 0
            
            if records_to_delete == 0:
                self.logger.logger.info("🧹 No old buffer records to clean up")
                return 0
            
            # Delete old records
            delete_query = """
                DELETE FROM `tabMigration Data Buffer`
                WHERE processing_status IN ('Processed', 'Skipped')
                AND processed_at < %s
            """
            
            frappe.db.sql(delete_query, cutoff_date)
            frappe.db.commit()
            
            self.logger.logger.info(f"🧹 Cleaned up {records_to_delete} old buffer records (older than {days_old} days)")
            
            return records_to_delete

        except Exception as e:
            self.logger.logger.error(f"❌ Buffer cleanup failed: {str(e)}")
            return 0