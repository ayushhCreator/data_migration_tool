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
                    "total_rows": int(len(df[col])),
                    "non_empty_rows": int(len(non_empty_values)),
                    "empty_rows": int(len(df[col]) - len(non_empty_values)),
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
                            on_bad_lines='skip'
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
                            "created_by": valid_user,
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

    def normalize_headers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add this new method after line 150"""
        # Remove BOM and normalize headers
        df.columns = df.columns.str.replace('\ufeff', '')  # Remove BOM
        df.columns = df.columns.str.strip().str.lower()
    
         # Normalize common variations
        header_mappings = {
           'e_mail': 'email', 'e-mail': 'email', 'emailid': 'email',
           'cust_id': 'customer_id', 'custid': 'customer_id',
           'phone_no': 'phone', 'mobile_no': 'phone', 'contact': 'phone',
           'comp_name': 'company_name', 'organization': 'company_name'
        }
        df.columns = [header_mappings.get(col, col) for col in df.columns]
        return df

    # PRIORITY 1 FIX: Enhanced upsert processing method
    def process_buffered_data_with_upsert(self, target_doctype: str, batch_size: int = 100) -> Dict[str, int]:
        """PRIORITY 1: Enhanced processing with true upsert capability"""
        results = {"success": 0, "failed": 0, "skipped": 0, "updated": 0}
        
        try:
            # Get pending records
            pending_records = frappe.db.sql("""
                SELECT name, raw_data, row_index, source_file
                FROM `tabMigration Data Buffer`
                WHERE target_doctype = %s AND processing_status = 'Pending'
                ORDER BY row_index
                LIMIT %s
            """, (target_doctype, batch_size), as_dict=True)
            
            if not pending_records:
                return results
                
            self.logger.logger.info(f"🔄 Processing {len(pending_records)} records with upsert for {target_doctype}")
            
            meta = frappe.get_meta(target_doctype)
            
            for record in pending_records:
                try:
                    # Parse and convert data
                    raw_data = json.loads(record.raw_data)
                    converted_data = self.apply_jit_conversion(raw_data, meta)
                    
                    # Validate data
                    validation_errors = self.validate_and_clean_data(converted_data, meta)
                    if validation_errors:
                        error_msg = "; ".join(validation_errors)
                        self._update_buffer_status(record.name, "Failed", error_msg)
                        results["failed"] += 1
                        continue
                    
                    # UPSERT LOGIC: Check if record exists
                    existing_name = self._find_existing_record(converted_data, target_doctype)
                    
                    if existing_name:
                        # UPDATE existing record
                        try:
                            existing_doc = frappe.get_doc(target_doctype, existing_name)
                            
                            # Update only non-empty fields from CSV
                            for field, value in converted_data.items():
                                if field != 'name' and value:  # Don't update name field
                                    setattr(existing_doc, field, value)
                            
                            existing_doc.save(ignore_permissions=True)
                            
                            self._update_buffer_status(record.name, "Processed", f"Updated {existing_name}")
                            results["updated"] += 1
                            self.logger.logger.info(f"✅ Row {record.row_index}: Updated {existing_name}")
                            
                        except Exception as update_error:
                            error_msg = f"Update failed: {str(update_error)[:200]}"
                            self._update_buffer_status(record.name, "Failed", error_msg)
                            results["failed"] += 1
                            self.logger.logger.error(f"❌ Row {record.row_index}: {error_msg}")
                            
                    else:
                        # INSERT new record
                        try:
                            doc_data = {"doctype": target_doctype}
                            doc_data.update(converted_data)
                            
                            new_doc = frappe.get_doc(doc_data)
                            new_doc.insert(ignore_permissions=True)
                            
                            self._update_buffer_status(record.name, "Processed", f"Created {new_doc.name}")
                            results["success"] += 1
                            self.logger.logger.info(f"✅ Row {record.row_index}: Created {new_doc.name}")
                            
                        except Exception as insert_error:
                            error_msg = f"Insert failed: {str(insert_error)[:200]}"
                            self._update_buffer_status(record.name, "Failed", error_msg)
                            results["failed"] += 1
                            self.logger.logger.error(f"❌ Row {record.row_index}: {error_msg}")
                            
                except Exception as record_error:
                    error_msg = f"Record processing failed: {str(record_error)[:200]}"
                    self._update_buffer_status(record.name, "Failed", error_msg)
                    results["failed"] += 1
                    self.logger.logger.error(f"❌ Row {record.row_index}: {error_msg}")
            
            # Commit all changes
            frappe.db.commit()
            
            total_processed = sum(results.values())
            self.logger.logger.info(f"✅ Batch completed: {results} | Total: {total_processed}")
            
            return results
            
        except Exception as e:
            frappe.db.rollback()
            self.logger.logger.error(f"💥 Batch processing failed: {str(e)}")
            return results

    def _find_existing_record(self, converted_data: Dict[str, Any], doctype: str) -> Optional[str]:
        """Find existing record using multiple strategies"""
        
        # Strategy 1: Check by name field if present
        if 'name' in converted_data and converted_data['name']:
            existing = frappe.db.get_value(doctype, converted_data['name'], 'name')
            if existing:
                return existing
        
        # Strategy 2: Check unique fields
        meta = frappe.get_meta(doctype)
        unique_fields = [f.fieldname for f in meta.fields if getattr(f, 'unique', False)]
        
        for field in unique_fields:
            if field in converted_data and converted_data[field]:
                existing = frappe.db.get_value(doctype, {field: converted_data[field]}, 'name')
                if existing:
                    return existing
        
        # Strategy 3: DocType-specific matching rules
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
                existing = frappe.db.get_value(doctype, filters, 'name')
                if existing:
                    return existing
        
        return None

    # PRIORITY 1 FIX: Enhanced field mapping with proper ID handling
    def apply_jit_conversion(self, raw_data: Dict[str, str], meta) -> Dict[str, Any]:
        """FIXED: Enhanced field mapping with proper error handling"""
        converted_data = {}
        available_fields = [f.fieldname for f in meta.fields]
        
        # Enhanced field mappings with ID prioritization
        field_mappings = {
            # ID fields - these should map to 'name' field for record identification
            'id': 'name',
            'customer_id': 'name', 
            'supplier_id': 'name',
            'item_id': 'item_code',  # For items, use item_code
            'contact_id': 'name',
            'user_id': 'name',
            
            # Universal mappings
            'email': 'email_id',
            'phone': 'mobile_no',
            'mobile': 'mobile_no',
            'company_name': 'supplier_name',  # For suppliers
            'display_name': 'supplier_name',
            'customer_name': 'customer_name',
        }
        
        # STEP 1: Identify the primary identifier from CSV
        primary_id_field = None
        primary_id_value = None
        
        # Priority order for ID fields
        id_priority = ['id', 'customer_id', 'supplier_id', 'email', 'name']
        
        for csv_field, raw_value in raw_data.items():
            if not raw_value or str(raw_value).strip() == '':
                continue
                
            clean_field = csv_field.lower().replace(' ', '_').replace('-', '_')
            
            # Check if this is a high-priority ID field
            for priority_field in id_priority:
                if priority_field in clean_field.lower():
                    if not primary_id_field:  # Take the first match
                        primary_id_field = clean_field
                        primary_id_value = str(raw_value).strip()
                        break
        
        # STEP 2: Process all fields including the primary ID
        for csv_field, raw_value in raw_data.items():
            if not raw_value or str(raw_value).strip() == '':
                continue
                
            clean_field = csv_field.lower().replace(' ', '_').replace('-', '_')
            target_field = field_mappings.get(clean_field, clean_field)
            
            # CRITICAL: Handle ID fields specially
            if clean_field == primary_id_field:
                if meta.name in ['Supplier', 'Customer', 'Contact', 'Lead']:
                    # For these DocTypes, use the ID as the record name
                    converted_data['name'] = self._generate_safe_name(raw_value, meta.name)
                elif meta.name == 'Item' and 'item_code' in available_fields:
                    converted_data['item_code'] = str(raw_value).strip()[:140]
                else:
                    # For other DocTypes, try to use as name if possible
                    converted_data['name'] = self._generate_safe_name(raw_value, meta.name)
            
            # Check if target field exists in DocType
            if target_field not in available_fields:
                similar_field = self._find_similar_field(target_field, available_fields)
                if similar_field:
                    target_field = similar_field
                else:
                    # Skip fields that don't exist in the target DocType
                    self.logger.logger.debug(f"Field '{target_field}' not found in {meta.name}, skipping")
                    continue
                    
            # FIXED: Initialize field_meta properly
            field_meta = None
            for f in meta.fields:
                if f.fieldname == target_field:
                    field_meta = f
                    break
                    
            if field_meta is None:
                # If no field metadata found, skip this field
                self.logger.logger.debug(f"No metadata found for field '{target_field}', skipping")
                continue
                
            # Convert value based on field type - NOW field_meta is guaranteed to exist
            try:
                self.current_field_name = clean_field
                converted_value = self._smart_type_conversion(raw_value, field_meta.fieldtype, field_meta)
                
                if converted_value is not None and target_field != 'name':
                    converted_data[target_field] = converted_value
                    
            except Exception as conversion_error:
                self.logger.logger.warning(f"Conversion error for field '{target_field}': {str(conversion_error)}")
                # Continue with next field instead of failing the entire record
                continue
        
        # STEP 3: Ensure we have a proper name field
        if 'name' not in converted_data and primary_id_value:
            converted_data['name'] = self._generate_safe_name(primary_id_value, meta.name)
        elif 'name' not in converted_data:
            # Generate a fallback name if no ID found
            import uuid
            converted_data['name'] = f"{meta.name}-{str(uuid.uuid4())[:8]}"
        
        # Apply DocType-specific processing
        try:
            converted_data = self._apply_doctype_specific_processing(converted_data, raw_data, meta)
        except Exception as processing_error:
            self.logger.logger.warning(f"DocType-specific processing error: {str(processing_error)}")
            # Continue with basic converted_data
        
        return converted_data

    def upsert_row(self, row_data: Dict, doctype: str, id_fields: List[str]) -> Dict[str, Any]:
            """Replace existing upsert method around line 400"""
            result = {"action": None, "record_name": None, "error": None}
            try:
                # Step 1: Find existing record using priority identifier strategy
                existing_name = None
                used_identifier = None
                for id_field in id_fields:
                    if id_field in row_data and row_data[id_field]:
                        value = str(row_data[id_field]).strip()
                        existing = frappe.db.get_value(doctype, {id_field: value}, 'name')
                        if existing:
                            existing_name = existing
                            used_identifier = id_field
                            break
                if existing_name:
                    # UPDATE existing record
                    doc = frappe.get_doc(doctype, existing_name)
                    updated_fields = []
                    for field, value in row_data.items():
                        if field != 'name' and value and str(value).strip():  # Don't overwrite with empty
                            if hasattr(doc, field) and getattr(doc, field) != value:
                                setattr(doc, field, value)
                                updated_fields.append(field)
                    if updated_fields:
                        doc.save(ignore_permissions=True)
                        result = {"action": "updated", "record_name": existing_name, "fields_updated": updated_fields}
                    else:
                        result = {"action": "skipped", "record_name": existing_name, "reason": "no_changes"}
                else:
                    # INSERT new record with meaningful name
                    doc_data = {"doctype": doctype}
                    doc_data.update(row_data)
                    # Generate meaningful name from identifiers
                    if 'name' not in doc_data and id_fields:
                        doc_data['name'] = self.generate_meaningful_name(row_data, doctype, id_fields)
                    new_doc = frappe.get_doc(doc_data)
                    new_doc.insert(ignore_permissions=True)
                    result = {"action": "inserted", "record_name": new_doc.name}
            except Exception as e:
                result = {"action": "failed", "error": str(e)}
            return result
    
    def generate_meaningful_name(self, row_data: Dict, doctype: str, id_fields: List[str]) -> str:
            """Generate meaningful names instead of random IDs"""
            # Priority order for name generation
            name_candidates = []
            for field in id_fields:
                if field in row_data and row_data[field]:
                    clean_value = re.sub(r'[^a-zA-Z0-9\-_]', '_', str(row_data[field]))
                    name_candidates.append(clean_value[:50])
            if name_candidates:
                base_name = name_candidates[0]
                # Ensure uniqueness
                counter = 1
                final_name = base_name
                while frappe.db.exists(doctype, final_name):
                    final_name = f"{base_name}_{counter}"
                    counter += 1
                return final_name
            # Fallback: use timestamp-based name
            import time
            return f"{doctype}_{int(time.time())}"
    
    def _generate_safe_name(self, raw_id: str, doctype: str) -> str:
        """Generate a safe, unique name from raw ID"""
        import re
        
        if not raw_id or str(raw_id).strip() == '':
            import uuid
            return f"{doctype}-{str(uuid.uuid4())[:8]}"
        
        # Clean the ID
        clean_id = str(raw_id).strip()
        
        # For long numeric IDs, use a prefix to make them meaningful
        if clean_id.isdigit() and len(clean_id) > 8:
            prefix = {
                'Supplier': 'SUP',
                'Customer': 'CUST', 
                'Contact': 'CONT',
                'Lead': 'LEAD',
                'Item': 'ITEM',
                'Expense': 'EXP'  # Add Expense prefix
            }.get(doctype, 'REC')
            return f"{prefix}-{clean_id[-8:]}"
        
        # For shorter IDs or alphanumeric IDs, use as-is with cleanup
        safe_name = re.sub(r'[^a-zA-Z0-9\-_]', '_', clean_id)
        return safe_name[:140]  # Frappe name field limit

    def _find_similar_field(self, target_field: str, available_fields: List[str]) -> Optional[str]:
        """Find similar field names using fuzzy matching"""
        import difflib
        
        # Direct match
        if target_field in available_fields:
            return target_field
        
        # Common field mappings for Expense doctype
        expense_mappings = {
            'amount': 'sanctioned_amount',
            'cost': 'sanctioned_amount', 
            'expense_amount': 'sanctioned_amount',
            'date': 'posting_date',
            'expense_date': 'posting_date',
            'description': 'expense_details',
            'details': 'expense_details',
            'category': 'expense_type',
            'type': 'expense_type',
            'employee': 'employee',
            'employee_name': 'employee_name'
        }
        
        if target_field in expense_mappings:
            mapped_field = expense_mappings[target_field]
            if mapped_field in available_fields:
                return mapped_field
        
        # Fuzzy matching as fallback
        matches = difflib.get_close_matches(target_field, available_fields, n=1, cutoff=0.6)
        return matches[0] if matches else None

    def _smart_type_conversion(self, raw_value: Any, field_type: str, field_meta) -> Any:
        """FIXED: Smart type conversion with better error handling"""
        if raw_value is None or str(raw_value).strip() == '':
            return None
            
        try:
            str_value = str(raw_value).strip()
            
            if field_type == 'Currency':
                # Remove currency symbols and convert to float
                clean_value = re.sub(r'[^\d.-]', '', str_value)
                return float(clean_value) if clean_value else 0.0
                
            elif field_type == 'Float':
                clean_value = re.sub(r'[^\d.-]', '', str_value)
                return float(clean_value) if clean_value else 0.0
                
            elif field_type == 'Int':
                clean_value = re.sub(r'[^\d-]', '', str_value)
                return int(clean_value) if clean_value else 0
                
            elif field_type == 'Date':
                from frappe.utils import getdate
                return getdate(str_value)
                
            elif field_type == 'Datetime':
                from frappe.utils import get_datetime
                return get_datetime(str_value)
                
            elif field_type in ['Link', 'Dynamic Link']:
                # For Link fields, return cleaned string
                return str_value[:140] if str_value else None
                
            elif field_type in ['Data', 'Small Text', 'Text', 'Long Text']:
                max_length = getattr(field_meta, 'length', 255) if field_meta else 255
                return str_value[:max_length] if str_value else None
                
            else:
                # Default: return as string
                return str_value
                
        except Exception as e:
            self.logger.logger.warning(f"Type conversion failed for '{raw_value}' to {field_type}: {str(e)}")
            # Return raw string as fallback
            return str(raw_value).strip()[:255] if raw_value else None

    def _apply_doctype_specific_processing(self, converted_data: Dict[str, Any], raw_data: Dict[str, str], meta) -> Dict[str, Any]:
        """Apply DocType-specific processing rules"""
        doctype_name = meta.name
        
        if doctype_name == 'Expense':
            # Ensure required fields for Expense
            if 'employee' not in converted_data:
                # Try to get default employee or set to Administrator
                converted_data['employee'] = frappe.db.get_value('Employee', {'user_id': frappe.session.user}, 'name')
                if not converted_data['employee']:
                    # Create a default employee record or use existing one
                    default_employee = frappe.db.get_value('Employee', {}, 'name')
                    if default_employee:
                        converted_data['employee'] = default_employee
            
            if 'expense_type' not in converted_data:
                # Set a default expense type
                converted_data['expense_type'] = 'Office'
                
            if 'company' not in converted_data:
                # Set default company
                converted_data['company'] = frappe.db.get_single_value('Global Defaults', 'default_company')
        
        return converted_data

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
    
    def create_import_log_table(self):
        """Add this method to track import history"""
        if not frappe.db.exists('DocType', 'Import Log'):
           # Create Import Log DocType via SQL
           frappe.db.sql("""
             CREATE TABLE IF NOT EXISTS `tabImport Log` (
                 name VARCHAR(140) PRIMARY KEY,
                 source_file VARCHAR(255),
                 row_hash VARCHAR(32),
                 doctype VARCHAR(140),
                 record_name VARCHAR(140),
                 import_timestamp DATETIME,
                 INDEX idx_row_hash (row_hash),
                 INDEX idx_source_file (source_file)
            )
            """)
    

    def compute_row_hash(self, row_data: Dict) -> str:
        """Compute deterministic hash for row data"""
        # Sort keys and create consistent string representation
        sorted_items = sorted([(k, str(v).strip()) for k, v in row_data.items() if v])
        row_string = '|'.join([f"{k}:{v}" for k, v in sorted_items])
        return hashlib.md5(row_string.encode('utf-8')).hexdigest()

    def is_duplicate_row(self, row_hash: str, source_file: str) -> bool:
        """Check if exact same row was imported before"""
        return frappe.db.exists('Import Log', {'row_hash': row_hash, 'source_file': source_file})