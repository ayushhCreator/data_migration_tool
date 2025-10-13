import os
import json
import frappe
from frappe.utils import now, get_datetime, add_to_date, cstr, cint, flt
import pandas as pd
import numpy as np
from pathlib import Path
import re
import hashlib
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple

class CSVConnector:
    """Enhanced CSV Connector with universal duplicate detection and improved error handling"""
    
    def __init__(self, logger):
        self.logger = logger
        self.supported_formats = ['.csv', '.xlsx', '.xls']
        self.current_field_name = ''
        
        # Generate unique session ID for this import session
        import frappe.utils
        self.import_session_id = frappe.utils.generate_hash()[:8]
        self.logger.logger.info(f"Starting new import session: {self.import_session_id}")

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
        """Enhanced data profiling with numpy type conversion"""
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
            self.logger.logger.info(f"“ˆ Data Profile for {filename}: {json.dumps(safe_profile, indent=2)}")
        except Exception as e:
            self.logger.logger.warning(f"âš ï¸ Data profiling failed: {str(e)}")

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
                        self.logger.logger.info(f"“„ Successfully read CSV with {encoding} encoding: {Path(file_path).name}")
                        break
                    except UnicodeDecodeError:
                        continue
                    except Exception as e:
                        self.logger.logger.warning(f"âš ï¸ Failed to read with {encoding}: {str(e)}")
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
                        self.logger.logger.warning("âš ï¸ Used error handling for encoding issues")
                        encoding_used = 'utf-8 (with error handling)'
                    except Exception as final_error:
                        raise Exception(f"Failed to read CSV file with all encoding attempts: {str(final_error)}")
                        
            elif file_ext in ['.xlsx', '.xls']:
                try:
                    df = pd.read_excel(file_path, dtype=str, keep_default_na=False)
                    encoding_used = 'Excel format'
                    self.logger.logger.info(f"“„ Successfully read Excel file: {Path(file_path).name}")
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
                f"“Š Successfully loaded {len(df)} rows and {len(df.columns)} columns from {Path(file_path).name} "
                f"using {encoding_used}"
            )
            
            # Data profiling for better insights
            self.profile_data(df, Path(file_path).name)
            return df
            
        except Exception as e:
            error_msg = f"Failed to read file {Path(file_path).name}: {str(e)}"
            self.logger.logger.error(f"âŒ {error_msg}")
            raise Exception(error_msg)

    # Hash-Based Deduplication Methods
    def compute_stable_hash(self, row_data: dict, row_number: int = None) -> str:
        """
        Compute stable hash for row deduplication - ensures each CSV row gets a unique hash
        This prevents false duplicate detection when CSV rows have similar but not identical data
        """
        # ALWAYS include row number for uniqueness if available
        if row_number is not None:
            # Create unique identifier using row number + data hash
            # This ensures each CSV row gets a unique hash even if data is similar
            sorted_items = sorted(row_data.items())
            non_empty_items = []
            for k, v in sorted_items:
                if v is not None and str(v).strip():
                    normalized_value = str(v).strip()
                    non_empty_items.append((k, normalized_value))
            
            # Include row number in hash to ensure uniqueness
            data_string = '|'.join([f"{k}:{v}" for k, v in non_empty_items])
            unique_identifier = f"ROW_{row_number}|{data_string}"
        else:
            # Fallback: Use all non-empty fields for full row hash (original behavior)
            sorted_items = sorted(row_data.items())
            non_empty_items = []
            for k, v in sorted_items:
                if v is not None and str(v).strip():
                    normalized_value = str(v).strip()
                    non_empty_items.append((k, normalized_value))
            unique_identifier = '|'.join([f"{k}:{v}" for k, v in non_empty_items])
        
        # Generate hash from the unique identifier
        return hashlib.sha256(unique_identifier.encode('utf-8')).hexdigest()[:16]

    def log_import_record(self, row_hash: str, source_file: str, target_doctype: str, record_name: str, action: str, raw_data: dict = None, error: str = None):
        """Log import operation for audit and deduplication"""
        try:
            # Check if Import Log DocType exists
            if not frappe.db.exists('DocType', 'Import Log'):
                self.logger.logger.warning("Import Log DocType not found - skipping logging")
                return
            
            # Check if this exact hash already exists to prevent duplicates
            if frappe.db.exists('Import Log', {'row_hash': row_hash}):
                self.logger.logger.debug(f"Import log already exists for hash {row_hash}, skipping duplicate log")
                return
                
            import_log = frappe.get_doc({
                'doctype': 'Import Log',
                'import_session': self.import_session_id,
                'source_file': source_file,
                'target_doctype': target_doctype,
                'record_name': record_name,
                'row_hash': row_hash,
                'import_timestamp': frappe.utils.now(),
                'action_taken': action.title(),
                'raw_data_preview': json.dumps(raw_data, indent=2)[:1000] if raw_data else None,
                'error_message': error
            })
            import_log.insert(ignore_permissions=True)
            frappe.db.commit()
        except Exception as e:
            self.logger.logger.warning(f"Failed to log import record: {str(e)}")

    def is_row_already_imported(self, row_hash: str, source_file: str, target_doctype: str) -> dict:
        """Check if exact same data was already imported"""
        try:
            # Check if Import Log DocType exists
            if not frappe.db.exists('DocType', 'Import Log'):
                return {'exists': False}
            
            existing = frappe.db.get_value(
                'Import Log',
                {'row_hash': row_hash, 'target_doctype': target_doctype},
                ['record_name', 'import_timestamp', 'action_taken'],
                as_dict=True
            )
            
            if existing:
                return {
                    'exists': True,
                    'record_name': existing.record_name,
                    'import_timestamp': existing.import_timestamp,
                    'action_taken': existing.action_taken
                }
            return {'exists': False}
        except Exception as e:
            self.logger.logger.warning(f"Error checking import log: {str(e)}")
            return {'exists': False}

    def detect_data_changes(self, new_data: dict, existing_record_name: str, doctype: str) -> tuple:
        """Detect what fields have changed in the new data vs existing record"""
        try:
            existing_doc = frappe.get_doc(doctype, existing_record_name)
            changed_fields = []
            
            for field, new_value in new_data.items():
                if field == 'name':  # Skip name field
                    continue
                    
                if hasattr(existing_doc, field):
                    existing_value = getattr(existing_doc, field)
                    
                    # Normalize both values for comparison
                    new_normalized = str(new_value).strip() if new_value else ""
                    existing_normalized = str(existing_value).strip() if existing_value else ""
                    
                    if new_normalized != existing_normalized:
                        # Only consider it changed if new value is not empty
                        if new_normalized:  # Don't overwrite with empty values
                            changed_fields.append(field)
            
            has_changes = len(changed_fields) > 0
            return has_changes, changed_fields
            
        except Exception as e:
            self.logger.logger.error(f"Error detecting changes: {str(e)}")
            return True, []  # Assume changes if we can't determine


    # def find_existing_record_by_business_rules(self, converted_data: Dict[str, Any], doctype: str) -> Optional[str]:
    #     """Find existing record using comprehensive business logic"""
    #     self.logger.logger.info(f"Finding existing {doctype} record with data: {list(converted_data.keys())}")
        
    #     # Strategy 1: Check by name field if present
    #     if "name" in converted_data and converted_data["name"]:
    #         existing = frappe.db.get_value(doctype, converted_data["name"], "name")
    #         if existing:
    #             self.logger.logger.info(f"Found existing by name: {existing}")
    #             return existing
        
    #     # Strategy 2: Check unique fields from DocType meta
    #     try:
    #         meta = frappe.get_meta(doctype)
    #         unique_fields = [f.fieldname for f in meta.fields if getattr(f, 'unique', False)]
            
    #         self.logger.logger.info(f"Unique fields for {doctype}: {unique_fields}")
            
    #         for field in unique_fields:
    #             if field in converted_data and converted_data[field]:
    #                 self.logger.logger.info(f"Checking unique field {field}={converted_data[field]}")
    #                 existing = frappe.db.get_value(doctype, {field: converted_data[field]}, "name")
    #                 if existing:
    #                     self.logger.logger.info(f"Found existing by {field}: {existing}")
    #                     return existing
                        
    #     except Exception as e:
    #         self.logger.logger.warning(f"Error checking unique fields: {str(e)}")
        
    #     # Strategy 3: Business identifier patterns
    #     id_patterns = ['id', 'customer_id', 'supplier_id', 'contact_id', 'user_id', 'code', 'reference', 'external_id', 'product_id']
    #     for pattern in id_patterns:
    #         if pattern in converted_data and converted_data[pattern]:
    #             # Try to find by this pattern in various likely fields
    #             potential_fields = [pattern, "name", "code", "reference"]
                
    #             for field in potential_fields:
    #                 try:
    #                     if frappe.db.has_column(doctype, field):
    #                         existing = frappe.db.get_value(doctype, {field: converted_data[pattern]}, "name")
    #                         if existing:
    #                             self.logger.logger.info(f"Found existing by {field}={converted_data[pattern]}: {existing}")
    #                             return existing
    #                 except Exception:
    #                     continue
        
    #     # Strategy 4: Email-based matching
    #     if doctype in ['Contact', 'User', 'Customer', 'Supplier']:
    #         email_fields = ['email', 'email_id', 'primary_email']
    #         for email_field in email_fields:
    #             if email_field in converted_data and converted_data[email_field]:
    #                 try:
    #                     existing = frappe.db.get_value(doctype, {email_field: converted_data[email_field]}, "name")
    #                     if existing:
    #                         self.logger.logger.info(f"Found existing by email: {existing}")
    #                         return existing
    #                 except Exception:
    #                     continue
        
    #     self.logger.logger.info(f"No existing record found for {doctype}")
    #     return None

    # =============================================================================
# INTELLIGENT UNIQUE KEY DETECTION SYSTEM
# Add these methods to the CSVConnector class in csv_connector.py
# Place them after the existing find_existing_record_by_business_rules() method
# =============================================================================

    def get_schema_intelligence(self, doctype: str) -> Dict[str, Any]:
        """
        Extract comprehensive schema intelligence from DocType metadata.
        This provides the highest priority signals for unique field detection.
        """
        try:
            meta = frappe.get_meta(doctype)
            intelligence = {
                'unique_constraints': [],      # Fields with unique=True
                'primary_key': 'name',          # Default primary key
                'foreign_keys': [],             # Link fields (relationships)
                'required_fields': [],          # Mandatory fields
                'indexed_fields': [],           # Fields with db index
                'field_types': {},              # Data type mapping
                'child_tables': []              # Child table fields
            }
            for field in meta.fields:
                # Unique constraints - HIGHEST PRIORITY
                if getattr(field, 'unique', False):
                    intelligence['unique_constraints'].append({
                        'fieldname': field.fieldname,
                        'fieldtype': field.fieldtype,
                        'priority': 100  # Highest confidence
                    })
                # Foreign keys - EXCLUDE from unique matching
                if field.fieldtype == 'Link':
                    intelligence['foreign_keys'].append(field.fieldname)
                # Child tables - also exclude
                if field.fieldtype == 'Table':
                    intelligence['child_tables'].append(field.fieldname)
                # Required fields - can be composite key candidates
                if getattr(field, 'reqd', False):
                    intelligence['required_fields'].append(field.fieldname)
                # Indexed fields - performance optimized lookups
                if getattr(field, 'search_index', False):
                    intelligence['indexed_fields'].append(field.fieldname)
                intelligence['field_types'][field.fieldname] = field.fieldtype
            self.logger.logger.debug(f"Schema intelligence for {doctype}: {intelligence}")
            return intelligence
        except Exception as e:
            self.logger.logger.warning(f"Error getting schema intelligence for {doctype}: {str(e)}")
            return {
                'unique_constraints': [],
                'primary_key': 'name',
                'foreign_keys': [],
                'required_fields': [],
                'indexed_fields': [],
                'field_types': {},
                'child_tables': []
            }


    def analyze_field_patterns(self, field_name: str, doctype: str) -> Dict[str, Any]:
        """
        Intelligent field pattern analysis with confidence scoring.
        Recognizes business patterns while excluding relationship fields.
        """
        try:
            patterns = {
            'unique_identifiers': {
                'patterns': ['_code', '_number', '_id', 'sku', 'barcode', 'serial', '_no', 'item_code'],
                'exclusions': ['customer_id', 'supplier_id', 'user_id', 'contact_id', 'company_id', 
                              'parent_id', 'owner_id', 'created_by', 'modified_by'],
                'confidence': 80
            },
            'foreign_keys': {
                'patterns': ['customer', 'supplier', 'contact', 'user', 'company', 'parent', 'owner'],
                'suffixes': ['_id', '_name'],
                'confidence': -100  # Negative score = exclude
            },
            'business_keys': {
                'patterns': ['reference', 'invoice_no', 'po_number', 'transaction_id', 'order_id', 
                           'voucher_no', 'receipt_no', 'bill_no'],
                'confidence': 90
            },
            'natural_keys': {
                'patterns': ['email', 'phone', 'mobile', 'tax_id', 'registration_no', 'pan', 
                           'gstin', 'ein', 'ssn'],
                'confidence': 85
            }
        }
        
            score = 0
            matched_pattern = None
            field_lower = field_name.lower()
        
            # Check if it's in the explicit exclusion list
            if field_lower in patterns['unique_identifiers']['exclusions']:
                return {
                    'is_unique_candidate': False,
                    'confidence': -100,
                    'pattern_type': 'excluded_foreign_key',
                    'reason': f'Explicitly excluded: {field_name}'
                }
        
            # Check if it's a foreign key pattern (should be EXCLUDED)
            for fk_pattern in patterns['foreign_keys']['patterns']:
                if fk_pattern in field_lower:
                    for suffix in patterns['foreign_keys']['suffixes']:
                        if field_lower.endswith(suffix):
                            return {
                                'is_unique_candidate': False,
                                'confidence': patterns['foreign_keys']['confidence'],
                                'pattern_type': 'foreign_key',
                                'reason': f'Foreign key pattern: {fk_pattern}{suffix}'
                            }
        
            # Check unique identifiers
            for pattern in patterns['unique_identifiers']['patterns']:
                if pattern in field_lower:
                    score = max(score, patterns['unique_identifiers']['confidence'])
                    matched_pattern = 'unique_identifier'
            # Check business keys
            for pattern in patterns['business_keys']['patterns']:
                if pattern in field_lower:
                    score = max(score, patterns['business_keys']['confidence'])
                    matched_pattern = 'business_key'
            # Check natural keys
            for pattern in patterns['natural_keys']['patterns']:
                if pattern in field_lower:
                    score = max(score, patterns['natural_keys']['confidence'])
                    matched_pattern = 'natural_key'
            return {
                'is_unique_candidate': score > 0,
                'confidence': score,
                'pattern_type': matched_pattern,
                'field_name': field_name
            }
        except Exception as e:
            self.logger.logger.warning(f"Error analyzing field patterns for {field_name}: {str(e)}")
            return {'is_unique_candidate': False, 'confidence': 0, 'pattern_type': None}


    def analyze_data_uniqueness(self, field_name: str, doctype: str) -> Dict[str, Any]:
        """
        Analyze actual data distribution to determine if field is unique.
        Uses statistical analysis to validate field uniqueness.
        """
        try:
            # Get total record count
            total_records = frappe.db.count(doctype)
            if total_records == 0:
                return {'is_unique': None, 'confidence': 0, 'reason': 'No existing data'}
            # For performance, sample if too many records
            sample_size = min(total_records, 10000)
            # Count distinct values
            distinct_query = f"""
                SELECT COUNT(DISTINCT `{field_name}`) as distinct_count,
                       COUNT(`{field_name}`) as non_null_count
                FROM `tab{doctype}`
                WHERE `{field_name}` IS NOT NULL
                AND `{field_name}` != ''
                LIMIT {sample_size}
            """
            result = frappe.db.sql(distinct_query, as_dict=True)
            if not result or result[0]['non_null_count'] == 0:
                return {'is_unique': None, 'confidence': 0, 'reason': 'All values are NULL/empty'}
            distinct_count = result[0]['distinct_count']
            non_null_count = result[0]['non_null_count']
            # Calculate uniqueness ratio
            uniqueness_ratio = distinct_count / non_null_count if non_null_count > 0 else 0
            # Score based on uniqueness ratio
            if uniqueness_ratio >= 0.98:  # 98%+ unique values
                confidence = 90
                is_unique = True
                reason = f"{uniqueness_ratio*100:.1f}% unique - very likely unique field"
            elif uniqueness_ratio >= 0.90:  # 90-98% unique
                confidence = 70
                is_unique = True
                reason = f"{uniqueness_ratio*100:.1f}% unique - likely unique field"
            elif uniqueness_ratio >= 0.75:  # 75-90% unique
                confidence = 40
                is_unique = True
                reason = f"{uniqueness_ratio*100:.1f}% unique - possibly unique"
            elif uniqueness_ratio < 0.30:  # Less than 30% unique (like customer_id)
                confidence = -50  # Negative = likely foreign key
                is_unique = False
                reason = f"{uniqueness_ratio*100:.1f}% unique - likely foreign key or category"
            else:
                confidence = 20
                is_unique = False
                reason = f"{uniqueness_ratio*100:.1f}% unique - not sufficiently unique"
            return {
                'is_unique': is_unique,
                'uniqueness_ratio': uniqueness_ratio,
                'confidence': confidence,
                'total_records': total_records,
                'distinct_values': distinct_count,
                'non_null_count': non_null_count,
                'reason': reason
            }
        except Exception as e:
            self.logger.logger.warning(f"Error analyzing data uniqueness for {field_name}: {str(e)}")
            return {'is_unique': None, 'confidence': 0, 'error': str(e)}


    def calculate_field_score(self, field_name: str, doctype: str, converted_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate final confidence score for a field as unique identifier.
        Combines schema, pattern, and data analysis with weighted scoring.
        """
        try:
            total_score = 0
            score_breakdown = []
            # 1. Schema intelligence (highest weight - 100%)
            schema_intel = self.get_schema_intelligence(doctype)
            # Check if it's a unique constraint field
            if field_name in [f['fieldname'] for f in schema_intel['unique_constraints']]:
                total_score = 100  # Definitive unique constraint
                score_breakdown.append(('Schema Unique Constraint', 100))
                return {
                    'total_score': total_score,
                    'breakdown': score_breakdown,
                    'is_candidate': True,
                    'reason': 'Database unique constraint'
                }
            # 2. Foreign key exclusion (blocking condition)
            if field_name in schema_intel['foreign_keys']:
                score_breakdown.append(('Foreign Key (Link field)', -100))
                return {
                    'total_score': -100,
                    'breakdown': score_breakdown,
                    'is_candidate': False,
                    'reason': 'Foreign key field - excluded'
                }
            # 3. Child table exclusion
            if field_name in schema_intel['child_tables']:
                score_breakdown.append(('Child Table', -100))
                return {
                    'total_score': -100,
                    'breakdown': score_breakdown,
                    'is_candidate': False,
                    'reason': 'Child table field - excluded'
                }
            # 4. Pattern analysis (80% weight)
            pattern_analysis = self.analyze_field_patterns(field_name, doctype)
            pattern_score = pattern_analysis['confidence']
            total_score += pattern_score
            score_breakdown.append(('Pattern Analysis', pattern_score))
            # If pattern confidence is negative (foreign key), return early
            if pattern_score < 0:
                return {
                    'total_score': pattern_score,
                    'breakdown': score_breakdown,
                    'is_candidate': False,
                    'reason': pattern_analysis.get('reason', 'Negative pattern match')
                }
            # 5. Data distribution analysis (50% weight)
            data_analysis = self.analyze_data_uniqueness(field_name, doctype)
            if data_analysis.get('confidence'):
                data_score = data_analysis['confidence'] * 0.5  # 50% weight
                total_score += data_score
                score_breakdown.append(('Data Uniqueness', data_score))
            # 6. Indexed field bonus (small boost)
            if field_name in schema_intel['indexed_fields']:
                total_score += 10
                score_breakdown.append(('Database Index', 10))
            is_candidate = total_score > 0
            reason = f"Combined score: {total_score:.1f}"
            return {
                'total_score': total_score,
                'breakdown': score_breakdown,
                'is_candidate': is_candidate,
                'reason': reason,
                'pattern_type': pattern_analysis.get('pattern_type'),
                'data_analysis': data_analysis
            }
        except Exception as e:
            self.logger.logger.warning(f"Error calculating field score for {field_name}: {str(e)}")
            return {
                'total_score': 0,
                'breakdown': [],
                'is_candidate': False,
                'reason': f'Error: {str(e)}'
            }


    def find_existing_record_intelligently(self, converted_data: Dict[str, Any], doctype: str) -> Optional[str]:
            """
            Intelligent record matching with confidence-based field ranking.
            Uses multi-layered analysis to find existing records without false positives.
            """
            try:
                self.logger.logger.info(f"” Intelligent matching for {doctype}")
                # Build candidate fields with confidence scores
                candidates = []
                for field_name, value in converted_data.items():
                    # Skip empty values
                    if not value or (isinstance(value, str) and not value.strip()):
                        continue
                    # Calculate confidence score
                    field_score = self.calculate_field_score(field_name, doctype, converted_data)
                    if field_score['is_candidate'] and field_score['total_score'] > 0:
                        candidates.append({
                            'field': field_name,
                            'value': value,
                            'score': field_score['total_score'],
                            'breakdown': field_score['breakdown'],
                            'reason': field_score['reason']
                        })
                # Sort by confidence score (highest first)
                candidates.sort(key=lambda x: x['score'], reverse=True)
                # ” FIX 1: Only use fields with score >= 70 (exclude low-confidence fields)
                high_confidence_candidates = [c for c in candidates if c['score'] >= 70]
                # Log top candidates
                if high_confidence_candidates:
                    self.logger.logger.info(f"“Š Top HIGH-CONFIDENCE field candidates:")
                    for i, c in enumerate(high_confidence_candidates[:5], 1):
                        value_str = str(c['value'])[:50] if len(str(c['value'])) > 50 else str(c['value'])
                        self.logger.logger.info(f"  {i}. {c['field']}={value_str} (score: {c['score']:.1f})")
                else:
                    self.logger.logger.info(f"âš ï¸  No high-confidence field candidates (score >= 70)")
                    self.logger.logger.info(f"’¡ This is likely a NEW record - will INSERT")
                # ” FIX 3: Only try high-confidence candidates (score >= 70)
                for candidate in high_confidence_candidates:
                    field = candidate['field']
                    value = candidate['value']
                    score = candidate['score']
                    try:
                        # Check if field exists in database
                        if not frappe.db.has_column(doctype, field):
                            self.logger.logger.debug(f"âŒ Field {field} doesn't exist in {doctype}")
                            continue
                        # Try to find existing record
                        existing = frappe.db.get_value(doctype, {field: value}, 'name')
                        if existing:
                            self.logger.logger.info(
                                f" MATCH FOUND by {field}={value} (confidence: {score:.1f}): {existing}"
                            )
                            return existing
                        else:
                            self.logger.logger.debug(
                                f"âŒ No match for {field}={value} (confidence: {score:.1f})"
                            )
                    except Exception as e:
                        self.logger.logger.debug(f"âš ï¸  Error checking {field}: {str(e)}")
                        continue
                self.logger.logger.info(f"” No existing record found for {doctype} (all high-confidence fields checked)")
                return None
            except Exception as e:
                self.logger.logger.error(f"Error in intelligent record matching: {str(e)}")
                return None

# =============================================================================
# END OF INTELLIGENT UNIQUE KEY DETECTION SYSTEM
# =============================================================================


    def generate_meaningful_name(self, data: Dict[str, Any], doctype: str) -> str:
        """
        Generate meaningful record name from CSV data instead of random hash.
        Enhanced to work with ANY CSV field names by using intelligent pattern matching.
        """
        # STEP 1: Define naming priority patterns (flexible matching)
        naming_patterns = {
            # High priority - unique identifiers
            'unique_codes': ['vin', 'code', 'serial', 'number', 'id'],
            'business_refs': ['reference', 'ref_no', 'invoice', 'order', 'voucher', 'receipt'],
            'natural_keys': ['email', 'phone', 'mobile', 'tax_id', 'pan', 'gstin'],
            'descriptive': ['name', 'title', 'label', 'description']
        }

        # STEP 2: Try to find best matching field from CSV data
        best_match = None
        best_score = 0

        for data_field, value in data.items():
            if not value or not str(value).strip():
                continue

            data_field_clean = data_field.lower().replace(' ', '').replace('_', '').replace('-', '')

            # Check against all naming patterns
            for pattern_type, patterns in naming_patterns.items():
                for pattern in patterns:
                    pattern_clean = pattern.replace('_', '').replace('-', '')

                    # Score the match
                    if pattern_clean in data_field_clean or data_field_clean in pattern_clean:
                        # Calculate confidence score based on pattern type
                        if pattern_type == 'unique_codes':
                            score = 100  # Highest priority
                        elif pattern_type == 'business_refs':
                            score = 90
                        elif pattern_type == 'natural_keys':
                            score = 85
                        else:  # descriptive
                            score = 70

                        # Exact match gets bonus
                        if data_field_clean == pattern_clean:
                            score += 10

                        if score > best_score:
                            best_score = score
                            best_match = (data_field, value, pattern_type)

        # STEP 3: Generate name from best match
        if best_match and best_score >= 70:
            field_name, raw_value, pattern_type = best_match

            # Clean the value for use as Frappe name
            clean_value = re.sub(r'[^a-zA-Z0-9-_]', '-', str(raw_value).strip())[:140]

            if clean_value and len(clean_value) >= 2:
                self.logger.logger.info(
                    f"“ Generated name from '{field_name}' (type: {pattern_type}, "
                    f"confidence: {best_score}): {clean_value}"
                )

                # Check if name already exists and make unique if needed
                return self._ensure_unique_name(doctype, clean_value)

        # STEP 4: Fallback - try first non-empty field
        for field_name, value in data.items():
            if value and str(value).strip() and field_name.lower() not in ['id', 'modified', 'created']:
                clean_value = re.sub(r'[^a-zA-Z0-9-_]', '-', str(value).strip())[:40]
                if clean_value and len(clean_value) >= 2:
                    doctype_prefix = doctype.lower().replace(' ', '')[:5]
                    fallback_name = f"{doctype_prefix}-{clean_value}"

                    self.logger.logger.warning(
                        f"âš ï¸  No high-confidence naming field found. "
                        f"Using fallback: {fallback_name}"
                    )

                    return self._ensure_unique_name(doctype, fallback_name)

        # STEP 5: Last resort - generate hash with meaningful prefix
        doctype_prefix = doctype.lower().replace(' ', '')[:5]
        unique_suffix = frappe.utils.generate_hash(8)
        hash_name = f"{doctype_prefix}-{unique_suffix}"

        self.logger.logger.warning(
            f"âš ï¸  Could not generate meaningful name from CSV data. "
            f"Using generated ID: {hash_name}"
        )

        return hash_name

    def _ensure_unique_name(self, doctype: str, base_name: str) -> str:
        """
        Ensure the name is unique by adding counter if needed.
        """
        clean_base = re.sub(r'[^a-zA-Z0-9-_]', '-', base_name)[:130]  # Leave room for counter

        # Check if base name is available
        if not frappe.db.exists(doctype, clean_base):
            return clean_base

        # Add counter to make unique
        counter = 1
        while counter < 1000:  # Reasonable limit
            test_name = f"{clean_base}-{counter}"
            if not frappe.db.exists(doctype, test_name):
                self.logger.logger.info(f"Name collision detected. Using: {test_name}")
                return test_name
            counter += 1

        # Ultimate fallback
        unique_suffix = frappe.utils.generate_hash(6)
        return f"{clean_base[:120]}-{unique_suffix}"


    def store_raw_data(self, df: pd.DataFrame, source_file: str, target_doctype: str) -> int:
        """Store raw data in buffer for processing"""
        stored_count = 0
        total_rows = len(df)
        
        try:
            self.logger.logger.info(f"“¦ Starting to store {total_rows} rows in buffer for {target_doctype}")
            
            batch_size = 50
            for batch_start in range(0, total_rows, batch_size):
                batch_end = min(batch_start + batch_size, total_rows)
                batch_df = df.iloc[batch_start:batch_end]
                
                for index, row in batch_df.iterrows():
                    try:
                        raw_data = row.to_dict()
                        cleaned_data = {k: str(v).strip() if v else '' for k, v in raw_data.items()}
                        
                        # Create buffer document
                        buffer_doc = frappe.get_doc({
                            "doctype": "Migration Data Buffer",
                            "source_file": source_file,
                            "target_doctype": target_doctype,
                            "row_index": int(index),
                            "raw_data": json.dumps(cleaned_data),
                            "processing_status": "Pending",
                            "created_at": now()
                        })
                        buffer_doc.insert(ignore_permissions=True)
                        stored_count += 1
                        
                    except Exception as row_error:
                        self.logger.logger.error(f"âŒ Failed to store row {index}: {str(row_error)}")
                        continue
                
                frappe.db.commit()
                progress = (batch_end / total_rows) * 100
                self.logger.logger.info(f"“¦ Stored batch {batch_start}-{batch_end}: {progress:.1f}% complete")
            
            self.logger.logger.info(f" Successfully stored {stored_count}/{total_rows} raw records in buffer")
            return stored_count
            
        except Exception as e:
            frappe.db.rollback()
            error_msg = f"Failed to store raw data: {str(e)}"
            self.logger.logger.error(f"âŒ {error_msg}")
            raise Exception(error_msg)
    
    def store_raw_data_with_mapping(self, df: pd.DataFrame, source_file: str, target_doctype: str, field_mappings: dict = None) -> int:
        """
        Store raw CSV data in Migration Data Buffer with optional field mappings and then process with intelligent upsert

        Args:
            df (pd.DataFrame): Dataframe of CSV contents
            source_file (str): CSV filename
            target_doctype (str): Target DocType name
            field_mappings (dict): Optional mapping from CSV field names to DocType fieldnames

        Returns:
            int: Number of records stored and processed
        """
        stored_count = 0
        total_rows = len(df)

        try:
            self.logger.logger.info(f"“¦ Starting to store {total_rows} rows in buffer for {target_doctype} (with mapping)")

            batch_size = 50
            for batch_start in range(0, total_rows, batch_size):
                batch_end = min(batch_start + batch_size, total_rows)
                batch_df = df.iloc[batch_start:batch_end]

                for index, row in batch_df.iterrows():
                    try:
                        raw_data = row.to_dict()

                        # Apply field mappings if provided
                        if field_mappings:
                            mapped_data = {}
                            for csv_field, value in raw_data.items():
                                dt_field = field_mappings.get(csv_field, csv_field)
                                mapped_data[dt_field] = str(value).strip() if value else ''
                            raw_data = mapped_data
                        else:
                            # Clean values as strings
                            raw_data = {k: str(v).strip() if v else '' for k, v in raw_data.items()}

                        buffer_doc = frappe.get_doc({
                            "doctype": "Migration Data Buffer",
                            "source_file": source_file,
                            "target_doctype": target_doctype,
                            "row_index": int(index),
                            "raw_data": json.dumps(raw_data),
                            "processing_status": "Pending",
                            "created_at": frappe.utils.now()
                        })
                        buffer_doc.insert(ignore_permissions=True)
                        stored_count += 1

                    except Exception as e:
                        self.logger.logger.error(f"âŒ Failed to store row {index}: {str(e)}")
                        continue

                frappe.db.commit()
                progress = (batch_end / total_rows) * 100
                self.logger.logger.info(f"“¦ Stored batch {batch_start}-{batch_end}: {progress:.1f}% complete")

            self.logger.logger.info(f" Successfully stored {stored_count}/{total_rows} raw records in buffer")

            # Process newly stored data via intelligent upsert
            self.logger.logger.info(f"š€ Starting intelligent upsert processing for {stored_count} records")
            results = self.process_buffered_data_with_upsert(target_doctype)
            self.logger.logger.info(f"“ˆ Upsert results: {results}")

            return stored_count
        except Exception as e:
            frappe.db.rollback()
            self.logger.logger.error(f"âŒ Failed in store_raw_data_with_mapping: {str(e)}")
            raise

    def normalize_headers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize CSV headers"""
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

    
    def process_buffered_data_with_upsert(self, target_doctype: str, batch_size: int = 100, field_mappings: dict = None) -> dict:
        """
        ENHANCED: Process buffered data with hash-based intelligent upsert
        
        Logic:
        1. Compute hash from row data
        2. Check if hash exists in DocType (using row_hash field)
        3. If exists with same hash -> SKIP (no changes)
        4. If exists with different hash -> UPDATE (data changed)
        5. If not exists -> INSERT (new record)
        """
        results = {'success': 0, 'updated': 0, 'skipped': 0, 'failed': 0}
        try:
            # Get pending records from buffer
            pending_records = frappe.db.sql("""
                SELECT name, raw_data, row_index, source_file
                FROM `tabMigration Data Buffer`
                WHERE target_doctype = %s AND processing_status = 'Pending'
                ORDER BY row_index
                LIMIT %s
            """, (target_doctype, batch_size), as_dict=True)
            if not pending_records:
                return results
            self.logger.logger.info(f"”„ Processing {len(pending_records)} records with HASH-BASED upsert for {target_doctype}")
            meta = frappe.get_meta(target_doctype)
            # Check if row_hash field exists
            has_hash_field = any(f.fieldname == 'row_hash' for f in meta.fields)
            # Get unique fields for business logic fallback
            unique_fields = [f.fieldname for f in meta.fields if getattr(f, 'unique', False) and f.fieldname != 'row_hash']
            self.logger.logger.info(f"” Hash field available: {has_hash_field}, Unique fields: {unique_fields}")
            for record in pending_records:
                try:
                    if not record.raw_data:
                        self.logger.logger.warning(f"Buffer {record.name}: Empty raw_data")
                        self._update_buffer_status(record.name, "Failed", "Empty raw_data")
                        results["failed"] += 1
                        continue
                    # Parse raw data
                    try:
                        raw_data = json.loads(record.raw_data)
                        converted_data = self.apply_jit_conversion(raw_data, meta)
                    except json.JSONDecodeError as e:
                        self.logger.logger.error(f"Buffer {record.name}: Invalid JSON: {str(e)}")
                        self._update_buffer_status(record.name, "Failed", f"Invalid JSON: {str(e)}")
                        results["failed"] += 1
                        continue
                    # Validate data
                    validation_errors = self.validate_and_clean_data(converted_data, meta)
                    if validation_errors:
                        error_msg = "; ".join(validation_errors)
                        self._update_buffer_status(record.name, "Failed", error_msg[:1000])
                        results["failed"] += 1
                        continue
                    # STEP 1: Compute hash from raw data with row number for uniqueness
                    row_hash = self.compute_stable_hash(raw_data, record.row_index)
                    # STEP 2: Try to find existing record using hash (PRIORITY METHOD)
                    existing_name = None
                    existing_hash = None
                    if has_hash_field:
                        # Method 1: Check by hash field (FASTEST and MOST ACCURATE)
                        try:
                            existing_record = frappe.db.get_value(
                                target_doctype,
                                {'row_hash': row_hash},
                                ['name', 'row_hash'],
                                as_dict=True
                            )
                            if existing_record:
                                existing_name = existing_record.name
                                existing_hash = existing_record.row_hash
                                self.logger.logger.info(f"Row {record.row_index}: Found by hash - {existing_name}")
                        except Exception as e:
                            self.logger.logger.warning(f"Hash lookup failed: {str(e)}")
                    # Method 2: Fallback to business logic if hash method didn't find anything
                    # DISABLED for line-item imports - we want each row to be unique based on hash only
                    # This prevents deduplication by customer_id, allowing all 69 line items to be imported
                    if not existing_name :  # Disabled business rules fallback
                        existing_name = self.find_existing_record_intelligently(converted_data, target_doctype)
                        if existing_name:
                            # Get existing hash if available
                            if has_hash_field:
                                try:
                                    existing_hash = frappe.db.get_value(target_doctype, existing_name, 'row_hash')
                                except:
                                    existing_hash = None
                            self.logger.logger.info(f"Row {record.row_index}: Found by business rules - {existing_name}")
                   
                   # STEP 3: Decide action based on hash comparison
                    if existing_name:
                        # Record exists - check if data has changed
                        if existing_hash == row_hash:
                            # EXACT SAME DATA - SKIP
                            self.logger.logger.info(f"Row {record.row_index}: â­ï¸ SKIPPED - identical data (hash match) - {existing_name}")
                            self._update_buffer_status(record.name, "Processed", f"Skipped - no changes - {existing_name}")
                            results["skipped"] += 1
                            continue
                        else:
                            # DATA HAS CHANGED - UPDATE
                            try:
                                existing_doc = frappe.get_doc(target_doctype, existing_name)
                                # Update changed fields
                                for field, value in converted_data.items():
                                    if field != "name" and value and hasattr(existing_doc, field):
                                        setattr(existing_doc, field, value)
                                # Update hash and timestamp
                                if has_hash_field:
                                    existing_doc.row_hash = row_hash
                                if hasattr(existing_doc, 'last_import_date'):
                                    existing_doc.last_import_date = frappe.utils.now()
                                existing_doc.save(ignore_permissions=True)
                                self._update_buffer_status(record.name, "Processed", f"âœï¸ Updated {existing_name}")
                                results["updated"] += 1
                                self.logger.logger.info(f"Row {record.row_index}: âœï¸ UPDATED - {existing_name}")
                            except Exception as update_error:
                                error_msg = f"Update failed: {str(update_error)[:200]}"
                                self._update_buffer_status(record.name, "Failed", error_msg)
                                results["failed"] += 1
                                self.logger.logger.error(f"Row {record.row_index}: {error_msg}")
                    else:
                        # NEW RECORD - INSERT
                        try:
                            # Ensure proper name field
                            if "name" not in converted_data:
                                converted_data["name"] = self.generate_meaningful_name(converted_data, target_doctype)
                            doc_data = {"doctype": target_doctype}
                            doc_data.update(converted_data)
                            # Add hash to new record
                            if has_hash_field:
                                doc_data["row_hash"] = row_hash
                            if "last_import_date" in [f.fieldname for f in meta.fields]:
                                doc_data["last_import_date"] = frappe.utils.now()
                            new_doc = frappe.get_doc(doc_data)
                            new_doc.insert(ignore_permissions=True)
                            self._update_buffer_status(record.name, "Processed", f" Created {new_doc.name}")
                            results["success"] += 1
                            self.logger.logger.info(f"Row {record.row_index}:  CREATED - {new_doc.name}")
                        except Exception as insert_error:
                            error_msg = f"Insert failed: {str(insert_error)[:200]}"
                            self._update_buffer_status(record.name, "Failed", error_msg)
                            results["failed"] += 1
                            self.logger.logger.error(f"Row {record.row_index}: {error_msg}")
                except Exception as record_error:
                    error_msg = f"Record processing failed: {str(record_error)[:200]}"
                    self._update_buffer_status(record.name, "Failed", error_msg)
                    results["failed"] += 1
                    self.logger.logger.error(f"Row {record.row_index}: {error_msg}")
            frappe.db.commit()
            total_processed = sum(results.values())
            self.logger.logger.info(f"""
     Hash-based upsert completed: {results}
     Inserted: {results['success']}
     Updated: {results['updated']}
     Skipped: {results['skipped']}
     Failed: {results['failed']}
     Total: {total_processed}
            """)
            return results
        except Exception as e:
            frappe.db.rollback()
            self.logger.logger.error(f"Batch processing failed: {str(e)}")
            return results


    def apply_jit_conversion(self, raw_data: Dict[str, str], meta) -> Dict[str, Any]:

        """
        Enhanced field mapping with EXACT MATCH priority for CSV imports.
        FIXED: Properly handles empty vs non-empty values
        """
        converted_data = {}
        available_fields = [f.fieldname for f in meta.fields]
        
        # Create case-insensitive field mapping for exact matches
        field_mapping_lower = {}
        field_meta_map = {}
        
        for field in meta.fields:
            normalized_name = field.fieldname.lower().replace(' ', '_').replace('-', '_')
            field_mapping_lower[normalized_name] = field.fieldname
            field_meta_map[field.fieldname] = field
        
        # STEP 1: Initialize primary identifier tracking
        primary_id_field = None
        primary_id_value = None
        
        # Priority order for ID fields (for name generation)
        id_priority = ['id', 'customer_id', 'supplier_id', 'email', 'code', 'reference', 'product_id', 'product_name']
        
        # Find the primary identifier first (for name generation)
        for csv_field, raw_value in raw_data.items():
            # FIXED: Check for None and empty string properly
            if raw_value is None or (isinstance(raw_value, str) and raw_value.strip() == ''):
                continue
                
            clean_field = csv_field.lower().replace(' ', '_').replace('-', '_')
            
            # Check if this is a high-priority ID field
            for priority_field in id_priority:
                if priority_field in clean_field.lower():
                    if not primary_id_field:  # Take the first match
                        primary_id_field = clean_field
                        primary_id_value = str(raw_value).strip()
                        break
            
            if primary_id_field:  # Stop searching once we find one
                break
        
        # STEP 2: Process all fields with EXACT MATCH PRIORITY
        for csv_field, raw_value in raw_data.items():
            # FIXED: Proper empty check
            if raw_value is None or (isinstance(raw_value, str) and raw_value.strip() == ''):
                self.logger.logger.debug(f"⏭️ Skipping empty field '{csv_field}'")
                continue
            
            # Normalize the CSV field name
            csv_field_normalized = csv_field.strip().lower().replace(' ', '_').replace('-', '_')
            
            target_field = None
            field_meta = None
            
            # PRIORITY 1: EXACT MATCH (case-insensitive)
            if csv_field_normalized in field_mapping_lower:
                target_field = field_mapping_lower[csv_field_normalized]
                field_meta = field_meta_map.get(target_field)
                self.logger.logger.debug(f"✓ Exact match: CSV '{csv_field}' (value='{raw_value}') → DocType '{target_field}'")
            
            # PRIORITY 2: Pattern-based detection (only if exact match fails)
            if not target_field:
                sample_values = [str(raw_value).strip()] if raw_value else []
                detected_field = self.detect_field_patterns(csv_field, sample_values)
                
                if detected_field and detected_field in available_fields:
                    target_field = detected_field
                    field_meta = field_meta_map.get(target_field)
                    self.logger.logger.debug(f"⚠️ Pattern match: CSV '{csv_field}' → DocType '{target_field}'")
            
            # PRIORITY 3: Find similar field name
            if not target_field:
                similar_field = self._find_similar_field(csv_field_normalized, available_fields)
                if similar_field:
                    target_field = similar_field
                    field_meta = field_meta_map.get(target_field)
                    self.logger.logger.debug(f"⚠️ Similarity match: CSV '{csv_field}' → DocType '{target_field}'")
            
            # Skip if no field mapping found
            if not target_field or target_field not in available_fields:
                self.logger.logger.debug(f"❌ No mapping found for CSV field '{csv_field}', skipping")
                continue
            
            # Skip if field metadata not found
            if not field_meta:
                self.logger.logger.debug(f"❌ No metadata found for field '{target_field}', skipping")
                continue
            
            # Handle primary identifier fields specially (for specific DocTypes)
            if csv_field_normalized == primary_id_field:
                if meta.name in ['Supplier', 'Customer', 'Contact', 'Lead']:
                    # For these DocTypes, use the ID as the record name
                    converted_data['name'] = self._generate_safe_name(raw_value, meta.name)
                elif meta.name == 'Item' and 'item_code' in available_fields:
                    converted_data['item_code'] = str(raw_value).strip()[:140]
                else:
                    # For other DocTypes, try to use as name if possible
                    converted_data['name'] = self._generate_safe_name(raw_value, meta.name)
            
            # Convert and set the field value (skip 'name' field to avoid duplication)
            if target_field != 'name':
                try:
                    self.current_field_name = csv_field_normalized
                    # FIXED: Pass raw_value directly, not processed
                    converted_value = self._smart_type_conversion(raw_value, field_meta.fieldtype, field_meta)
                    
                    # FIXED: Check for actual None, not empty string (empty string is valid!)
                    if converted_value is not None:
                        converted_data[target_field] = converted_value
                        self.logger.logger.debug(f"✓ Converted: '{csv_field}' (raw='{raw_value}') → '{target_field}' = '{converted_value}'")
                    else:
                        self.logger.logger.warning(f"⚠️ Conversion returned None for '{csv_field}' (raw='{raw_value}')")
                        
                except Exception as conversion_error:
                    self.logger.logger.warning(f"⚠️ Conversion error for field '{target_field}': {str(conversion_error)}")
                    continue
        
        # STEP 3: Ensure we have a proper name field
        if 'name' not in converted_data:
            if primary_id_value:
                converted_data['name'] = self._generate_safe_name(primary_id_value, meta.name)
            else:
                # Generate a fallback name from available data
                converted_data['name'] = self.generate_meaningful_name(converted_data, meta.name)
        
        self.logger.logger.info(f"✓ Conversion complete: {len(converted_data)} fields mapped for {meta.name}")
        self.logger.logger.debug(f"✓ Converted data: {converted_data}")
        return converted_data
   

    def detect_field_patterns(self, field_name: str, sample_values: list) -> str:
        """Dynamic field detection using intelligent pattern matching and similarity scoring"""
        
        # Clean the field name to create a base field name
        cleaned_field = self._clean_field_name(field_name)
        
        # Return the cleaned field name - this will be matched against DocType fields later
        return cleaned_field
    
    def _clean_field_name(self, field_name: str) -> str:
        """Convert any field name to a clean, standardized format"""
        # Convert to lowercase and replace spaces, hyphens, dots with underscores
        cleaned = field_name.lower()
        cleaned = re.sub(r'[^a-z0-9]+', '_', cleaned)
        
        # Remove leading/trailing underscores and multiple consecutive underscores
        cleaned = re.sub(r'^_+|_+$', '', cleaned)
        cleaned = re.sub(r'_+', '_', cleaned)
        
        return cleaned

    def _generate_safe_name(self, raw_id: str, doctype: str) -> str:
        """Generate a safe, unique name from raw ID"""
        import uuid
        
        if not raw_id or str(raw_id).strip() == '':
            return f"{doctype}-{str(uuid.uuid4())[:8]}"
        
        clean_id = str(raw_id).strip()
        
        # For long numeric IDs, use a prefix to make them meaningful
        if clean_id.isdigit() and len(clean_id) > 8:
            prefix = {
                'Supplier': 'SUP',
                'Customer': 'CUST',
                'Contact': 'CONT',
                'Lead': 'LEAD',
                'Item': 'ITEM'
            }.get(doctype, 'REC')
            return f"{prefix}-{clean_id[-8:]}"
        
        # For shorter IDs or alphanumeric IDs, use as-is with cleanup
        safe_name = re.sub(r'[^a-zA-Z0-9\-_]', '', clean_id)
        return safe_name[:140]  # Frappe name field limit

    def _find_similar_field(self, target_field: str, available_fields: list) -> str:
        """Find similar field names using intelligent matching with similarity scoring"""
        if target_field in available_fields:
            return target_field
        
        # Calculate similarity scores for all available fields
        best_match = None
        best_score = 0.0
        
        for field in available_fields:
            # Calculate multiple similarity metrics
            score = self._calculate_field_similarity(target_field, field)
            
            if score > best_score and score > 0.6:  # Minimum threshold
                best_score = score
                best_match = field
        
        return best_match
    
    def _calculate_field_similarity(self, field1: str, field2: str) -> float:
        """Calculate similarity score between two field names using multiple metrics"""
        import difflib
        
        # Normalize both fields
        f1_clean = self._clean_field_name(field1)
        f2_clean = self._clean_field_name(field2)
        
        # Exact match gets highest score
        if f1_clean == f2_clean:
            return 1.0
        
        # Calculate different similarity metrics
        scores = []
        
        # 1. Sequence similarity
        seq_similarity = difflib.SequenceMatcher(None, f1_clean, f2_clean).ratio()
        scores.append(seq_similarity)
        
        # 2. Substring matching (bidirectional)
        if f1_clean in f2_clean or f2_clean in f1_clean:
            substring_score = min(len(f1_clean), len(f2_clean)) / max(len(f1_clean), len(f2_clean))
            scores.append(substring_score)
        
        # 3. Word-based similarity (split by underscore)
        f1_words = set(f1_clean.split('_'))
        f2_words = set(f2_clean.split('_'))
        
        if f1_words and f2_words:
            common_words = f1_words.intersection(f2_words)
            total_words = f1_words.union(f2_words)
            word_similarity = len(common_words) / len(total_words) if total_words else 0
            scores.append(word_similarity)
        
        # 4. Semantic similarity for common patterns
        semantic_score = self._get_semantic_similarity(f1_clean, f2_clean)
        if semantic_score > 0:
            scores.append(semantic_score)
        
        # Return the maximum score from all metrics
        return max(scores) if scores else 0.0
    
    def _get_semantic_similarity(self, field1: str, field2: str) -> float:
        """Get semantic similarity for common field patterns"""
        # Define semantic equivalents
        semantic_groups = [
            ['id', 'code', 'number', 'ref', 'reference'],
            ['name', 'title', 'label', 'description'],
            ['date', 'time', 'datetime', 'created', 'modified'],
            ['price', 'cost', 'amount', 'rate', 'value', 'total'],
            ['email', 'mail', 'email_id'],
            ['phone', 'mobile', 'contact', 'tel'],
            ['address', 'location', 'addr'],
            ['quantity', 'qty', 'count', 'num'],
            ['status', 'state', 'condition'],
            ['type', 'category', 'class', 'kind']
        ]
        
        # Check if both fields belong to the same semantic group
        for group in semantic_groups:
            field1_in_group = any(keyword in field1 for keyword in group)
            field2_in_group = any(keyword in field2 for keyword in group)
            
            if field1_in_group and field2_in_group:
                return 0.8  # High semantic similarity
        
        return 0.0

    def _smart_type_conversion(self, raw_value, field_type: str, field_meta) -> Any:
        """Smart type conversion with better error handling"""
        from frappe.utils import getdate, get_datetime
        
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
                return getdate(str_value)
                
            elif field_type == 'Datetime':
                return get_datetime(str_value)
                
            elif field_type in ['Link', 'Dynamic Link']:
                # For Link fields, return cleaned string
                return str_value[:140] if str_value else None
                
            elif field_type in ['Data', 'Small Text', 'Text', 'Long Text']:
                max_length = getattr(field_meta, 'length', 255) if field_meta else 255
                return str_value[:max_length] if str_value else None
                
            else:
                # Default return as string
                return str_value
                
        except Exception as e:
            self.logger.logger.warning(f"Type conversion failed for {raw_value} to {field_type}: {str(e)}")
            # Return raw string as fallback
            return str(raw_value).strip()[:255] if raw_value else None

    def validate_and_clean_data(self, data: Dict[str, Any], meta) -> List[str]:
        """Comprehensive data validation with better error messages"""
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
            # Also log to console for debugging
            if status == "Failed" and error_log:
                self.logger.logger.error(f"âŒ Buffer {buffer_name} failed: {error_log}")
            
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
            self.logger.logger.error(f"âŒ Failed to update buffer status: {str(e)}")

    def get_buffer_statistics(self, target_doctype: str = None) -> Dict[str, Any]:
        """Get comprehensive buffer statistics"""
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
            
            result = {
                "total_records": sum(s['count'] for s in stats),
                "by_status": {},
                "by_doctype": {},
                "processing_summary": stats,
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
            self.logger.logger.error(f"âŒ Failed to get buffer statistics: {str(e)}")
            return {"error": str(e), "total_records": 0}

    def cleanup_processed_buffer(self, days_old: int = 7) -> int:
        """Clean up old processed records with better logging"""
        try:
            cutoff_date = frappe.utils.add_to_date(now(), -days_old)
            
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
                self.logger.logger.info("¹ No old buffer records to clean up")
                return 0
            
            # Delete old records
            delete_query = """
                DELETE FROM `tabMigration Data Buffer`
                WHERE processing_status IN ('Processed', 'Skipped')
                AND processed_at < %s
            """
            
            frappe.db.sql(delete_query, cutoff_date)
            frappe.db.commit()
            
            self.logger.logger.info(f"¹ Cleaned up {records_to_delete} old buffer records (older than {days_old} days)")
            return records_to_delete
            
        except Exception as e:
            self.logger.logger.error(f"âŒ Buffer cleanup failed: {str(e)}")
            return 0
    
    def auto_detect_and_import(self, file_path: str, settings=None) -> Dict[str, Any]:
        """
         MAIN METHOD: Auto-detect existing DocType or create new, then import data
        
        This replaces the manual DocType creation request workflow
        """
        try:
            self.logger.logger.info(f" Starting auto-detection and import for: {Path(file_path).name}")
            
            # Step 1: Read and analyze CSV
            df = self.read_file_as_strings(file_path)
            headers = list(df.columns)
            sample_data = df.head(3).to_dict('records')[0] if not df.empty else {}
            
            self.logger.logger.info(f" CSV Analysis: {len(df)} rows, {len(headers)} columns")
            self.logger.logger.info(f" Headers: {headers}")
            
            # Step 2: Try to find existing DocType with matching headers
            from data_migration_tool.data_migration.mappers.doctype_creator import DynamicDocTypeCreator
            
            creator = DynamicDocTypeCreator(logger=self.logger)
            confidence_threshold = creator.get_dynamic_confidence_threshold(settings)
            
            detection_result = creator.find_existing_doctype_by_headers(
                headers=headers,
                sample_data=sample_data,
                confidence_threshold=confidence_threshold
            )
            
            target_doctype = None
            action_taken = None
            
            # Step 3: Decide on action based on detection results
            if detection_result['doctype'] and not detection_result['should_create_new']:
                # FOUND EXISTING DOCTYPE - USE IT
                target_doctype = detection_result['doctype']
                action_taken = "matched_existing"
                
                self.logger.logger.info(f" Using existing DocType: {target_doctype} (confidence: {detection_result['confidence']:.1%})")
                
                # Ensure hash field exists in the existing DocType
                creator.ensure_hash_field_exists(target_doctype)
                
            else:
                # NO GOOD MATCH - CHECK IF APPROVAL IS REQUIRED
                self.logger.logger.info(f"” No suitable DocType match found (confidence threshold: {confidence_threshold:.1%})")
                
                # Check if auto-creation is enabled or if approval is required
                require_approval = getattr(settings, 'require_user_permission_for_doctype_creation', True)
                
                if require_approval:
                    # CREATE APPROVAL REQUEST FOR NEW DOCTYPE
                    self.logger.logger.info("“‹ Creating approval request for new DocType creation")
                    
                    approval_result = self._create_doctype_creation_approval_request(
                        file_path=file_path,
                        headers=headers,
                        detection_result=detection_result,
                        suggested_name=creator.clean_doctype_name(Path(file_path).stem),
                        csv_analysis={'rows': len(df), 'columns': len(headers)}
                    )
                    
                    # Return early with approval request info
                    return {
                        'success': True,
                        'action_taken': 'approval_requested',
                        'approval_request_id': approval_result['request_id'],
                        'message': f"DocType creation approval request created: {approval_result['request_id']}",
                        'detection_details': detection_result,
                        'requires_manual_approval': True,
                        'csv_analysis': {
                            'total_rows': len(df),
                            'total_columns': len(headers),
                            'headers': headers,
                            'file_name': Path(file_path).name
                        }
                    }
                else:
                    # AUTO-CREATE NEW DOCTYPE WITHOUT APPROVAL
                    self.logger.logger.info(f"†• Auto-creating new DocType (approval not required)")
                    
                    # Analyze CSV structure for new DocType creation
                    analysis = creator.analyze_csv_structure(df)
                    
                    # Generate DocType name from filename
                    suggested_name = creator.clean_doctype_name(Path(file_path).stem)
                    
                    # Create new DocType
                    target_doctype = creator.create_doctype_from_analysis(analysis, suggested_name)
                    action_taken = "created_new"
                    
                    self.logger.logger.info(f" Auto-created new DocType: {target_doctype}")
            
            # Step 4: Import data into the target DocType
            self.logger.logger.info(f" Starting data import into {target_doctype}")
            
            # Store raw data in buffer and process with intelligent upsert
            stored_count = self.store_raw_data_with_mapping(
                df=df,
                source_file=Path(file_path).name,
                target_doctype=target_doctype,
                field_mappings=None  # Auto-mapping handled by intelligent conversion
            )
            
            # Get final results
            final_results = self.process_buffered_data_with_upsert(target_doctype)
            
            # Prepare comprehensive response
            response = {
                'success': True,
                'target_doctype': target_doctype,
                'action_taken': action_taken,
                'detection_details': detection_result,
                'confidence_threshold': confidence_threshold,
                'csv_analysis': {
                    'total_rows': len(df),
                    'total_columns': len(headers),
                    'headers': headers,
                    'file_name': Path(file_path).name
                },
                'import_results': {
                    'stored_in_buffer': stored_count,
                    'processing_results': final_results
                },
                'recommendations': self._generate_import_recommendations(detection_result, final_results)
            }

            self.logger.logger.info(f" AUTO-IMPORT COMPLETED: {response['import_results']['processing_results']}")

            return response
            
        except Exception as e:
            error_msg = f"Auto-detection and import failed: {str(e)}"
            self.logger.logger.error(f"âŒ {error_msg}")
            
            return {
                'success': False,
                'error': error_msg,
                'target_doctype': None,
                'action_taken': 'failed',
                'detection_details': {},
                'import_results': {}
            }

    def _generate_import_recommendations(self, detection_result: Dict, import_results: Dict) -> List[str]:
        """Generate recommendations based on import results"""
        recommendations = []
        
        if detection_result.get('should_create_new'):
            recommendations.append("âœ¨ New DocType was created - consider reviewing field types and adding validations")
        
        if detection_result.get('match_details', {}).get('missing_in_doctype'):
            missing_fields = detection_result['match_details']['missing_in_doctype']
            recommendations.append(f"“‹ Consider adding these fields to DocType: {missing_fields}")
        
        success_rate = 0
        if import_results:
            total = sum([import_results.get(k, 0) for k in ['success', 'updated', 'skipped', 'failed']])
            successful = import_results.get('success', 0) + import_results.get('updated', 0) + import_results.get('skipped', 0)
            success_rate = (successful / total * 100) if total > 0 else 0
        
        if success_rate < 90:
            recommendations.append("âš ï¸ Consider reviewing failed records and adjusting field mappings")
        
        if import_results.get('updated', 0) > 0:
            recommendations.append("”„ Some existing records were updated - review changes if needed")
        
        return recommendations

    def _create_doctype_creation_approval_request(self, file_path: str, headers: list, detection_result: dict, suggested_name: str, csv_analysis: dict) -> dict:
        """
        Create an approval request for new DocType creation when no suitable match is found
        """
        try:
            import uuid
            from pathlib import Path
            
            # Generate unique request ID
            request_id = str(uuid.uuid4())[:10]
            
            # Prepare detailed information for approval
            approval_data = {
                'request_id': request_id,
                'file_path': file_path,
                'file_name': Path(file_path).name,
                'suggested_doctype_name': suggested_name,
                'csv_headers': headers,
                'csv_analysis': csv_analysis,
                'detection_details': {
                    'best_match': detection_result.get('doctype'),
                    'confidence': detection_result.get('confidence', 0),
                    'threshold_required': detection_result.get('confidence_threshold', 0.7),
                    'reason_for_rejection': f"Best match confidence {detection_result.get('confidence', 0):.1%} is below required threshold {detection_result.get('confidence_threshold', 0.8):.1%}"
                },
                'requested_at': frappe.utils.now(),
                'requested_by': frappe.session.user,
                'status': 'Pending Approval',
                'approval_type': 'doctype_creation'
            }
            
            # Create DocType Creation Request document
            request_doc = frappe.get_doc({
                'doctype': 'DocType Creation Request',
                'request_id': request_id,
                'csv_file_path': file_path,
                'csv_file_name': Path(file_path).name,
                'suggested_doctype_name': suggested_name,
                'csv_headers': '\n'.join(headers),
                'total_rows': csv_analysis.get('rows', 0),
                'total_columns': csv_analysis.get('columns', 0),
                'detection_confidence': detection_result.get('confidence', 0),
                'confidence_threshold': detection_result.get('confidence_threshold', 0.8),
                'best_existing_match': detection_result.get('doctype', 'None'),
                'rejection_reason': f"No suitable DocType match found. Best match: {detection_result.get('doctype', 'None')} (confidence: {detection_result.get('confidence', 0):.1%})",
                'request_data': frappe.as_json(approval_data),
                'status': 'Pending Approval',
                'requested_by': frappe.session.user
            })
            
            request_doc.insert(ignore_permissions=True)
            frappe.db.commit()
            
            self.logger.logger.info(f"“ Created DocType creation approval request: {request_id}")
            
            # Send notifications to system managers
            self._send_doctype_approval_notifications(request_id, suggested_name, Path(file_path).name)
            
            return {
                'success': True,
                'request_id': request_id,
                'message': f"DocType creation approval request created successfully"
            }
            
        except Exception as e:
            self.logger.logger.error(f"âŒ Failed to create approval request: {str(e)}")
            raise e
    
    def _send_doctype_approval_notifications(self, request_id: str, suggested_name: str, file_name: str):
        """  Send notifications to system managers about DocType creation approval request  """
        try:
            # Get system managers
            system_managers = frappe.get_all("User", 
                filters={"role_profile_name": ["like", "%System Manager%"], "enabled": 1},
                fields=["email", "name"]
            )
            
            if not system_managers:
                system_managers = frappe.get_all("Has Role",
                    filters={"role": "System Manager", "parenttype": "User"},
                    fields=["parent as name"]
                )
                system_managers = frappe.get_all("User",
                    filters={"name": ["in", [sm.name for sm in system_managers]], "enabled": 1},
                    fields=["email", "name"]
                )
            
            self.logger.logger.info(f" Found system managers: {[sm.name for sm in system_managers]}")
            
            # Send notifications
            for manager in system_managers:
                try:
                    notification_doc = frappe.get_doc({
                        "doctype": "Notification Log",
                        "for_user": manager.name,
                        "type": "Alert",
                        "document_type": "DocType Creation Request",
                        "document_name": request_id,
                        "subject": f"New DocType Creation Approval Required",
                        "email_content": f"A new DocType creation request requires your approval:\n\n" +
                                       f"Request ID: {request_id}\n" +
                                       f"CSV File: {file_name}\n" +
                                       f"Suggested DocType Name: {suggested_name}\n" +
                                       f"Reason: No suitable existing DocType match found\n\n" +
                                       f"Please review and approve/reject this request in the DocType Creation Request list."
                    })
                    
                    notification_doc.insert(ignore_permissions=True)
                    self.logger.logger.info(f" Sent approval notification to {manager.name}")
                    
                except Exception as e:
                    self.logger.logger.error(f"Failed to send notification to {manager.name}: {str(e)}")
            
            # Send real-time notifications
            frappe.publish_realtime("doctype_creation_approval_request", {
                "request_id": request_id,
                "suggested_name": suggested_name,
                "file_name": file_name,
                "message": f"DocType creation approval required for: {suggested_name}"
            }, user="System Manager")
            
            self.logger.logger.info(" Sent real-time notifications for DocType creation approval")
            
            frappe.db.commit()
            
        except Exception as e:
            self.logger.logger.error(f"Failed to send approval notifications: {str(e)}")