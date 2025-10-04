# Scheduler tasks for data migration - Enhanced Phase 1 Version with User Approval Fix
import os
import shutil
import frappe
from frappe.utils import now, add_to_date, get_datetime
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
import hashlib
import json
from datetime import datetime
import numpy as np
import pandas as pd
import re

from data_migration_tool.data_migration_tool.doctype.csv_schema_registry.csv_schema_registry import get_field_mappings_from_registry

# Add after existing imports:
try:
    from data_migration_tool.data_migration.connectors.csv_connector import CSVConnector
    CSV_CONNECTOR_AVAILABLE = True
except ImportError as e:
    frappe.log_error(f"CSVConnector import failed: {str(e)}")
    CSV_CONNECTOR_AVAILABLE = False


# Add this to scheduler_tasks.py after existing imports
class IntelligentSchemaDetector:
    """AI-powered schema detection and field mapping"""
    
    def __init__(self):
        self.field_patterns = self._load_learned_patterns()
        self.confidence_threshold = 0.75
        
    def _load_learned_patterns(self):
        """Load learned field patterns from database"""
        try:
            patterns = frappe.get_all("CSV Schema Registry", 
                fields=["headers_json", "target_doctype", "schema_fingerprint"])
            return {p.schema_fingerprint: p for p in patterns}
        except:
            return {}
    
    def analyze_csv_structure_advanced(self, df: pd.DataFrame, filename: str) -> Dict[str, Any]:
        """Enhanced analysis with pattern recognition"""
        column_profiles = {}
        
        for col in df.columns:
            sample_values = df[col].dropna().astype(str).head(100).tolist()
            
            column_profiles[col] = {
                'original_name': col,
                'clean_name': self._clean_field_name(col),
                'suggested_type': self._detect_data_type_advanced(df[col]),
                'sample_values': sample_values[:5],
                'null_count': df[col].isna().sum(),
                'unique_count': df[col].nunique(),
                'max_length': max([len(str(v)) for v in sample_values]) if sample_values else 0,
                'business_context': self._detect_business_context(col, sample_values)
            }
        
        # Predict DocType using ensemble methods
        doctype_prediction = self._predict_doctype_ensemble(column_profiles, filename)
        
        return {
            'predicted_doctype': doctype_prediction['name'],
            'confidence': doctype_prediction['confidence'],
            'column_profiles': column_profiles,
            'requires_user_confirmation': doctype_prediction['confidence'] < self.confidence_threshold,
            'field_mappings': self._generate_intelligent_mappings(column_profiles, doctype_prediction['name'])
        }
    
    def _detect_data_type_advanced(self, series: pd.Series) -> Dict[str, Any]:
        """Advanced data type detection with confidence scoring"""
        sample_values = series.dropna().astype(str).head(50).tolist()
        type_scores = {}
        
        # Currency patterns
        currency_patterns = [
            r'^\$?[\d,]+\.?\d{0,2}$',
            r'^INR\s*[\d,]+\.?\d{0,2}$',
            r'^[\d,]+\.?\d{0,2}\s*(USD|EUR|GBP|INR)$'
        ]
        
        # ID patterns
        id_patterns = [
            r'^[A-Z]{2,4}-\d{4,}$',
            r'^\d{8,}$',
            r'^[A-Z]+\d+$'
        ]
        
        # Date patterns
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',
            r'\d{2}/\d{2}/\d{4}',
            r'\d{2}-\d{2}-\d{4}'
        ]
        
        # Email pattern
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        # Phone pattern
        phone_pattern = r'^[\+]?[1-9][\d\s\-\(\)]{7,15}$'
        
        # Score each type
        for pattern_type, patterns in [
            ('Currency', currency_patterns),
            ('ID', id_patterns),
            ('Date', date_patterns),
            ('Email', [email_pattern]),
            ('Phone', [phone_pattern])
        ]:
            if isinstance(patterns, list):
                score = max([self._pattern_match_score(sample_values, pattern) for pattern in patterns])
            else:
                score = self._pattern_match_score(sample_values, patterns)
            type_scores[pattern_type] = score
        
        # Numeric check
        numeric_count = 0
        for val in sample_values:
            try:
                float(val.replace(',', ''))
                numeric_count += 1
            except:
                pass
        
        type_scores['Float'] = numeric_count / len(sample_values) if sample_values else 0
        type_scores['Text'] = 0.3  # Default fallback
        
        best_type = max(type_scores, key=type_scores.get)
        frappe_type = self._map_to_frappe_fieldtype(best_type)
        
        return {
            'suggested_type': frappe_type,
            'confidence': type_scores[best_type],
            'alternatives': {k: v for k, v in type_scores.items() if k != best_type}
        }
    
    def _pattern_match_score(self, values: List[str], pattern: str) -> float:
        """Calculate pattern match score"""
        if not values:
            return 0.0
        
        matches = sum(1 for val in values if re.match(pattern, str(val).strip()))
        return matches / len(values)
    
    def _map_to_frappe_fieldtype(self, detected_type: str) -> str:
        """Map detected type to Frappe field type"""
        mapping = {
            'Currency': 'Currency',
            'Date': 'Date',
            'Email': 'Data',
            'Phone': 'Phone',
            'ID': 'Data',
            'Float': 'Float',
            'Text': 'Data'
        }
        return mapping.get(detected_type, 'Data')
    
    def _clean_field_name(self, name: str) -> str:
        """Clean field name for Frappe compatibility"""
        clean = re.sub(r'[^a-zA-Z0-9_]', '_', name.lower())
        clean = re.sub(r'_+', '_', clean).strip('_')
        return clean[:140]  # Frappe field name limit
    
    def _detect_business_context(self, field_name: str, sample_values: List[str]) -> str:
        """Detect business context of field"""
        field_lower = field_name.lower()
        
        if any(word in field_lower for word in ['customer', 'client', 'buyer']):
            return 'Customer'
        elif any(word in field_lower for word in ['vendor', 'supplier', 'seller']):
            return 'Supplier'
        elif any(word in field_lower for word in ['product', 'item', 'inventory']):
            return 'Item'
        elif any(word in field_lower for word in ['address', 'location', 'city', 'state']):
            return 'Address'
        elif any(word in field_lower for word in ['invoice', 'bill', 'payment']):
            return 'Accounting'
        
        return 'General'
    
    def _predict_doctype_ensemble(self, column_profiles: Dict, filename: str) -> Dict[str, Any]:
        """Predict DocType using multiple strategies"""
        predictions = {}
        
        # Strategy 1: Filename analysis
        filename_lower = filename.lower()
        if 'customer' in filename_lower:
            predictions['filename'] = ('Customer', 0.8)
        elif 'supplier' in filename_lower or 'vendor' in filename_lower:
            predictions['filename'] = ('Supplier', 0.8)
        elif 'item' in filename_lower or 'product' in filename_lower:
            predictions['filename'] = ('Item', 0.8)
        elif 'invoice' in filename_lower:
            predictions['filename'] = ('Sales Invoice', 0.8)
        elif 'payment' in filename_lower:
            predictions['filename'] = ('Payment Entry', 0.8)
        
        # Strategy 2: Field pattern analysis
        field_contexts = [profile['business_context'] for profile in column_profiles.values()]
        context_counts = {}
        for context in field_contexts:
            context_counts[context] = context_counts.get(context, 0) + 1
        
        if context_counts:
            dominant_context = max(context_counts, key=context_counts.get)
            confidence = context_counts[dominant_context] / len(field_contexts)
            predictions['fields'] = (dominant_context, confidence)
        
        # Combine predictions
        if predictions:
            best_prediction = max(predictions.values(), key=lambda x: x[1])
            return {'name': best_prediction[0], 'confidence': best_prediction[1]}
        
        # Fallback: use cleaned filename
        return {'name': clean_doctype_name(filename), 'confidence': 0.5}
    
    def _generate_intelligent_mappings(self, column_profiles: Dict, target_doctype: str) -> Dict[str, str]:
        """Generate intelligent field mappings"""
        mappings = {}
        
        # Get target DocType fields if it exists
        target_fields = []
        if frappe.db.exists('DocType', target_doctype):
            target_fields = frappe.get_meta(target_doctype).fields
        
        for source_field, profile in column_profiles.items():
            clean_name = profile['clean_name']
            business_context = profile['business_context']
            
            # Try exact match first
            exact_matches = [f for f in target_fields if f.fieldname == clean_name]
            if exact_matches:
                mappings[source_field] = clean_name
                continue
            
            # Try semantic matching based on business context
            semantic_mapping = self._get_semantic_mapping(source_field, business_context, target_doctype)
            if semantic_mapping:
                mappings[source_field] = semantic_mapping
            else:
                mappings[source_field] = clean_name
        
        return mappings
    
    def _get_semantic_mapping(self, source_field: str, context: str, target_doctype: str) -> str:
        """Get semantic field mapping"""
        field_lower = source_field.lower()
        
        # Common mappings
        common_mappings = {
            'customer': 'customer_name',
            'supplier': 'supplier_name',
            'vendor': 'supplier_name',
            'item': 'item_name',
            'product': 'item_name',
            'email': 'email_id',
            'phone': 'mobile_no',
            'mobile': 'mobile_no',
            'address': 'address_line1',
            'city': 'city',
            'state': 'state',
            'country': 'country',
            'total': 'grand_total',
            'amount': 'amount',
            'quantity': 'qty',
            'rate': 'rate',
            'date': 'date'
        }
        
        for key, value in common_mappings.items():
            if key in field_lower:
                return value
        
        return None

# Modify the existing process_csv_files_with_jit function
# def process_csv_files_with_jit():
#     """ENHANCED CSV processing with intelligent schema detection"""
#     from data_migration_tool.data_migration.connectors.csv_connector import CSVConnector
#     from data_migration_tool.data_migration.mappers.doctype_creator import DynamicDocTypeCreator
#     from data_migration_tool.data_migration.utils.logger_config import migration_logger
    
#     try:
#         # Set proper user context
#         try:
#             system_managers = frappe.db.sql("""
#                 SELECT DISTINCT u.name FROM `tabUser` u
#                 INNER JOIN `tabHas Role` hr ON u.name = hr.parent
#                 WHERE hr.role = 'System Manager' AND u.enabled = 1 
#                 AND u.name != 'Guest' LIMIT 1
#             """, as_dict=True)
            
#             if system_managers:
#                 frappe.set_user(system_managers[0].name)
#             else:
#                 frappe.set_user('Administrator')
#         except Exception:
#             frappe.set_user('Administrator')
        
#         migration_logger.logger.info("Starting INTELLIGENT CSV processing")
        
#         settings = frappe.get_single('Migration Settings')
#         if not settings.enable_csv_processing or not settings.csv_watch_directory:
#             return
        
#         # Initialize enhanced components
#         csv_connector = CSVConnector(migration_logger)
#         mapper = DynamicDocTypeCreator(migration_logger)
#         intelligent_detector = IntelligentSchemaDetector()
        
#         watch_dir = settings.csv_watch_directory
#         processed_dir = os.path.join(watch_dir, 'processed')
#         error_dir = os.path.join(watch_dir, 'errors')
#         pending_dir = os.path.join(watch_dir, 'pending')
        
#         # Ensure directories exist
#         for directory in [processed_dir, error_dir, pending_dir]:
#             os.makedirs(directory, exist_ok=True)
        
#         # Get processable files
#         processable_files = []
#         for filename in os.listdir(watch_dir):
#             if filename.startswith('.') or not os.path.isfile(os.path.join(watch_dir, filename)):
#                 continue
#             if Path(filename).suffix.lower() in csv_connector.supported_formats:
#                 processable_files.append((filename, os.path.join(watch_dir, filename)))
        
#         if not processable_files:
#             migration_logger.logger.info("No CSV files found to process")
#             return
        
#         migration_logger.logger.info(f"Found {len(processable_files)} files for INTELLIGENT processing")
        
#         processed_count = 0
#         error_count = 0
#         pending_count = 0
        
#         for filename, filepath in processable_files:
#             try:
#                 migration_logger.logger.info(f"Intelligently processing file: {filename}")
                
#                 # Step 1: Read and analyze CSV with intelligence
#                 df = csv_connector.read_file_as_strings(filepath)
#                 if df.empty:
#                     migration_logger.logger.warning(f"Empty file: {filename}")
#                     error_count += 1
#                     continue
                
#                 # Step 2: ENHANCED - Intelligent schema analysis
#                 analysis = intelligent_detector.analyze_csv_structure_advanced(df, filename)
#                 migration_logger.logger.info(f"Intelligent analysis complete for {filename}: {analysis['predicted_doctype']} (confidence: {analysis['confidence']})")
                
#                 # Step 3: Check for existing schema match
#                 existing_doctype, registry_id = find_existing_doctype_by_schema(
#                     list(df.columns), 
#                     get_data_sample_from_df(df)
#                 )
                
#                 if existing_doctype:
#                     migration_logger.logger.info(f"Found matching schema! Using existing DocType: {existing_doctype}")
#                     target_doctype = existing_doctype
#                 elif analysis['confidence'] >= intelligent_detector.confidence_threshold:
#                     # High confidence - proceed with intelligent creation
#                     target_doctype = analysis['predicted_doctype']
                    
#                     if not frappe.db.exists('DocType', target_doctype):
#                         # Create DocType with intelligent field mapping
#                         created_doctype = mapper.create_doctype_from_analysis(
#                             analysis, 
#                             target_doctype
#                         )
#                         migration_logger.logger.info(f"Intelligently created DocType: {created_doctype}")
                        
#                         # Register schema for future recognition
#                         register_csv_schema(
#                             filename, 
#                             list(df.columns), 
#                             created_doctype, 
#                             get_data_sample_from_df(df)
#                         )
#                 else:
#                     # Low confidence - require approval
#                     migration_logger.logger.info(f"Low confidence ({analysis['confidence']}) - requesting approval")
                    
#                     existing_request = frappe.db.exists('DocType Creation Request', {
#                         'source_file': filename,
#                         'status': ['in', ['Pending', 'Approved', 'Redirected']]
#                     })
                    
#                     if not existing_request:
#                         request_id = send_doctype_creation_request(
#                             filename, 
#                             analysis['predicted_doctype'], 
#                             analysis
#                         )
#                         migration_logger.logger.info(f"Sent intelligent approval request: {request_id}")
                    
#                     # Move to pending
#                     pending_path = os.path.join(pending_dir, filename)
#                     shutil.move(filepath, pending_path)
#                     pending_count += 1
#                     continue
                
#                 # Step 4: Process data with intelligent transformations
#                 migration_logger.logger.info(f"Processing data for {filename} -> {target_doctype}")
                
#                 # Store raw data with intelligent field mapping
#                 stored_count = csv_connector.store_raw_data_with_mapping(
#                     df, 
#                     filename, 
#                     target_doctype,
#                     analysis.get('field_mappings', {})
#                 )
#                 migration_logger.logger.info(f"Stored {stored_count} records with intelligent mapping")
                
#                 # Process with enhanced merging logic
#                 total_results = process_data_with_intelligent_merge(
#                     csv_connector, 
#                     target_doctype, 
#                     df, 
#                     settings, 
#                     migration_logger,
#                     analysis.get('field_mappings', {})
#                 )
                
#                 if total_results['success'] > 0 or total_results['updated'] > 0:
#                     processed_path = os.path.join(processed_dir, filename)
#                     shutil.move(filepath, processed_path)
#                     migration_logger.logger.info(f"Successfully processed {filename}: {total_results}")
#                     processed_count += 1
#                 else:
#                     error_path = os.path.join(error_dir, filename)
#                     shutil.move(filepath, error_path)
#                     migration_logger.logger.warning(f"No successful operations for {filename}")
#                     error_count += 1
                    
#             except Exception as e:
#                 migration_logger.logger.error(f"Failed to process {filename}: {str(e)}")
#                 error_path = os.path.join(error_dir, filename)
#                 try:
#                     shutil.move(filepath, error_path)
#                 except:
#                     pass
#                 error_count += 1
        
#         migration_logger.logger.info(f"INTELLIGENT processing completed - Processed: {processed_count}, Errors: {error_count}, Pending: {pending_count}")
        
#     except Exception as e:
#         migration_logger.logger.error(f"Enhanced CSV processing failed: {str(e)}")

# Add this new function for intelligent data processing
def process_data_with_intelligent_merge(csv_connector, target_doctype, df, settings, migration_logger, field_mappings):
    """Process data with intelligent insert/update logic and handle empty fields"""
    
    batch_size = int(getattr(settings, 'csv_chunk_size', 1000))
    total_results = {'success': 0, 'failed': 0, 'skipped': 0, 'updated': 0}
    
    # Get existing records for merge detection
    try:
        doctype_meta = frappe.get_meta(target_doctype)
        unique_fields = []
        for field in doctype_meta.fields:
            if field.unique or field.fieldname in ['name', 'id', 'code', 'email']:
                unique_fields.append(field.fieldname)
        
        existing_records = {}
        if unique_fields:
            identifier_field = unique_fields[0]
            migration_logger.logger.info(f"Using {identifier_field} as unique identifier for merge operations")
            
            # Get existing records
            existing_values = frappe.db.sql(f"""
                SELECT name, {identifier_field} 
                FROM `tab{target_doctype}` 
                WHERE {identifier_field} IS NOT NULL AND {identifier_field} != ''
            """)
            
            for record in existing_values:
                if len(record) >= 2 and record[1]:
                    existing_records[str(record[1]).strip().lower()] = record[0]
                    
    except Exception as e:
        migration_logger.logger.warning(f"Could not determine merge strategy: {str(e)} - will insert only")
        existing_records = {}
    
    # Process in batches
    batch_count = 0
    max_batches = 100
    
    while batch_count < max_batches:
        batch_count += 1
        
        # Process batch with intelligent merging
        if existing_records:
            batch_results = csv_connector.process_buffered_data_with_intelligent_merge(
                target_doctype, 
                batch_size, 
                existing_records,
                field_mappings
            )
        else:
            batch_results = csv_connector.process_buffered_data_with_upsert(
                target_doctype, 
                batch_size,
                field_mappings
            )
        
        # Aggregate results
        for key in total_results:
            if key in batch_results:
                total_results[key] += batch_results[key]
        
        migration_logger.logger.info(f"Batch {batch_count} results: {batch_results}")
        
        # Stop if no more data to process
        if sum(batch_results.values()) == 0:
            break
    
    return total_results


def convert_numpy_types(obj):
    """Recursively convert numpy types to Python native types for JSON serialization."""
    if isinstance(obj, dict):
        return {k: convert_numpy_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(i) for i in obj]
    elif isinstance(obj, (np.integer, np.int64)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64)):
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    else:
        return obj

def periodic_crm_sync():
    """Main scheduled function for CRM synchronization with JIT processing"""
    from data_migration_tool.data_migration.utils.logger_config import migration_logger
    
    if not CSV_CONNECTOR_AVAILABLE:
        migration_logger.logger.error("❌ CSVConnector not available - skipping CSV processing") 
        return
    
    try:
        migration_logger.logger.info("🚀 Starting periodic CRM sync with JIT processing")
        
        # Get Migration Settings
        settings = frappe.get_single('Migration Settings')
        
        # Process CSV files with JIT if enabled
        if getattr(settings, 'enable_csv_processing', True):
            process_csv_files_with_jit()
        
        # Process integrations with safe checks
        if getattr(settings, 'enable_zoho_sync', False):
            sync_zoho_data(settings)
        
        if getattr(settings, 'enable_odoo_sync', False):
            sync_odoo_data(settings)
        
        # Update last sync time
        try:
            if hasattr(settings, 'last_sync_time'):
                settings.reload()  # FIXED: Reload before save
                settings.last_sync_time = now()
                settings.save()
                frappe.db.commit()
                migration_logger.logger.info(f"✅ Sync completed at {settings.last_sync_time}")
        except Exception as save_conflict:
             error_msg = str(save_conflict).lower()
             if 'document modified' in error_msg or 'conflict' in error_msg:
                migration_logger.logger.warning("⚠️ Settings save conflict - sync completed but timestamp not updated")
             else:
                migration_logger.logger.warning(f"⚠️ Settings save failed: {str(save_conflict)}")
 
        except Exception as save_error:
            migration_logger.logger.warning(f"⚠️ Settings save failed: {str(save_error)}")
        
        migration_logger.logger.info("🎉 Scheduled JIT sync completed successfully")
        
    except Exception as e:
        migration_logger.logger.error(f"❌ Periodic CRM sync failed: {str(e)}")
        try:
            send_sync_failure_notification(str(e))
        except:
            pass

def clean_doctype_name(filename: str) -> str:
    """
    FIXED: Clean filename to create VALID Frappe DocType name WITH SPACES
    
    Frappe DocType Rules:
    - Must have Title Case WITH spaces (e.g., "Yawlit Customers")
    - Max 61 characters
    - Only alphanumeric and spaces
    - Cannot start with number
    
    Examples:
    - "Yawlit Customers.csv" → "Yawlit Customers"
    - "customers_updted.csv" → "Customers Updted"
    - "test_products.csv" → "Test Products"  
    - "customer_data.csv" → "Customer Data"
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
    
    # ✅ CRITICAL FIX: Remove "updated" and similar suffixes that cause confusion
    clean_name = re.sub(r'\s+(updated?|updted|new|final|latest|copy)\s*$', '', clean_name, flags=re.IGNORECASE)
    
    # ✅ CRITICAL: DO NOT REMOVE SPACES!
    # The old line: clean_name = clean_name.replace(' ', '')  # ❌ THIS WAS THE BUG!
    
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
            frappe.logger().info(f"✅ Mapped '{filename}' to existing DocType: '{mapped_doctype}'")
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
    
    frappe.logger().info(f"📝 Cleaned DocType name: '{filename}' → '{clean_name}'")
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

def process_csv_files_with_jit():
    """CONFIGURABLE CSV processing with intelligent DocType detection
    
    Behavior controlled by Migration Settings.auto_create_doctypes:
    
    When auto_create_doctypes = True (AUTOMATED):
    1. Read CSV headers and analyze structure
    2. Auto-detect existing DocType with matching headers
    3. If match found (confidence >= threshold): Import directly into existing DocType
    4. If no match: Create new DocType automatically and import
    5. Only create manual approval request if auto-detection fails
    
    When auto_create_doctypes = False (MANUAL):
    1. Read CSV headers and analyze structure  
    2. Always create DocType Creation Request for manual approval
    3. Wait for user approval before proceeding
    
    This provides flexibility between full automation and manual control.
    """
    from data_migration_tool.data_migration.connectors.csv_connector import CSVConnector
    from data_migration_tool.data_migration.mappers.doctype_creator import DynamicDocTypeCreator
    from data_migration_tool.data_migration.utils.logger_config import migration_logger
    
    try:
        # Set proper user context
        # Use safe user context management
        try:
            from data_migration_tool.data_migration.utils.user_context import UserContextManager
            UserContextManager.set_migration_user()
        except Exception as e:
            frappe.log_error(f"Failed to set migration user context: {str(e)}")
            frappe.set_user('Administrator')

        migration_logger.logger.info("🚀 Starting INTELLIGENT CSV processing with schema detection")
        
        settings = frappe.get_single('Migration Settings')
        if not settings.enable_csv_processing or not settings.csv_watch_directory:
            return

        # Initialize components
        csv_connector = CSVConnector(migration_logger)
        mapper = DynamicDocTypeCreator(migration_logger)
        
        watch_dir = settings.csv_watch_directory
        processed_dir = os.path.join(watch_dir, 'processed')
        error_dir = os.path.join(watch_dir, 'errors')
        pending_dir = os.path.join(watch_dir, 'pending')
        
        # Ensure directories exist
        for directory in [processed_dir, error_dir, pending_dir]:
            os.makedirs(directory, exist_ok=True)

        # Get processable files
        processable_files = []
        for filename in os.listdir(watch_dir):
            if filename.startswith('.') or not os.path.isfile(os.path.join(watch_dir, filename)):
                continue
            if Path(filename).suffix.lower() in csv_connector.supported_formats:
                processable_files.append((filename, os.path.join(watch_dir, filename)))

        if not processable_files:
            migration_logger.logger.info("No CSV files found to process")
            return

        migration_logger.logger.info(f"📁 Found {len(processable_files)} files for intelligent processing")
        
        processed_count = 0
        error_count = 0
        pending_count = 0
        approval_needed_count = 0
        schema_matched_count = 0
        pending_count = 0
        schema_matched_count = 0

        for filename, filepath in processable_files:
            try:
                migration_logger.logger.info(f"🔍 Analyzing file: {filename}")
                
                # Step 1: Read and analyze CSV structure
                df = csv_connector.read_file_as_strings(filepath)
                if df.empty:
                    migration_logger.logger.warning(f"⚠️ Empty file: {filename}")
                    error_count += 1
                    continue

                headers = list(df.columns)
                data_sample = get_data_sample_from_df(df)
                
                # Step 2: Check if auto-creation is enabled, otherwise use manual request workflow
                auto_create_enabled = getattr(settings, 'auto_create_doctypes', True)
                
                if auto_create_enabled:
                    # Use auto-detection and import workflow
                    migration_logger.logger.info(f"🤖 Starting automated DocType detection and import for: {filename}")
                    
                    # Use the auto_detect_and_import method that handles both cases:
                    # 1. Find existing DocType with matching headers -> import directly
                    # 2. No match found -> create new DocType and import
                    try:
                        result = csv_connector.auto_detect_and_import(filepath, settings)
                        
                        if result['success']:
                            target_doctype = result['target_doctype']
                            action_taken = result['action_taken']
                            import_results = result['import_results']['processing_results']
                            
                            # Log the action taken
                            if action_taken == "matched_existing":
                                migration_logger.logger.info(f"✅ Used existing DocType: {target_doctype} (confidence: {result['detection_details']['confidence']:.1%})")
                                schema_matched_count += 1
                            elif action_taken == "created_new":
                                migration_logger.logger.info(f"🆕 Created new DocType: {target_doctype}")
                                # Register this schema for future use
                                try:
                                    register_csv_schema(
                                        filename,
                                        headers,
                                        target_doctype,
                                        data_sample,
                                        result['detection_details'].get('confidence', 1.0)
                                    )
                                except Exception as reg_error:
                                    migration_logger.logger.warning(f"⚠️ Could not register schema: {str(reg_error)}")
                            elif action_taken == "approval_requested":
                                migration_logger.logger.info(f"📝 DocType creation approval requested: {result['approval_request_id']}")
                                migration_logger.logger.info(f"⏳ CSV will be processed after approval")
                                
                                # Move to pending approval directory (or keep in pending)
                                migration_logger.logger.info(f"📂 File kept in pending for approval: {filename}")
                                approval_needed_count += 1
                                continue
                            
                            # Move to processed
                            processed_path = os.path.join(processed_dir, filename)
                            shutil.move(filepath, processed_path)
                            
                            migration_logger.logger.info(f"🎉 Auto-import completed: {import_results}")
                            processed_count += 1
                            
                            # Log recommendations if any
                            if result.get('recommendations'):
                                for rec in result['recommendations']:
                                    migration_logger.logger.info(f"💡 Recommendation: {rec}")
                            
                            continue
                            
                        else:
                            # Auto-import failed - fall back to manual request
                            error_msg = result.get('error', 'Unknown error in auto-import')
                            migration_logger.logger.warning(f"⚠️ Auto-import failed for {filename}: {error_msg}")
                            migration_logger.logger.info(f"📝 Falling back to manual DocType creation request")
                            
                            # Fall through to manual request creation
                            
                    except Exception as auto_error:
                        migration_logger.logger.error(f"❌ Auto-detection failed for {filename}: {str(auto_error)}")
                        migration_logger.logger.info(f"📝 Falling back to manual DocType creation request")
                        # Fall through to manual request creation
                
                # Manual DocType creation request workflow (when auto_create_doctypes=False or as fallback)
                migration_logger.logger.info(f"📝 Creating manual DocType creation request for: {filename}")
                
                # Analyze CSV structure for manual request
                analysis = mapper.analyze_csv_structure(df)
                suggested_doctype = clean_doctype_name(filename)
                
                # Check if we already have a pending request for this file
                existing_request = frappe.db.get_value(
                    'DocType Creation Request',
                    {'source_file': filename, 'status': ['in', ['Pending', 'Approved', 'Redirected']]},
                    'name'
                )
                
                if existing_request:
                    migration_logger.logger.info(f"⏳ Request already exists for {filename}: {existing_request}")
                    # Move to pending if not already there
                    pending_path = os.path.join(pending_dir, filename)
                    if not os.path.exists(pending_path):
                        shutil.move(filepath, pending_path)
                    pending_count += 1
                    continue
                
                # Create new approval request
                request_id = send_doctype_creation_request(filename, suggested_doctype, analysis)
                migration_logger.logger.info(f"📝 Created manual approval request: {request_id}")
                
                # Move to pending
                pending_path = os.path.join(pending_dir, filename)
                shutil.move(filepath, pending_path)
                pending_count += 1

            except Exception as e:
                migration_logger.logger.error(f"❌ Failed to process {filename}: {str(e)}")
                error_path = os.path.join(error_dir, filename)
                try:
                    shutil.move(filepath, error_path)
                except:
                    pass
                error_count += 1

        migration_logger.logger.info(f"""
🎉 AUTOMATED processing completed:
   📊 Auto-Processed: {processed_count}
   🎯 Schema Matched: {schema_matched_count}  
   📝 DocType Approval Needed: {approval_needed_count}
   ⏳ Manual Approval Needed: {pending_count}
   ❌ Errors: {error_count}
   🤖 Success Rate: {(processed_count/(processed_count+pending_count+error_count+approval_needed_count)*100):.1f}% automated
        """)

    except Exception as e:
        migration_logger.logger.error(f"❌ Enhanced CSV processing failed: {str(e)}")


# Helper function for intelligent data processing
def process_data_with_intelligent_merge(csv_connector, target_doctype, df, settings, migration_logger, field_mappings):
    """Process data with intelligent insert/update logic"""
    
    batch_size = int(getattr(settings, 'csv_chunk_size', 1000))
    total_results = {'success': 0, 'failed': 0, 'skipped': 0, 'updated': 0}
    
    try:
        # Get existing records for merge detection
        doctype_meta = frappe.get_meta(target_doctype)
        unique_fields = []
        for field in doctype_meta.fields:
            if field.unique or field.fieldname in ['name', 'id', 'code', 'email_id']:
                unique_fields.append(field.fieldname)
        
        existing_records = {}
        if unique_fields:
            identifier_field = unique_fields[0]
            migration_logger.logger.info(f"🔍 Using '{identifier_field}' as unique identifier for merge operations")
            
            # Get existing records
            try:
                existing_values = frappe.db.sql(f"""
                    SELECT name, {identifier_field} 
                    FROM `tab{target_doctype}` 
                    WHERE {identifier_field} IS NOT NULL AND {identifier_field} != ''
                """)
                
                for record in existing_values:
                    if len(record) >= 2 and record[1]:
                        existing_records[str(record[1]).strip().lower()] = record[0]
                        
            except Exception as e:
                migration_logger.logger.warning(f"Could not load existing records: {str(e)}")
                
    except Exception as e:
        migration_logger.logger.warning(f"⚠️ Could not determine merge strategy: {str(e)} - will insert only")
        existing_records = {}
    
    # Process in batches
    batch_count = 0
    max_batches = 50
    
    while batch_count < max_batches:
        batch_count += 1
        
        # Process batch with intelligent merging
        if existing_records:
            batch_results = csv_connector.process_buffered_data_with_intelligent_merge(
                target_doctype, 
                batch_size, 
                existing_records,
                field_mappings
            )
        else:
            batch_results = csv_connector.process_buffered_data_with_upsert(
                target_doctype, 
                batch_size,
                field_mappings
            )
        
        # Aggregate results
        for key in total_results:
            if key in batch_results:
                total_results[key] += batch_results[key]
        
        migration_logger.logger.info(f"📈 Batch {batch_count} results: {batch_results}")
        
        # Stop if no more data to process
        if sum(batch_results.values()) == 0:
            break
    
    return total_results

def send_doctype_creation_request_with_analysis(filename, target_doctype, headers, data_sample, field_analysis):
    """FIXED: Send enhanced DocType creation request with detailed analysis"""
    from data_migration_tool.data_migration.utils.logger_config import migration_logger
    
    try:
        # ✅ ALWAYS use clean_doctype_name for proper naming with spaces
        clean_target_doctype = clean_doctype_name(filename)
        
        # ✅ FIXED: If target_doctype is provided, ALSO clean it!
        if target_doctype and target_doctype != filename:
            # Don't blindly use the provided target_doctype - clean it too!
            clean_target_doctype = clean_doctype_name(target_doctype)
        
        migration_logger.logger.info(f"📝 Creating request for DocType: '{clean_target_doctype}'")
        
        request_doc = frappe.get_doc({
            "doctype": "DocType Creation Request",
            "source_file": filename,
            "suggested_doctype": clean_target_doctype,  # ✅ Now properly cleaned with spaces!
            "field_analysis": json.dumps(convert_numpy_types(field_analysis)),
            "total_records": len(data_sample) if isinstance(data_sample, (list, dict)) else 0,
            "field_count": len(headers),
            "status": "Pending",
            "created_by": frappe.session.user,
            "created_at": now()
        })
        
        request_doc.insert(ignore_permissions=True)
        frappe.db.commit()
        
        migration_logger.logger.info(f"✅ Created request: {request_doc.name} for DocType: '{clean_target_doctype}'")
        return request_doc.name
        
    except Exception as e:
        migration_logger.logger.error(f"❌ Failed to create request: {str(e)}")
        frappe.log_error(f"DocType request creation failed: {str(e)}", "DocType Request Error")
        raise e


# Helper function to send notifications
def send_approval_notifications(request_id):
    """Send notifications to system managers"""
    from data_migration_tool.data_migration.utils.logger_config import migration_logger
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
        
        migration_logger.logger.info(f"📤 Found system managers: {[sm.name for sm in system_managers]}")
        
        # Send notifications
        for manager in system_managers:
            try:
                notification_doc = frappe.get_doc({
                    "doctype": "Notification Log",
                    "for_user": manager.name,
                    "type": "Alert",
                    "document_type": "DocType Creation Request",
                    "document_name": request_id,
                    "subject": f"DocType Creation Approval Required",
                    "email_content": f"A new DocType creation request {request_id} requires your approval."
                })
                
                notification_doc.insert(ignore_permissions=True)
                migration_logger.logger.info(f"📤 Sent notification to {manager.name}")
                
            except Exception as e:
                migration_logger.logger.error(f"Failed to send notification to {manager.name}: {str(e)}")
        
        # Send real-time notifications
        frappe.publish_realtime("doctype_creation_request", {"request_id": request_id}, user="System Manager")
        migration_logger.logger.info("📤 Sent real-time notifications for DocType creation request")
        
        frappe.db.commit()
        
    except Exception as e:
        migration_logger.logger.error(f"Failed to send notifications: {str(e)}")

def process_data_with_merge_logic(csv_connector, target_doctype, df, settings, migration_logger):
    """ FIXED: Process data with intelligent insert/update logic """
    batch_size = int(getattr(settings, 'csv_chunk_size', 1000))
    total_results = {"success": 0, "failed": 0, "skipped": 0, "updated": 0}

    # Get existing records to determine insert vs update
    existing_records = {}
    try:
        # Try to find a unique identifier field
        doctype_meta = frappe.get_meta(target_doctype)
        unique_fields = []
        for field in doctype_meta.fields:
            if field.unique or field.fieldname in ['name', 'id', 'code', 'email']:
                unique_fields.append(field.fieldname)

        if unique_fields:
            # Use first unique field as identifier
            identifier_field = unique_fields[0]
            migration_logger.logger.info(f"🔍 Using '{identifier_field}' as unique identifier for merge operations")

            # FIXED: Remove the problematic as_dict parameter
            existing_values = frappe.db.sql(f"""
                SELECT name, {identifier_field}
                FROM `tab{target_doctype}`
                WHERE {identifier_field} IS NOT NULL
                AND {identifier_field} != ''
            """)
            
            # Convert to dict format manually
            for record in existing_values:
                if len(record) >= 2 and record[1]:  # Ensure record has both values
                    existing_records[str(record[1]).strip().lower()] = record[0]
                    
    except Exception as e:
        migration_logger.logger.warning(f"⚠️ Could not determine merge strategy: {str(e)} - will insert only")

    # Process in batches with merge logic
    batch_count = 0
    max_batches = 100

    while batch_count < max_batches:
        batch_count += 1
        
        # Enhanced processing with merge capabilities
        if existing_records:
            batch_results = csv_connector.process_buffered_data_with_merge(
                target_doctype, batch_size, existing_records
            )
        else:
            batch_results = csv_connector.process_buffered_data_with_upsert(target_doctype, batch_size)

        for key in total_results:
            if key in batch_results:
                total_results[key] += batch_results[key]

        migration_logger.logger.info(f"📈 Batch {batch_count} results: {batch_results}")
        
        if sum(batch_results.values()) == 0:
            break

    return total_results

def process_csv_batch(self, df_chunk: pd.DataFrame, target_doctype: str, 
                     field_mapping: Dict, identifier_fields: List[str]) -> Dict:
    """Process CSV in optimized batches"""
    results = {"inserted": 0, "updated": 0, "failed": 0, "skipped": 0}
    
    # Pre-load existing records for faster lookup
    existing_lookup = self.build_existing_records_lookup(target_doctype, identifier_fields)
    
    for index, row in df_chunk.iterrows():
        try:
            # Apply field mapping
            mapped_data = {field_mapping[k]: v for k, v in row.to_dict().items() 
                          if k in field_mapping and pd.notna(v) and v != ''}
            
            # Check for duplicates using lookup
            existing_name = self.find_existing_in_lookup(mapped_data, existing_lookup, identifier_fields)
            
            if existing_name:
                # Update existing
                if self.update_record_if_changed(existing_name, mapped_data, target_doctype):
                    results["updated"] += 1
                else:
                    results["skipped"] += 1
            else:
                # Insert new
                new_name = self.insert_new_record(mapped_data, target_doctype, identifier_fields)
                if new_name:
                    results["inserted"] += 1
                    # Add to lookup for subsequent rows
                    self.add_to_lookup(existing_lookup, mapped_data, new_name, identifier_fields)
                else:
                    results["failed"] += 1
                    
        except Exception as e:
            results["failed"] += 1
            self.logger.logger.error(f"Row {index} failed: {str(e)}")
    
    return results

def check_pending_requests_and_process():
    """FIXED: Check for approved requests with correct exception handling"""
    from data_migration_tool.data_migration.mappers.doctype_creator import DynamicDocTypeCreator
    from data_migration_tool.data_migration.connectors.csv_connector import CSVConnector
    from data_migration_tool.data_migration.utils.logger_config import migration_logger
    
    try:
        migration_logger.logger.info("🔄 Checking for approved DocType requests to process")
        
        # Get approved requests that haven't been processed yet
        pending_requests = frappe.get_all(
            'DocType Creation Request',
            filters={
                'status': ['in', ['Approved', 'Redirected']],
                'created_doctype': ['in', ['', None]]
            },
            fields=['name', 'source_file', 'suggested_doctype', 'final_doctype', 'field_analysis', 'status'],
            order_by='modified desc'
        )
        
        if not pending_requests:
            migration_logger.logger.info("🔍 No approved DocType requests to process")
            return {"processed": 0}
        
        migration_logger.logger.info(f"🔄 Found {len(pending_requests)} approved requests to process")
        
        # Get Migration Settings for directory paths
        settings = frappe.get_single('Migration Settings')
        try:
            from data_migration_tool.data_migration.utils.path_manager import SecurePathManager
            watch_dir = SecurePathManager.get_watch_directory()
        except Exception as e:
            frappe.log_error(f"Failed to get secure watch directory: {str(e)}")
            watch_dir = frappe.get_site_path('private', 'files', 'migration')
            os.makedirs(watch_dir, exist_ok=True)
        pending_dir = os.path.join(watch_dir, 'pending')
        processed_dir = os.path.join(watch_dir, 'processed')
        error_dir = os.path.join(watch_dir, 'errors')
        
        # Ensure directories exist
        for directory in [processed_dir, error_dir, pending_dir]:
            os.makedirs(directory, exist_ok=True)
        
        csv_connector = CSVConnector(migration_logger)
        mapper = DynamicDocTypeCreator(migration_logger)
        processed_count = 0
        
        for request in pending_requests:
            try:
                # Get fresh document instance
                request_doc = frappe.get_doc('DocType Creation Request', request.name)
            
                 # ✅ FIXED: Always clean the target_doctype name
                target_doctype = clean_doctype_name(request_doc.final_doctype or request_doc.suggested_doctype)
                csv_filename = request_doc.source_file
               
                migration_logger.logger.info(f"🔄 Processing approved request: {csv_filename} → {target_doctype}")
                
                # Find CSV file
                search_dirs = [
                    pending_dir,
                    watch_dir,
                    os.path.join(watch_dir, 'staging'),
                    frappe.get_site_path('public', 'files'),
                    frappe.get_site_path('private', 'files')
                ]
                
                csv_file_path = None
                for search_dir in search_dirs:
                    potential_path = os.path.join(search_dir, csv_filename)
                    if os.path.exists(potential_path):
                        csv_file_path = potential_path
                        migration_logger.logger.info(f"📁 Found CSV file at: {csv_file_path}")
                        break
                
                if not csv_file_path:
                    migration_logger.logger.error(f"⚠️ CSV file not found: {csv_filename}")
                    # FIXED: Simple update without complex exception handling
                    try:
                        frappe.db.set_value('DocType Creation Request', request_doc.name, {
                            'status': 'Failed',
                            'created_doctype': 'File Not Found'
                        })
                        frappe.db.commit()
                    except Exception as update_error:
                        migration_logger.logger.warning(f"⚠️ Could not update failed status: {str(update_error)}")
                    continue
                
                # Create or confirm DocType exists
                if request_doc.status == 'Approved':
                    try:
                        field_analysis = json.loads(request_doc.field_analysis)
                        created_doctype = mapper.create_doctype_from_analysis(field_analysis, target_doctype)
                        migration_logger.logger.info(f"✅ Created DocType: {created_doctype}")
                        target_doctype = created_doctype
                        
                        # Update created_doctype using db.set_value to avoid conflicts
                        frappe.db.set_value('DocType Creation Request', request_doc.name, 'created_doctype', created_doctype)
                        
                    except Exception as doctype_error:
                        migration_logger.logger.error(f"❌ Failed to create DocType {target_doctype}: {str(doctype_error)}")
                        try:
                            frappe.db.set_value('DocType Creation Request', request_doc.name, {
                                'status': 'Failed',
                                'created_doctype': f'Creation Failed: {str(doctype_error)[:100]}'
                            })
                        except Exception:
                            pass
                        continue
                        
                elif request_doc.status == 'Redirected':
                    if not frappe.db.exists('DocType', target_doctype):
                        migration_logger.logger.error(f"❌ Target DocType {target_doctype} does not exist")
                        try:
                            frappe.db.set_value('DocType Creation Request', request_doc.name, {
                                'status': 'Failed',
                                'created_doctype': 'Target DocType Not Found'
                            })
                        except Exception:
                            pass
                        continue
                    
                    migration_logger.logger.info(f"🔄 Using existing DocType: {target_doctype}")
                    frappe.db.set_value('DocType Creation Request', request_doc.name, 'created_doctype', target_doctype)
                
                # Process the CSV file
                try:
                    migration_logger.logger.info(f"📄 Processing CSV file: {csv_filename}")
                    
                    df = csv_connector.read_file_as_strings(csv_file_path)
                    if df.empty:
                        migration_logger.logger.warning(f"⚠️ Empty CSV file: {csv_filename}")
                        frappe.db.set_value('DocType Creation Request', request_doc.name, {
                            'status': 'Failed',
                            'created_doctype': 'Empty File'
                        })
                        continue
                    
                    migration_logger.logger.info(f"📊 Loaded {len(df)} rows from {csv_filename}")
                    
                    stored_count = csv_connector.store_raw_data(df, csv_filename, target_doctype)
                    migration_logger.logger.info(f"📦 Stored {stored_count} raw records for processing")
                    
                    # Process with JIT conversion in batches
                    batch_size = int(getattr(settings, 'csv_chunk_size', 1000))
                    total_results = {"success": 0, "failed": 0, "skipped": 0}
                    
                    batch_count = 0
                    max_batches = 100
                    
                    while batch_count < max_batches:
                        batch_count += 1
                        batch_results = csv_connector.process_buffered_data_with_upsert(target_doctype, batch_size)
                        
                        for key in total_results:
                            total_results[key] += batch_results[key]
                        
                        migration_logger.logger.info(f"📈 Batch {batch_count} results: {batch_results}")
                        
                        if sum(batch_results.values()) == 0:
                            break
                    
                    migration_logger.logger.info(f"📈 Final import results for {csv_filename}: {total_results}")
                    
                    # Move file to processed directory
                    processed_path = os.path.join(processed_dir, csv_filename)
                    shutil.move(csv_file_path, processed_path)
                    migration_logger.logger.info(f"📁 Moved file to processed: {processed_path}")
                    
                    # FIXED: Update request status using db.set_value to avoid conflicts
                    try:
                        frappe.db.set_value('DocType Creation Request', request_doc.name, {
                            'status': 'Completed',
                            'processing_results': json.dumps(total_results)
                        })
                        frappe.db.commit()
                        migration_logger.logger.info(f"✅ Successfully completed request: {request.name}")
                    except Exception as update_error:
                        migration_logger.logger.warning(f"⚠️ Could not update completion status: {str(update_error)} - but processing succeeded")
                    
                    processed_count += 1
                    
                    # Send completion notification
                    try:
                        frappe.publish_realtime(
                            event='doctype_processing_completed',
                            message={
                                'request_id': request_doc.name,
                                'filename': csv_filename,
                                'doctype': target_doctype,
                                'results': total_results
                            }
                        )
                    except Exception as notify_error:
                        migration_logger.logger.warning(f"⚠️ Failed to send completion notification: {str(notify_error)}")
                    
                except Exception as processing_error:
                    migration_logger.logger.error(f"❌ Failed to process CSV {csv_filename}: {str(processing_error)}")
                    
                    # Move to error directory
                    try:
                        error_path = os.path.join(error_dir, csv_filename)
                        if os.path.exists(csv_file_path):
                            shutil.move(csv_file_path, error_path)
                    except:
                        pass
                    
                    try:
                        frappe.db.set_value('DocType Creation Request', request_doc.name, {
                            'status': 'Failed',
                            'created_doctype': f'Processing Failed: {str(processing_error)[:100]}'
                        })
                    except Exception:
                        pass
                
            except Exception as e:
                migration_logger.logger.error(f"❌ Failed to process request {request.name}: {str(e)}")
                continue
        
        migration_logger.logger.info(f"🎉 Completed processing {processed_count} approved requests")
        return {"processed": processed_count}
    
    except Exception as e:
        migration_logger.logger.error(f"❌ Error in check_pending_requests_and_process: {str(e)}")
        return {"processed": 0}

def send_doctype_creation_request(filename, target_doctype, field_analysis):
    """ENHANCED: Create DocType creation request with proper error handling"""
    from data_migration_tool.data_migration.utils.logger_config import migration_logger
    
    try:
        # Set proper user context
        try:
            from data_migration_tool.data_migration.utils.user_context import UserContextManager
            current_user = UserContextManager.get_migration_user()
        except Exception:
            current_user = frappe.session.user if hasattr(frappe, 'session') and frappe.session.user != 'Guest' else 'Administrator'
        frappe.set_user(current_user)
        
        # Convert numpy types to native Python types
        safe_field_analysis = convert_numpy_types(field_analysis)
        
        # Create the request document
        request_doc = frappe.get_doc({
            'doctype': 'DocType Creation Request',
            'source_file': filename,
            'suggested_doctype': target_doctype,
            'field_analysis': json.dumps(safe_field_analysis),
            'status': 'Pending',
            'created_by': current_user,
            'owner': current_user
        })
        
        request_doc.insert(ignore_permissions=True, ignore_mandatory=True)
        frappe.db.commit()
        
        migration_logger.logger.info(f"🔔 Created DocType creation request: {request_doc.name}")
        
        # Get system managers using SQL query
        system_managers = frappe.db.sql("""
            SELECT DISTINCT u.name 
            FROM `tabUser` u
            INNER JOIN `tabHas Role` hr ON u.name = hr.parent
            WHERE hr.role = 'System Manager' 
            AND u.enabled = 1 
            AND u.name != 'Guest'
        """, as_dict=True)
        
        manager_emails = [manager.name for manager in system_managers]
        if not manager_emails:
            try:
                from data_migration_tool.data_migration.utils.user_context import UserContextManager
                system_managers = UserContextManager.get_system_managers()
                manager_emails = [manager.get('name', 'Administrator') for manager in system_managers]
            except Exception:
                manager_emails = ['Administrator']
            
        migration_logger.logger.info(f"📤 Found system managers: {manager_emails}")
        
        # Send real-time notifications
        notification_data = {
            'request_id': request_doc.name,
            'filename': filename,
            'suggested_doctype': target_doctype,
            'field_count': len(safe_field_analysis.get('fields', {})),
            'sample_fields': list(safe_field_analysis.get('fields', {}).keys())[:5]
        }
        
        for manager in manager_emails:
            try:
                frappe.publish_realtime(
                    event='doctype_creation_request',
                    message=notification_data,
                    user=manager
                )
                migration_logger.logger.info(f"📤 Sent notification to {manager}")
            except Exception as notify_error:
                migration_logger.logger.warning(f"⚠️ Failed to send notification to {manager}: {str(notify_error)}")
        
        # Also send general notification
        try:
            frappe.publish_realtime(
                event='doctype_creation_request',
                message=notification_data
            )
        except Exception as general_notify_error:
            migration_logger.logger.warning(f"⚠️ Failed to send general notification: {str(general_notify_error)}")
        
        migration_logger.logger.info(f"📤 Sent real-time notifications for DocType creation request")
        
        return request_doc.name
        
    except Exception as e:
        migration_logger.logger.error(f"❌ Failed to create DocType creation request: {str(e)}")
        import traceback
        migration_logger.logger.error(f"❌ Full traceback: {traceback.format_exc()}")
        raise e

def compute_schema_fingerprint(headers: list, data_sample: dict = None) -> str:
    """
    Compute a unique fingerprint for CSV schema based on headers and data types
    """
    # Sort headers to ensure consistent fingerprints regardless of column order
    sorted_headers = sorted([h.strip().lower() for h in headers])
    
    # Create fingerprint from headers
    headers_string = '|'.join(sorted_headers)
    
    # Optionally include data type information for more precision
    if data_sample:
        type_info = []
        for header in sorted_headers:
            if header in data_sample:
                sample_value = str(data_sample[header])
                # Simple type detection
                if sample_value.isdigit():
                    type_info.append(f"{header}:int")
                elif sample_value.replace('.', '').isdigit():
                    type_info.append(f"{header}:float")
                else:
                    type_info.append(f"{header}:string")
        
        if type_info:
            headers_string += '||' + '|'.join(type_info)
    
    # Generate MD5 hash
    return hashlib.md5(headers_string.encode('utf-8')).hexdigest()

def find_existing_doctype_by_schema(headers: list, data_sample: dict = None) -> tuple:
    """
    ENHANCED: Find existing DocType by analyzing CSV schema fingerprint
    Returns: (doctype_name, registry_id) or (None, None)
    """
    try:
        # Generate schema fingerprint
        schema_fingerprint = compute_schema_fingerprint(headers, data_sample)
        
        # Check if we have processed this exact schema before
        existing_registry = frappe.db.get_value(
            'CSV Schema Registry',
            {'schema_fingerprint': schema_fingerprint},
            ['target_doctype', 'name'],
            as_dict=True
        )
        
        if existing_registry:
            doctype_name = existing_registry.target_doctype
            
            # Verify the DocType still exists
            if frappe.db.exists('DocType', doctype_name):
                from data_migration_tool.data_migration.utils.logger_config import migration_logger
                migration_logger.logger.info(f"🎯 Found existing schema match: {doctype_name} (fingerprint: {schema_fingerprint[:8]}...)")
                return doctype_name, existing_registry.name
            else:
                # Clean up orphaned registry entry
                frappe.delete_doc('CSV Schema Registry', existing_registry.name, ignore_permissions=True)
                
        # Check for similar schemas (80% header match)
        similar_registries = frappe.get_all(
            'CSV Schema Registry',
            fields=['target_doctype', 'headers_json', 'name'],
            limit=50
        )
        
        for registry in similar_registries:
            if registry.headers_json:
                try:
                    stored_headers = json.loads(registry.headers_json)
                    similarity = calculate_header_similarity(headers, stored_headers)
                    
                    if similarity >= 0.8:  # 80% similarity threshold
                        if frappe.db.exists('DocType', registry.target_doctype):
                            from data_migration_tool.data_migration.utils.logger_config import migration_logger
                            migration_logger.logger.info(f"🎯 Found similar schema ({similarity*100:.1f}%): {registry.target_doctype}")
                            return registry.target_doctype, registry.name
                except Exception as e:
                    migration_logger.logger.warning(f"⚠️ Error comparing schemas: {str(e)}")
                    continue
                    
        return None, None
        
    except Exception as e:
        frappe.log_error(f"Error finding existing DocType by schema: {str(e)}")
        return None, None
    
def calculate_header_similarity(headers1: list, headers2: list) -> float:
    """Calculate similarity between two header lists"""
    if not headers1 or not headers2:
        return 0.0
    
    # Normalize headers for comparison
    norm_headers1 = set(h.lower().strip().replace(' ', '_').replace('-', '_') for h in headers1)
    norm_headers2 = set(h.lower().strip().replace(' ', '_').replace('-', '_') for h in headers2)
    
    # Calculate Jaccard similarity
    intersection = len(norm_headers1.intersection(norm_headers2))
    union = len(norm_headers1.union(norm_headers2))
    
    return intersection / union if union > 0 else 0.0

def register_csv_schema(filename: str, headers: list, target_doctype: str, data_sample: dict = None, confidence: float = 1.0):
    """
    ENHANCED: Register CSV schema with comprehensive metadata
    """
    try:
        schema_fingerprint = compute_schema_fingerprint(headers, data_sample)
        
        # Check if already exists
        existing = frappe.db.exists('CSV Schema Registry', {'schema_fingerprint': schema_fingerprint})
        if existing:
            # Update existing record
            doc = frappe.get_doc('CSV Schema Registry', existing)
            doc.last_used = frappe.utils.now()
            doc.usage_count = (doc.usage_count or 0) + 1
            doc.save(ignore_permissions=True)
            return existing
        
        # Create new registry entry
        registry_doc = frappe.get_doc({
            'doctype': 'CSV Schema Registry',
            'schema_fingerprint': schema_fingerprint,
            'target_doctype': target_doctype,
            'headers_json': json.dumps(headers),
            'sample_filename': filename,
            'data_sample': json.dumps(data_sample) if data_sample else None,
            'confidence_score': confidence,
            'created_at': frappe.utils.now(),
            'last_used': frappe.utils.now(),
            'usage_count': 1
        })
        
        registry_doc.insert(ignore_permissions=True)
        frappe.db.commit()
        
        from data_migration_tool.data_migration.utils.logger_config import migration_logger
        migration_logger.logger.info(f"📝 Registered CSV schema: {target_doctype} (fingerprint: {schema_fingerprint[:8]}...)")
        
        return registry_doc.name
        
    except Exception as e:
        frappe.log_error(f"Failed to register CSV schema: {str(e)}")
        return None



def register_csv_schema(source_file: str, headers: list, target_doctype: str, data_sample: dict = None):
    """
    Register a new CSV schema in the registry
    """
    try:
        fingerprint = compute_schema_fingerprint(headers, data_sample)
        
        # Create registry entry
        registry_doc = frappe.get_doc({
            'doctype': 'CSV Schema Registry',
            'source_file': source_file,
            'schema_fingerprint': fingerprint,
            'headers_json': json.dumps(headers),
            'target_doctype': target_doctype,
            'field_count': len(headers)
        })
        
        registry_doc.insert(ignore_permissions=True, ignore_if_duplicate=True)
        frappe.db.commit()
        
        return registry_doc.name
        
    except Exception as e:
        frappe.log_error(f"Failed to register schema for {source_file}: {str(e)}")
        return None

def get_data_sample_from_df(df: pd.DataFrame, max_samples=5) -> dict:
    """Extract data sample from DataFrame for schema analysis"""
    if df.empty:
        return {}
    
    sample_data = {}
    for col in df.columns:
        non_empty = df[col].dropna()
        if not non_empty.empty:
            sample_data[col] = non_empty.head(max_samples).tolist()
    
    return sample_data


def on_doctype_request_update(doc, method):
    """Trigger processing when DocType Creation Request is approved"""
    from data_migration_tool.data_migration.utils.logger_config import migration_logger
    
    try:
        if doc.status in ['Approved', 'Redirected'] and doc.has_value_changed('status'):
            migration_logger.logger.info(f"🔄 DocType request {doc.name} status changed to {doc.status} - triggering processing")
            
            frappe.enqueue(
                'data_migration_tool.data_migration.utils.scheduler_tasks.check_pending_requests_and_process',
                queue='long',
                timeout=3600,
                is_async=True,
                job_name=f'auto_process_{doc.name}_{frappe.utils.now_datetime().strftime("%H%M%S")}'
            )
            
            migration_logger.logger.info(f"📋 Auto-triggered processing for approved request {doc.name}")
            
    except Exception as e:
        migration_logger.logger.error(f"❌ Failed to trigger auto-processing: {str(e)}")

def sync_zoho_data(settings):
    """Enhanced Zoho sync - Phase 2 implementation"""
    from data_migration_tool.data_migration.utils.logger_config import migration_logger
    migration_logger.logger.info("🔄 Zoho sync - Phase 2 implementation needed")

def sync_odoo_data(settings):
    """Enhanced Odoo sync - Phase 2 implementation"""
    from data_migration_tool.data_migration.utils.logger_config import migration_logger
    migration_logger.logger.info("🔄 Odoo sync - Phase 2 implementation needed")

def send_sync_failure_notification(error_message: str):
    """Enhanced notification system with fallback"""
    try:
        short_error = error_message[:500] + "..." if len(error_message) > 500 else error_message
        
        # Get system managers using SQL query for fallback compatibility
        try:
            system_managers = frappe.db.sql("""
                SELECT DISTINCT u.name 
                FROM `tabUser` u
                INNER JOIN `tabHas Role` hr ON u.name = hr.parent
                WHERE hr.role = 'System Manager' 
                AND u.enabled = 1 
                AND u.name != 'Guest'
            """, pluck=True)
        except Exception as e:
            frappe.log_error(f"Failed to get system managers for notification: {str(e)}")
            system_managers = ['Administrator']
        
        frappe.sendmail(
            recipients=system_managers,
            subject="Data Migration Sync Failed",
            message=f"""
            <h3>Data Migration Sync Failure</h3>
            <p>The scheduled data migration sync has failed with the following error:</p>
            <pre>{short_error}</pre>
            <p><strong>Time:</strong> {frappe.utils.now()}</p>
            <p><strong>Site:</strong> {frappe.local.site}</p>
            <p><strong>System:</strong> Data Migration Tool</p>
            <p>Please check the system logs for more details.</p>
            """
        )
        
    except Exception as e:
        frappe.log_error(f"Failed to send sync failure notification: {str(e)}")

# Additional helper functions
def setup_job_context():
    """Setup context for background jobs"""
    pass

def cleanup_job_context():
    """Cleanup context after background jobs"""
    pass

def on_settings_update(doc=None, method=None):
    """Handle Migration Settings updates"""
    from data_migration_tool.data_migration.utils.logger_config import migration_logger
    migration_logger.logger.info("Migration Settings updated")
    
def cleanup_old_logs():
    """Clean up old migration logs with enhanced error handling"""
    from data_migration_tool.data_migration.utils.logger_config import migration_logger
    
    try:
        cutoff_date = frappe.utils.add_to_date(now(), -30)
        
        # Clean up old buffer records
        try:
            deleted_buffer = frappe.db.sql("""
                DELETE FROM `tabMigration Data Buffer`
                WHERE processing_status IN ('Processed', 'Skipped')
                AND processed_at < %s
            """, cutoff_date)
            
            migration_logger.logger.info(f"🧹 Cleaned up {len(deleted_buffer) if deleted_buffer else 0} old buffer records")
        except Exception as buffer_cleanup_error:
            migration_logger.logger.warning(f"⚠️ Buffer cleanup failed: {str(buffer_cleanup_error)}")
        
        # Clean up old creation requests
        try:
            deleted_requests = frappe.db.sql("""
                DELETE FROM `tabDocType Creation Request`
                WHERE status IN ('Completed', 'Rejected', 'Failed')
                AND responded_at < %s
            """, cutoff_date)
            
            migration_logger.logger.info(f"🧹 Cleaned up {len(deleted_requests) if deleted_requests else 0} old creation requests")
        except Exception as request_cleanup_error:
            migration_logger.logger.warning(f"⚠️ Request cleanup failed: {str(request_cleanup_error)}")
        
        frappe.db.commit()
        
    except Exception as e:
        migration_logger.logger.error(f"❌ Log cleanup failed: {str(e)}")