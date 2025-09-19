# Scheduler tasks for data migration - Enhanced Phase 1 Version with User Approval Fix
import os
import shutil
import frappe
from frappe.utils import now, add_days, get_datetime
from pathlib import Path
from typing import Dict, Any
import hashlib
import json
from datetime import datetime
import numpy as np
import pandas as pd
import re

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
        except frappe.exceptions.DocumentModifiedError:
            migration_logger.logger.warning("⚠️ Settings save conflict - sync completed but timestamp not updated")
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
    """FIXED: Enhanced DocType name mapping that ONLY maps exact matches"""
    
    # Remove file extension
    base_name = Path(filename).stem
    
    # Remove all special characters and spaces, keep only alphanumeric
    clean_name = re.sub(r'[^a-zA-Z0-9]', '', base_name)
    
    # Convert to title case
    clean_name = ''.join(word.capitalize() for word in re.findall(r'[a-zA-Z0-9]+', clean_name))
    
    # ONLY map if it's an EXACT match to common patterns
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
    
    # Check if it exactly matches a known pattern
    if clean_name in exact_mappings:
        mapped_doctype = exact_mappings[clean_name]
        if frappe.db.exists('DocType', mapped_doctype):
            frappe.logger().info(f"✅ Mapped {filename} to existing DocType: {mapped_doctype}")
            return mapped_doctype
    
    # For unknown files, return the cleaned name to trigger user approval
    frappe.logger().info(f"🔄 Unknown DocType pattern: {clean_name} - will request user approval")
    return clean_name

def process_csv_files_with_jit():
    """ENHANCED: CSV processing with schema recognition"""
    from data_migration_tool.data_migration.connectors.csv_connector import CSVConnector
    from data_migration_tool.data_migration.mappers.doctype_creator import DynamicDocTypeCreator
    from data_migration_tool.data_migration.utils.logger_config import migration_logger
    
    # Set proper user context
    try:
        system_managers = frappe.db.sql("""
            SELECT DISTINCT u.name FROM `tabUser` u
            INNER JOIN `tabHas Role` hr ON u.name = hr.parent
            WHERE hr.role = 'System Manager' AND u.enabled = 1 AND u.name != 'Guest'
            LIMIT 1
        """, as_dict=True)
        
        if system_managers:
            frappe.set_user(system_managers[0].name)
        else:
            frappe.set_user('Administrator')
    except Exception:
        frappe.set_user('Administrator')
    
    try:
        migration_logger.logger.info("🚀 Starting enhanced CSV processing with schema recognition")
        
        # Get Migration Settings
        settings = frappe.get_single('Migration Settings')
        if not settings.enable_csv_processing or not settings.csv_watch_directory:
            return
        
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
            migration_logger.logger.info("📂 No CSV files found to process")
            return
        
        migration_logger.logger.info(f"📋 Found {len(processable_files)} files for processing")
        
        processed_count = 0
        error_count = 0
        pending_count = 0
        
        for filename, file_path in processable_files:
            try:
                migration_logger.logger.info(f"📄 Processing file with schema recognition: {filename}")
                
                # Step 1: Read CSV and analyze headers
                df = csv_connector.read_file_as_strings(file_path)
                if df.empty:
                    migration_logger.logger.warning(f"⚠️ Empty file: {filename}")
                    error_count += 1
                    continue
                
                headers = list(df.columns)
                data_sample = get_data_sample_from_df(df)
                
                migration_logger.logger.info(f"📊 Detected headers: {headers}")
                
                # Step 2: ENHANCED - Check for existing schema match
                existing_doctype, registry_id = find_existing_doctype_by_schema(headers, data_sample)
                
                if existing_doctype:
                    migration_logger.logger.info(f"✅ Found matching schema! Mapping to existing DocType: {existing_doctype}")
                    target_doctype = existing_doctype
                    
                    # Process directly - no approval needed
                    try:
                        # Store and process data with intelligent merging
                        stored_count = csv_connector.store_raw_data(df, filename, target_doctype)
                        migration_logger.logger.info(f"📦 Stored {stored_count} records for processing")
                        
                        # Process with enhanced merging logic
                        total_results = process_data_with_merge_logic(
                            csv_connector, target_doctype, df, settings, migration_logger
                        )
                        
                        if total_results["success"] > 0 or total_results["updated"] > 0:
                            processed_path = os.path.join(processed_dir, filename)
                            shutil.move(file_path, processed_path)
                            migration_logger.logger.info(f"✅ Successfully processed {filename} with existing schema: {total_results}")
                            processed_count += 1
                        else:
                            error_path = os.path.join(error_dir, filename)
                            shutil.move(file_path, error_path)
                            migration_logger.logger.warning(f"⚠️ No successful operations for {filename}")
                            error_count += 1
                    
                    except Exception as e:
                        migration_logger.logger.error(f"❌ Failed to process existing schema file {filename}: {str(e)}")
                        error_path = os.path.join(error_dir, filename)
                        shutil.move(file_path, error_path)
                        error_count += 1
                
                else:
                    # Step 3: No existing schema - proceed with original logic
                    migration_logger.logger.info(f"🔍 No matching schema found - checking DocType creation options")
                    
                    target_doctype = clean_doctype_name(filename)
                    
                    if not frappe.db.exists('DocType', target_doctype):
                        if getattr(settings, 'require_user_permission_for_doctype_creation', True):
                            # Check for existing request
                            existing_request = frappe.db.exists('DocType Creation Request', {
                                'source_file': filename,
                                'status': ['in', ['Pending', 'Approved', 'Redirected']]
                            })
                            
                            if not existing_request:
                                # Create approval request
                                field_analysis = mapper.analyze_csv_structure(df)
                                request_id = send_doctype_creation_request(filename, target_doctype, field_analysis)
                                migration_logger.logger.info(f"📋 Sent approval request {request_id} for new schema")
                            
                            # Move to pending
                            pending_path = os.path.join(pending_dir, filename)
                            shutil.move(file_path, pending_path)
                            pending_count += 1
                            continue
                        
                        elif getattr(settings, 'auto_create_doctypes', False):
                            # Auto-create DocType and register schema
                            try:
                                field_analysis = mapper.analyze_csv_structure(df)
                                created_doctype = mapper.create_doctype_from_analysis(field_analysis, target_doctype)
                                migration_logger.logger.info(f"✅ Auto-created DocType: {created_doctype}")
                                
                                # Register the new schema
                                register_csv_schema(filename, headers, created_doctype, data_sample)
                                migration_logger.logger.info(f"📝 Registered new schema for future recognition")
                                
                                target_doctype = created_doctype
                            except Exception as e:
                                migration_logger.logger.error(f"❌ Failed to auto-create DocType: {str(e)}")
                                error_path = os.path.join(error_dir, filename)
                                shutil.move(file_path, error_path)
                                error_count += 1
                                continue
                    
                    # Process the new DocType
                    try:
                        stored_count = csv_connector.store_raw_data(df, filename, target_doctype)
                        total_results = process_data_with_merge_logic(
                            csv_connector, target_doctype, df, settings, migration_logger
                        )
                        
                        if total_results["success"] > 0:
                            processed_path = os.path.join(processed_dir, filename)
                            shutil.move(file_path, processed_path)
                            processed_count += 1
                        else:
                            error_path = os.path.join(error_dir, filename)
                            shutil.move(file_path, error_path)
                            error_count += 1
                            
                    except Exception as e:
                        migration_logger.logger.error(f"❌ Processing failed: {str(e)}")
                        error_path = os.path.join(error_dir, filename)
                        shutil.move(file_path, error_path)
                        error_count += 1
                
            except Exception as e:
                migration_logger.logger.error(f"❌ Failed to process {filename}: {str(e)}")
                error_count += 1
        
        migration_logger.logger.info(f"🎉 Enhanced processing completed - Processed: {processed_count}, Errors: {error_count}, Pending: {pending_count}")
        
    except Exception as e:
        migration_logger.logger.error(f"❌ Enhanced CSV processing failed: {str(e)}")

def process_data_with_merge_logic(csv_connector, target_doctype, df, settings, migration_logger):
    """
    Process data with intelligent insert/update logic
    """
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
            
            # Get all existing values for this field
            existing_values = frappe.db.get_all(
                target_doctype, 
                fields=['name', identifier_field],
                as_dict=True
            )
            
            for record in existing_values:
                existing_records[str(record[identifier_field]).strip().lower()] = record.name
    
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
            batch_results = csv_connector.process_buffered_data(target_doctype, batch_size)
        
        for key in total_results:
            if key in batch_results:
                total_results[key] += batch_results[key]
        
        migration_logger.logger.info(f"📈 Batch {batch_count} results: {batch_results}")
        
        if sum(batch_results.values()) == 0:
            break
    
    return total_results


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
        watch_dir = getattr(settings, 'csv_watch_directory', '/tmp/csv_watch')
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
                
                target_doctype = request_doc.final_doctype or request_doc.suggested_doctype
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
                        batch_results = csv_connector.process_buffered_data(target_doctype, batch_size)
                        
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
    Find existing DocType that matches the CSV schema
    Returns: (doctype_name, registry_id) or (None, None) if not found
    """
    fingerprint = compute_schema_fingerprint(headers, data_sample)
    
    # Check registry for existing schema
    existing_schema = frappe.db.get_value(
        'CSV Schema Registry',
        {'schema_fingerprint': fingerprint},
        ['target_doctype', 'name'],
        as_dict=True
    )
    
    if existing_schema and frappe.db.exists('DocType', existing_schema.target_doctype):
        return existing_schema.target_doctype, existing_schema.name
    
    return None, None

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

def get_data_sample_from_df(df):
    """
    Get a representative data sample from DataFrame for type detection
    """
    if df.empty:
        return {}
    
    # Get first non-empty row as sample
    sample = {}
    for col in df.columns:
        non_empty_values = df[col].dropna()
        if len(non_empty_values) > 0:
            sample[col.strip().lower()] = str(non_empty_values.iloc[0])
    
    return sample

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
        except:
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
        cutoff_date = frappe.utils.add_days(now(), -30)
        
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