# Scheduler tasks for data migration - Enhanced Phase 1 Version with User Approval Fix
import os
import shutil
import frappe
from frappe.utils import now, add_days, get_datetime
from pathlib import Path
from typing import Dict, Any
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
    """ENHANCED: CSV processing with proper user context and improved error handling"""
    from data_migration_tool.data_migration.connectors.csv_connector import CSVConnector
    from data_migration_tool.data_migration.mappers.doctype_creator import DynamicDocTypeCreator
    from data_migration_tool.data_migration.utils.logger_config import migration_logger
    
    # Set proper user context at the very beginning
    try:
        # Get system managers using SQL query instead of frappe.get_system_managers()
        system_managers = frappe.db.sql("""
            SELECT DISTINCT u.name 
            FROM `tabUser` u
            INNER JOIN `tabHas Role` hr ON u.name = hr.parent
            WHERE hr.role = 'System Manager' 
            AND u.enabled = 1 
            AND u.name != 'Guest'
            LIMIT 1
        """, as_dict=True)
        
        if system_managers:
            frappe.set_user(system_managers[0].name)
            migration_logger.logger.info(f"🔧 Set user context to: {system_managers[0].name}")
        else:
            frappe.set_user('Administrator')
            migration_logger.logger.info(f"🔧 Set user context to: Administrator")
    except Exception as user_error:
        migration_logger.logger.warning(f"⚠️ Could not set user context: {str(user_error)}")
        frappe.set_user('Administrator')
    
    try:
        migration_logger.logger.info("🚀 Starting JIT CSV file processing")
        
        # Get Migration Settings
        try:
            settings = frappe.get_single('Migration Settings')
        except frappe.DoesNotExistError:
            migration_logger.logger.warning("⚠️ Migration Settings not found, skipping JIT CSV processing")
            return
        
        if not settings.enable_csv_processing:
            migration_logger.logger.info("📄 CSV processing disabled")
            return
        
        if not settings.csv_watch_directory:
            migration_logger.logger.error("💥 CSV Watch Directory not configured")
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
        
        if not os.path.exists(watch_dir):
            migration_logger.logger.error(f"💥 Watch directory does not exist: {watch_dir}")
            return
        
        # Get processable files
        processable_files = []
        for filename in os.listdir(watch_dir):
            if filename.startswith('.'):
                continue
            file_path = os.path.join(watch_dir, filename)
            if not os.path.isfile(file_path):
                continue
            file_ext = Path(file_path).suffix.lower()
            if file_ext not in csv_connector.supported_formats:
                continue
            processable_files.append((filename, file_path))
        
        if not processable_files:
            migration_logger.logger.info("📂 No CSV files found to process")
            return
        
        migration_logger.logger.info(f"📋 Found {len(processable_files)} files for JIT processing")
        
        processed_count = 0
        error_count = 0
        pending_count = 0
        
        # Process each file with enhanced error handling
        for filename, file_path in processable_files:
            try:
                migration_logger.logger.info(f"📄 Starting JIT processing for: {filename}")
                
                # Step 1: Read as strings
                df = csv_connector.read_file_as_strings(file_path)
                if df.empty:
                    migration_logger.logger.warning(f"⚠️ Empty file: {filename}")
                    error_count += 1
                    continue
                
                # Step 2: Use enhanced DocType name cleaning
                target_doctype = clean_doctype_name(filename)
                migration_logger.logger.info(f"🎯 Determined target DocType: {target_doctype}")
                
                # Step 3: Check if DocType exists, if not trigger approval workflow
                if not frappe.db.exists('DocType', target_doctype):
                    migration_logger.logger.info(f"❓ DocType '{target_doctype}' does not exist - checking approval settings")
                    
                    if getattr(settings, 'require_user_permission_for_doctype_creation', True):
                        # Check if request already exists for this file
                        existing_request = frappe.db.exists('DocType Creation Request', {
                            'source_file': filename,
                            'status': ['in', ['Pending', 'Approved', 'Redirected']]
                        })
                        
                        if not existing_request:
                            # Analyze CSV structure for request
                            migration_logger.logger.info(f"📊 Analyzing CSV structure for approval request")
                            field_analysis = mapper.analyze_csv_structure(df)
                            
                            # Send user permission request
                            request_id = send_doctype_creation_request(filename, target_doctype, field_analysis)
                            migration_logger.logger.info(f"📋 Sent user permission request {request_id} for DocType: {target_doctype}")
                        else:
                            migration_logger.logger.info(f"📋 Approval request already exists for {filename}")
                        
                        # Move file to pending directory
                        pending_path = os.path.join(pending_dir, filename)
                        shutil.move(file_path, pending_path)
                        pending_count += 1
                        migration_logger.logger.info(f"📁 Moved {filename} to pending directory")
                        continue
                    
                    elif getattr(settings, 'auto_create_doctypes', False):
                        # Auto-create mode
                        try:
                            field_analysis = mapper.analyze_csv_structure(df)
                            created_doctype = mapper.create_doctype_from_analysis(field_analysis, target_doctype)
                            migration_logger.logger.info(f"✅ Auto-created DocType: {created_doctype}")
                            target_doctype = created_doctype
                        except Exception as e:
                            migration_logger.logger.error(f"❌ Failed to auto-create DocType {target_doctype}: {str(e)}")
                            error_path = os.path.join(error_dir, filename)
                            shutil.move(file_path, error_path)
                            error_count += 1
                            continue
                    else:
                        # Neither approval nor auto-create enabled
                        migration_logger.logger.error(f"❌ DocType {target_doctype} does not exist and creation not enabled")
                        error_path = os.path.join(error_dir, filename)
                        shutil.move(file_path, error_path)
                        error_count += 1
                        continue
                
                # Step 4: Process if DocType exists
                migration_logger.logger.info(f"✅ DocType '{target_doctype}' exists - proceeding with data import")
                
                try:
                    stored_count = csv_connector.store_raw_data(df, filename, target_doctype)
                    migration_logger.logger.info(f"📦 Stored {stored_count} raw records for JIT processing")
                    
                    # Step 5: Process with JIT conversion in batches
                    batch_size = int(getattr(settings, 'csv_chunk_size', 1000))
                    total_results = {"success": 0, "failed": 0, "skipped": 0}
                    
                    migration_logger.logger.info(f"🔄 Starting JIT batch processing with batch size: {batch_size}")
                    
                    batch_count = 0
                    max_batches = 100
                    
                    while batch_count < max_batches:
                        batch_count += 1
                        migration_logger.logger.info(f"📊 Processing JIT batch {batch_count}")
                        
                        try:
                            batch_results = csv_connector.process_buffered_data(target_doctype, batch_size)
                        except Exception as e:
                            migration_logger.logger.error(f"❌ JIT batch processing failed: {str(e)}")
                            break
                        
                        for key in total_results:
                            total_results[key] += batch_results[key]
                        
                        migration_logger.logger.info(f"📈 Batch {batch_count} results: {batch_results}")
                        
                        if sum(batch_results.values()) == 0:
                            migration_logger.logger.info("📭 No more records to process")
                            break
                    
                    # Move file based on results
                    if total_results["success"] > 0:
                        processed_path = os.path.join(processed_dir, filename)
                        shutil.move(file_path, processed_path)
                        migration_logger.logger.info(f"✅ JIT processing completed for {filename}: {total_results}")
                        processed_count += 1
                    else:
                        error_path = os.path.join(error_dir, filename)
                        shutil.move(file_path, error_path)
                        migration_logger.logger.warning(f"⚠️ No successful records for {filename}: {total_results}")
                        error_count += 1
                        
                except Exception as e:
                    migration_logger.logger.error(f"❌ Failed to store/process raw data: {str(e)}")
                    error_path = os.path.join(error_dir, filename)
                    shutil.move(file_path, error_path)
                    error_count += 1
                    continue
                
            except Exception as e:
                migration_logger.logger.error(f"❌ JIT processing failed for {filename}: {str(e)}")
                try:
                    error_path = os.path.join(error_dir, filename)
                    if os.path.exists(file_path):
                        shutil.move(file_path, error_path)
                except Exception as move_error:
                    migration_logger.logger.error(f"❌ Failed to move error file: {str(move_error)}")
                error_count += 1
        
        # Summary logging
        migration_logger.logger.info(f"🎉 JIT CSV processing completed - Processed: {processed_count}, Errors: {error_count}, Pending: {pending_count}")
        
        # Cleanup old buffer records
        if processed_count > 0:
            try:
                csv_connector.cleanup_processed_buffer(days_old=7)
            except Exception as cleanup_error:
                migration_logger.logger.warning(f"⚠️ Buffer cleanup failed: {str(cleanup_error)}")
    
    except Exception as e:
        migration_logger.logger.error(f"❌ JIT CSV processing failed: {str(e)}")

def check_pending_requests_and_process():
    """ENHANCED: Check for approved requests with document conflict fix"""
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
                # FIXED: Get fresh document instance and reload to prevent conflicts
                request_doc = frappe.get_doc('DocType Creation Request', request.name)
                request_doc.reload()
                
                target_doctype = request_doc.final_doctype or request_doc.suggested_doctype
                csv_filename = request_doc.source_file
                
                migration_logger.logger.info(f"🔄 Processing approved request: {csv_filename} → {target_doctype}")
                
                # Find the CSV file in multiple possible locations
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
                    try:
                        request_doc.reload()
                        request_doc.status = 'Failed'
                        request_doc.created_doctype = 'File Not Found'
                        request_doc.save(ignore_permissions=True)
                        frappe.db.commit()
                    except frappe.exceptions.DocumentModifiedError:
                        migration_logger.logger.warning(f"⚠️ Document conflict updating failed status for {request_doc.name}")
                    continue
                
                # Create or confirm DocType exists
                if request_doc.status == 'Approved':
                    try:
                        field_analysis = json.loads(request_doc.field_analysis)
                        created_doctype = mapper.create_doctype_from_analysis(field_analysis, target_doctype)
                        migration_logger.logger.info(f"✅ Created DocType: {created_doctype}")
                        
                        # Update with conflict handling
                        try:
                            request_doc.reload()
                            request_doc.created_doctype = created_doctype
                            target_doctype = created_doctype
                        except frappe.exceptions.DocumentModifiedError:
                            migration_logger.logger.warning(f"⚠️ Document conflict - DocType created but status not updated")
                            target_doctype = created_doctype
                            
                    except Exception as doctype_error:
                        migration_logger.logger.error(f"❌ Failed to create DocType {target_doctype}: {str(doctype_error)}")
                        try:
                            request_doc.reload()
                            request_doc.status = 'Failed'
                            request_doc.created_doctype = f'Creation Failed: {str(doctype_error)[:100]}'
                            request_doc.save(ignore_permissions=True)
                        except frappe.exceptions.DocumentModifiedError:
                            migration_logger.logger.warning(f"⚠️ Document conflict updating error status for {request_doc.name}")
                        continue
                        
                elif request_doc.status == 'Redirected':
                    if not frappe.db.exists('DocType', target_doctype):
                        migration_logger.logger.error(f"❌ Target DocType {target_doctype} does not exist")
                        try:
                            request_doc.reload()
                            request_doc.status = 'Failed'
                            request_doc.created_doctype = 'Target DocType Not Found'
                            request_doc.save(ignore_permissions=True)
                        except frappe.exceptions.DocumentModifiedError:
                            migration_logger.logger.warning(f"⚠️ Document conflict updating redirect error for {request_doc.name}")
                        continue
                    
                    migration_logger.logger.info(f"🔄 Using existing DocType: {target_doctype}")
                    try:
                        request_doc.reload()
                        request_doc.created_doctype = target_doctype
                    except frappe.exceptions.DocumentModifiedError:
                        migration_logger.logger.warning(f"⚠️ Document conflict - using existing DocType but status not updated")
                
                # Process the CSV file
                try:
                    migration_logger.logger.info(f"📄 Processing CSV file: {csv_filename}")
                    
                    df = csv_connector.read_file_as_strings(csv_file_path)
                    if df.empty:
                        migration_logger.logger.warning(f"⚠️ Empty CSV file: {csv_filename}")
                        try:
                            request_doc.reload()
                            request_doc.status = 'Failed'
                            request_doc.created_doctype = 'Empty File'
                            request_doc.save(ignore_permissions=True)
                        except frappe.exceptions.DocumentModifiedError:
                            migration_logger.logger.warning(f"⚠️ Document conflict updating empty file status")
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
                    
                    # FIXED: Update request status with conflict handling
                    try:
                        request_doc.reload()  # Always reload before save
                        request_doc.status = 'Completed'
                        request_doc.processing_results = json.dumps(total_results)
                        request_doc.save(ignore_permissions=True)
                        frappe.db.commit()
                        migration_logger.logger.info(f"✅ Successfully completed request: {request.name}")
                    except frappe.exceptions.DocumentModifiedError:
                        migration_logger.logger.warning(f"⚠️ Document conflict for request {request.name} - processing completed but status not updated")
                    except Exception as save_error:
                        migration_logger.logger.error(f"❌ Failed to update request status: {str(save_error)}")
                    
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
                        request_doc.reload()
                        request_doc.status = 'Failed'
                        request_doc.created_doctype = f'Processing Failed: {str(processing_error)[:100]}'
                        request_doc.save(ignore_permissions=True)
                    except frappe.exceptions.DocumentModifiedError:
                        migration_logger.logger.warning(f"⚠️ Document conflict updating processing error status")
                
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

# Trigger function for document events
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

def on_settings_update():
    """Handle Migration Settings updates"""
    from data_migration_tool.data_migration.utils.logger_config import migration_logger
    migration_logger.logger.info("🔄 Migration Settings updated")

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
