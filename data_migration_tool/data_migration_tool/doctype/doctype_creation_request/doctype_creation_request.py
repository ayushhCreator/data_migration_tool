# Copyright (c) 2025, Ayush and contributors
# Enhanced DocType Creation Request Controller - Phase 1

import frappe
from frappe.model.document import Document
from frappe.utils import now, get_file_size
import json
import os

class DocTypeCreationRequest(Document):
    def before_save(self):
        """FIXED: Ensure proper user assignment and timestamps"""
        # Set current user if not already set
        current_user = frappe.session.user if (hasattr(frappe, 'session') and frappe.session.user != 'Guest') else 'Administrator'
        
        if not self.created_by or self.created_by == 'user':
            self.created_by = current_user
        
        if not self.responded_by or self.responded_by == 'user':
            if self.status in ['Approved', 'Rejected', 'Redirected']:
                self.responded_by = current_user
        
        # Update timestamps
        if self.status != 'Pending' and not self.responded_at and self.has_value_changed('status'):
            self.responded_at = now()
            if not self.responded_by:
                self.responded_by = current_user
        
        if self.status in ['Completed', 'Failed'] and not self.processed_at and self.has_value_changed('status'):
            self.processed_at = now()
    
    def validate(self):
        """Enhanced validation with user field checks and metadata extraction"""
        # Validate user references exist
        if self.created_by and self.created_by != 'Administrator':
            if not frappe.db.exists('User', self.created_by):
                self.created_by = 'Administrator'
        
        if self.responded_by and self.responded_by != 'Administrator':
            if not frappe.db.exists('User', self.responded_by):
                self.responded_by = 'Administrator'
        
        # Extract field metadata and validate source file
        self.extract_field_metadata()
        self.validate_source_file()
        
    def validate_source_file(self):
        """Validate source file exists and is accessible"""
        if not self.source_file:
            frappe.throw("Source file is required")
        
        # Try to find the file and get its details
        file_found = False
        possible_paths = []
        
        # Get Migration Settings for directory paths
        try:
            settings = frappe.get_single('Migration Settings')
            if settings.csv_watch_directory:
                watch_dir = settings.csv_watch_directory
                possible_paths.extend([
                    os.path.join(watch_dir, self.source_file),
                    os.path.join(watch_dir, 'pending', self.source_file),
                    os.path.join(watch_dir, 'staging', self.source_file)
                ])
        except:
            pass
        
        # Add other possible locations
        possible_paths.extend([
            frappe.get_site_path('public', 'files', self.source_file),
            frappe.get_site_path('private', 'files', self.source_file),
            frappe.get_site_path('public', 'files', 'migration', self.source_file)
        ])
        
        for path in possible_paths:
            if os.path.exists(path):
                file_found = True
                self.file_path = path
                try:
                    self.file_size = get_file_size(path)
                except:
                    self.file_size = str(os.path.getsize(path)) + " bytes"
                break
        
        if not file_found:
            frappe.log_error(f"Source file not found: {self.source_file}. Searched paths: {possible_paths}")
    
    def extract_field_metadata(self):
        """Extract and set field metadata from field_analysis"""
        if self.field_analysis:
            try:
                analysis = json.loads(self.field_analysis)
                self.field_count = len(analysis.get('fields', {}))
                self.total_records = analysis.get('total_records', 0)
            except json.JSONDecodeError:
                frappe.log_error(f"Invalid field_analysis JSON for request {self.name}")
                self.field_count = 0
                self.total_records = 0
    
    def on_update(self):
        """ENHANCED: Actions after update - send notifications and trigger processing"""
        # Send notifications based on status changes
        if self.has_value_changed('status'):
            self.send_status_notification()
            
            # 🔥 KEY FIX: Trigger processing when status changes to Approved or Redirected
            if (self.status in ['Approved', 'Redirected'] and 
                not self.get('created_doctype')):
                
                from data_migration_tool.data_migration.utils.logger_config import migration_logger
                migration_logger.logger.info(f"🔄 Status changed to {self.status} - triggering processing for {self.name}")
                
                # Enqueue processing job
                frappe.enqueue(
                    'data_migration_tool.data_migration.utils.scheduler_tasks.check_pending_requests_and_process',
                    queue='long',
                    timeout=3600,
                    job_name=f'process_approved_{self.name}_{frappe.utils.now_datetime().strftime("%H%M%S")}'
                )
                
                migration_logger.logger.info(f"📋 Queued processing job for approved request {self.name}")
    
    def send_status_notification(self):
        """FIXED: Send real-time notifications using SQL query instead of frappe.get_system_managers()"""
        try:
            # Notify the user who made the request
            if self.created_by and self.created_by != 'Administrator':
                message = self.get_status_message()
                frappe.publish_realtime(
                    event='doctype_request_status_update',
                    message={
                        'request_id': self.name,
                        'status': self.status,
                        'message': message,
                        'source_file': self.source_file,
                        'final_doctype': self.final_doctype
                    },
                    user=self.created_by
                )
            
            # FIXED: Get system managers using SQL query instead of frappe.get_system_managers()
            system_managers = frappe.db.sql("""
                SELECT DISTINCT u.name 
                FROM `tabUser` u
                INNER JOIN `tabHas Role` hr ON u.name = hr.parent
                WHERE hr.role = 'System Manager' 
                AND u.enabled = 1 
                AND u.name != 'Guest'
            """, as_dict=True)
            
            for manager in system_managers:
                frappe.publish_realtime(
                    event='doctype_request_status_update',
                    message={
                        'request_id': self.name,
                        'status': self.status,
                        'message': self.get_status_message(),
                        'source_file': self.source_file,
                        'created_by': self.created_by,
                        'final_doctype': self.final_doctype
                    },
                    user=manager.name
                )
        except Exception as e:
            frappe.log_error(f"Failed to send status notification for request {self.name}: {str(e)}")
    
    def get_status_message(self):
        """Get appropriate status message"""
        status_messages = {
            'Approved': f"DocType creation approved for {self.source_file}. Processing will begin shortly.",
            'Rejected': f"DocType creation rejected for {self.source_file}. Reason: {self.rejection_reason or 'No reason provided'}",
            'Redirected': f"File {self.source_file} will be processed using existing DocType: {self.final_doctype}",
            'Completed': f"Successfully processed {self.source_file} into DocType: {self.created_doctype}",
            'Failed': f"Failed to process {self.source_file}. Error: {self.error_message or 'Unknown error'}"
        }
        
        return status_messages.get(self.status, f"Status updated to {self.status}")
    
    @frappe.whitelist()
    def retry_processing(self):
        """Retry processing this request"""
        if self.status not in ['Failed', 'Rejected']:
            frappe.throw("Can only retry failed or rejected requests")
        
        # Reset status to pending
        self.status = 'Pending'
        self.error_message = None
        self.processed_at = None
        self.processing_results = None
        self.save(ignore_permissions=True)
        
        # Trigger processing
        frappe.enqueue(
            'data_migration_tool.data_migration.utils.scheduler_tasks.check_pending_requests_and_process',
            queue='long',
            timeout=3600
        )
        
        return {"message": "Request queued for retry processing"}
    
    @frappe.whitelist()
    def get_field_preview(self):
        """Get field preview for UI display"""
        if not self.field_analysis:
            return {"fields": []}
        
        try:
            analysis = json.loads(self.field_analysis)
            fields = analysis.get('fields', {})
            
            preview = []
            for field_name, field_info in fields.items():
                preview.append({
                    'original_name': field_info.get('original_name', field_name),
                    'clean_name': field_info.get('clean_name', field_name),
                    'suggested_type': field_info.get('suggested_type', 'Data'),
                    'sample_values': field_info.get('sample_values', []),
                    'max_length': field_info.get('max_length', 0)
                })
            
            return {"fields": preview}
            
        except json.JSONDecodeError:
            return {"fields": [], "error": "Invalid field analysis data"}
    
    def get_dashboard_data(self):
        """Get dashboard data for this request"""
        return {
            'fieldname': 'doctype_creation_request',
            'transactions': [
                {
                    'label': 'Migration Buffer Records',
                    'items': ['Migration Data Buffer']
                }
            ]
        }
# 🔥 KEY FIX: Added doc_events hook to trigger processing on status change to Approved or Redirected