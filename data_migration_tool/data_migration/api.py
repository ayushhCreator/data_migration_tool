# Copyright (c) 2025, Ayush and contributors
# Enhanced API endpoints for Phase 1

import frappe
import json
from frappe import _
from frappe.utils import now
from frappe.model.document import Document

@frappe.whitelist()
def handle_doctype_creation_response(request_id, action, target_doctype=None, rejection_reason=None):
    """FIXED: Handle approval and auto-trigger processing"""
    from data_migration_tool.data_migration.utils.logger_config import migration_logger
    
    try:
        # Set proper user context
        if not frappe.session.user or frappe.session.user == 'Guest':
            frappe.set_user('Administrator')
        
        # Get the request document
        request_doc = frappe.get_doc('DocType Creation Request', request_id)
        
        if request_doc.status != 'Pending':
            return {"status": "error", "message": f"Request already processed (Status: {request_doc.status})"}
        
        # Update request based on user action
        current_user = frappe.session.user
        request_doc.user_response = action
        request_doc.responded_at = now()
        request_doc.responded_by = current_user
        
        if action == 'Approve':
            request_doc.status = 'Approved'
            request_doc.final_doctype = target_doctype or request_doc.suggested_doctype
            
        elif action == 'Redirect':
            if not target_doctype or not frappe.db.exists('DocType', target_doctype):
                return {"status": "error", "message": "Invalid target DocType"}
            request_doc.status = 'Redirected'
            request_doc.final_doctype = target_doctype
            
        elif action == 'Reject':
            request_doc.status = 'Rejected'
            request_doc.rejection_reason = rejection_reason or 'User rejected DocType creation'
        
        # Save the request
        request_doc.save(ignore_permissions=True)
        frappe.db.commit()
        
        migration_logger.logger.info(f"✅ {action} request {request_id} by user {current_user}")
        
        # 🔥 KEY FIX: Auto-trigger processing for approved/redirected requests
        if action in ['Approve', 'Redirect']:
            migration_logger.logger.info(f"🚀 Triggering automatic processing for approved request {request_id}")
            
            # Enqueue background job to process approved requests immediately
            frappe.enqueue(
                'data_migration_tool.data_migration.utils.scheduler_tasks.check_pending_requests_and_process',
                queue='long',
                timeout=3600,
                job_name=f'process_approved_{request_id}_{frappe.utils.now_datetime().strftime("%Y%m%d_%H%M%S")}'
            )
            
            migration_logger.logger.info(f"📋 Queued processing job for request {request_id}")
        
        # Send real-time notification
        frappe.publish_realtime(
            event='doctype_request_approved',
            message={
                'request_id': request_id,
                'final_doctype': request_doc.final_doctype,
                'source_file': request_doc.source_file,
                'action': action,
                'processing_triggered': action in ['Approve', 'Redirect']
            },
            user=current_user
        )
        
        return {
            "status": "success",
            "message": f"Request {action.lower()}ed successfully. Processing will begin shortly." if action in ['Approve', 'Redirect'] else f"Request {action.lower()}ed.",
            "final_doctype": getattr(request_doc, 'final_doctype', None)
        }
        
    except Exception as e:
        migration_logger.logger.error(f"❌ Error handling approval: {str(e)}")
        return {"status": "error", "message": f"Internal error: {str(e)}"}

@frappe.whitelist()
def get_pending_doctype_requests():
    """ENHANCED: Get all pending DocType creation requests with better formatting"""
    try:
        requests = frappe.get_all(
            'DocType Creation Request',
            filters={'status': 'Pending'},
            fields=[
                'name', 'source_file', 'suggested_doctype', 'created_at', 
                'field_analysis', 'status'
            ],
            order_by='created_at desc'
        )
        
        # Parse field analysis for each request
        for request in requests:
            try:
                if request.field_analysis:
                    field_analysis = json.loads(request.field_analysis)
                    request['field_count'] = len(field_analysis.get('fields', {}))
                    request['sample_fields'] = list(field_analysis.get('fields', {}).keys())[:5]
                    request['total_records'] = field_analysis.get('total_records', 0)
                else:
                    request['field_count'] = 0
                    request['sample_fields'] = []
                    request['total_records'] = 0
            except Exception as parse_error:
                frappe.log_error(f"Failed to parse field analysis for request {request.name}: {str(parse_error)}")
                request['field_count'] = 0
                request['sample_fields'] = []
                request['total_records'] = 0
            
            # Format datetime
            if request.created_at:
                request['created_at_formatted'] = frappe.utils.format_datetime(request.created_at)
        
        return {"status": "success", "requests": requests, "count": len(requests)}
        
    except Exception as e:
        frappe.log_error(f"Error getting pending DocType requests: {str(e)}")
        return {"status": "error", "message": str(e)}

@frappe.whitelist()
def get_existing_doctypes(search_term=None):
    """ENHANCED: Get list of existing DocTypes for redirect option with search"""
    try:
        filters = {'custom': 0, 'istable': 0}
        
        if search_term:
            filters['name'] = ['like', f'%{search_term}%']
        
        doctypes = frappe.get_all(
            'DocType',
            filters=filters,
            fields=['name', 'module', 'description'],
            order_by='name',
            limit_page_length=50
        )
        
        return {"status": "success", "doctypes": doctypes}
        
    except Exception as e:
        return {"status": "error", "message": str(e)}

@frappe.whitelist()
def get_migration_status():
    """NEW: Get comprehensive migration status information"""
    try:
        # Get buffer statistics
        buffer_stats = frappe.db.sql("""
            SELECT 
                processing_status,
                COUNT(*) as count,
                target_doctype
            FROM `tabMigration Data Buffer`
            GROUP BY processing_status, target_doctype
            ORDER BY target_doctype, processing_status
        """, as_dict=True)
        
        # Get recent requests
        recent_requests = frappe.get_all(
            'DocType Creation Request',
            fields=['name', 'source_file', 'status', 'created_at', 'final_doctype'],
            order_by='created_at desc',
            limit_page_length=10
        )
        
        # Get settings info
        settings = frappe.get_single('Migration Settings')
        settings_info = {
            'csv_processing_enabled': settings.enable_csv_processing,
            'zoho_sync_enabled': settings.enable_zoho_sync,
            'odoo_sync_enabled': settings.enable_odoo_sync,
            'last_sync_time': settings.last_sync_time,
            'csv_watch_directory': settings.csv_watch_directory
        }
        
        return {
            "status": "success",
            "buffer_stats": buffer_stats,
            "recent_requests": recent_requests,
            "settings": settings_info
        }
        
    except Exception as e:
        return {"status": "error", "message": str(e)}

@frappe.whitelist()
def test_connection(source):
    """ENHANCED: Test connection to external systems"""
    try:
        settings = frappe.get_single('Migration Settings')
        
        if source == 'zoho':
            if not settings.enable_zoho_sync:
                return {"status": "error", "message": "Zoho sync is not enabled"}
            
            from data_migration_tool.data_migration.connectors.zoho_connector import ZohoConnector
            from data_migration_tool.data_migration.utils.logger_config import migration_logger
            
            zoho = ZohoConnector(migration_logger)
            return zoho.test_connection()
            
        elif source == 'odoo':
            if not settings.enable_odoo_sync:
                return {"status": "error", "message": "Odoo sync is not enabled"}
            
            from data_migration_tool.data_migration.connectors.odoo_connector import OdooConnector  
            from data_migration_tool.data_migration.utils.logger_config import migration_logger
            
            odoo = OdooConnector(migration_logger)
            return odoo.test_connection()
            
        elif source == 'csv':
            return settings.test_csv_directory()
            
        else:
            return {"status": "error", "message": "Invalid source specified"}
            
    except Exception as e:
        return {"status": "error", "message": str(e)}

@frappe.whitelist()
def trigger_manual_sync(source=None):
    """ENHANCED: Trigger manual synchronization with better job management"""
    try:
        if not frappe.has_permission("Migration Settings", "write"):
            return {"status": "error", "message": "Insufficient permissions"}
        
        settings = frappe.get_single('Migration Settings')
        job_name = f"manual_sync_{source or 'all'}_{frappe.utils.now_datetime().strftime('%Y%m%d_%H%M%S')}"
        
        job_params = {
            "queue": "long",
            "timeout": 7200,  # 2 hours
            "job_name": job_name
        }
        
        if source == "zoho" and settings.enable_zoho_sync:
            frappe.enqueue(
                'data_migration_tool.data_migration.utils.scheduler_tasks.sync_zoho_data',
                settings=settings,
                **job_params
            )
            return {"status": "success", "message": "Zoho sync started", "job_name": job_name}
            
        elif source == "odoo" and settings.enable_odoo_sync:
            frappe.enqueue(
                'data_migration_tool.data_migration.utils.scheduler_tasks.sync_odoo_data',
                settings=settings,
                **job_params
            )
            return {"status": "success", "message": "Odoo sync started", "job_name": job_name}
            
        elif source == "csv" and settings.enable_csv_processing:
            frappe.enqueue(
                'data_migration_tool.data_migration.utils.scheduler_tasks.process_csv_files_with_jit',
                **job_params
            )
            return {"status": "success", "message": "CSV processing started", "job_name": job_name}
            
        else:
            # Trigger all enabled syncs
            frappe.enqueue(
                'data_migration_tool.data_migration.utils.scheduler_tasks.periodic_crm_sync',
                **job_params
            )
            return {"status": "success", "message": "Full sync started", "job_name": job_name}
            
    except Exception as e:
        return {"status": "error", "message": str(e)}

@frappe.whitelist()
def upload_csv_file():
    """NEW: Handle direct CSV file upload via UI"""
    try:
        if not frappe.has_permission("Migration Settings", "write"):
            return {"status": "error", "message": "Insufficient permissions"}
        
        # This would handle file upload from the UI
        # Implementation depends on your specific upload mechanism
        return {"status": "success", "message": "File upload handler - Phase 2 implementation"}
        
    except Exception as e:
        return {"status": "error", "message": str(e)}


@frappe.whitelist()
def get_job_status(job_name):
    """Get background job status - whitelisted version"""
    try:
        if not frappe.has_permission("Migration Settings", "read"):
            return {"status": "error", "message": "Insufficient permissions"}
        
        # Get job status from RQ Job table
        job_status = frappe.db.get_value("RQ Job", {"job_name": job_name}, ["status", "exc_info"])
        
        if job_status:
            status, exc_info = job_status
            return {
                "status": "success", 
                "job_status": status,
                "error": exc_info if status == "failed" else None
            }
        else:
            return {"status": "error", "message": "Job not found"}
            
    except Exception as e:
        return {"status": "error", "message": str(e)}

@frappe.whitelist() 
def get_buffer_statistics():
    """Get buffer statistics - whitelisted version"""
    try:
        if not frappe.has_permission("Migration Settings", "read"):
            return {"status": "error", "message": "Insufficient permissions"}
            
        from data_migration_tool.data_migration.connectors.csv_connector import CSVConnector
        from data_migration_tool.data_migration.utils.logger_config import migration_logger
        
        csv_connector = CSVConnector(migration_logger)
        stats = csv_connector.get_buffer_statistics()
        
        return {"status": "success", "data": stats}
        
    except Exception as e:
        return {"status": "error", "message": str(e)}
