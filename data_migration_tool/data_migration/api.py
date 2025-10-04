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
        
        migration_logger.logger.info(f"âœ… {action} request {request_id} by user {current_user}")
        
        # ðŸ”¥ KEY FIX: Auto-trigger processing for approved/redirected requests
        if action in ['Approve', 'Redirect']:
            migration_logger.logger.info(f"ðŸš€ Triggering automatic processing for approved request {request_id}")
            
            # Enqueue background job to process approved requests immediately
            frappe.enqueue(
                'data_migration_tool.data_migration.utils.scheduler_tasks.check_pending_requests_and_process',
                queue='long',
                timeout=3600,
                job_name=f'process_approved_{request_id}_{frappe.utils.now_datetime().strftime("%Y%m%d_%H%M%S")}'
            )
            
            migration_logger.logger.info(f"ðŸ“‹ Queued processing job for request {request_id}")
        
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
        migration_logger.logger.error(f"âŒ Error handling approval: {str(e)}")
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
@frappe.whitelist()
def get_existing_doctypes(search_term=None, include_custom=True, include_standard=True):
    """ENHANCED: Get list of existing DocTypes for redirect option with comprehensive filtering"""
    try:
        filters = {'istable': 0}  # Don't include child tables
        
        # Build dynamic filters
        if not include_custom:
            filters['custom'] = 0
        if not include_standard:
            filters['custom'] = 1
            
        if search_term:
            # Enhanced search - check both name and description
            search_conditions = [
                ['name', 'like', f'%{search_term}%'],
                ['description', 'like', f'%{search_term}%']
            ]
            # Use OR condition for search
            sql_query = """
                SELECT name, module, description, custom
                FROM `tabDocType` 
                WHERE istable = 0 
                AND (name LIKE %(search)s OR COALESCE(description, '') LIKE %(search)s)
                {custom_filter}
                ORDER BY custom DESC, name ASC
                LIMIT 100
            """
            
            custom_filter = ""
            if not include_custom:
                custom_filter = "AND custom = 0"
            elif not include_standard:  
                custom_filter = "AND custom = 1"
                
            doctypes = frappe.db.sql(
                sql_query.format(custom_filter=custom_filter),
                {'search': f'%{search_term}%'},
                as_dict=True
            )
        else:
            doctypes = frappe.get_all(
                'DocType',
                filters=filters,
                fields=['name', 'module', 'description', 'custom'],
                order_by='custom desc, name',
                limit_page_length=100
            )
        
        # Enhance with additional metadata
        enhanced_doctypes = []
        for dt in doctypes:
            # Check if DocType has any records
            try:
                record_count = frappe.db.count(dt.name)
            except:
                record_count = 0
                
            enhanced_doctypes.append({
                'name': dt.name,
                'label': dt.name,  # For frontend display
                'module': dt.module,
                'description': dt.description or '',
                'custom': dt.custom,
                'record_count': record_count,
                'type': 'Custom' if dt.custom else 'Standard'
            })
        
        # Group by type for better UI
        result = {
            'doctypes': enhanced_doctypes,
            'grouped': {
                'custom': [dt for dt in enhanced_doctypes if dt['custom']],
                'standard': [dt for dt in enhanced_doctypes if not dt['custom']]
            },
            'total': len(enhanced_doctypes)
        }

        return {"status": "success", "data": result}

    except Exception as e:
        frappe.log_error(f"Error getting existing DocTypes: {str(e)}")
        return {"status": "error", "message": str(e)}
    
# Add new API endpoint for schema suggestions
@frappe.whitelist() 
def suggest_similar_doctypes(source_file, csv_headers_json):
    """NEW: Suggest similar DocTypes based on CSV structure"""
    try:
        import json
        csv_headers = json.loads(csv_headers_json) if isinstance(csv_headers_json, str) else csv_headers_json
        
        # Get all custom DocTypes with their fields
        custom_doctypes = frappe.get_all(
            'DocType',
            filters={'custom': 1, 'istable': 0},
            fields=['name']
        )
        
        suggestions = []
        for dt in custom_doctypes:
            try:
                meta = frappe.get_meta(dt.name)
                doctype_fields = [f.fieldname for f in meta.fields if f.fieldtype not in ['Section Break', 'Column Break', 'Tab Break']]
                
                # Calculate field similarity
                similarity = calculate_header_similarity(csv_headers, doctype_fields)
                if similarity > 0.3:  # 30% minimum similarity
                    suggestions.append({
                        'doctype': dt.name,
                        'similarity': round(similarity * 100, 1),
                        'matching_fields': len(set(csv_headers).intersection(set(doctype_fields))),
                        'total_fields': len(doctype_fields),
                        'csv_fields': len(csv_headers)
                    })
            except:
                continue
        
        # Sort by similarity
        suggestions.sort(key=lambda x: x['similarity'], reverse=True)
        
        return {
            "status": "success", 
            "suggestions": suggestions[:10],  # Top 10 matches
            "source_file": source_file
        }
        
    except Exception as e:
        return {"status": "error", "message": str(e)}

@frappe.whitelist()
def add_hash_field_to_doctype(doctype_name):
    """
    API to add row_hash field and mark ID field as unique for existing DocTypes
    """
    try:
        if not frappe.db.exists('DocType', doctype_name):
            return {"status": "error", "message": f"DocType {doctype_name} does not exist"}
        
        doc = frappe.get_doc('DocType', doctype_name)
        
        # Check if row_hash field exists
        hash_exists = any(f.fieldname == 'row_hash' for f in doc.fields)
        
        if not hash_exists:
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
        
        # Find and mark ID field as unique
        id_field_updated = False
        for field in doc.fields:
            field_lower = field.fieldname.lower()
            if any(pattern in field_lower for pattern in ['id', '_id', 'code', 'reference']) and not field.unique:
                field.unique = 1
                id_field_updated = True
                frappe.logger().info(f"✅ Marked '{field.fieldname}' as unique in {doctype_name}")
                break
        
        doc.save(ignore_permissions=True)
        frappe.db.commit()
        
        # Clear cache
        frappe.clear_cache(doctype=doctype_name)
        frappe.get_meta(doctype_name, cached=False)
        
        return {
            "status": "success",
            "message": f"Enhanced {doctype_name} with hash field and unique constraints",
            "hash_added": not hash_exists,
            "id_field_updated": id_field_updated
        }
        
    except Exception as e:
        frappe.db.rollback()
        return {"status": "error", "message": str(e)}


@frappe.whitelist()
def backfill_hashes_for_doctype(doctype_name):
    """
    Backfill row_hash values for existing records in a DocType
    This is useful after adding hash field to existing DocTypes
    """
    try:
        from data_migration_tool.data_migration.connectors.csv_connector import CSVConnector
        from data_migration_tool.data_migration.utils.logger_config import migration_logger
        
        if not frappe.db.exists('DocType', doctype_name):
            return {"status": "error", "message": f"DocType {doctype_name} does not exist"}
        
        # Check if row_hash field exists
        meta = frappe.get_meta(doctype_name)
        if not any(f.fieldname == 'row_hash' for f in meta.fields):
            return {"status": "error", "message": f"row_hash field does not exist in {doctype_name}. Add it first."}
        
        # Get all records without hash
        records = frappe.get_all(doctype_name, filters={'row_hash': ['in', ['', None]]}, limit_page_length=0)
        
        if not records:
            return {"status": "success", "message": "All records already have hashes"}
        
        csv_connector = CSVConnector(migration_logger)
        updated_count = 0
        
        for idx, record in enumerate(records, 1):
            try:
                doc = frappe.get_doc(doctype_name, record.name)
                
                # Create hash from all field values
                row_data = {}
                for field in meta.fields:
                    if field.fieldname not in ['name', 'owner', 'creation', 'modified', 'modified_by', 'row_hash']:
                        value = getattr(doc, field.fieldname, None)
                        if value is not None:
                            row_data[field.fieldname] = value
                
                # Compute and update hash with row number for uniqueness
                row_hash = csv_connector.compute_stable_hash(row_data, idx)
                doc.row_hash = row_hash
                doc.save(ignore_permissions=True)
                updated_count += 1
                
            except Exception as e:
                frappe.logger().error(f"Failed to update hash for {record.name}: {str(e)}")
                continue
        
        frappe.db.commit()
        
        return {
            "status": "success",
            "message": f"Backfilled hashes for {updated_count}/{len(records)} records",
            "updated": updated_count,
            "total": len(records)
        }
        
    except Exception as e:
        frappe.db.rollback()
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



@frappe.whitelist()
def get_product_catalog():
    """Get complete product catalog with relationships"""
    try:
        products = frappe.get_all("Product", 
            fields=["name", "product_name", "service_category", "vehicle_type", 
                   "service_type", "one_time_price", "is_active"],
            filters={"is_active": 1}
        )
        
        return {"status": "success", "data": products}
        
    except Exception as e:
        return {"status": "error", "message": str(e)}
    

@frappe.whitelist()
def get_processing_status(doctype_name):
    """Get real-time processing status"""
    try:
        # Get buffer statistics for specific DocType
        stats = frappe.db.sql("""
            SELECT 
                processing_status,
                COUNT(*) as count
            FROM `tabMigration Data Buffer`
            WHERE target_doctype = %s
            GROUP BY processing_status
        """, doctype_name, as_dict=True)
        
        result = {status['processing_status']: status['count'] for status in stats}
        
        return {
            "status": "success",
            "data": result,
            "total": sum(result.values()),
            "doctype": doctype_name
        }
        
    except Exception as e:
        return {"status": "error", "message": str(e)}


# Add this function to your api.py file

@frappe.whitelist()
def manual_csv_processing():
    """Manual CSV processing trigger - whitelisted version"""
    try:
        if not frappe.has_permission("Migration Settings", "write"):
            return {"status": "error", "message": "Insufficient permissions"}
        
        from data_migration_tool.data_migration.utils.scheduler_tasks import process_csv_files_with_jit
        
        # Enqueue the processing job
        frappe.enqueue(
            process_csv_files_with_jit,
            queue='long',
            timeout=3600,
            job_name=f'manual_csv_processing_{frappe.utils.now_datetime().strftime("%Y%m%d_%H%M%S")}'
        )
        
        return {
            "status": "success",
            "message": "CSV processing started manually"
        }
        
    except Exception as e:
        frappe.log_error(f"Manual CSV processing failed: {str(e)}")
        return {"status": "error", "message": str(e)}
    

@frappe.whitelist()
def get_import_statistics(source_file=None):

    """Get comprehensive import statistics"""
    try:
        conditions = ["1=1"]
        params = []

        if source_file:
            conditions.append("source_file = %s")
            params.append(source_file)

        where_clause = " AND ".join(conditions)

        stats = frappe.db.sql(f"""
            SELECT 
                doctype,
                source_file,
                COUNT(*) as total_imports,
                COUNT(DISTINCT row_hash) as unique_rows,
                MIN(import_timestamp) as first_import,
                MAX(import_timestamp) as latest_import
            FROM `tabImport Log`
            WHERE {where_clause}
            GROUP BY doctype, source_file
            ORDER BY latest_import DESC
        """, params, as_dict=True)

        summary = {
            "import_sessions": len(set([s.get('source_file') for s in stats])),
            "total_records": sum([s.get('total_imports', 0) for s in stats]),
            "by_doctype": {},
            "recent_imports": stats[:10]
        }

        # Group by DocType
        for stat in stats:
            doctype = stat.get('doctype')
            if doctype not in summary["by_doctype"]:
                summary["by_doctype"][doctype] = {
                    "total": 0,
                    "files": 0,
                    "latest": None
                }

            summary["by_doctype"][doctype]["total"] += stat.get('total_imports', 0)
            summary["by_doctype"][doctype]["files"] += 1

            latest = stat.get('latest_import')
            if not summary["by_doctype"][doctype]["latest"] or latest > summary["by_doctype"][doctype]["latest"]:
                summary["by_doctype"][doctype]["latest"] = latest

        return {"status": "success", "data": summary}

    except Exception as e:
        return {"status": "error", "message": str(e)}
    

@frappe.whitelist()
def process_csv_intelligent(file_path: str, confidence_threshold: float = None, force_auto: bool = False):
    """
    🧠 INTELLIGENT API: Process CSV with configurable workflow
    
    This endpoint respects Migration Settings and provides intelligent processing:
    
    Args:
        file_path: Path to the CSV file
        confidence_threshold: Optional threshold for DocType matching (0.0-1.0)
        force_auto: Force automated workflow regardless of settings
    
    Workflow:
    1. Check Migration Settings.auto_create_doctypes (unless force_auto=True)
    2. If auto_create_doctypes=True: Use automated workflow
    3. If auto_create_doctypes=False: Use manual approval workflow
    4. Return appropriate results based on chosen workflow
    
    Returns:
        dict: Results appropriate for the chosen workflow
    """
    try:
        from data_migration_tool.data_migration.connectors.csv_connector import CSVConnector
        from data_migration_tool.data_migration.utils.logger_config import migration_logger
        
        migration_logger.logger.info(f"🧠 Starting intelligent CSV processing for: {file_path}")
        
        # Get migration settings
        settings = None
        auto_create_enabled = True  # Default to automated
        try:
            settings = frappe.get_single("Migration Settings")
            auto_create_enabled = getattr(settings, 'auto_create_doctypes', True)
        except Exception as e:
            migration_logger.logger.warning(f"Could not get Migration Settings: {str(e)}")
        
        # Override with force_auto parameter
        if force_auto:
            auto_create_enabled = True
            migration_logger.logger.info("🔧 Force auto mode enabled - using automated workflow")
        
        # Override confidence threshold if provided
        if confidence_threshold is not None and settings:
            if hasattr(settings, 'doctype_match_threshold'):
                settings.doctype_match_threshold = confidence_threshold * 100
        
        if auto_create_enabled:
            # Use automated workflow
            migration_logger.logger.info("🤖 Using automated DocType detection and import workflow")
            
            connector = CSVConnector(logger=migration_logger)
            result = connector.auto_detect_and_import(file_path, settings)
            
            if result['success']:
                migration_logger.logger.info(f"✅ Automated processing completed: {result['target_doctype']} - {result['action_taken']}")
            else:
                migration_logger.logger.error(f"❌ Automated processing failed: {result.get('error', 'Unknown error')}")
            
            return {
                'status': 'success' if result['success'] else 'error',
                'workflow': 'automated',
                'message': _('CSV processed automatically') if result['success'] else result.get('error', _('Unknown error')),
                'data': result
            }
            
        else:
            # Use manual approval workflow
            migration_logger.logger.info("📝 Using manual DocType creation request workflow")
            
            # Create DocType creation request
            from data_migration_tool.data_migration.utils.scheduler_tasks import send_doctype_creation_request, clean_doctype_name
            from data_migration_tool.data_migration.mappers.doctype_creator import DynamicDocTypeCreator
            from pathlib import Path
            
            # Read and analyze CSV
            connector = CSVConnector(logger=migration_logger)
            df = connector.read_file_as_strings(file_path)
            
            if df.empty:
                return {
                    'status': 'error',
                    'workflow': 'manual',
                    'message': _('CSV file is empty'),
                    'data': {}
                }
            
            # Analyze structure
            mapper = DynamicDocTypeCreator(migration_logger)
            analysis = mapper.analyze_csv_structure(df)
            filename = Path(file_path).name
            suggested_doctype = clean_doctype_name(filename)
            
            # Create request
            request_id = send_doctype_creation_request(filename, suggested_doctype, analysis)
            
            migration_logger.logger.info(f"📝 Created DocType creation request: {request_id}")
            
            return {
                'status': 'success',
                'workflow': 'manual',
                'message': _('DocType creation request submitted for approval'),
                'data': {
                    'request_id': request_id,
                    'suggested_doctype': suggested_doctype,
                    'requires_approval': True,
                    'csv_analysis': {
                        'total_rows': len(df),
                        'total_columns': len(df.columns),
                        'headers': list(df.columns),
                        'file_name': filename
                    }
                }
            }
        
    except Exception as e:
        frappe.log_error(f"Intelligent CSV processing failed: {str(e)}", "CSV Intelligent Processing Error")
        return {
            'status': 'error',
            'workflow': 'unknown',
            'message': f"Processing failed: {str(e)}",
            'data': {}
        }

@frappe.whitelist()
def process_csv_with_auto_detection(file_path: str, confidence_threshold: float = None):
    """
    🎯 AUTOMATED API: Process CSV with automatic DocType detection and import
    
    This endpoint always uses automated workflow regardless of settings:
    1. Analyzes CSV headers
    2. Checks for existing DocTypes with matching headers  
    3. If match found: Import data directly into existing DocType
    4. If no match: Create new DocType automatically and import
    5. Returns comprehensive results
    
    Args:
        file_path: Path to the CSV file
        confidence_threshold: Optional threshold for DocType matching (0.0-1.0)
    
    Returns:
        dict: Complete import results with action taken
    """
    # Call the intelligent processor with force_auto=True
    result = process_csv_intelligent(file_path, confidence_threshold, force_auto=True)
    return result

@frappe.whitelist()
def process_csv_with_manual_request(file_path: str, confidence_threshold: float = None):
    """
    📝 MANUAL API: Process CSV with manual DocType creation request
    
    This endpoint always creates a DocType Creation Request regardless of settings.
    Use this when you want to force manual approval workflow.
    
    Args:
        file_path: Path to the CSV file
        confidence_threshold: Optional threshold for DocType matching (0.0-1.0)
    
    Returns:
        dict: DocType creation request details
    """
    try:
        from data_migration_tool.data_migration.connectors.csv_connector import CSVConnector
        from data_migration_tool.data_migration.utils.logger_config import migration_logger
        from data_migration_tool.data_migration.utils.scheduler_tasks import send_doctype_creation_request, clean_doctype_name
        from data_migration_tool.data_migration.mappers.doctype_creator import DynamicDocTypeCreator
        from pathlib import Path
        
        migration_logger.logger.info(f"📝 Starting manual DocType creation request for: {file_path}")
        
        # Initialize connector
        connector = CSVConnector(logger=migration_logger)
        
        # Get migration settings if available
        settings = None
        try:
            settings = frappe.get_single("Migration Settings")
        except:
            pass
        
        # Override confidence threshold if provided
        if confidence_threshold is not None and settings:
            if hasattr(settings, 'doctype_match_threshold'):
                settings.doctype_match_threshold = confidence_threshold * 100
        
        # Read and analyze CSV
        df = connector.read_file_as_strings(file_path)
        
        if df.empty:
            return {
                'status': 'error',
                'message': _('CSV file is empty'),
                'data': {}
            }
        
        # Analyze structure
        mapper = DynamicDocTypeCreator(migration_logger)
        analysis = mapper.analyze_csv_structure(df)
        filename = Path(file_path).name
        suggested_doctype = clean_doctype_name(filename)
        
        # Check for existing request
        existing_request = frappe.db.get_value(
            'DocType Creation Request',
            {'source_file': filename, 'status': ['in', ['Pending', 'Approved', 'Redirected']]},
            'name'
        )
        
        if existing_request:
            migration_logger.logger.info(f"⏳ Request already exists for {filename}: {existing_request}")
            return {
                'status': 'info',
                'message': _('DocType creation request already exists for this file'),
                'data': {
                    'request_id': existing_request,
                    'suggested_doctype': suggested_doctype,
                    'requires_approval': True,
                    'csv_analysis': {
                        'total_rows': len(df),
                        'total_columns': len(df.columns),
                        'headers': list(df.columns),
                        'file_name': filename
                    }
                }
            }
        
        # Create new request
        request_id = send_doctype_creation_request(filename, suggested_doctype, analysis)
        
        migration_logger.logger.info(f"📝 Created DocType creation request: {request_id}")
        
        return {
            'status': 'success',
            'message': _('DocType creation request submitted for approval'),
            'data': {
                'request_id': request_id,
                'suggested_doctype': suggested_doctype,
                'requires_approval': True,
                'csv_analysis': {
                    'total_rows': len(df),
                    'total_columns': len(df.columns),
                    'headers': list(df.columns),
                    'file_name': filename
                }
            }
        }
        
    except Exception as e:
        frappe.log_error(f"Manual CSV request failed: {str(e)}", "CSV Manual Request Error")
        return {
            'status': 'error',
            'message': f"Request creation failed: {str(e)}",
            'data': {}
        }
        
        # Override confidence threshold if provided
        if confidence_threshold is not None:
            if hasattr(settings, 'doctype_match_threshold'):
                settings.doctype_match_threshold = confidence_threshold * 100
        
        # Process with auto-detection
        result = connector.auto_detect_and_import(file_path, settings)
        
        # Return structured response
        return {
            'status': 'success' if result['success'] else 'error',
            'message': 'CSV processed successfully' if result['success'] else result.get('error', 'Unknown error'),
            'data': result
        }
        
    except Exception as e:
        frappe.log_error(f"CSV auto-detection failed: {str(e)}", "CSV Auto-Detection Error")
        return {
            'status': 'error',
            'message': f"Processing failed: {str(e)}",
            'data': {}
        }
