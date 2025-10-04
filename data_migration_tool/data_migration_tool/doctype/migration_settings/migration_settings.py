# Copyright (c) 2025, Ayush and contributors
# For license information, please see license.txt

import frappe
import os
from frappe.model.document import Document, flt
from frappe.utils import now, cint
from typing import Dict, Any

class MigrationSettings(Document):
    """Enhanced Migration Settings with JIT processing support"""
    
    def validate(self):
        """Comprehensive validation of migration settings"""
        # Add configuration validation
        try:
            from data_migration_tool.data_migration.utils.config_validator import ConfigValidator
            
            config_errors = ConfigValidator.validate_migration_settings()
            if config_errors:
                frappe.throw("<br>".join(config_errors))
                
        except ImportError:
            # Fallback to original validation if utils not available
            pass
        
        self.validate_basic_settings()
        # self.validate_zoho_config()
        self.validate_csv_config()
        # self.validate_odoo_config()
        self.validate_performance_settings()
        self.validate_auto_detection_settings()  # NE
        # Removed validate_jit_settings() - causing issues with non-existent fields

    def validate_basic_settings(self):
        """Validate basic configuration settings"""
        if self.csv_chunk_size:
            chunk_size = cint(self.csv_chunk_size)
            if chunk_size < 100:
                frappe.throw("CSV Chunk Size must be at least 100 records")
            elif chunk_size > 10000:
                frappe.throw("CSV Chunk Size should not exceed 10,000 records")
            self.csv_chunk_size = chunk_size
        
        if self.max_concurrent_jobs:
            max_jobs = cint(self.max_concurrent_jobs)
            if max_jobs < 1:
                frappe.throw("Maximum Concurrent Jobs must be at least 1")
            elif max_jobs > 20:
                frappe.throw("Maximum Concurrent Jobs should not exceed 20")
            self.max_concurrent_jobs = max_jobs

    # def validate_zoho_config(self):
    #     """Validate Zoho configuration"""
    #     if self.enable_zoho_sync:
    #         required_fields = [
    #             ('zoho_client_id', 'Zoho Client ID'),
    #             ('zoho_client_secret', 'Zoho Client Secret'),
    #             ('zoho_refresh_token', 'Zoho Refresh Token')
    #         ]
            
    #         for field, label in required_fields:
    #             if not getattr(self, field, None):
    #                 frappe.throw(f"{label} is required when Zoho sync is enabled")

    def validate_csv_config(self):
        """Enhanced CSV configuration validation"""
        if self.enable_csv_processing:
            if not self.csv_watch_directory:
                frappe.throw("CSV Watch Directory is required when CSV processing is enabled")
            
            try:
                # Expand user path and make absolute
                watch_dir = os.path.expanduser(self.csv_watch_directory)
                self.csv_watch_directory = os.path.abspath(watch_dir)
                
                # Create directory structure
                self.setup_csv_directories()
                
                # Validate permissions
                self.validate_directory_permissions()
                
            except PermissionError:
                frappe.throw(f"Permission denied: Cannot access {self.csv_watch_directory}")
            except Exception as e:
                frappe.throw(f"Cannot setup CSV directory: {str(e)}")

    def setup_csv_directories(self):
        """Create complete CSV directory structure"""
        base_dir = self.csv_watch_directory
        subdirs = ['processed', 'errors', 'backup']
        
        # Create main directory if it doesn't exist
        if not os.path.exists(base_dir):
            os.makedirs(base_dir, exist_ok=True)
        
        # Create subdirectories
        for subdir in subdirs:
            dir_path = os.path.join(base_dir, subdir)
            if not os.path.exists(dir_path):
                os.makedirs(dir_path, exist_ok=True)
        
        # Create .gitkeep files
        for subdir in subdirs:
            gitkeep_path = os.path.join(base_dir, subdir, '.gitkeep')
            if not os.path.exists(gitkeep_path):
                with open(gitkeep_path, 'w') as f:
                    f.write('# Keep this directory in version control\n')

    def validate_directory_permissions(self):
        """Validate directory read/write permissions"""
        try:
            # Test write permission
            test_file = os.path.join(self.csv_watch_directory, '.permission_test')
            with open(test_file, 'w') as f:
                f.write('Permission test')
            os.remove(test_file)
        except Exception as e:
            frappe.throw(f"CSV directory lacks proper read/write permissions: {str(e)}")

    # def validate_odoo_config(self):
    #     """Validate Odoo configuration"""
    #     if self.enable_odoo_sync:
    #         required_fields = [
    #             ('odoo_url', 'Odoo Server URL'),
    #             ('odoo_database', 'Odoo Database Name'),
    #             ('odoo_username', 'Odoo Username'),
    #             ('odoo_password', 'Odoo Password')
    #         ]
            
    #         for field, label in required_fields:
    #             if not getattr(self, field, None):
    #                 frappe.throw(f"{label} is required when Odoo sync is enabled")
            
    #         if self.odoo_url:
    #             if not (self.odoo_url.startswith('http://') or self.odoo_url.startswith('https://')):
    #                 frappe.throw("Odoo Server URL must start with http:// or https://")
    #             self.odoo_url = self.odoo_url.rstrip('/')

    def validate_performance_settings(self):
        """Validate performance and monitoring settings"""
        if self.enable_performance_monitoring:
            if not self.max_concurrent_jobs:
                self.max_concurrent_jobs = 5
            if not self.csv_chunk_size:
                self.csv_chunk_size = 1000

    def on_update(self):
        """Enhanced update handling - FIXED"""
        try:
            # Handle scheduler frequency changes
            if self.has_value_changed('sync_frequency'):
                frappe.msgprint(
                    f"Processing frequency updated to: {self.sync_frequency}",
                    indicator='blue'
                )

            # Handle directory changes
            if self.has_value_changed('csv_watch_directory') and self.enable_csv_processing:
                try:
                    self.setup_csv_directories()
                    frappe.msgprint(f"CSV directory structure updated: {self.csv_watch_directory}", indicator='green')
                except Exception as e:
                    frappe.msgprint(f"Directory setup warning: {str(e)}", indicator='orange')

            # Handle service changes
            services_changed = []
            if self.has_value_changed('enable_zoho_sync'):
                services_changed.append(f"Zoho: {'Enabled' if self.enable_zoho_sync else 'Disabled'}")
            if self.has_value_changed('enable_odoo_sync'):
                services_changed.append(f"Odoo: {'Enabled' if self.enable_odoo_sync else 'Disabled'}")
            if self.has_value_changed('enable_csv_processing'):
                services_changed.append(f"CSV: {'Enabled' if self.enable_csv_processing else 'Disabled'}")

            if services_changed:
                frappe.msgprint(f"Service changes: {', '.join(services_changed)}", indicator='blue')

        except Exception:
            # Silent failure to prevent cascading errors
            pass
   
    def validate_auto_detection_settings(self):
        """Validate auto-detection configuration"""
        if self.doctype_match_threshold:
           threshold = flt(self.doctype_match_threshold)
           if threshold < 50:
               frappe.throw("DocType Match Threshold cannot be less than 50%")
           elif threshold > 95:
               frappe.throw("DocType Match Threshold cannot be more than 95%")
        
           self.doctype_match_threshold = threshold


    # @frappe.whitelist()
    # def test_zoho_connection(self):
    #     """Test Zoho connection"""
    #     if not self.enable_zoho_sync:
    #         return {"status": "error", "message": "Zoho sync is not enabled"}
        
    #     try:
    #         # Simplified connection test without complex imports
    #         return {"status": "success", "message": "Zoho configuration appears valid"}
    #     except Exception as e:
    #         return {"status": "error", "message": str(e)}

    # @frappe.whitelist()
    # def test_odoo_connection(self):
    #     """Test Odoo connection"""
    #     if not self.enable_odoo_sync:
    #         return {"status": "error", "message": "Odoo sync is not enabled"}
        
    #     try:
    #         # Simplified connection test
    #         return {"status": "success", "message": "Odoo configuration appears valid"}
    #     except Exception as e:
    #         return {"status": "error", "message": str(e)}

    @frappe.whitelist()
    def test_csv_directory(self):
        """Enhanced CSV directory testing"""
        if not self.enable_csv_processing:
            return {"status": "error", "message": "CSV processing is not enabled"}
        
        try:
            result = {
                "status": "success",
                "directory": self.csv_watch_directory,
                "permissions": {},
                "subdirectories": {}
            }
            
            if os.path.exists(self.csv_watch_directory):
                result["permissions"]["readable"] = os.access(self.csv_watch_directory, os.R_OK)
                result["permissions"]["writable"] = os.access(self.csv_watch_directory, os.W_OK)
                
                # Test subdirectories
                subdirs = ['processed', 'errors', 'backup']
                for subdir in subdirs:
                    subdir_path = os.path.join(self.csv_watch_directory, subdir)
                    result["subdirectories"][subdir] = {
                        "exists": os.path.exists(subdir_path),
                        "readable": os.access(subdir_path, os.R_OK) if os.path.exists(subdir_path) else False,
                        "writable": os.access(subdir_path, os.W_OK) if os.path.exists(subdir_path) else False
                    }
                
                # Count files
                try:
                    files = [f for f in os.listdir(self.csv_watch_directory)
                            if f.endswith('.csv') and os.path.isfile(os.path.join(self.csv_watch_directory, f))]
                    result["file_count"] = len(files)
                    result["csv_files"] = files[:10]  # Show first 10 CSV files
                except:
                    result["file_count"] = 0
                    result["csv_files"] = []
            else:
                result["status"] = "error"
                result["message"] = "Directory does not exist"
            
            return result
            
        except Exception as e:
            return {"status": "error", "message": str(e)}

    @frappe.whitelist()
    def trigger_manual_sync(self, source=None):
        """FIXED: Manual synchronization triggering"""
        try:
            if not frappe.has_permission("Migration Settings", "write"):
                return {"status": "error", "message": "Insufficient permissions"}

            # Import here to avoid circular imports
            from frappe.utils.background_jobs import enqueue
            import datetime
            
            # Generate unique job identifier for tracking
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if source == "csv" and self.enable_csv_processing:
                # Check if CSV files exist
                if not os.path.exists(self.csv_watch_directory):
                    return {"status": "error", "message": "CSV watch directory does not exist"}
                
                csv_files = [f for f in os.listdir(self.csv_watch_directory) if f.endswith('.csv')]
                if not csv_files:
                    return {"status": "warning", "message": "No CSV files found to process"}
                
                # Create background job with correct method reference
                job = enqueue(
                    'data_migration_tool.data_migration.api.process_csv_files_manual',
                    queue='default',
                    timeout=1800
                )
                
                return {
                    "status": "success", 
                    "message": f"CSV processing started for {len(csv_files)} files",
                    "job_id": job.id,
                    "files_found": len(csv_files)
                }
            
            elif source == "zoho" and self.enable_zoho_sync:
                job = enqueue(
                    'data_migration_tool.data_migration.utils.scheduler_tasks.sync_zoho_data',
                    queue='default',
                    timeout=1800
                )
                return {"status": "success", "message": "Zoho sync started", "job_id": job.id}
            
            elif source == "odoo" and self.enable_odoo_sync:
                job = enqueue(
                    'data_migration_tool.data_migration.utils.scheduler_tasks.sync_odoo_data',
                    queue='default',
                    timeout=1800
                )
                return {"status": "success", "message": "Odoo sync started", "job_id": job.id}
            
            else:
                return {"status": "error", "message": "Invalid source or service not enabled"}
                
        except Exception as e:
            frappe.log_error(f"Manual sync error: {str(e)}", "Manual Sync Error")
            return {"status": "error", "message": str(e)}

    @frappe.whitelist()
    def get_migration_statistics(self):
        """Get comprehensive migration statistics"""
        try:
            stats = {
                "migration_settings": {
                    "last_sync_time": getattr(self, 'last_sync_time', None),
                    "services_enabled": {
                        "zoho": self.enable_zoho_sync or 0,
                        "odoo": self.enable_odoo_sync or 0,
                        "csv": self.enable_csv_processing or 0
                    }
                },
                "processing_config": {
                    "sync_frequency": self.sync_frequency,
                    "csv_chunk_size": self.csv_chunk_size,
                    "max_concurrent_jobs": self.max_concurrent_jobs
                }
            }
            
            # Get recent background jobs
            try:
                recent_jobs = frappe.get_all("RQ Job",
                    filters={
                        "creation": [">=", frappe.utils.add_days(frappe.utils.nowdate(), -7)]
                    },
                    fields=["job_name", "status", "creation", "started_at", "ended_at"],
                    order_by="creation desc",
                    limit=10
                )
                stats["recent_jobs"] = recent_jobs
            except Exception as job_error:
                stats["job_error"] = str(job_error)
            
            # CSV directory statistics
            if self.enable_csv_processing and self.csv_watch_directory:
                stats["csv_directory"] = self.get_csv_directory_stats()
            
            return {"status": "success", "data": stats}
            
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def get_csv_directory_stats(self):
        """Get CSV directory statistics"""
        try:
            stats = {"files": 0, "processed": 0, "errors": 0}
            
            if os.path.exists(self.csv_watch_directory):
                # Count files in main directory
                files = [f for f in os.listdir(self.csv_watch_directory) 
                        if f.endswith('.csv') and os.path.isfile(os.path.join(self.csv_watch_directory, f))]
                stats["files"] = len(files)
                
                # Count files in subdirectories
                subdirs = ['processed', 'errors']
                for subdir in subdirs:
                    subdir_path = os.path.join(self.csv_watch_directory, subdir)
                    if os.path.exists(subdir_path):
                        subdir_files = [f for f in os.listdir(subdir_path) 
                                      if f.endswith('.csv') and os.path.isfile(os.path.join(subdir_path, f))]
                        stats[subdir] = len(subdir_files)
            
            return stats
        except:
            return {"files": 0, "processed": 0, "errors": 0}

    @frappe.whitelist() 
    def cleanup_old_files(self, days_old=7):
        """Clean up old processed/error files"""
        try:
            if not self.csv_watch_directory or not os.path.exists(self.csv_watch_directory):
                return {"status": "error", "message": "CSV directory not found"}
            
            import time
            cutoff_time = time.time() - (int(days_old) * 24 * 60 * 60)
            deleted_count = 0
            
            # Clean up processed and error directories
            for subdir in ['processed', 'errors']:
                subdir_path = os.path.join(self.csv_watch_directory, subdir)
                if os.path.exists(subdir_path):
                    for filename in os.listdir(subdir_path):
                        file_path = os.path.join(subdir_path, filename)
                        if os.path.isfile(file_path):
                            if os.path.getmtime(file_path) < cutoff_time:
                                os.remove(file_path)
                                deleted_count += 1
            
            return {
                "status": "success", 
                "message": f"Cleaned up {deleted_count} old files",
                "deleted_count": deleted_count
            }
            
        except Exception as e:
            return {"status": "error", "message": str(e)}

# DO NOT ADD ANY STANDALONE FUNCTIONS AFTER THIS POINT!
# All functions must be methods within the MigrationSettings class.
     # Add this method to MigrationSettings class

    @frappe.whitelist()
    def trigger_intelligent_processing(self):
        """Trigger intelligent CSV processing manually"""
        try:
            if not frappe.has_permission('Migration Settings', 'write'):
                return {'status': 'error', 'message': 'Insufficient permissions'}
            from frappe.utils.background_jobs import enqueue
            import datetime
            if not self.enable_csv_processing:
                return {'status': 'error', 'message': 'CSV processing is not enabled'}
            if not os.path.exists(self.csv_watch_directory):
                return {'status': 'error', 'message': 'CSV watch directory does not exist'}
            # Count CSV files
            csv_files = [f for f in os.listdir(self.csv_watch_directory)
                        if f.endswith('.csv') and os.path.isfile(os.path.join(self.csv_watch_directory, f))]
            if not csv_files:
                return {'status': 'warning', 'message': 'No CSV files found to process'}
            # Enqueue intelligent processing
            job = enqueue(
                'data_migration_tool.data_migration.utils.scheduler_tasks.process_csv_files_with_jit',
                queue='default',
                timeout=1800
            )
            return {
                'status': 'success',
                'message': f'Intelligent CSV processing started for {len(csv_files)} files',
                'job_id': job.id,
                'files_found': len(csv_files)
            }
        except Exception as e:
            frappe.log_error(f"Intelligent processing error: {str(e)}", "Intelligent Processing Error")
            return {'status': 'error', 'message': str(e)}
