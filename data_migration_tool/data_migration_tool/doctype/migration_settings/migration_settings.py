# Copyright (c) 2025, Ayush and contributors
# For license information, please see license.txt

import frappe
import os
from frappe.model.document import Document
from frappe.utils import now, cint
from typing import Dict, Any


class MigrationSettings(Document):
    """Enhanced Migration Settings with JIT processing support"""
    
    def validate(self):
        """Comprehensive validation of migration settings"""
        self.validate_basic_settings()
        self.validate_zoho_config()
        self.validate_csv_config()
        self.validate_odoo_config()
        self.validate_performance_settings()
        self.validate_jit_settings()
    
    def validate_basic_settings(self):
        """Validate basic configuration settings"""
        if self.csv_chunk_size:
            chunk_size = cint(self.csv_chunk_size)
            if chunk_size < 100:
                frappe.throw("CSV Chunk Size must be at least 100 records")
            elif chunk_size > 10000:  # Increased limit for JIT processing
                frappe.throw("CSV Chunk Size should not exceed 10,000 records")
            self.csv_chunk_size = chunk_size
        
        if self.max_concurrent_jobs:
            max_jobs = cint(self.max_concurrent_jobs)
            if max_jobs < 1:
                frappe.throw("Maximum Concurrent Jobs must be at least 1")
            elif max_jobs > 20:
                frappe.throw("Maximum Concurrent Jobs should not exceed 20")
            self.max_concurrent_jobs = max_jobs
    
    def validate_zoho_config(self):
        """Validate Zoho configuration"""
        if self.enable_zoho_sync:
            required_fields = [
                ('zoho_client_id', 'Zoho Client ID'),
                ('zoho_client_secret', 'Zoho Client Secret'),
                ('zoho_refresh_token', 'Zoho Refresh Token')
            ]
            
            for field, label in required_fields:
                if not getattr(self, field):
                    frappe.throw(f"{label} is required when Zoho sync is enabled")
    
    def validate_csv_config(self):
        """Enhanced CSV configuration validation with JIT support"""
        if self.enable_csv_processing:
            if not self.csv_watch_directory:
                frappe.throw("CSV Watch Directory is required when CSV processing is enabled")
            
            try:
                # Expand user path and make absolute
                watch_dir = os.path.expanduser(self.csv_watch_directory)
                self.csv_watch_directory = os.path.abspath(watch_dir)
                
                # Create directory structure with JIT buffer support
                self.setup_csv_directories()
                
                # Validate permissions
                self.validate_directory_permissions()
                
            except PermissionError:
                frappe.throw(f"Permission denied: Cannot access {self.csv_watch_directory}")
            except Exception as e:
                frappe.throw(f"Cannot setup CSV directory: {str(e)}")
    
    def setup_csv_directories(self):
        """Create complete CSV directory structure with JIT support"""
        base_dir = self.csv_watch_directory
        subdirs = ['processed', 'errors', 'backup', 'staging']  # Added staging for JIT
        
        for subdir in [''] + subdirs:
            dir_path = os.path.join(base_dir, subdir) if subdir else base_dir
            
            if not os.path.exists(dir_path):
                os.makedirs(dir_path, exist_ok=True)
        
        # Create .gitkeep files and JIT documentation
        for subdir in subdirs:
            gitkeep_path = os.path.join(base_dir, subdir, '.gitkeep')
            if not os.path.exists(gitkeep_path):
                with open(gitkeep_path, 'w') as f:
                    if subdir == 'staging':
                        f.write('# JIT staging directory for temporary files\n')
                    else:
                        f.write('# Keep this directory in version control\n')
    
    def validate_directory_permissions(self):
        """Validate directory read/write permissions"""
        import tempfile
        
        try:
            # Test write permission
            test_file = os.path.join(self.csv_watch_directory, '.jit_permission_test')
            with open(test_file, 'w') as f:
                f.write('JIT permission test')
            os.remove(test_file)
            
        except Exception as e:
            frappe.throw(f"CSV directory lacks proper read/write permissions: {str(e)}")
    
    def validate_odoo_config(self):
        """Validate Odoo configuration"""
        if self.enable_odoo_sync:
            required_fields = [
                ('odoo_url', 'Odoo Server URL'),
                ('odoo_database', 'Odoo Database Name'),
                ('odoo_username', 'Odoo Username'),
                ('odoo_password', 'Odoo Password')
            ]
            
            for field, label in required_fields:
                if not getattr(self, field):
                    frappe.throw(f"{label} is required when Odoo sync is enabled")
            
            if self.odoo_url:
                if not (self.odoo_url.startswith('http://') or self.odoo_url.startswith('https://')):
                    frappe.throw("Odoo Server URL must start with http:// or https://")
                self.odoo_url = self.odoo_url.rstrip('/')
    
    def validate_performance_settings(self):
        """Validate performance and monitoring settings"""
        if self.enable_performance_monitoring:
            if not self.max_concurrent_jobs:
                self.max_concurrent_jobs = 5
            if not self.csv_chunk_size:
                self.csv_chunk_size = 1000
    
    def validate_jit_settings(self):
        """Validate JIT-specific settings"""
        # Set JIT defaults if not specified
        if not hasattr(self, 'enable_jit_processing'):
            self.enable_jit_processing = 1  # Enable JIT by default
        
        if not hasattr(self, 'jit_buffer_retention_days'):
            self.jit_buffer_retention_days = 7  # Keep buffer records for 7 days
        
        if self.jit_buffer_retention_days and cint(self.jit_buffer_retention_days) < 1:
            frappe.throw("JIT buffer retention days must be at least 1")
    
    def on_update(self):
        """Enhanced update handling with JIT support"""
        try:
            # Handle scheduler frequency changes
            if self.has_value_changed('sync_frequency'):
                frappe.msgprint(
                    f"JIT processing frequency updated to: {self.sync_frequency}. Restart services to apply changes.",
                    indicator='blue'
                )
            
            # Handle directory changes
            if self.has_value_changed('csv_watch_directory') and self.enable_csv_processing:
                try:
                    self.setup_csv_directories()
                    frappe.msgprint(f"CSV directory structure updated with JIT support: {self.csv_watch_directory}", indicator='green')
                except Exception as e:
                    frappe.msgprint(f"Directory setup warning: {str(e)}", indicator='orange')
            
            # Handle JIT processing changes
            if hasattr(self, 'enable_jit_processing') and self.has_value_changed('enable_jit_processing'):
                status = "enabled" if self.enable_jit_processing else "disabled"
                frappe.msgprint(f"JIT processing {status}", indicator='blue')
            
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
    
    @frappe.whitelist()
    def test_zoho_connection(self):
        """Test Zoho connection"""
        if not self.enable_zoho_sync:
            return {"status": "error", "message": "Zoho sync is not enabled"}
        
        try:
            from data_migration_tool.data_migration.connectors.zoho_connector import ZohoConnector
            from data_migration_tool.data_migration.utils.logger_config import migration_logger
            
            zoho = ZohoConnector(migration_logger)
            result = zoho.test_connection(
                client_id=self.zoho_client_id,
                client_secret=self.zoho_client_secret,
                refresh_token=self.zoho_refresh_token
            )
            return result
            
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    @frappe.whitelist()
    def test_odoo_connection(self):
        """Test Odoo connection"""
        if not self.enable_odoo_sync:
            return {"status": "error", "message": "Odoo sync is not enabled"}
        
        try:
            from data_migration_tool.data_migration.connectors.odoo_connector import OdooConnector
            from data_migration_tool.data_migration.utils.logger_config import migration_logger
            
            odoo = OdooConnector(migration_logger)
            result = odoo.test_connection(
                url=self.odoo_url,
                database=self.odoo_database,
                username=self.odoo_username,
                password=self.odoo_password
            )
            return result
            
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    @frappe.whitelist()
    def test_csv_directory(self):
        """Enhanced CSV directory testing with JIT support"""
        if not self.enable_csv_processing:
            return {"status": "error", "message": "CSV processing is not enabled"}
        
        try:
            result = {
                "status": "success",
                "directory": self.csv_watch_directory,
                "jit_enabled": getattr(self, 'enable_jit_processing', True),
                "permissions": {},
                "subdirectories": {}
            }
            
            if os.path.exists(self.csv_watch_directory):
                result["permissions"]["readable"] = os.access(self.csv_watch_directory, os.R_OK)
                result["permissions"]["writable"] = os.access(self.csv_watch_directory, os.W_OK)
                
                # Test JIT-specific subdirectories
                subdirs = ['processed', 'errors', 'backup', 'staging']
                for subdir in subdirs:
                    subdir_path = os.path.join(self.csv_watch_directory, subdir)
                    result["subdirectories"][subdir] = {
                        "exists": os.path.exists(subdir_path),
                        "readable": os.access(subdir_path, os.R_OK) if os.path.exists(subdir_path) else False,
                        "writable": os.access(subdir_path, os.W_OK) if os.path.exists(subdir_path) else False
                    }
                
                # Count files in main directory
                try:
                    files = [f for f in os.listdir(self.csv_watch_directory) 
                           if os.path.isfile(os.path.join(self.csv_watch_directory, f))]
                    result["file_count"] = len(files)
                    result["sample_files"] = files[:5]
                except:
                    result["file_count"] = 0
                    result["sample_files"] = []
                
                # JIT buffer statistics
                try:
                    from data_migration_tool.data_migration.connectors.csv_connector import CSVConnector
                    from data_migration_tool.data_migration.utils.logger_config import migration_logger
                    
                    csv_connector = CSVConnector(migration_logger)
                    buffer_stats = csv_connector.get_buffer_statistics()
                    result["buffer_statistics"] = buffer_stats
                except Exception as buffer_error:
                    result["buffer_error"] = str(buffer_error)
                    
            else:
                result["status"] = "error"
                result["message"] = "Directory does not exist"
            
            return result
            
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    @frappe.whitelist()
    def trigger_manual_sync(self, source=None):
        """Enhanced manual synchronization triggering with JIT support"""
        try:
            if not frappe.has_permission("Migration Settings", "write"):
                return {"status": "error", "message": "Insufficient permissions"}
            
            job_params = {
                "queue": "long",
                "timeout": 7200,  # Increased timeout for JIT processing
                "job_name": f"jit_migration_sync_{source or 'all'}_{now()}"
            }
            
            if source == "zoho" and self.enable_zoho_sync:
                frappe.enqueue(
                    'data_migration_tool.data_migration.utils.scheduler_tasks.sync_zoho_data',
                    settings=self,
                    **job_params
                )
                return {"status": "success", "message": "Zoho sync started with JIT support"}
            
            elif source == "odoo" and self.enable_odoo_sync:
                frappe.enqueue(
                    'data_migration_tool.data_migration.utils.scheduler_tasks.sync_odoo_data',
                    settings=self,
                    **job_params
                )
                return {"status": "success", "message": "Odoo sync started with JIT support"}
            
            elif source == "csv" and self.enable_csv_processing:
                frappe.enqueue(
                    'data_migration_tool.data_migration.utils.scheduler_tasks.process_csv_files_with_jit',
                    **job_params
                )
                return {"status": "success", "message": "JIT CSV processing started"}
            
            elif source == "jit_buffer":
                # Manual JIT buffer processing
                frappe.enqueue(
                    'data_migration_tool.data_migration.utils.scheduler_tasks.manual_jit_processing',
                    **job_params
                )
                return {"status": "success", "message": "Manual JIT buffer processing started"}
            
            else:
                # Trigger all enabled syncs with JIT
                frappe.enqueue(
                    'data_migration_tool.data_migration.utils.scheduler_tasks.periodic_crm_sync',
                    **job_params
                )
                return {"status": "success", "message": "Full JIT sync started"}
                
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    @frappe.whitelist()
    def get_jit_statistics(self):
        """Get comprehensive JIT processing statistics"""
        try:
            stats = {
                "migration_settings": {
                    "last_sync_time": self.last_sync_time,
                    "jit_enabled": getattr(self, 'enable_jit_processing', True),
                    "buffer_retention_days": getattr(self, 'jit_buffer_retention_days', 7),
                    "services_enabled": {
                        "zoho": self.enable_zoho_sync,
                        "odoo": self.enable_odoo_sync,
                        "csv": self.enable_csv_processing
                    }
                },
                "processing_config": {
                    "sync_frequency": self.sync_frequency,
                    "csv_chunk_size": self.csv_chunk_size,
                    "max_concurrent_jobs": self.max_concurrent_jobs
                }
            }
            
            # Get JIT buffer statistics
            try:
                from data_migration_tool.data_migration.connectors.csv_connector import CSVConnector
                from data_migration_tool.data_migration.utils.logger_config import migration_logger
                
                csv_connector = CSVConnector(migration_logger)
                buffer_stats = csv_connector.get_buffer_statistics()
                stats["buffer_statistics"] = buffer_stats
            except Exception as buffer_error:
                stats["buffer_error"] = str(buffer_error)
            
            # Get recent background jobs
            try:
                recent_jobs = frappe.db.sql("""
                    SELECT status, queue, job_name, creation, started_at, ended_at
                    FROM `tabRQ Job`
                    WHERE job_name LIKE '%jit%' OR job_name LIKE '%migration%'
                    ORDER BY creation DESC
                    LIMIT 10
                """, as_dict=True)
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
        """Get enhanced CSV directory statistics with JIT support"""
        try:
            stats = {"files": 0, "processed": 0, "errors": 0, "staging": 0}
            
            if os.path.exists(self.csv_watch_directory):
                # Count files in main directory
                files = [f for f in os.listdir(self.csv_watch_directory) 
                        if os.path.isfile(os.path.join(self.csv_watch_directory, f))]
                stats["files"] = len(files)
                
                # Count files in subdirectories
                subdirs = ['processed', 'errors', 'staging']
                for subdir in subdirs:
                    subdir_path = os.path.join(self.csv_watch_directory, subdir)
                    if os.path.exists(subdir_path):
                        subdir_files = [f for f in os.listdir(subdir_path) 
                                      if os.path.isfile(os.path.join(subdir_path, f))]
                        stats[subdir] = len(subdir_files)
            
            return stats
        except:
            return {"files": 0, "processed": 0, "errors": 0, "staging": 0}
    
    @frappe.whitelist() 
    def cleanup_jit_buffer(self, days_old=None):
        """Clean up old JIT buffer records"""
        try:
            if days_old is None:
                days_old = getattr(self, 'jit_buffer_retention_days', 7)
            
            from data_migration_tool.data_migration.connectors.csv_connector import CSVConnector
            from data_migration_tool.data_migration.utils.logger_config import migration_logger
            
            csv_connector = CSVConnector(migration_logger)
            deleted_count = csv_connector.cleanup_processed_buffer(int(days_old))
            
            return {
                "status": "success", 
                "message": f"Cleaned up {deleted_count} old buffer records",
                "deleted_count": deleted_count
            }
            
        except Exception as e:
            return {"status": "error", "message": str(e)}
