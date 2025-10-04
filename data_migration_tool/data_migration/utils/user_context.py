import frappe
from typing import Optional

class UserContextManager:
    """Manages user context for migration operations with proper fallback logic"""
    
    @staticmethod
    def get_migration_user() -> str:
        """Get appropriate user for migration operations with fallback logic"""
        try:
            # Try current session user first
            if hasattr(frappe, 'session') and frappe.session.user and frappe.session.user != 'Guest':
                return frappe.session.user
            
            # Get system managers
            system_managers = frappe.db.sql("""
                SELECT DISTINCT u.name 
                FROM `tabUser` u
                INNER JOIN `tabHas Role` hr ON u.name = hr.parent
                WHERE hr.role = 'System Manager' 
                AND u.enabled = 1 
                AND u.name != 'Guest'
                ORDER BY u.creation
                LIMIT 1
            """, as_dict=True)
            
            if system_managers:
                return system_managers[0].name
                
            # Final fallback
            return 'Administrator'
            
        except Exception as e:
            frappe.log_error(f"Failed to get migration user: {str(e)}")
            return 'Administrator'
    
    @staticmethod
    def set_migration_user():
        """Set appropriate user context for migration"""
        user = UserContextManager.get_migration_user()
        frappe.set_user(user)
        return user
    
    @staticmethod
    def get_system_managers() -> list:
        """Get list of all system managers"""
        try:
            system_managers = frappe.db.sql("""
                SELECT DISTINCT u.name, u.email
                FROM `tabUser` u
                INNER JOIN `tabHas Role` hr ON u.name = hr.parent
                WHERE hr.role = 'System Manager' 
                AND u.enabled = 1 
                AND u.name != 'Guest'
                ORDER BY u.creation
            """, as_dict=True)
            
            if not system_managers:
                # Fallback to Administrator
                admin_user = frappe.db.get_value('User', 'Administrator', ['name', 'email'], as_dict=True)
                if admin_user:
                    system_managers = [admin_user]
                else:
                    system_managers = [{'name': 'Administrator', 'email': 'admin@example.com'}]
            
            return system_managers
            
        except Exception as e:
            frappe.log_error(f"Failed to get system managers: {str(e)}")
            return [{'name': 'Administrator', 'email': 'admin@example.com'}]