import frappe,json
from frappe.model.document import Document

def get_field_mappings_from_registry(registry_id):
    """Get field mappings from CSV schema registry"""
    try:
        if not registry_id:
            return {}
            
        registry = frappe.get_doc("CSV Schema Registry", registry_id)
        if not registry:
            return {}
            
        # If field mappings are stored in JSON format
        if hasattr(registry, 'field_mappings') and registry.field_mappings:
            try:
                return json.loads(registry.field_mappings)
            except (json.JSONDecodeError, TypeError):
                pass
                
        # If headers are stored, create basic mappings
        if hasattr(registry, 'headers_json') and registry.headers_json:
            try:
                headers = json.loads(registry.headers_json)
                mappings = {}
                for header in headers:
                    clean_header = header.lower().replace(' ', '_').replace('-', '_')
                    mappings[header] = clean_header
                return mappings
            except (json.JSONDecodeError, TypeError):
                pass
                
        return {}
        
    except Exception as e:
        frappe.log_error(f"Error getting field mappings from registry: {str(e)}", "Schema Registry Error")
        return {}

# Aliases for the function called in scheduler_tasks.py
getFieldMappingsFromRegistry = get_field_mappings_from_registry
getfieldmappingsfromregistry = get_field_mappings_from_registry


class CSVSchemaRegistry(Document):
    def validate(self):
        """Validate schema registry entry"""
        if not self.source_file:
            frappe.throw("Source file is required")
        
        if not self.schema_fingerprint:
            frappe.throw("Schema fingerprint is required")
        
        if not self.target_doctype:
            frappe.throw("Target DocType is required")
        
        # Ensure target DocType exists
        if not frappe.db.exists('DocType', self.target_doctype):
            frappe.throw(f"DocType '{self.target_doctype}' does not exist")
    
