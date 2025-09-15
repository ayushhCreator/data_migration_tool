import frappe
import unittest
from unittest.mock import Mock, patch
from data_migration_tool.data_migration.connectors.zoho_connector import ZohoConnector
from data_migration_tool.data_migration.utils.logger_config import migration_logger

class TestDataMigration(unittest.TestCase):
    
    def setUp(self):
        """Set up test environment"""
        self.logger = migration_logger
        self.test_site = frappe.local.site
    
    def test_zoho_authentication(self):
        """Test Zoho API authentication"""
        with patch('requests.post') as mock_post:
            mock_post.return_value.json.return_value = {'access_token': 'test_token'}
            mock_post.return_value.raise_for_status.return_value = None
            
            zoho = ZohoConnector(self.logger)
            result = zoho.authenticate()
            
            self.assertTrue(result)
            self.assertEqual(zoho.access_token, 'test_token')
    
    def test_csv_validation(self):
        """Test CSV file validation"""
        from data_migration_tool.data_migration.connectors.csv_connector import CSVConnector
        
        csv_connector = CSVConnector(self.logger)
        
        # Test with non-existent file
        result = csv_connector.validate_file('/non/existent/file.csv')
        self.assertEqual(result['status'], 'invalid')
    
    def test_doctype_creation(self):
        """Test dynamic DocType creation"""
        from data_migration_tool.data_migration.mappers.doctype_creator import DynamicDocTypeCreator
        
        creator = DynamicDocTypeCreator(self.logger)
        
        # Test field name cleaning
        clean_name = creator._clean_field_name("Customer Name!")
        self.assertEqual(clean_name, "customer_name")
    
    def test_field_mapping(self):
        """Test field mapping logic"""
        from data_migration_tool.data_migration.mappers.doctype_creator import DynamicDocTypeCreator
        
        creator = DynamicDocTypeCreator(self.logger)
        
        source_data = {"customer_name": "Test Customer", "email_id": "test@example.com"}
        
        # Mock target doctype meta
        with patch('frappe.get_meta') as mock_meta:
            mock_field = Mock()
            mock_field.fieldname = 'customer_name'
            mock_meta.return_value.fields = [mock_field]
            
            mapping = creator.map_external_fields(source_data, 'Customer')
            self.assertIn('customer_name', mapping)
    
    def tearDown(self):
        """Clean up after tests"""
        frappe.db.rollback()

if __name__ == '__main__':
    unittest.main()
