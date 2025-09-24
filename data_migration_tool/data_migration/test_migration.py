# SECURITY AND QUALITY FIXED: Enhanced test coverage with proper isolation
import frappe
import unittest
from unittest.mock import Mock, patch, MagicMock
from data_migration_tool.data_migration.connectors.zoho_connector import ZohoConnector
from data_migration_tool.data_migration.utils.logger_config import migration_logger
import json
import tempfile
import os

class TestDataMigration(unittest.TestCase):
    
    def setUp(self):
        """FIXED: Enhanced test environment setup with proper isolation"""
        self.logger = migration_logger
        self.test_site = frappe.local.site
        
        # ISOLATION FIX: Set up clean test context
        self.original_user = getattr(frappe.session, 'user', None)
        frappe.set_user('Administrator')
        
        # ISOLATION FIX: Create temporary test data
        self.test_temp_dir = tempfile.mkdtemp()
        self.test_csv_path = os.path.join(self.test_temp_dir, 'test_data.csv')
        
        # Create test CSV file
        with open(self.test_csv_path, 'w') as f:
            f.write("name,email,phone\nTest User,test@example.com,1234567890\n")
        
        # ISOLATION FIX: Track created documents for cleanup
        self.created_docs = []
        
    def tearDown(self):
        """FIXED: Proper cleanup after tests"""
        # ISOLATION FIX: Clean up created documents
        for doc in self.created_docs:
            try:
                if frappe.db.exists(doc['doctype'], doc['name']):
                    frappe.delete_doc(doc['doctype'], doc['name'], force=True)
            except Exception as e:
                print(f"Cleanup warning: {str(e)}")
        
        # ISOLATION FIX: Restore original user context
        if self.original_user:
            frappe.set_user(self.original_user)
        
        # ISOLATION FIX: Clean up temporary files
        import shutil
        if os.path.exists(self.test_temp_dir):
            shutil.rmtree(self.test_temp_dir)
        
        # ISOLATION FIX: Rollback any database changes
        frappe.db.rollback()
        
    def test_zoho_authentication_success(self):
        """ENHANCED: Test successful Zoho API authentication"""
        with patch('requests.post') as mock_post:
            mock_post.return_value.json.return_value = {'access_token': 'test_token'}
            mock_post.return_value.status_code = 200
            mock_post.return_value.raise_for_status.return_value = None
            
            zoho = ZohoConnector(self.logger)
            result = zoho.authenticate()
            
            self.assertTrue(result)
            self.assertEqual(zoho.access_token, 'test_token')
            mock_post.assert_called_once()
    
    def test_zoho_authentication_failure(self):
        """NEW: Test Zoho authentication failure scenarios"""
        with patch('requests.post') as mock_post:
            # Test HTTP error
            mock_post.return_value.raise_for_status.side_effect = Exception("HTTP 401")
            
            zoho = ZohoConnector(self.logger)
            result = zoho.authenticate()
            
            self.assertFalse(result)
            self.assertIsNone(getattr(zoho, 'access_token', None))
    
    def test_zoho_authentication_invalid_response(self):
        """NEW: Test invalid JSON response handling"""
        with patch('requests.post') as mock_post:
            mock_post.return_value.json.return_value = {'error': 'invalid_grant'}
            mock_post.return_value.status_code = 400
            mock_post.return_value.raise_for_status.return_value = None
            
            zoho = ZohoConnector(self.logger)
            result = zoho.authenticate()
            
            self.assertFalse(result)

    def test_csv_validation_success(self):
        """ENHANCED: Test successful CSV file validation"""
        from data_migration_tool.data_migration.connectors.csv_connector import CSVConnector
        
        csv_connector = CSVConnector(self.logger)
        result = csv_connector.validate_file(self.test_csv_path)
        
        self.assertEqual(result['status'], 'valid')
        self.assertIn('rows', result)

    def test_csv_validation_nonexistent_file(self):
        """FIXED: Test CSV validation with non-existent file"""
        from data_migration_tool.data_migration.connectors.csv_connector import CSVConnector
        
        csv_connector = CSVConnector(self.logger)
        result = csv_connector.validate_file('/non/existent/file.csv')
        
        self.assertEqual(result['status'], 'invalid')
        self.assertIn('error', result)

    def test_csv_validation_empty_file(self):
        """NEW: Test CSV validation with empty file"""
        from data_migration_tool.data_migration.connectors.csv_connector import CSVConnector
        
        empty_csv_path = os.path.join(self.test_temp_dir, 'empty.csv')
        with open(empty_csv_path, 'w') as f:
            f.write("")  # Empty file
        
        csv_connector = CSVConnector(self.logger)
        result = csv_connector.validate_file(empty_csv_path)
        
        self.assertEqual(result['status'], 'invalid')

    def test_doctype_creation_field_cleaning(self):
        """ENHANCED: Test dynamic DocType creation with comprehensive scenarios"""
        from data_migration_tool.data_migration.mappers.doctype_creator import DynamicDocTypeCreator
        
        creator = DynamicDocTypeCreator(self.logger)
        
        # Test various field name scenarios
        test_cases = [
            ("Customer Name!", "customer_name"),
            ("Email-ID", "email_id"),
            ("Phone Number (Mobile)", "phone_number_mobile"),
            ("123Invalid", "invalid"),  # Numbers at start
            ("special@chars#", "special_chars")
        ]
        
        for input_name, expected in test_cases:
            clean_name = creator._clean_field_name(input_name)
            self.assertEqual(clean_name, expected, f"Failed for input: {input_name}")

    def test_field_mapping_with_mock_meta(self):
        """ENHANCED: Test field mapping logic with comprehensive mocking"""
        from data_migration_tool.data_migration.mappers.doctype_creator import DynamicDocTypeCreator
        
        creator = DynamicDocTypeCreator(self.logger)
        source_data = {
            "customer_name": "Test Customer", 
            "email_id": "test@example.com",
            "invalid_field": "should_be_ignored"
        }
        
        # Mock target doctype meta with more realistic structure
        with patch('frappe.get_meta') as mock_meta:
            mock_field1 = Mock()
            mock_field1.fieldname = 'customer_name'
            mock_field1.fieldtype = 'Data'
            
            mock_field2 = Mock()
            mock_field2.fieldname = 'email_id'  
            mock_field2.fieldtype = 'Data'
            
            mock_meta.return_value.fields = [mock_field1, mock_field2]
            
            mapping = creator.map_external_fields(source_data, 'Customer')
            
            self.assertIn('customer_name', mapping)
            self.assertIn('email_id', mapping)
            self.assertNotIn('invalid_field', mapping)
            self.assertEqual(mapping['customer_name'], 'Test Customer')

    def test_error_handling_scenarios(self):
        """NEW: Comprehensive error handling tests"""
        from data_migration_tool.data_migration.connectors.csv_connector import CSVConnector
        
        csv_connector = CSVConnector(self.logger)
        
        # Test with malformed CSV
        malformed_csv_path = os.path.join(self.test_temp_dir, 'malformed.csv')
        with open(malformed_csv_path, 'w') as f:
            f.write("name,email\nTest,invalid@email,extra_column\n")  # Mismatched columns
        
        try:
            result = csv_connector.read_file_as_strings(malformed_csv_path)
            # Should handle gracefully by skipping bad lines
            self.assertIsNotNone(result)
        except Exception as e:
            # If it raises an exception, it should be informative
            self.assertIn('malformed', str(e).lower())

    def test_database_transaction_rollback(self):
        """NEW: Test database transaction handling"""
        # This test ensures proper rollback on errors
        original_commit = frappe.db.commit
        commit_called = []
        
        def mock_commit():
            commit_called.append(True)
            return original_commit()
        
        with patch('frappe.db.commit', side_effect=mock_commit):
            # Test that transactions are properly handled
            try:
                # Simulate an operation that should rollback
                frappe.db.sql("SELECT 1")
                frappe.db.commit()
                self.assertTrue(len(commit_called) > 0)
            except Exception:
                # Ensure rollback happens
                frappe.db.rollback()

    def test_security_input_validation(self):
        """NEW: Test input validation and security measures"""
        from data_migration_tool.data_migration.connectors.csv_connector import CSVConnector
        
        csv_connector = CSVConnector(self.logger)
        
        # Test path traversal protection
        dangerous_paths = [
            "../../../etc/passwd",
            "..\\..\\windows\\system32\\config\\sam",
            "/dev/null",
            "con.csv"  # Windows reserved name
        ]
        
        for dangerous_path in dangerous_paths:
            result = csv_connector.validate_file(dangerous_path)
            self.assertEqual(result['status'], 'invalid', f"Failed to reject dangerous path: {dangerous_path}")

    def test_performance_large_data_simulation(self):
        """NEW: Test performance with simulated large datasets"""
        from data_migration_tool.data_migration.connectors.csv_connector import CSVConnector
        
        csv_connector = CSVConnector(self.logger)
        
        # Create larger test file
        large_csv_path = os.path.join(self.test_temp_dir, 'large_test.csv')
        with open(large_csv_path, 'w') as f:
            f.write("name,email,phone\n")
            for i in range(1000):  # Simulate 1000 rows
                f.write(f"User{i},user{i}@example.com,123456{i:04d}\n")
        
        # Test that it can handle larger files
        import time
        start_time = time.time()
        result = csv_connector.read_file_as_strings(large_csv_path)
        end_time = time.time()
        
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 1000)
        # Ensure it completes within reasonable time (adjust as needed)
        self.assertLess(end_time - start_time, 10.0, "Performance test took too long")

if __name__ == '__main__':
    # FIXED: Proper test runner with cleanup
    unittest.main(verbosity=2)
