# -*- coding: utf-8 -*-
# Copyright (c) 2025, Your Company and contributors
# For license information, please see license.txt

import frappe
import json
from frappe.model.document import Document

class DuplicationPolicy(Document):
    """DocType to manage duplicate handling policies for different DocTypes"""

    def validate(self):
        """Validate the duplication policy settings"""
        self.validate_json_fields()
        self.validate_doctype_exists()
        self.validate_unique_fields()

    def validate_json_fields(self):
        """Validate JSON field formats"""
        # Validate unique_fields JSON
        if self.unique_fields:
            try:
                unique_fields_data = json.loads(self.unique_fields)
                if not isinstance(unique_fields_data, list):
                    frappe.throw("Unique Fields must be a JSON array")

                for field in unique_fields_data:
                    if not isinstance(field, str):
                        frappe.throw("Each unique field must be a string")

            except json.JSONDecodeError:
                frappe.throw("Invalid JSON format for Unique Fields")

        # Validate business_rules JSON
        if self.business_rules:
            try:
                business_rules_data = json.loads(self.business_rules)
                if not isinstance(business_rules_data, list):
                    frappe.throw("Business Rules must be a JSON array")

                for rule in business_rules_data:
                    if not isinstance(rule, list):
                        frappe.throw("Each business rule must be an array of field names")

                    for field in rule:
                        if not isinstance(field, str):
                            frappe.throw("Each field in business rule must be a string")

            except json.JSONDecodeError:
                frappe.throw("Invalid JSON format for Business Rules")

    def validate_doctype_exists(self):
        """Validate that the specified DocType exists"""
        if not frappe.db.exists("DocType", self.doctype_name):
            frappe.throw(f"DocType '{self.doctype_name}' does not exist")

    def validate_unique_fields(self):
        """Validate that specified unique fields exist in the DocType"""
        if self.unique_fields and self.doctype_name:
            try:
                unique_fields_data = json.loads(self.unique_fields)
                meta = frappe.get_meta(self.doctype_name)
                doctype_fields = [field.fieldname for field in meta.fields]

                for field in unique_fields_data:
                    if field not in doctype_fields:
                        frappe.throw(f"Field '{field}' does not exist in DocType '{self.doctype_name}'")

            except json.JSONDecodeError:
                pass  # Already handled in validate_json_fields

    def get_policy_config(self):
        """Get the policy configuration as a dictionary"""
        return {
            "allow_duplicates": self.allow_duplicates,
            "unique_fields": json.loads(self.unique_fields or "[]"),
            "business_rules": json.loads(self.business_rules or "[]"),
            "enabled": self.enabled,
            "priority": self.priority
        }

    @staticmethod
    def get_policy_for_doctype(doctype_name):
        """Static method to get policy for a specific DocType"""
        policy_doc = frappe.db.get_value(
            "Duplication Policy",
            {"doctype_name": doctype_name, "enabled": 1},
            ["allow_duplicates", "unique_fields", "business_rules"],
            as_dict=True
        )

        if policy_doc:
            return {
                "allow_duplicates": policy_doc.allow_duplicates,
                "unique_fields": json.loads(policy_doc.unique_fields or "[]"),
                "business_rules": json.loads(policy_doc.business_rules or "[]")
            }

        # Default policy if none found
        return {
            "allow_duplicates": False,
            "unique_fields": [],
            "business_rules": []
        }

    def on_update(self):
        """Clear cache when policy is updated"""
        frappe.cache().delete_key("duplication_policies")

    def on_trash(self):
        """Clear cache when policy is deleted"""
        frappe.cache().delete_key("duplication_policies")
