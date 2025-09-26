// Copyright (c) 2025, Your Company and contributors
// For license information, please see license.txt

frappe.ui.form.on('Duplication Policy', {
    refresh: function(frm) {
        // Add custom buttons and help text
        frm.add_custom_button(__('Test Policy'), function() {
            test_duplication_policy(frm);
        });

        // Add help text for JSON fields
        if (frm.doc.doctype_name) {
            add_field_help(frm);
        }

        // Set field dependencies
        frm.toggle_reqd('unique_fields', !frm.doc.allow_duplicates);
        frm.toggle_display('unique_fields', !frm.doc.allow_duplicates);
        frm.toggle_display('business_rules', !frm.doc.allow_duplicates);
    },

    doctype_name: function(frm) {
        if (frm.doc.doctype_name) {
            load_doctype_fields(frm);
        }
    },

    allow_duplicates: function(frm) {
        frm.toggle_reqd('unique_fields', !frm.doc.allow_duplicates);
        frm.toggle_display('unique_fields', !frm.doc.allow_duplicates);
        frm.toggle_display('business_rules', !frm.doc.allow_duplicates);

        if (frm.doc.allow_duplicates) {
            frm.set_value('unique_fields', '');
            frm.set_value('business_rules', '');
        }
    },

    unique_fields: function(frm) {
        if (frm.doc.unique_fields) {
            try {
                let parsed = JSON.parse(frm.doc.unique_fields);
                if (!Array.isArray(parsed)) {
                    frappe.msgprint(__('Unique Fields must be a JSON array like: ["field1", "field2"]'));
                    frm.set_value('unique_fields', '');
                }
            } catch (e) {
                frappe.msgprint(__('Invalid JSON format in Unique Fields'));
                frm.set_value('unique_fields', '');
            }
        }
    }
});

function load_doctype_fields(frm) {
    if (!frm.doc.doctype_name) return;

    frappe.call({
        method: 'frappe.client.get',
        args: {
            doctype: 'DocType',
            name: frm.doc.doctype_name
        },
        callback: function(r) {
            if (r.message) {
                let fields = r.message.fields || [];
                let field_names = fields.map(f => f.fieldname).filter(f => f);

                let help_html = `<div class="help-box">
                    <h5>Available Fields for ${frm.doc.doctype_name}:</h5>
                    <p>${field_names.slice(0, 10).join(', ')}</p>
                    <hr>
                    <h5>Example Unique Fields:</h5>
                    <code>["${field_names.slice(0, 2).join('", "')}"]</code>
                </div>`;

                frm.get_field('unique_fields').$wrapper.find('.help-box').remove();
                frm.get_field('unique_fields').$wrapper.append(help_html);
            }
        }
    });
}

function add_field_help(frm) {
    let general_help = `
        <div class="alert alert-info">
            <h5>How Duplication Policies Work:</h5>
            <ul>
                <li><b>Allow Duplicates = Yes:</b> All duplicate checks are skipped</li>
                <li><b>Allow Duplicates = No:</b> Uses unique fields and business rules</li>
            </ul>
        </div>
    `;

    if (!frm.get_field('description').$wrapper.find('.alert-info').length) {
        frm.get_field('description').$wrapper.prepend(general_help);
    }
}

function test_duplication_policy(frm) {
    frappe.msgprint({
        title: __('Policy Configuration'),
        message: `DocType: ${frm.doc.doctype_name}<br>Allow Duplicates: ${frm.doc.allow_duplicates ? 'Yes' : 'No'}`,
        indicator: 'blue'
    });
}
