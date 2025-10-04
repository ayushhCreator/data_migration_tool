// DocType Creation Request Client Script - FIXED VERSION
frappe.ui.form.on('DocType Creation Request', {
    refresh: function(frm) {
        // Add custom buttons
        if (frm.doc.status === 'Pending') {
            frm.add_custom_button(__('Approve'), function() {
                handle_approval_response(frm, 'Approve');
            }, __('Actions'));
            
            frm.add_custom_button(__('Redirect to Existing DocType'), function() {
                show_redirect_dialog(frm);
            }, __('Actions'));
            
            frm.add_custom_button(__('Reject'), function() {
                show_rejection_dialog(frm);
            }, __('Actions'));
        }
        
        // Show field preview if available
        if (frm.doc.field_analysis) {
            try {
                const analysis = JSON.parse(frm.doc.field_analysis);
                show_field_preview(frm, analysis);
            } catch (e) {
                console.log('Could not parse field analysis');
            }
        }
        
        // Initialize real-time event listeners (only once)
        if (!window.migration_realtime_initialized) {
            setup_realtime_listeners();
            window.migration_realtime_initialized = true;
        }
    },
    
    onload: function(frm) {
        // Set up form when it loads
        frm.set_df_property('field_analysis', 'read_only', 1);
    }
});

function setup_realtime_listeners() {
    // Listen for DocType creation requests
    frappe.realtime.on('doctype_creation_request', function(data) {
        frappe.show_alert({
            message: `New DocType request: ${data.filename} → ${data.suggested_doctype}`,
            indicator: 'blue'
        });
        
        // Show approval dialog
        show_approval_dialog(data);
    });
    
    // Listen for processing completion
    frappe.realtime.on('doctype_processing_completed', function(data) {
        frappe.show_alert({
            message: `Processing completed for ${data.filename}`,
            indicator: 'green'
        });
    });
}

function show_approval_dialog(data) {
    const d = new frappe.ui.Dialog({
        title: __('DocType Creation Request'),
        size: 'large',
        fields: [
            {
                fieldtype: 'HTML',
                fieldname: 'request_info',
                options: `
                    <div class="migration-request-info">
                        <h5>📄 Source File: ${data.filename}</h5>
                        <h5>🎯 Suggested DocType: ${data.suggested_doctype}</h5>
                        <h5>📊 Fields: ${data.field_count}</h5>
                        <h5>🔍 Sample Fields:</h5>
                        <p>${(data.sample_fields || []).join(', ')}</p>
                    </div>
                `
            },
            {
                fieldtype: 'Section Break'
            },
            {
                fieldtype: 'Select',
                fieldname: 'action',
                label: 'Choose Action',
                options: '\nApprove\nRedirect\nReject',
                reqd: 1,
                change: function() {
                    const action = d.get_value('action');
                    d.toggle_display('target_doctype', action === 'Redirect');
                    d.toggle_display('rejection_reason', action === 'Reject');
                }
            },
            {
                fieldtype: 'Link',
                fieldname: 'target_doctype',
                label: 'Redirect to Existing DocType',
                options: 'DocType',
                filters: {
                    'custom': 0,
                    'istable': 0
                },
                depends_on: 'eval:doc.action=="Redirect"'
            },
            {
                fieldtype: 'Small Text',
                fieldname: 'rejection_reason',
                label: 'Reason for Rejection',
                depends_on: 'eval:doc.action=="Reject"'
            }
        ],
        primary_action_label: __('Submit Response'),
        primary_action: function(values) {
            handle_approval_response_api(data.request_id, values, d);
        }
    });
    
    d.show();
}

function handle_approval_response(frm, action) {
    handle_approval_response_api(frm.doc.name, { action: action });
}

function show_redirect_dialog(frm) {
    frappe.prompt([
        {
            fieldtype: 'Link',
            fieldname: 'target_doctype',
            label: 'Target DocType',
            options: 'DocType',
            reqd: 1,
            filters: {
                'custom': 0,
                'istable': 0
            }
        }
    ], function(values) {
        handle_approval_response_api(frm.doc.name, {
            action: 'Redirect',
            target_doctype: values.target_doctype
        });
    }, 'Redirect to Existing DocType');
}

function show_rejection_dialog(frm) {
    frappe.prompt([
        {
            fieldtype: 'Small Text',
            fieldname: 'rejection_reason',
            label: 'Reason for Rejection',
            reqd: 1
        }
    ], function(values) {
        handle_approval_response_api(frm.doc.name, {
            action: 'Reject',
            rejection_reason: values.rejection_reason
        });
    }, 'Reject DocType Creation');
}

function handle_approval_response_api(request_id, values, dialog) {
    frappe.call({
        method: 'data_migration_tool.data_migration.api.handle_doctype_creation_response',
        args: {
            request_id: request_id,
            action: values.action,
            target_doctype: values.target_doctype,
            rejection_reason: values.rejection_reason
        },
        callback: function(r) {
            if (r.message && r.message.status === 'success') {
                frappe.show_alert({
                    message: r.message.message,
                    indicator: 'green'
                });
                
                if (dialog) dialog.hide();
                
                // Refresh current page if viewing the request
                if (cur_frm && cur_frm.doc.name === request_id) {
                    cur_frm.reload_doc();
                }
                
            } else {
                frappe.msgprint({
                    title: 'Error',
                    message: r.message ? r.message.message : 'Failed to process response',
                    indicator: 'red'
                });
            }
        }
    });
}

function show_field_preview(frm, analysis) {
    if (!analysis.fields) return;
    
    let preview_html = '<div class="field-preview"><h6>Field Preview:</h6>';
    const fields = Object.keys(analysis.fields).slice(0, 10); // Show first 10 fields
    
    fields.forEach(field => {
        preview_html += `<p><strong>${field}</strong> → ${field.toLowerCase().replace(/[^a-zA-Z0-9]/g, '_')}</p>`;
    });
    
    preview_html += '</div>';
    
    frm.set_df_property('field_analysis', 'description', preview_html);
}
