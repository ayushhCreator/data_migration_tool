// PRIORITY 1: Modal Dialog Implementation - Replace Redirect Flow
frappe.ui.form.on('DocType Creation Request', {
    refresh: function(frm) {
        // Add custom buttons only for pending requests
        if (frm.doc.status === 'Pending') {
            frm.add_custom_button(__('Approve'), function() {
                show_approval_modal(frm, 'Approve');
            }, __('Actions'));
            
            frm.add_custom_button(__('Redirect to Existing'), function() {
                show_approval_modal(frm, 'Redirect');
            }, __('Actions'));
            
            frm.add_custom_button(__('Reject'), function() {
                show_approval_modal(frm, 'Reject');
            }, __('Actions'));
        }
        
        // Show field preview if available
        if (frm.doc.field_analysis) {
            show_field_analysis(frm);
        }
        
        // Initialize real-time listeners
        if (!window.migration_realtime_initialized) {
            setup_realtime_listeners();
            window.migration_realtime_initialized = true;
        }
    }
});

// PRIORITY 1 FIX: Replace redirect with modal dialog
function show_approval_modal(frm, initial_action) {
    let field_analysis = {};
    
    try {
        field_analysis = JSON.parse(frm.doc.field_analysis || '{}');
    } catch (e) {
        console.log('Could not parse field analysis');
    }
    
    const d = new frappe.ui.Dialog({
        title: __('DocType Creation Request - {0}', [frm.doc.suggested_doctype]),
        size: 'large',
        fields: [
            {
                fieldtype: 'HTML',
                fieldname: 'request_info',
                options: `
                    <div class="request-summary" style="background: #f8f9fa; padding: 15px; border-radius: 5px; margin-bottom: 15px;">
                        <h5 style="margin-bottom: 10px; color: #495057;">📄 Request Summary</h5>
                        <div class="row">
                            <div class="col-md-6">
                                <p><strong>Source File:</strong> ${frm.doc.source_file || 'Unknown'}</p>
                                <p><strong>Suggested DocType:</strong> <span style="color: #007bff;">${frm.doc.suggested_doctype}</span></p>
                            </div>
                            <div class="col-md-6">
                                <p><strong>Field Count:</strong> ${frm.doc.field_count || 0}</p>
                                <p><strong>Total Records:</strong> ${frm.doc.total_records || 0}</p>
                            </div>
                        </div>
                    </div>
                `
            },
            {
                fieldtype: 'Section Break',
                label: 'Field Analysis'
            },
            {
                fieldtype: 'HTML',
                fieldname: 'field_preview',
                options: generate_field_preview_html(field_analysis)
            },
            {
                fieldtype: 'Section Break',
                label: 'Action Selection'
            },
            {
                fieldtype: 'Select',
                fieldname: 'action',
                label: 'Choose Action',
                options: '\nApprove\nRedirect\nReject',
                default: initial_action,
                reqd: 1,
                change: function() {
                    const action = d.get_value('action');
                    d.fields_dict.target_doctype.$wrapper.toggle(action === 'Redirect');
                    d.fields_dict.rejection_reason.$wrapper.toggle(action === 'Reject');
                }
            },
            {
                fieldtype: 'Link',
                fieldname: 'target_doctype',
                label: 'Redirect to Existing DocType',
                options: 'DocType',
                depends_on: 'eval:doc.action=="Redirect"',
                hidden: 1
            },
            {
                fieldtype: 'Small Text',
                fieldname: 'rejection_reason',
                label: 'Rejection Reason',
                depends_on: 'eval:doc.action=="Reject"',
                hidden: 1
            }
        ],
        primary_action_label: __('Submit Response'),
        primary_action: function(values) {
            handle_approval_response_api(frm, values, d);
        },
        secondary_action_label: __('Cancel'),
        secondary_action: function() {
            d.hide();
        }
    });
    
    // Show/hide conditional fields based on initial action
    setTimeout(() => {
        d.fields_dict.target_doctype.$wrapper.toggle(initial_action === 'Redirect');
        d.fields_dict.rejection_reason.$wrapper.toggle(initial_action === 'Reject');
    }, 100);
    
    d.show();
}

function generate_field_preview_html(field_analysis) {
    if (!field_analysis.fields || field_analysis.fields.length === 0) {
        return '<p class="text-muted">No field analysis available.</p>';
    }
    
    let html = '<div class="field-analysis-table" style="max-height: 300px; overflow-y: auto;">';
    html += '<table class="table table-sm table-bordered">';
    html += '<thead style="background: #e9ecef;"><tr>';
    html += '<th>CSV Field</th><th>Mapped Field</th><th>Type</th><th>Sample Data</th>';
    html += '</tr></thead><tbody>';
    
    field_analysis.fields.forEach(field => {
        html += '<tr>';
        html += `<td><strong>${field.original_name}</strong></td>`;
        html += `<td><code>${field.clean_name}</code></td>`;
        html += `<td><span class="badge badge-info">${field.suggested_type}</span></td>`;
        html += `<td><small>${(field.sample_values || []).slice(0, 2).join(', ')}</small></td>`;
        html += '</tr>';
    });
    
    html += '</tbody></table></div>';
    return html;
}

function handle_approval_response_api(frm, values, dialog) {
    frappe.call({
        method: 'data_migration_tool.data_migration.api.handle_doctype_creation_response',
        args: {
            request_id: frm.doc.name,
            action: values.action,
            target_doctype: values.target_doctype,
            rejection_reason: values.rejection_reason
        },
        freeze: true,
        freeze_message: __('Processing request...'),
        callback: function(r) {
            dialog.hide();
            
            if (r.message && r.message.status === 'success') {
                frappe.show_alert({
                    message: __('Request processed successfully'),
                    indicator: 'green'
                });
                
                frm.reload_doc();
                
                // If approved, show processing status
                if (values.action === 'Approve') {
                    show_processing_status_dialog(frm.doc.name);
                }
            } else {
                frappe.show_alert({
                    message: r.message ? r.message.message : __('Failed to process request'),
                    indicator: 'red'
                });
            }
        },
        error: function(err) {
            dialog.hide();
            frappe.show_alert({
                message: __('Error processing request: {0}', [err.message || 'Unknown error']),
                indicator: 'red'
            });
        }
    });
}

function show_processing_status_dialog(request_id) {
    const status_dialog = new frappe.ui.Dialog({
        title: __('Processing Status'),
        size: 'small',
        fields: [
            {
                fieldtype: 'HTML',
                fieldname: 'status_info',
                options: `
                    <div class="text-center" style="padding: 20px;">
                        <div class="spinner-border text-primary" role="status">
                            <span class="sr-only">Loading...</span>
                        </div>
                        <p style="margin-top: 15px;">Processing your data migration request...</p>
                        <p class="text-muted">This may take a few minutes depending on the file size.</p>
                    </div>
                `
            }
        ],
        primary_action_label: __('Close'),
        primary_action: function() {
            status_dialog.hide();
        }
    });
    
    status_dialog.show();
    
    // Auto-close after 10 seconds
    setTimeout(() => {
        if (status_dialog && status_dialog.$wrapper.is(':visible')) {
            status_dialog.hide();
        }
    }, 10000);
}

// Real-time event listeners for automatic modal triggering
function setup_realtime_listeners() {
    frappe.realtime.on('doctype_creation_request', function(data) {
        // Show notification
        frappe.show_alert({
            message: __('New DocType request: {0} → {1}', [data.filename, data.suggested_doctype]),
            indicator: 'blue'
        });
        
        // Auto-show modal for immediate action (optional)
        if (data.auto_show_modal) {
            show_approval_modal_from_realtime(data);
        }
    });
    
    frappe.realtime.on('doctype_processing_completed', function(data) {
        frappe.show_alert({
            message: __('Processing completed for {0}', [data.filename]),
            indicator: 'green'
        });
    });
}

function show_field_analysis(frm) {
    // This function shows field analysis in the form itself
    if (!frm.doc.field_analysis) return;
    
    try {
        const analysis = JSON.parse(frm.doc.field_analysis);
        const html = generate_field_preview_html(analysis);
        
        frm.fields_dict.field_analysis.$wrapper.html(`
            <div style="margin-top: 10px;">
                <label class="control-label">Field Analysis Preview</label>
                ${html}
            </div>
        `);
    } catch (e) {
        console.log('Could not display field analysis:', e);
    }
}
