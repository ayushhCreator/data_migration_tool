frappe.ui.form.on('DocType Creation Request', {
    refresh: function(frm) {
        if (frm.doc.status === 'Pending') {
            frm.add_custom_button(__('Approve with Mapping'), function() {
                show_intelligent_approval_modal(frm);
            }).addClass('btn-primary');
            
            frm.add_custom_button(__('Quick Approve'), function() {
                quick_approve_request(frm);
            }).addClass('btn-success');
        }
    }
});

function show_intelligent_approval_modal(frm) {
    let field_analysis = {};
    let sample_data = [];
    
    try {
        field_analysis = JSON.parse(frm.doc.field_analysis || '{}');
        sample_data = field_analysis.sample_data || [];
    } catch (e) {
        console.error('Error parsing field analysis:', e);
    }
    
    let dialog = new frappe.ui.Dialog({
        title: 'Approve DocType Creation with Field Mapping',
        size: 'extra-large',
        fields: [
            {
                fieldname: 'doctype_section',
                fieldtype: 'Section Break',
                label: 'DocType Configuration'
            },
            {
                fieldname: 'final_doctype',
                fieldtype: 'Data',
                label: 'Final DocType Name',
                reqd: 1,
                default: frm.doc.suggested_doctype
            },
            {
                fieldname: 'confidence_info',
                fieldtype: 'HTML',
                options: `<div class="alert alert-info">
                    <strong>Confidence Score:</strong> ${field_analysis.confidence || 0}%<br>
                    <strong>Suggested:</strong> ${frm.doc.suggested_doctype}
                </div>`
            },
            {
                fieldname: 'mapping_section',
                fieldtype: 'Section Break', 
                label: 'Field Mapping Configuration'
            },
            {
                fieldname: 'field_mappings',
                fieldtype: 'HTML',
                options: build_field_mapping_interface(field_analysis)
            },
            {
                fieldname: 'preview_section',
                fieldtype: 'Section Break',
                label: 'Data Preview'
            },
            {
                fieldname: 'data_preview',
                fieldtype: 'HTML',
                options: build_data_preview(sample_data)
            }
        ],
        primary_action_label: __('Approve & Process'),
        primary_action: function(values) {
            approve_with_field_mapping(frm, dialog, values);
        }
    });
    
    dialog.show();
}

function build_field_mapping_interface(field_analysis) {
    if (!field_analysis.fields) {
        return '<p>No field analysis available</p>';
    }
    
    let html = '<div class="row"><div class="col-md-12">';
    html += '<table class="table table-bordered">';
    html += '<thead><tr><th>CSV Field</th><th>Field Type</th><th>Label</th><th>Required</th><th>Unique</th></tr></thead>';
    html += '<tbody>';
    
    Object.keys(field_analysis.fields).forEach(field => {
        const info = field_analysis.fields[field];
        const isIdField = field.toLowerCase().includes('id');
        const isEmailField = field.toLowerCase().includes('email');
        
        html += `
        <tr data-field="${field}">
            <td><strong>${field}</strong><br><small class="text-muted">${(info.sample_values || []).slice(0,2).join(', ')}</small></td>
            <td>
                <select class="form-control field-type-select" data-field="${field}">
                    <option value="Data" ${info.suggested_type === 'Data' ? 'selected' : ''}>Data</option>
                    <option value="Text" ${info.suggested_type === 'Text' ? 'selected' : ''}>Text</option>
                    <option value="Email" ${info.suggested_type === 'Email' ? 'selected' : ''}>Email</option>
                    <option value="Phone" ${info.suggested_type === 'Phone' ? 'selected' : ''}>Phone</option>
                    <option value="Int" ${info.suggested_type === 'Int' ? 'selected' : ''}>Integer</option>
                    <option value="Float" ${info.suggested_type === 'Float' ? 'selected' : ''}>Float</option>
                    <option value="Currency" ${info.suggested_type === 'Currency' ? 'selected' : ''}>Currency</option>
                    <option value="Date" ${info.suggested_type === 'Date' ? 'selected' : ''}>Date</option>
                </select>
            </td>
            <td><input type="text" class="form-control field-label-input" value="${field.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}" data-field="${field}"></td>
            <td><input type="checkbox" class="field-required-check" data-field="${field}" ${isIdField || isEmailField ? 'checked' : ''}></td>
            <td><input type="checkbox" class="field-unique-check" data-field="${field}" ${isIdField || isEmailField ? 'checked' : ''}></td>
        </tr>
        `;
    });
    
    html += '</tbody></table></div></div>';
    return html;
}

function approve_with_field_mapping(frm, dialog, values) {
    // Collect field mappings
    const field_mappings = {};
    dialog.$wrapper.find('tbody tr').each(function() {
        const field = $(this).data('field');
        field_mappings[field] = {
            fieldtype: $(this).find('.field-type-select').val(),
            label: $(this).find('.field-label-input').val(),
            reqd: $(this).find('.field-required-check').is(':checked'),
            unique: $(this).find('.field-unique-check').is(':checked')
        };
    });
    
    // Show processing status
    frappe.show_alert(__('Processing approval...'), 'blue');
    
    frappe.call({
        method: 'data_migration_tool.api.handle_doctype_creation_response',
        args: {
            request_id: frm.doc.name,
            action: 'Approve',
            target_doctype: values.final_doctype,
            field_mappings: field_mappings
        },
        callback: function(r) {
            if (r.message && r.message.status === 'success') {
                frappe.show_alert(__('DocType approved and processing started!'), 'green');
                dialog.hide();
                frm.refresh();
                show_processing_status(values.final_doctype);
            } else {
                frappe.msgprint(__('Error: ') + (r.message ? r.message.message : 'Unknown error'));
            }
        }
    });
}

function show_processing_status(doctype_name) {
    let status_dialog = new frappe.ui.Dialog({
        title: __('Processing Status'),
        fields: [
            {
                fieldname: 'status_html',
                fieldtype: 'HTML',
                options: `
                <div class="text-center">
                    <i class="fa fa-spinner fa-spin fa-3x text-primary"></i>
                    <h4>Processing CSV data...</h4>
                    <p>Creating records in DocType: <strong>${doctype_name}</strong></p>
                    <div id="progress-details" class="text-muted mt-3">
                        <p>Please wait while we process your data...</p>
                    </div>
                </div>
                `
            }
        ]
    });
    
    status_dialog.show();
    
    // Listen for real-time updates
    frappe.realtime.on('doctype_processing_complete', function(data) {
        if (data.doctype === doctype_name) {
            status_dialog.$wrapper.find('#progress-details').html(`
                <div class="alert alert-success">
                    <h5>Processing Complete!</h5>
                    <p><strong>Results:</strong></p>
                    <ul>
                        <li>Created: ${data.results.success || 0} records</li>
                        <li>Updated: ${data.results.updated || 0} records</li>
                        <li>Failed: ${data.results.failed || 0} records</li>
                    </ul>
                </div>
            `);
        }
    });
}
