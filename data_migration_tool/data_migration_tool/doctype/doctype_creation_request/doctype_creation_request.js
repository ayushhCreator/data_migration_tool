// Replace the entire file with this enhanced version

frappe.ui.form.on('DocType Creation Request', {
    refresh: function(frm) {
        // Add custom buttons only for pending requests
        if (frm.doc.status === 'Pending') {
            frm.add_custom_button(__('Approve'), function() {
                show_approval_modal(frm, 'Approve');
            }, __('Actions'));
            
            frm.add_custom_button(__('Redirect to Existing'), function() {
                show_doctype_selection_dialog(frm);
            }, __('Actions'));
            
            frm.add_custom_button(__('Reject'), function() {
                show_approval_modal(frm, 'Reject');
            }, __('Actions'));
        }

        // ENHANCED: Show proper "Go to DocType" button if completed
        if (frm.doc.status === 'Completed' && frm.doc.created_doctype) {
            frm.add_custom_button(__('View Records'), function() {
                // Use proper URL encoding for DocType names with spaces
                frappe.set_route('List', frm.doc.created_doctype);
            }, __('View'));
            
            frm.add_custom_button(__('Edit DocType'), function() {
                frappe.set_route('Form', 'DocType', frm.doc.created_doctype);
            }, __('View'));
        }

        // Add button to view all created DocTypes
        frm.add_custom_button(__('All Created DocTypes'), function() {
            show_all_created_doctypes();
        }, __('View'));

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

// ENHANCED: DocType selection dialog with search and similarity
function show_doctype_selection_dialog(frm) {
    // First get similar DocTypes based on CSV structure
    let csv_headers = [];
    if (frm.doc.field_analysis) {
        try {
            let analysis = JSON.parse(frm.doc.field_analysis);
            csv_headers = Object.keys(analysis.fields || {});
        } catch(e) {
            console.warn('Could not parse field analysis');
        }
    }
    
    let dialog = new frappe.ui.Dialog({
        title: __('Redirect to Existing DocType'),
        size: 'large',
        fields: [
            {
                label: __('Search DocTypes'),
                fieldname: 'search',
                fieldtype: 'Data',
                placeholder: __('Type to search existing DocTypes...')
            },
            {
                fieldname: 'include_custom',
                fieldtype: 'Check',
                label: __('Include Custom DocTypes'),
                default: 1
            },
            {
                fieldname: 'include_standard', 
                fieldtype: 'Check',
                label: __('Include Standard DocTypes'),
                default: 1
            },
            {
                fieldname: 'doctype_list',
                fieldtype: 'HTML'
            },
            {
                label: __('Selected DocType'),
                fieldname: 'selected_doctype',
                fieldtype: 'Data',
                read_only: 1
            }
        ],
        primary_action_label: __('Redirect'),
        primary_action: function() {
            let selected = dialog.get_value('selected_doctype');
            if (!selected) {
                frappe.msgprint(__('Please select a DocType'));
                return;
            }
            
            // Confirm redirect
            frappe.confirm(
                __('Redirect to existing DocType "{0}"?', [selected]),
                function() {
                    redirect_to_doctype(frm, selected);
                    dialog.hide();
                }
            );
        }
    });
    
    // Load and display DocTypes
    function load_doctypes(search_term = '') {
        frappe.call({
            method: 'data_migration_tool.data_migration.api.get_existing_doctypes',
            args: {
                search_term: search_term,
                include_custom: dialog.get_value('include_custom'),
                include_standard: dialog.get_value('include_standard')
            },
            callback: function(r) {
                if (r.message && r.message.status === 'success') {
                    display_doctypes_with_similarity(dialog, r.message.data, csv_headers);
                }
            }
        });
    }
    
    // Search functionality
    dialog.fields_dict.search.$input.on('input', frappe.utils.debounce(function() {
        load_doctypes(dialog.get_value('search'));
    }, 300));
    
    // Filter checkboxes
    dialog.fields_dict.include_custom.$input.on('change', function() {
        load_doctypes(dialog.get_value('search'));
    });
    
    dialog.fields_dict.include_standard.$input.on('change', function() {
        load_doctypes(dialog.get_value('search'));
    });
    
    dialog.show();
    load_doctypes(); // Initial load
    
    // Also get similarity suggestions if we have CSV headers
    if (csv_headers.length > 0) {
        frappe.call({
            method: 'data_migration_tool.data_migration.api.suggest_similar_doctypes',
            args: {
                source_file: frm.doc.source_file,
                csv_headers_json: JSON.stringify(csv_headers)
            },
            callback: function(r) {
                if (r.message && r.message.status === 'success' && r.message.suggestions.length > 0) {
                    show_similarity_suggestions(dialog, r.message.suggestions);
                }
            }
        });
    }
}

function display_doctypes_with_similarity(dialog, data, csv_headers) {
    let html = `<div class="doctype-selection-container">`;
    
    if (data.grouped.custom.length > 0) {
        html += `<h5>Custom DocTypes</h5>`;
        html += create_doctype_table(data.grouped.custom, true);
    }
    
    if (data.grouped.standard.length > 0) {
        html += `<h5>Standard DocTypes</h5>`;
        html += create_doctype_table(data.grouped.standard, false);
    }
    
    html += `</div>`;
    
    dialog.fields_dict.doctype_list.$wrapper.html(html);
    
    // Add click handlers
    dialog.fields_dict.doctype_list.$wrapper.find('.doctype-row').on('click', function() {
        let doctype_name = $(this).data('doctype');
        dialog.fields_dict.doctype_list.$wrapper.find('.doctype-row').removeClass('selected');
        $(this).addClass('selected');
        dialog.set_value('selected_doctype', doctype_name);
    });
}

function create_doctype_table(doctypes, is_custom) {
    let html = `<table class="table table-bordered table-hover">
        <thead>
            <tr>
                <th>DocType</th>
                <th>Module</th>
                <th>Records</th>
                <th>Description</th>
            </tr>
        </thead>
        <tbody>`;
    
    doctypes.forEach(function(dt) {
        html += `
            <tr class="doctype-row" data-doctype="${dt.name}" style="cursor: pointer;">
                <td><strong>${dt.name}</strong></td>
                <td><small class="text-muted">${dt.module}</small></td>
                <td><span class="badge badge-info">${dt.record_count}</span></td>
                <td><small>${dt.description || 'No description'}</small></td>
            </tr>`;
    });
    
    html += '</tbody></table>';
    return html;
}

function show_similarity_suggestions(dialog, suggestions) {
    if (suggestions.length === 0) return;
    
    let html = `<div class="alert alert-info">
        <strong>🎯 Similar DocTypes Found:</strong><br>
        Based on your CSV structure, these DocTypes might be a good match:
        <ul class="list-unstyled mt-2">`;
    
    suggestions.slice(0, 3).forEach(function(suggestion) {
        html += `<li class="mt-1">
            <strong>${suggestion.doctype}</strong> 
            <span class="badge badge-success">${suggestion.similarity}% match</span>
            <small class="text-muted">(${suggestion.matching_fields}/${suggestion.csv_fields} fields)</small>
        </li>`;
    });
    
    html += '</ul></div>';
    
    // Prepend to the doctype list
    dialog.fields_dict.doctype_list.$wrapper.prepend(html);
}

function redirect_to_doctype(frm, target_doctype) {
    frappe.call({
        method: 'data_migration_tool.data_migration.api.handle_doctype_creation_response',
        args: {
            request_id: frm.doc.name,
            action: 'Redirect',
            target_doctype: target_doctype
        },
        callback: function(r) {
            if (r.message && r.message.status === 'success') {
                frappe.msgprint({
                    message: __('Request redirected to {0}', [target_doctype]),
                    indicator: 'green'
                });
                frm.reload_doc();
            } else {
                frappe.msgprint({
                    message: r.message?.message || 'Failed to redirect request',
                    indicator: 'red'
                });
            }
        }
    });
}

// Rest of the existing functions remain the same...
function show_approval_modal(frm, action) {
    let title = action === 'Approve' ? 'Approve DocType Creation' : 'Reject DocType Creation';
    let fields = [];
    
    if (action === 'Approve') {
        fields.push({
            label: 'Target DocType Name',
            fieldname: 'target_doctype',
            fieldtype: 'Data',
            default: frm.doc.suggested_doctype,
            reqd: 1,
            description: 'Name for the new DocType (will be cleaned automatically)'
        });
    } else if (action === 'Reject') {
        fields.push({
            label: 'Rejection Reason',
            fieldname: 'rejection_reason',
            fieldtype: 'Text',
            reqd: 1,
            description: 'Please provide a reason for rejection'
        });
    }
    
    let dialog = new frappe.ui.Dialog({
        title: __(title),
        fields: fields,
        primary_action_label: __(action),
        primary_action: function() {
            let values = dialog.get_values();
            
            frappe.call({
                method: 'data_migration_tool.data_migration.api.handle_doctype_creation_response',
                args: {
                    request_id: frm.doc.name,
                    action: action,
                    target_doctype: values.target_doctype,
                    rejection_reason: values.rejection_reason
                },
                callback: function(r) {
                    if (r.message && r.message.status === 'success') {
                        frappe.msgprint({
                            message: r.message.message,
                            indicator: action === 'Approve' ? 'green' : 'orange'
                        });
                        frm.reload_doc();
                        dialog.hide();
                    } else {
                        frappe.msgprint({
                            message: r.message?.message || `Failed to ${action.toLowerCase()} request`,
                            indicator: 'red'
                        });
                    }
                }
            });
        }
    });
    
    dialog.show();
}

// Keep all other existing functions...
function show_all_created_doctypes() {
    frappe.call({
        method: 'data_migration_tool.data_migration_tool.doctype.doctype_creation_request.doctype_creation_request.get_created_doctypes_list',
        callback: function(r) {
            if (r.message && r.message.success) {
                let doctypes = r.message.doctypes;
                if (doctypes.length === 0) {
                    frappe.msgprint(__('No DocTypes have been created yet.'));
                    return;
                }

                let html = `<table class="table table-bordered">
                    <thead>
                        <tr>
                            <th>DocType Name</th>
                            <th>Created On</th>
                            <th>Last Modified</th>
                            <th>Actions</th>
                        </tr>
                    </thead>
                    <tbody>`;
                
                doctypes.forEach(function(dt) {
                    html += `<tr>
                        <td><strong>${dt.name}</strong></td>
                        <td>${frappe.datetime.str_to_user(dt.creation)}</td>
                        <td>${frappe.datetime.str_to_user(dt.modified)}</td>
                        <td>
                            <button class="btn btn-xs btn-primary" onclick="frappe.set_route('List', '${dt.name}')">
                                View Records
                            </button>
                            <button class="btn btn-xs btn-default" onclick="frappe.set_route('Form', 'DocType', '${dt.name}')">
                                Edit DocType
                            </button>
                        </td>
                    </tr>`;
                });
                
                html += '</tbody></table>';

                let dialog = new frappe.ui.Dialog({
                    title: __('All Created DocTypes'),
                    size: 'large',
                    fields: [{
                        fieldname: 'doctypes_html',
                        fieldtype: 'HTML',
                        options: html
                    }]
                });
                dialog.show();
            } else {
                frappe.msgprint(__('Error fetching DocTypes: {0}', [r.message?.error || 'Unknown error']));
            }
        }
    });
}

function show_field_analysis(frm) {
    if (!frm.doc.field_analysis) return;
    
    try {
        let analysis = JSON.parse(frm.doc.field_analysis);
        let fields = analysis.fields || {};
        
        if (Object.keys(fields).length === 0) {
            return;
        }
        
        let html = `<div class="alert alert-info">
            <h5>📊 CSV Field Analysis</h5>
            <p><strong>Source File:</strong> ${frm.doc.source_file || 'Unknown'}</p>
            <p><strong>Suggested DocType:</strong> ${frm.doc.suggested_doctype}</p>
            <p><strong>Field Count:</strong> ${frm.doc.field_count || 0}</p>
            <p><strong>Total Records:</strong> ${frm.doc.total_records || 0}</p>
        `;
        
        if (Object.keys(fields).length > 0) {
            html += '<h6>Field Mapping Preview:</h6>';
            html += '<table class="table table-sm table-bordered">';
            html += '<thead><tr><th>CSV Field</th><th>Mapped Field</th><th>Type</th><th>Sample Data</th></tr></thead><tbody>';
            
            Object.keys(fields).slice(0, 10).forEach(function(fieldName) {
                let field = fields[fieldName];
                html += '<tr>';
                html += `<td><code>${field.original_name}</code></td>`;
                html += `<td><code>${field.clean_name}</code></td>`;
                html += `<td><span class="badge badge-secondary">${field.suggested_type}</span></td>`;
                html += `<td><small>${(field.sample_values || []).slice(0, 2).join(', ')}</small></td>`;
                html += '</tr>';
            });
            
            html += '</tbody></table>';
            
            if (Object.keys(fields).length > 10) {
                html += `<p><small class="text-muted">... and ${Object.keys(fields).length - 10} more fields</small></p>`;
            }
        } else {
            html += '<p>No field analysis available.</p>';
        }
        
        html += '</div>';
        
        // Add field analysis to the form
        frm.dashboard.add_section(html, __('Field Analysis'));
        
    } catch (e) {
        console.error('Error parsing field analysis:', e);
    }
}

function setup_realtime_listeners() {
    frappe.realtime.on('doctype_request_status_update', function(data) {
        if (data.request_id === cur_frm?.doc?.name) {
            frappe.show_alert({
                message: data.message,
                indicator: data.status === 'Completed' ? 'green' : 'blue'
            });
            
            setTimeout(function() {
                cur_frm.reload_doc();
            }, 1000);
        }
    });
    
    frappe.realtime.on('doctype_processing_completed', function(data) {
        if (data.request_id === cur_frm?.doc?.name) {
            frappe.show_alert({
                message: `✅ Processing completed for ${data.filename}`,
                indicator: 'green'
            });
            
            setTimeout(function() {
                cur_frm.reload_doc();
            }, 1000);
        }
    });
    
    frappe.realtime.on('doctype_request_approved', function(data) {
        frappe.show_alert({
            message: `🚀 ${data.action} - Processing will begin shortly`,
            indicator: 'green'
        });
    });
}
