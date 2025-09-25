// Enhanced Migration Settings JavaScript - Phase 1
frappe.ui.form.on('Migration Settings', {
    refresh: function(frm) {
        // Add enhanced custom buttons with better error handling
        frm.add_custom_button(__('Test Zoho Connection'), function() {
            test_connection(frm, 'zoho');
        }, __('Test Connections'));

        frm.add_custom_button(__('Test Odoo Connection'), function() {
            test_connection(frm, 'odoo');
        }, __('Test Connections'));

        frm.add_custom_button(__('Test CSV Directory'), function() {
            test_connection(frm, 'csv');
        }, __('Test Connections'));

        frm.add_custom_button(__('Trigger Zoho Sync'), function() {
            trigger_sync(frm, 'zoho');
        }, __('Manual Sync'));

        frm.add_custom_button(__('Trigger Odoo Sync'), function() {
            trigger_sync(frm, 'odoo');
        }, __('Manual Sync'));

       // Replace the existing "Process CSV Files" button with this enhanced version
frm.add_custom_button(__("Intelligent CSV Processing"), function() {
    frappe.confirm(
        'Are you sure you want to start intelligent CSV processing? This will analyze and automatically process CSV files with smart field mapping.',
        function() {
            frappe.show_alert({
                message: __('Starting intelligent CSV processing...'),
                indicator: 'blue'
            });
            
            frappe.call({
                method: 'trigger_intelligent_processing',
                doc: frm.doc,
                callback: function(r) {
                    if (r.message && r.message.status === 'success') {
                        frappe.show_alert({
                            message: __(r.message.message),
                            indicator: 'green'
                        });
                        
                        // Show processing details
                        frappe.msgprint({
                            title: __('Processing Started'),
                            message: `
                                <p><strong>Files Found:</strong> ${r.message.files_found}</p>
                                <p><strong>Job ID:</strong> ${r.message.job_id}</p>
                                <p>The system will intelligently analyze CSV headers and create appropriate DocTypes.</p>
                                <p><em>Check Migration Status for updates.</em></p>
                            `,
                            indicator: 'blue'
                        });
                        
                        // Auto-refresh status after 5 seconds
                        if (r.message.job_id) {
                            setTimeout(() => check_job_status(r.message.job_id), 5000);
                        }
                    } else {
                        frappe.msgprint({
                            title: __('Processing Failed'),
                            message: r.message ? r.message.message : 'Failed to start intelligent processing',
                            indicator: 'red'
                        });
                    }
                },
                error: function(r) {
                    frappe.msgprint({
                        title: __('Processing Error'),
                        message: 'Failed to trigger intelligent processing. Please try again.',
                        indicator: 'red'
                    });
                }
            });
        }
    );
}, __("Manual Sync"));

        frm.add_custom_button(__('Full Sync'), function() {
            trigger_sync(frm, 'all');
        }, __('Manual Sync'));

        // NEW: Add monitoring buttons
        frm.add_custom_button(__('Migration Status'), function() {
            show_migration_status(frm);
        }, __('Monitoring'));

        frm.add_custom_button(__('Pending Requests'), function() {
            show_pending_requests(frm);
        }, __('Monitoring'));

        frm.add_custom_button(__('Buffer Statistics'), function() {
            show_buffer_statistics(frm);
        }, __('Monitoring'));

        // Add enhanced migration dashboard
        add_enhanced_migration_dashboard(frm);

        // Initialize real-time updates
        setup_realtime_updates(frm);
    },

    enable_zoho_sync: function(frm) {
        frm.toggle_reqd(['zoho_client_id', 'zoho_client_secret', 'zoho_refresh_token'], frm.doc.enable_zoho_sync);
    },

    enable_odoo_sync: function(frm) {
        frm.toggle_reqd(['odoo_url', 'odoo_database', 'odoo_username', 'odoo_password'], frm.doc.enable_odoo_sync);
    },

    enable_csv_processing: function(frm) {
        frm.toggle_reqd(['csv_watch_directory'], frm.doc.enable_csv_processing);
    }
});

function test_connection(frm, source) {
    frappe.show_alert({
        message: `Testing ${source} connection...`,
        indicator: 'blue'
    });

    frappe.call({
        method: 'data_migration_tool.data_migration.api.test_connection',
        args: { source: source },
        callback: function(r) {
            if (r.message && r.message.status === 'success') {
                frappe.show_alert({
                    message: `${source.toUpperCase()} connection successful`,
                    indicator: 'green'
                });
                
                if (r.message.details) {
                    frappe.msgprint({
                        title: __('Connection Details'),
                        message: `<pre>${JSON.stringify(r.message.details, null, 2)}</pre>`,
                        indicator: 'green'
                    });
                }
            } else {
                frappe.msgprint({
                    title: __('Connection Failed'),
                    message: r.message ? r.message.message : 'Unknown error occurred',
                    indicator: 'red'
                });
            }
        },
        error: function(r) {
            frappe.msgprint({
                title: __('Connection Error'),
                message: 'Failed to test connection. Please check your settings.',
                indicator: 'red'
            });
        }
    });
}

function trigger_sync(frm, source) {
    frappe.confirm(
        __(`Are you sure you want to trigger ${source} sync manually? This may take some time.`),
        function() {
            frappe.show_alert({
                message: `Starting ${source} sync...`,
                indicator: 'blue'
            });

            frappe.call({
                method: 'data_migration_tool.data_migration.api.trigger_manual_sync',
                args: { source: source },
                callback: function(r) {
                    if (r.message && r.message.status === 'success') {
                        frappe.show_alert({
                            message: r.message.message,
                            indicator: 'green'
                        });
                        
                        if (r.message.job_name) {
                            setTimeout(() => {
                                check_job_status(r.message.job_name);
                            }, 2000);
                        }
                    } else {
                        frappe.msgprint({
                            title: __('Sync Failed'),
                            message: r.message ? r.message.message : 'Failed to start sync',
                            indicator: 'red'
                        });
                    }
                },
                error: function(r) {
                    frappe.msgprint({
                        title: __('Sync Error'),
                        message: 'Failed to trigger sync. Please try again.',
                        indicator: 'red'
                    });
                }
            });
        }
    );
}

function show_migration_status(frm) {
    frappe.call({
        method: 'data_migration_tool.data_migration.api.get_migration_status',
        callback: function(r) {
            if (r.message && r.message.status === 'success') {
                const data = r.message;
                let html = `
                    <div class="migration-status-report">
                        <h5>Migration Status Overview</h5>
                        
                        <div class="row">
                            <div class="col-sm-6">
                                <h6>Settings Status</h6>
                                <ul>
                                    <li>CSV Processing: ${data.settings.csv_processing_enabled ? '✅ Enabled' : '❌ Disabled'}</li>
                                    <li>Zoho Sync: ${data.settings.zoho_sync_enabled ? '✅ Enabled' : '❌ Disabled'}</li>
                                    <li>Odoo Sync: ${data.settings.odoo_sync_enabled ? '✅ Enabled' : '❌ Disabled'}</li>
                                    <li>Last Sync: ${data.settings.last_sync_time || 'Never'}</li>
                                </ul>
                            </div>
                            <div class="col-sm-6">
                                <h6>Buffer Statistics</h6>
                                <ul>
                `;
                
                if (data.buffer_stats && data.buffer_stats.length > 0) {
                    const stats = {};
                    data.buffer_stats.forEach(stat => {
                        if (!stats[stat.processing_status]) stats[stat.processing_status] = 0;
                        stats[stat.processing_status] += stat.count;
                    });
                    
                    Object.keys(stats).forEach(status => {
                        const icon = status === 'Processed' ? '✅' : 
                                   status === 'Failed' ? '❌' : 
                                   status === 'Pending' ? '⏳' : '⚠️';
                        html += `<li>${icon} ${status}: ${stats[status]}</li>`;
                    });
                } else {
                    html += '<li>No buffer records found</li>';
                }
                
                html += `
                                </ul>
                            </div>
                        </div>
                        
                        <h6>Recent Requests</h6>
                        <table class="table table-sm">
                            <thead><tr><th>File</th><th>Status</th><th>DocType</th><th>Date</th></tr></thead>
                            <tbody>
                `;
                
                if (data.recent_requests && data.recent_requests.length > 0) {
                    data.recent_requests.forEach(req => {
                        const statusIcon = req.status === 'Completed' ? '✅' : 
                                         req.status === 'Failed' ? '❌' : 
                                         req.status === 'Pending' ? '⏳' : '⚠️';
                        html += `
                            <tr>
                                <td>${req.source_file}</td>
                                <td>${statusIcon} ${req.status}</td>
                                <td>${req.final_doctype || '-'}</td>
                                <td>${moment(req.created_at).format('DD/MM HH:mm')}</td>
                            </tr>
                        `;
                    });
                } else {
                    html += '<tr><td colspan="4">No recent requests</td></tr>';
                }
                
                html += `
                            </tbody>
                        </table>
                    </div>
                `;
                
                frappe.msgprint({
                    title: __('Migration Status'),
                    message: html,
                    wide: true
                });
            } else {
                frappe.msgprint('Failed to fetch migration status');
            }
        }
    });
}

function show_pending_requests(frm) {
    frappe.call({
        method: 'data_migration_tool.data_migration.api.get_pending_doctype_requests',
        callback: function(r) {
            if (r.message && r.message.status === 'success') {
                if (r.message.requests.length === 0) {
                    frappe.msgprint('No pending DocType creation requests found');
                    return;
                }
                
                // Use the dialog from migration_tool
                if (migration_tool && migration_tool.dialog) {
                    migration_tool.dialog.show_pending_requests_list(r.message.requests);
                } else {
                    frappe.msgprint('Dialog handler not available. Please refresh the page.');
                }
            } else {
                frappe.msgprint('Failed to fetch pending requests');
            }
        }
    });
}

function show_buffer_statistics(frm) {
    frappe.call({
        method: 'data_migration_tool.data_migration.api.get_buffer_statistics',  // Use whitelisted method
        callback: function(r) {
            if (r.message && r.message.status === 'success') {
                const data = r.message.data;
                let html = `
                    <div class="buffer-statistics">
                        <h5>Migration Buffer Statistics</h5>
                        <p><strong>Total Records:</strong> ${data.total_records}</p>
                        
                        <h6>By Status</h6>
                        <ul>
                `;
                
                Object.keys(data.by_status || {}).forEach(status => {
                    const icon = status === 'Processed' ? '✅' : 
                               status === 'Failed' ? '❌' : 
                               status === 'Pending' ? '⏳' : '⚠️';
                    html += `<li>${icon} ${status}: ${data.by_status[status]}</li>`;
                });
                
                html += `
                        </ul>
                        
                        <h6>By DocType</h6>
                        <table class="table table-sm">
                            <thead><tr><th>DocType</th><th>Pending</th><th>Processed</th><th>Failed</th></tr></thead>
                            <tbody>
                `;
                
                Object.keys(data.by_doctype || {}).forEach(doctype => {
                    const stats = data.by_doctype[doctype];
                    html += `
                        <tr>
                            <td>${doctype}</td>
                            <td>${stats.Pending || 0}</td>
                            <td>${stats.Processed || 0}</td>
                            <td>${stats.Failed || 0}</td>
                        </tr>
                    `;
                });
                
                html += `
                            </tbody>
                        </table>
                    </div>
                `;
                
                frappe.msgprint({
                    title: __('Buffer Statistics'),
                    message: html,
                    wide: true
                });
            } else {
                frappe.msgprint('Failed to fetch buffer statistics');
            }
        }
    });
}

function check_job_status(job_name) {
    frappe.call({
        method: 'data_migration_tool.data_migration.api.get_job_status',  // Use our whitelisted method
        args: { job_name: job_name },
        callback: function(r) {
            if (r.message && r.message.status === 'success') {
                const job_status = r.message.job_status;
                if (job_status === 'finished') {
                    frappe.show_alert({
                        message: 'Sync job completed successfully',
                        indicator: 'green'
                    });
                } else if (job_status === 'failed') {
                    frappe.show_alert({
                        message: 'Sync job failed',
                        indicator: 'red'
                    });
                } else if (job_status === 'started') {
                    frappe.show_alert({
                        message: 'Sync job still running...',
                        indicator: 'blue'
                    });
                    // Check again in 5 seconds
                    setTimeout(() => {
                        check_job_status(job_name);
                    }, 5000);
                }
            }
        }
    });
}
function add_enhanced_migration_dashboard(frm) {
    // Enhanced dashboard with real-time updates
    const last_sync_formatted = frm.doc.last_sync_time ? 
        moment(frm.doc.last_sync_time).format('DD/MM/YYYY HH:mm:ss') : 'Never';
    
    const active_sources = [];
    if (frm.doc.enable_zoho_sync) active_sources.push('Zoho');
    if (frm.doc.enable_odoo_sync) active_sources.push('Odoo');
    if (frm.doc.enable_csv_processing) active_sources.push('CSV');
    
    let dashboard_html = `
        <div class="migration-dashboard">
            <div class="row">
                <div class="col-sm-4">
                    <div class="card">
                        <div class="card-body">
                            <h6><i class="fa fa-clock-o"></i> Last Sync</h6>
                            <p>${last_sync_formatted}</p>
                        </div>
                    </div>
                </div>
                <div class="col-sm-4">
                    <div class="card">
                        <div class="card-body">
                            <h6><i class="fa fa-cogs"></i> Active Sources</h6>
                            <p>${active_sources.length > 0 ? active_sources.join(', ') : 'None configured'}</p>
                        </div>
                    </div>
                </div>
                <div class="col-sm-4">
                    <div class="card">
                        <div class="card-body">
                            <h6><i class="fa fa-refresh"></i> Sync Frequency</h6>
                            <p>${frm.doc.sync_frequency || 'Not set'}</p>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    `;
    
    $(frm.fields_dict['dashboard_html'].wrapper).html(dashboard_html);
}

function setup_realtime_updates(frm) {
    // Listen for migration events and update the dashboard
    frappe.realtime.on('migration_status_update', (data) => {
        frappe.show_alert({
            message: `Migration update: ${data.message}`,
            indicator: data.indicator || 'blue'
        });
        
        // Refresh the form if needed
        if (data.refresh_form) {
            frm.reload_doc();
        }
    });
    
    frappe.realtime.on('doctype_processing_completed', (data) => {
        frappe.show_alert({
            message: `Processing completed for ${data.filename}`,
            indicator: 'green'
        });
    });
}
