// Migration Tool JavaScript Functions
frappe.provide('migration_tool');

migration_tool.utils = {
    // Format datetime for display
    format_datetime: function(datetime_str) {
        if (!datetime_str) return 'Never';
        return moment(datetime_str).format('YYYY-MM-DD HH:mm:ss');
    },
    
    // Get status indicator HTML
    get_status_indicator: function(status) {
        const indicators = {
            'success': '<span class="migration-status-indicator success"></span>',
            'warning': '<span class="migration-status-indicator warning"></span>',
            'error': '<span class="migration-status-indicator error"></span>'
        };
        return indicators[status] || '';
    },
    
    // Show sync progress dialog
    show_sync_progress: function(source) {
        const d = new frappe.ui.Dialog({
            title: `${source.toUpperCase()} Sync Progress`,
            size: 'large',
            fields: [
                {
                    fieldtype: 'HTML',
                    fieldname: 'progress_html',
                    options: `
                        <div class="migration-log-viewer" id="sync-progress-log">
                            <div class="log-entry">🚀 Starting ${source} synchronization...</div>
                            <div class="log-entry">📊 Initializing connection...</div>
                            <div class="log-entry">🔄 Please wait while the sync is in progress...</div>
                            <div class="text-center" style="margin-top: 20px;">
                                <div class="migration-loading"></div>
                            </div>
                        </div>
                    `
                }
            ],
            primary_action_label: 'Close',
            primary_action: function() {
                d.hide();
            }
        });
        
        d.show();
        
        // Simulate progress updates
        setTimeout(() => {
            $('#sync-progress-log').append('<div class="log-entry success">✅ Connection established</div>');
        }, 1000);
        
        return d;
    },
    
    // Update migration statistics
    update_stats: function(stats) {
        const stats_html = `
            <div class="migration-stats">
                <div class="stat-item">
                    <div class="stat-number">${stats.total_records || 0}</div>
                    <div class="stat-label">Total Records</div>
                </div>
                <div class="stat-item">
                    <div class="stat-number">${stats.successful || 0}</div>
                    <div class="stat-label">Successful</div>
                </div>
                <div class="stat-item">
                    <div class="stat-number">${stats.failed || 0}</div>
                    <div class="stat-label">Failed</div>
                </div>
                <div class="stat-item">
                    <div class="stat-number">${stats.warnings || 0}</div>
                    <div class="stat-label">Warnings</div>
                </div>
            </div>
        `;
        return stats_html;
    }
};

// Global functions for form scripts
window.test_connection = function(frm, source) {
    frappe.call({
        method: `test_${source}_connection`,
        doc: frm.doc,
        freeze: true,
        freeze_message: `Testing ${source.toUpperCase()} connection...`,
        callback: function(r) {
            if (r.message) {
                if (r.message.status === 'success') {
                    frappe.show_alert({
                        message: `${source.toUpperCase()} connection successful! 🎉`,
                        indicator: 'green'
                    });
                    
                    // Show additional info if available
                    if (r.message.version_info || r.message.user_info) {
                        frappe.msgprint({
                            title: 'Connection Details',
                            message: `Connection successful!<br>
                                     Details: ${JSON.stringify(r.message.version_info || r.message.user_info, null, 2)}`,
                            indicator: 'green'
                        });
                    }
                } else {
                    frappe.show_alert({
                        message: `${source.toUpperCase()} connection failed: ${r.message.message}`,
                        indicator: 'red'
                    });
                }
            }
        }
    });
};

window.trigger_sync = function(frm, source) {
    frappe.confirm(
        `Are you sure you want to trigger ${source.toUpperCase()} sync manually?<br><br>
         <small class="text-muted">This will start a background job to synchronize data from ${source}.</small>`,
        function() {
            // Show progress dialog
            const progress_dialog = migration_tool.utils.show_sync_progress(source);
            
            frappe.call({
                method: 'trigger_manual_sync',
                doc: frm.doc,
                args: {
                    source: source
                },
                callback: function(r) {
                    if (r.message && r.message.status === 'success') {
                        frappe.show_alert({
                            message: r.message.message,
                            indicator: 'blue'
                        });
                        
                        // Update progress dialog
                        setTimeout(() => {
                            $('#sync-progress-log').append('<div class="log-entry success">✅ Sync job queued successfully</div>');
                            setTimeout(() => {
                                progress_dialog.hide();
                                frappe.msgprint({
                                    title: 'Sync Started',
                                    message: `${source.toUpperCase()} sync has been queued for processing.<br>
                                             Check the background jobs and logs for progress.<br><br>
                                             <small>You can continue using the system while sync runs in background.</small>`,
                                    indicator: 'blue'
                                });
                            }, 2000);
                        }, 1500);
                    } else {
                        progress_dialog.hide();
                        frappe.msgprint({
                            title: 'Sync Failed',
                            message: r.message ? r.message.message : 'Unknown error occurred',
                            indicator: 'red'
                        });
                    }
                }
            });
        }
    );
};

window.add_migration_dashboard = function(frm) {
    // Clear existing dashboard
    $(frm.dashboard.wrapper).empty();
    
    const last_sync_formatted = migration_tool.utils.format_datetime(frm.doc.last_sync_time);
    const active_sources = [];
    
    if (frm.doc.enable_zoho_sync) active_sources.push('Zoho CRM');
    if (frm.doc.enable_odoo_sync) active_sources.push('Odoo ERP'); 
    if (frm.doc.enable_csv_processing) active_sources.push('CSV/Excel');
    
    const dashboard_html = `
        <div class="migration-dashboard">
            <div class="row">
                <div class="col-sm-4">
                    <div class="card">
                        <div class="card-body">
                            <h6>📅 Last Sync</h6>
                            <p class="text-muted">${last_sync_formatted}</p>
                        </div>
                    </div>
                </div>
                <div class="col-sm-4">
                    <div class="card">
                        <div class="card-body">
                            <h6>🔗 Active Sources</h6>
                            <p class="text-muted">
                                ${active_sources.length > 0 ? active_sources.join(', ') : 'None configured'}
                            </p>
                        </div>
                    </div>
                </div>
                <div class="col-sm-4">
                    <div class="card">
                        <div class="card-body">
                            <h6>⏰ Sync Frequency</h6>
                            <p class="text-muted">${frm.doc.sync_frequency || 'Not set'}</p>
                        </div>
                    </div>
                </div>
            </div>
            <div class="sync-frequency-info">
                <strong>💡 Note:</strong> Changes to sync frequency require a service restart to take effect. 
                Background jobs run automatically based on the configured schedule.
            </div>
        </div>
    `;
    
    $(frm.dashboard.wrapper).html(dashboard_html);
};

// Auto-refresh dashboard every 30 seconds
setInterval(function() {
    if (cur_frm && cur_frm.doctype === 'Migration Settings') {
        // Refresh last sync time without full page reload
        frappe.call({
            method: 'frappe.client.get_value',
            args: {
                doctype: 'Migration Settings',
                fieldname: 'last_sync_time',
                filters: {'name': 'Migration Settings'}
            },
            callback: function(r) {
                if (r.message && r.message.last_sync_time !== cur_frm.doc.last_sync_time) {
                    cur_frm.doc.last_sync_time = r.message.last_sync_time;
                    add_migration_dashboard(cur_frm);
                }
            }
        });
    }
}, 30000); // 30 seconds
