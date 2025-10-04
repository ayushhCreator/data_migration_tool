// Global migration notification handler
$(document).ready(function() {
    if (typeof frappe !== 'undefined' && frappe.realtime) {
        setup_global_migration_listeners();
    }
});

function setup_global_migration_listeners() {
    // Global listener for DocType creation requests
    frappe.realtime.on('doctype_creation_request', function(data) {
        // Only show to system managers
        if (frappe.user.has_role('System Manager')) {
            show_global_approval_notification(data);
        }
    });
}

function show_global_approval_notification(data) {
    frappe.show_alert({
        message: `📋 New DocType Request: ${data.filename}`,
        indicator: 'blue'
    }, 5);
    
    // Show desktop notification if supported
    if (Notification.permission === 'granted') {
        new Notification('DocType Creation Request', {
            body: `File: ${data.filename}\nSuggested DocType: ${data.suggested_doctype}`,
            icon: '/assets/frappe/images/frappe-favicon.svg'
        });
    }
    
    // Auto-open the DocType Creation Request list after 2 seconds
    setTimeout(() => {
        frappe.set_route('List', 'DocType Creation Request', {status: 'Pending'});
    }, 2000);
}
