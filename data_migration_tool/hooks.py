import frappe

app_name = "data_migration_tool"
app_title = "Data Migration Tool"
app_publisher = "Ayush Raj"
app_description = "Migrate Data from different sources to Frappe/ERPNext"
app_email = "araj09510@gmail.com"
app_license = "mit"
app_version = "0.1.0"

fixtures = []

# Required for marketplace
required_apps = ["frappe"]

# Page JS configuration
page_js = {
    "migration_dashboard": "public/js/migration_dashboard.js"
}

# App level JS includes
app_include_js = [
    "assets/data_migration_tool/js/data_migration_tool.js",
    "assets/data_migration_tool/js/migration_notifications.js",
    "assets/data_migration_tool/js/doctype_creation_dialog.js"
]

# App level CSS includes
app_include_css = [
    "/assets/data_migration_tool/css/data_migration_tool.css",
    "/assets/data_migration_tool/css/dashboard.css"
]

# DocType specific JS
doctype_js = {
    "DocType Creation Request": "public/js/doctype_creation_request.js",
    "Migration Settings": "public/js/migration_settings.js"
}

# Extend desk menu
extend_desk_menu = [
    {
        "label": "Migration Dashboard",
        "type": "page",
        "name": "migration_dashboard",
        "icon": "fa fa-dashboard"
    }
]

# Document Events
doc_events = {
    "DocType Creation Request": {
        "on_update": "data_migration_tool.data_migration.utils.scheduler_tasks.on_doctype_request_update"
    },
    "Migration Settings": {
        "on_update": "data_migration_tool.data_migration.utils.scheduler_tasks.on_settings_update"
    }
}

# Scheduled Tasks
scheduler_events = {
    "cron": {
        "*/15 * * * *": [
            "data_migration_tool.data_migration.utils.scheduler_tasks.periodic_crm_sync"
        ],
        "0 3 * * *": [
            "data_migration_tool.data_migration.utils.scheduler_tasks.cleanup_old_logs"
        ]
    }
}

# REST API whitelist
api_methods = [
    "data_migration_tool.data_migration.api.test_connection",
    "data_migration_tool.data_migration.api.trigger_manual_sync",
    "data_migration_tool.data_migration.api.get_migration_status",
    "data_migration_tool.data_migration.api.handle_doctype_creation_response",
    "data_migration_tool.data_migration.api.get_pending_doctype_requests",
    "data_migration_tool.data_migration.api.get_existing_doctypes",
    "data_migration_tool.data_migration.api.get_job_status",
    "data_migration_tool.data_migration.connectors.csv_connector.get_buffer_statistics",
    "data_migration_tool.data_migration.api.get_import_statistics",
    "data_migration_tool.data_migration.utils.scheduler_tasks.manual_csv_processing",
    "data_migration_tool.data_migration_tool.page.migration_dashboard.migration_dashboard.get_dashboard_data",
    "data_migration_tool.data_migration_tool.page.migration_dashboard.migration_dashboard.trigger_migration"
]



# Background job events
background_job_events = {
    "before_job": [
        "data_migration_tool.hooks.setup_job_context"
    ],
    "after_job": [
        "data_migration_tool.hooks.cleanup_job_context"
    ]
}

def setup_job_context():
    """Setup context for background jobs"""
    pass

def cleanup_job_context():
    """Cleanup context after background jobs"""
    pass