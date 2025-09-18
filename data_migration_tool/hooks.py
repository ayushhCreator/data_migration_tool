import frappe


app_name = "data_migration_tool"
app_title = "Data Migration Tool"
app_publisher = "Ayush Raj"
app_description = "Migrate Data from different sources to Frappe/ERPNext"
app_email = "araj09510@gmail.com"
app_license = "mit"

# Apps
# ------------------

# required_apps = []

# Each item in the list will be shown as an app in the apps page
# add_to_apps_screen = [
# 	{
# 		"name": "data_migration_tool",
# 		"logo": "/assets/data_migration_tool/logo.png",
# 		"title": "Data Migration Tool",
# 		"route": "/data_migration_tool",
# 		"has_permission": "data_migration_tool.api.permission.has_app_permission"
# 	}
# ]

# Includes in <head>
# ------------------

# include js, css files in header of desk.html
app_include_css = "/assets/data_migration_tool/css/data_migration_tool.css"

# app_include_js = [
#     "/assets/data_migration_tool/js/doctype_creation_dialog.js",
#       "/assets/data_migration_tool/js/doctype_creation_request.js",
#     "/assets/data_migration_tool/js/migration_settings.js" # Add this line
# ]


doctype_js = {
    "DocType Creation Request": "public/js/doctype_creation_request.js"  # Keep this if file exists
}
# Include JS files globally
app_include_js = [

    "/assets/data_migration_tool/js/data_migration_tool.js",
    "assets/data_migration_tool/js/migration_notifications.js",
    "assets/data_migration_tool/js/doctype_creation_dialog.js"
]

# DocType-specific JS files
doctype_js = {
    "DocType Creation Request": "public/js/doctype_creation_request.js",
    "Migration Settings": "public/js/migration_settings.js"
}

# # Application logo
# app_logo_url = '/assets/data_migration_tool/images/logo.png'

# hooks.py - Add document event hooks
doc_events = {
    "DocType Creation Request": {
        "on_update": "data_migration_tool.data_migration.utils.scheduler_tasks.on_doctype_request_update"
    }
}


# Website route rules
website_route_rules = [
    {"from_route": "/migration-settings", "to_route": "/app/migration-settings"},
]



# include js, css files in header of web template
# web_include_css = "/assets/data_migration_tool/css/data_migration_tool.css"
# web_include_js = "/assets/data_migration_tool/js/data_migration_tool.js"

# include custom scss in every website theme (without file extension ".scss")
# website_theme_scss = "data_migration_tool/public/scss/website"

# include js, css files in header of web form
# webform_include_js = {"doctype": "public/js/doctype.js"}
# webform_include_css = {"doctype": "public/css/doctype.css"}

# include js in page
# page_js = {"page" : "public/js/file.js"}

# include js in doctype views
# doctype_js = {"doctype" : "public/js/doctype.js"}
# doctype_list_js = {"doctype" : "public/js/doctype_list.js"}
# doctype_tree_js = {"doctype" : "public/js/doctype_tree.js"}
# doctype_calendar_js = {"doctype" : "public/js/doctype_calendar.js"}



# Add to existing hooks.py after the existing doctype_js section

# Domain-specific DocTypes
domains = {
    "Car Services": "data_migration_tool.domains.car_services"
}

# Add custom permissions
# permission_query_conditions = {
#     "Product": "data_migration_tool.data_migration_tool.doctype.product.product.get_permission_query_conditions_for_product",
#     "Service Category": "data_migration_tool.data_migration_tool.doctype.service_category.service_category.get_permission_query_conditions_for_service_category",
# }

# Add to document events
doc_events.update({
    "Product": {
        "validate": "data_migration_tool.data_migration_tool.doctype.product.product.validate_product_data",
        "on_update": "data_migration_tool.data_migration_tool.doctype.product.product.on_product_update"
    }
})



# Add document events for new DocTypes
doc_events.update({
    "Product": {
        "validate": "data_migration_tool.data_migration_tool.doctype.product.product.validate_product_relationships",
        "on_update": "data_migration_tool.data_migration.utils.scheduler_tasks.on_product_update"
    },
    "Service Category": {
        "on_update": "data_migration_tool.data_migration.utils.scheduler_tasks.on_master_data_update"
    },
    "Vehicle Type": {
        "on_update": "data_migration_tool.data_migration.utils.scheduler_tasks.on_master_data_update"  
    },
    "Service Type": {
        "on_update": "data_migration_tool.data_migration.utils.scheduler_tasks.on_master_data_update"
    }
})

# Add fixtures for easy deployment
fixtures = [
    {
        "dt": "Service Category",
        "filters": [["name", "in", ["Wash", "Polishing", "Detailing", "Other Services"]]]
    },
    {
        "dt": "Vehicle Type", 
        "filters": [["name", "in", ["HatchBack", "Sedan/SUV", "Luxury", "General"]]]
    },
    {
        "dt": "Service Type",
        "filters": [["name", "in", ["One-time", "Subscription"]]]
    }
]

# Svg Icons
# ------------------
# include app icons in desk
# app_include_icons = "data_migration_tool/public/icons.svg"

# Home Pages
# ----------

# application home page (will override Website Settings)
# home_page = "login"

# website user home page (by Role)
# role_home_page = {
# 	"Role": "home_page"
# }

# Generators
# ----------

# automatically create page for each record of this doctype
# website_generators = ["Web Page"]

# Jinja
# ----------

# add methods and filters to jinja environment
# jinja = {
# 	"methods": "data_migration_tool.utils.jinja_methods",
# 	"filters": "data_migration_tool.utils.jinja_filters"
# }

# Installation
# ------------

# before_install = "data_migration_tool.install.before_install"
# after_install = "data_migration_tool.install.after_install"

# Uninstallation
# ------------

# before_uninstall = "data_migration_tool.uninstall.before_uninstall"
# after_uninstall = "data_migration_tool.uninstall.after_uninstall"

# Integration Setup
# ------------------
# To set up dependencies/integrations with other apps
# Name of the app being installed is passed as an argument

# before_app_install = "data_migration_tool.utils.before_app_install"
# after_app_install = "data_migration_tool.utils.after_app_install"

# Integration Cleanup
# -------------------
# To clean up dependencies/integrations with other apps
# Name of the app being uninstalled is passed as an argument

# before_app_uninstall = "data_migration_tool.utils.before_app_uninstall"
# after_app_uninstall = "data_migration_tool.utils.after_app_uninstall"

# Desk Notifications
# ------------------
# See frappe.core.notifications.get_notification_config

# notification_config = "data_migration_tool.notifications.get_notification_config"

# Permissions
# -----------
# Permissions evaluated in scripted ways

# permission_query_conditions = {
# 	"Event": "frappe.desk.doctype.event.event.get_permission_query_conditions",
# }
#
# has_permission = {
# 	"Event": "frappe.desk.doctype.event.event.has_permission",
# }

# DocType Class
# ---------------
# Override standard doctype classes

# override_doctype_class = {
# 	"ToDo": "custom_app.overrides.CustomToDo"
# }

# Document Events
# ---------------
# Hook on document methods and events

# doc_events = {
# 	"*": {
# 		"on_update": "method",
# 		"on_cancel": "method",
# 		"on_trash": "method"
# 	}
# }

# Scheduled Tasks
# ---------------

# scheduler_events = {
# 	"all": [
# 		"data_migration_tool.tasks.all"
# 	],
# 	"daily": [
# 		"data_migration_tool.tasks.daily"
# 	],
# 	"hourly": [
# 		"data_migration_tool.tasks.hourly"
# 	],
# 	"weekly": [
# 		"data_migration_tool.tasks.weekly"
# 	],
# 	"monthly": [
# 		"data_migration_tool.tasks.monthly"
# 	],
# }

# scheduler_events = {
#     "cron": {
#         "*/15 * * * *": [  # Every 15 minutes
#             "data_migration_tool.data_migration.utils.scheduler_tasks.periodic_crm_sync",
#             "data_migration_tool.data_migration.utils.scheduler_tasks.check_pending_requests_and_process"  # Add this
#         ],
#         "0 3 * * *": [  # Daily at 3 AM
#             "data_migration_tool.data_migration.utils.scheduler_tasks.cleanup_old_logs"
#         ]
#     }
# }

# Document Events (in hooks.py)
doc_events = {
    "Your DocType": {
        "on_update": "your.module.your_function"
    }
}

# Your function (must accept doc, method)
def your_function(doc, method):
    # Your code here
    pass


# Background job context setup
def setup_job_context():
    """Setup proper context for background jobs"""
    try:
        if not hasattr(frappe, 'session') or not frappe.session.user:
            frappe.set_user('Administrator')
    except Exception:
        pass

def cleanup_job_context():
    """Cleanup job context"""
    pass

# Update your existing scheduler events:
scheduler_events = {
    "cron": {
        "*/5 * * * *": [
            "data_migration_tool.data_migration.utils.scheduler_tasks.check_pending_requests_and_process"
        ],
        "*/15 * * * *": [
            "data_migration_tool.data_migration.utils.scheduler_tasks.process_csv_files_with_jit"
        ],
        "0 3 * * *": [
            "data_migration_tool.data_migration.utils.scheduler_tasks.cleanup_old_logs"
        ]
    }
}

# Background job events
background_job_events = {
    "before_job": [
        "data_migration_tool.hooks.setup_job_context"  # Add this
    ],
    "after_job": [
        "data_migration_tool.hooks.cleanup_job_context"  # Add this
    ]
}


# Document Events
doc_events = {
    "Migration Settings": {
        "on_update": "data_migration_tool.data_migration.utils.scheduler_tasks.on_settings_update"
    }
}


# REST API whitelist
# api_methods = [
#     "data_migration_tool.data_migration.api.test_connection",
#     "data_migration_tool.data_migration.api.trigger_manual_sync", 
#     "data_migration_tool.data_migration.api.upload_csv_file",
#     "data_migration_tool.data_migration.api.get_migration_status"
# ]

# Updated hooks.py - Add these to your existing api_methods list
api_methods = [
    "data_migration_tool.data_migration.api.test_connection",
    "data_migration_tool.data_migration.api.trigger_manual_sync", 
    "data_migration_tool.data_migration.api.upload_csv_file",
    "data_migration_tool.data_migration.api.get_migration_status",
    "data_migration_tool.data_migration.api.handle_doctype_creation_response",
    "data_migration_tool.data_migration.api.get_pending_doctype_requests", 
    "data_migration_tool.data_migration.api.get_existing_doctypes",
    "data_migration_tool.data_migration.api.get_job_status",  # New method we'll create
    "data_migration_tool.data_migration.connectors.csv_connector.get_buffer_statistics",  # Fix buffer stats
    "data_migration_tool.data_migration.utils.scheduler_tasks.manual_csv_processing"  # For manual triggers
    "data_migration_tool.data_migration.importers.yawlit_importer.import_yawlit_services",  # ADD THIS
    "data_migration_tool.data_migration.api.get_product_catalog" # ADD THIS
]




# # Installation
# after_install = "data_migration_tool.install.after_install"


# Testing
# -------

# before_tests = "data_migration_tool.install.before_tests"

# Overriding Methods
# ------------------------------
#
# override_whitelisted_methods = {
# 	"frappe.desk.doctype.event.event.get_events": "data_migration_tool.event.get_events"
# }
#
# each overriding function accepts a `data` argument;
# generated from the base implementation of the doctype dashboard,
# along with any modifications made in other Frappe apps
# override_doctype_dashboards = {
# 	"Task": "data_migration_tool.task.get_dashboard_data"
# }

# exempt linked doctypes from being automatically cancelled
#
# auto_cancel_exempted_doctypes = ["Auto Repeat"]

# Ignore links to specified DocTypes when deleting documents
# -----------------------------------------------------------

# ignore_links_on_delete = ["Communication", "ToDo"]

# Request Events
# ----------------
# before_request = ["data_migration_tool.utils.before_request"]
# after_request = ["data_migration_tool.utils.after_request"]

# Job Events
# ----------
# before_job = ["data_migration_tool.utils.before_job"]
# after_job = ["data_migration_tool.utils.after_job"]

# User Data Protection
# --------------------

# user_data_fields = [
# 	{
# 		"doctype": "{doctype_1}",
# 		"filter_by": "{filter_by}",
# 		"redact_fields": ["{field_1}", "{field_2}"],
# 		"partial": 1,
# 	},
# 	{
# 		"doctype": "{doctype_2}",
# 		"filter_by": "{filter_by}",
# 		"partial": 1,
# 	},
# 	{
# 		"doctype": "{doctype_3}",
# 		"strict": False,
# 	},
# 	{
# 		"doctype": "{doctype_4}"
# 	}
# ]

# Authentication and authorization
# --------------------------------

# auth_hooks = [
# 	"data_migration_tool.auth.validate"
# ]

# Automatically update python controller files with type annotations for this app.
# export_python_type_annotations = True

# default_log_clearing_doctypes = {
# 	"Logging DocType Name": 30  # days to retain logs
# }

