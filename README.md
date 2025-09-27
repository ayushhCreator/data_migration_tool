# Data Migration Tool

A comprehensive data migration solution for Frappe/ERPNext that supports multiple data sources including CSV files, Odoo, Zoho CRM, and custom connectors with automated DocType creation and intelligent field mapping.

## Features

🔄 **Multi-Source Migration**: Support for CSV, Odoo, Zoho CRM, and extensible connector architecture
📊 **Automated DocType Creation**: Intelligent analysis and creation of Frappe DocTypes from source data
🗺️ **Smart Field Mapping**: Advanced field mapping with type detection and transformation
⚡ **Background Processing**: Scheduled and manual migration jobs with progress tracking
🔍 **Data Validation**: Comprehensive validation and error handling during migration
📈 **Migration Analytics**: Detailed statistics and reporting for migration processes
🛠️ **Developer Friendly**: Easy to extend with custom connectors and transformations

## Installation

Install this app using the bench CLI:

cd $PATH_TO_YOUR_BENCH
bench get-app https://github.com/ayushhCreator/data_migration_tool --branch main
bench install-app data_migration_too



## Quick Start

1. **Configure Migration Settings**: Set up your data source connections
2. **Create Migration Jobs**: Define source-to-target mapping
3. **Monitor Progress**: Track migration status and handle errors
4. **Validate Results**: Review migrated data and run validation reports

## Supported Data Sources

- **CSV Files**: Direct file upload and processing
- **Odoo**: Connect to Odoo databases via XML-RPC
- **Zoho CRM**: API-based data extraction
- **Custom Connectors**: Extensible architecture for additional sources

## Documentation

- [Installation Guide](https://github.com/ayushhCreator/data_migration_tool/wiki/Installation)
- [Configuration](https://github.com/ayushhCreator/data_migration_tool/wiki/Configuration)
- [Creating Custom Connectors](https://github.com/ayushhCreator/data_migration_tool/wiki/Custom-Connectors)
- [API Reference](https://github.com/ayushhCreator/data_migration_tool/wiki/API)

## Contributing

This app uses `pre-commit` for code formatting and linting. Please install pre-commit and enable it for this repository:

