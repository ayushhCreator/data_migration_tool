frappe.pages['migration_control_center'].on_page_load = function(wrapper) {
    var page = frappe.ui.make_app_page({
        parent: wrapper,
        title: 'Migration Control Center',
        single_column: true
    });
    
    // Simple HTML content that will definitely work
    page.main.html(`
        <div style="padding: 30px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;">
            
            <!-- Header -->
            <div style="text-align: center; margin-bottom: 40px; padding: 20px; background: #f8f9fa; border-radius: 10px;">
                <h1 style="color: #495057; margin: 0; font-size: 2rem;">
                    🚀 Migration Control Center
                </h1>
                <p style="margin: 10px 0 0 0; color: #6c757d;">
                    Comprehensive data migration management dashboard
                </p>
            </div>

            <!-- Quick Actions -->
            <div style="background: white; padding: 25px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); margin-bottom: 30px;">
                <h3 style="margin: 0 0 20px 0; color: #495057;">⚡ Quick Actions</h3>
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px;">
                    <button onclick="frappe.set_route('List', 'Migration Settings')" 
                            style="padding: 15px 20px; background: #007bff; color: white; border: none; border-radius: 8px; cursor: pointer; font-size: 14px; font-weight: 500;">
                        📊 CSV Import Settings
                    </button>
                    <button onclick="frappe.new_doc('Product')" 
                            style="padding: 15px 20px; background: #28a745; color: white; border: none; border-radius: 8px; cursor: pointer; font-size: 14px; font-weight: 500;">
                        📦 Add Product
                    </button>
                    <button onclick="frappe.set_route('List', 'DocType Creation Request', {'status': 'Pending'})" 
                            style="padding: 15px 20px; background: #ffc107; color: #212529; border: none; border-radius: 8px; cursor: pointer; font-size: 14px; font-weight: 500;">
                        ⏳ Pending Requests
                    </button>
                </div>
            </div>

            <!-- Migration Management -->
            <div style="background: white; padding: 25px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); margin-bottom: 30px;">
                <h3 style="margin: 0 0 20px 0; color: #495057; text-align: center;">🔄 Migration Management</h3>
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px;">
                    
                    <div style="border: 1px solid #dee2e6; border-radius: 8px; padding: 20px; cursor: pointer;" onclick="frappe.set_route('List', 'Migration Settings')">
                        <div style="display: flex; align-items: center; margin-bottom: 15px;">
                            <div style="width: 50px; height: 50px; background: #007bff; color: white; border-radius: 8px; display: flex; align-items: center; justify-content: center; font-size: 20px; margin-right: 15px;">⚙️</div>
                            <h4 style="margin: 0; color: #495057;">Migration Settings</h4>
                        </div>
                        <p style="margin: 0 0 15px 0; color: #6c757d; font-size: 14px;">Configure data sources and migration parameters</p>
                        <div style="display: flex; gap: 10px;">
                            <button onclick="event.stopPropagation(); frappe.new_doc('Migration Settings')" 
                                    style="flex: 1; padding: 8px; background: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">
                                New Setting
                            </button>
                            <button onclick="event.stopPropagation(); frappe.set_route('List', 'Migration Settings')" 
                                    style="flex: 1; padding: 8px; background: #f8f9fa; color: #495057; border: 1px solid #dee2e6; border-radius: 4px; cursor: pointer;">
                                View All
                            </button>
                        </div>
                    </div>

                    <div style="border: 1px solid #dee2e6; border-radius: 8px; padding: 20px; cursor: pointer;" onclick="frappe.set_route('List', 'Migration Data Buffer')">
                        <div style="display: flex; align-items: center; margin-bottom: 15px;">
                            <div style="width: 50px; height: 50px; background: #17a2b8; color: white; border-radius: 8px; display: flex; align-items: center; justify-content: center; font-size: 20px; margin-right: 15px;">💾</div>
                            <h4 style="margin: 0; color: #495057;">Migration Buffer</h4>
                        </div>
                        <p style="margin: 0 0 15px 0; color: #6c757d; font-size: 14px;">Temporary storage during migration</p>
                        <div style="display: flex; gap: 10px;">
                            <button onclick="event.stopPropagation(); frappe.set_route('List', 'Migration Data Buffer')" 
                                    style="flex: 1; padding: 8px; background: #17a2b8; color: white; border: none; border-radius: 4px; cursor: pointer;">
                                View Buffer
                            </button>
                            <button onclick="event.stopPropagation(); frappe.set_route('Report', 'Migration Data Buffer')" 
                                    style="flex: 1; padding: 8px; background: #f8f9fa; color: #495057; border: 1px solid #dee2e6; border-radius: 4px; cursor: pointer;">
                                Reports
                            </button>
                        </div>
                    </div>

                </div>
            </div>

            <!-- DocType Management -->
            <div style="background: white; padding: 25px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); margin-bottom: 30px;">
                <h3 style="margin: 0 0 20px 0; color: #495057; text-align: center;">📄 DocType Management</h3>
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px;">
                    
                    <div style="border: 1px solid #dee2e6; border-radius: 8px; padding: 20px; cursor: pointer;" onclick="frappe.set_route('List', 'DocType Creation Request')">
                        <div style="display: flex; align-items: center; margin-bottom: 15px;">
                            <div style="width: 50px; height: 50px; background: #ffc107; color: #212529; border-radius: 8px; display: flex; align-items: center; justify-content: center; font-size: 20px; margin-right: 15px;">📋</div>
                            <h4 style="margin: 0; color: #495057;">DocType Requests</h4>
                        </div>
                        <p style="margin: 0 0 15px 0; color: #6c757d; font-size: 14px;">Handle dynamic DocType creation</p>
                        <div style="display: flex; gap: 10px;">
                            <button onclick="event.stopPropagation(); frappe.new_doc('DocType Creation Request')" 
                                    style="flex: 1; padding: 8px; background: #ffc107; color: #212529; border: none; border-radius: 4px; cursor: pointer;">
                                New Request
                            </button>
                            <button onclick="event.stopPropagation(); frappe.set_route('List', 'DocType Creation Request')" 
                                    style="flex: 1; padding: 8px; background: #f8f9fa; color: #495057; border: 1px solid #dee2e6; border-radius: 4px; cursor: pointer;">
                                View All
                            </button>
                        </div>
                    </div>

                    <div style="border: 1px solid #dee2e6; border-radius: 8px; padding: 20px; cursor: pointer;" onclick="frappe.set_route('List', 'CSV Schema Registry')">
                        <div style="display: flex; align-items: center; margin-bottom: 15px;">
                            <div style="width: 50px; height: 50px; background: #6f42c1; color: white; border-radius: 8px; display: flex; align-items: center; justify-content: center; font-size: 20px; margin-right: 15px;">📊</div>
                            <h4 style="margin: 0; color: #495057;">CSV Schema Registry</h4>
                        </div>
                        <p style="margin: 0 0 15px 0; color: #6c757d; font-size: 14px;">CSV mapping configurations</p>
                        <div style="display: flex; gap: 10px;">
                            <button onclick="event.stopPropagation(); frappe.new_doc('CSV Schema Registry')" 
                                    style="flex: 1; padding: 8px; background: #6f42c1; color: white; border: none; border-radius: 4px; cursor: pointer;">
                                New Schema
                            </button>
                            <button onclick="event.stopPropagation(); frappe.set_route('List', 'CSV Schema Registry')" 
                                    style="flex: 1; padding: 8px; background: #f8f9fa; color: #495057; border: 1px solid #dee2e6; border-radius: 4px; cursor: pointer;">
                                View All
                            </button>
                        </div>
                    </div>

                </div>
            </div>

            <!-- Master Data -->
            <div style="background: white; padding: 25px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); margin-bottom: 30px;">
                <h3 style="margin: 0 0 20px 0; color: #495057; text-align: center;">📦 Master Data</h3>
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 20px;">
                    
                    <div style="border: 1px solid #dee2e6; border-radius: 8px; padding: 20px; cursor: pointer;" onclick="frappe.set_route('List', 'Product')">
                        <div style="display: flex; align-items: center; margin-bottom: 15px;">
                            <div style="width: 50px; height: 50px; background: #28a745; color: white; border-radius: 8px; display: flex; align-items: center; justify-content: center; font-size: 20px; margin-right: 15px;">📦</div>
                            <h4 style="margin: 0; color: #495057;">Products</h4>
                        </div>
                        <p style="margin: 0 0 15px 0; color: #6c757d; font-size: 14px;">Product catalog management</p>
                        <div style="display: flex; gap: 10px;">
                            <button onclick="event.stopPropagation(); frappe.new_doc('Product')" 
                                    style="flex: 1; padding: 8px; background: #28a745; color: white; border: none; border-radius: 4px; cursor: pointer;">
                                New Product
                            </button>
                            <button onclick="event.stopPropagation(); frappe.set_route('List', 'Product')" 
                                    style="flex: 1; padding: 8px; background: #f8f9fa; color: #495057; border: 1px solid #dee2e6; border-radius: 4px; cursor: pointer;">
                                View All
                            </button>
                        </div>
                    </div>

                    <div style="border: 1px solid #dee2e6; border-radius: 8px; padding: 20px; cursor: pointer;" onclick="frappe.set_route('List', 'Service Category')">
                        <div style="display: flex; align-items: center; margin-bottom: 15px;">
                            <div style="width: 50px; height: 50px; background: #6f42c1; color: white; border-radius: 8px; display: flex; align-items: center; justify-content: center; font-size: 20px; margin-right: 15px;">🏷️</div>
                            <h4 style="margin: 0; color: #495057;">Service Categories</h4>
                        </div>
                        <p style="margin: 0 0 15px 0; color: #6c757d; font-size: 14px;">Service categorization</p>
                        <div style="display: flex; gap: 10px;">
                            <button onclick="event.stopPropagation(); frappe.new_doc('Service Category')" 
                                    style="flex: 1; padding: 8px; background: #6f42c1; color: white; border: none; border-radius: 4px; cursor: pointer;">
                                New Category
                            </button>
                            <button onclick="event.stopPropagation(); frappe.set_route('List', 'Service Category')" 
                                    style="flex: 1; padding: 8px; background: #f8f9fa; color: #495057; border: 1px solid #dee2e6; border-radius: 4px; cursor: pointer;">
                                View All
                            </button>
                        </div>
                    </div>

                    <div style="border: 1px solid #dee2e6; border-radius: 8px; padding: 20px; cursor: pointer;" onclick="frappe.set_route('List', 'Vehicle Type')">
                        <div style="display: flex; align-items: center; margin-bottom: 15px;">
                            <div style="width: 50px; height: 50px; background: #fd7e14; color: white; border-radius: 8px; display: flex; align-items: center; justify-content: center; font-size: 20px; margin-right: 15px;">🚗</div>
                            <h4 style="margin: 0; color: #495057;">Vehicle Types</h4>
                        </div>
                        <p style="margin: 0 0 15px 0; color: #6c757d; font-size: 14px;">Vehicle classifications</p>
                        <div style="display: flex; gap: 10px;">
                            <button onclick="event.stopPropagation(); frappe.new_doc('Vehicle Type')" 
                                    style="flex: 1; padding: 8px; background: #fd7e14; color: white; border: none; border-radius: 4px; cursor: pointer;">
                                New Type
                            </button>
                            <button onclick="event.stopPropagation(); frappe.set_route('List', 'Vehicle Type')" 
                                    style="flex: 1; padding: 8px; background: #f8f9fa; color: #495057; border: 1px solid #dee2e6; border-radius: 4px; cursor: pointer;">
                                View All
                            </button>
                        </div>
                    </div>

                </div>
            </div>

            <!-- Business Data -->
            <div style="background: white; padding: 25px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">
                <h3 style="margin: 0 0 20px 0; color: #495057; text-align: center;">💼 Business Data</h3>
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 15px;">
                    
                    <div style="border: 1px solid #dee2e6; border-radius: 8px; padding: 15px; text-align: center; cursor: pointer;" onclick="frappe.set_route('List', 'Yawlitquote')">
                        <div style="width: 40px; height: 40px; background: #17a2b8; color: white; border-radius: 8px; display: flex; align-items: center; justify-content: center; margin: 0 auto 10px;">💰</div>
                        <h5 style="margin: 0; color: #495057;">Quotes</h5>
                    </div>

                    <div style="border: 1px solid #dee2e6; border-radius: 8px; padding: 15px; text-align: center; cursor: pointer;" onclick="frappe.set_route('List', 'Yawlitvendor')">
                        <div style="width: 40px; height: 40px; background: #28a745; color: white; border-radius: 8px; display: flex; align-items: center; justify-content: center; margin: 0 auto 10px;">🏢</div>
                        <h5 style="margin: 0; color: #495057;">Vendors</h5>
                    </div>

                    <div style="border: 1px solid #dee2e6; border-radius: 8px; padding: 15px; text-align: center; cursor: pointer;" onclick="frappe.set_route('List', 'Yawlitcontacts')">
                        <div style="width: 40px; height: 40px; background: #007bff; color: white; border-radius: 8px; display: flex; align-items: center; justify-content: center; margin: 0 auto 10px;">👥</div>
                        <h5 style="margin: 0; color: #495057;">Contacts</h5>
                    </div>

                    <div style="border: 1px solid #dee2e6; border-radius: 8px; padding: 15px; text-align: center; cursor: pointer;" onclick="frappe.set_route('List', 'Yawlitexpense')">
                        <div style="width: 40px; height: 40px; background: #ffc107; color: #212529; border-radius: 8px; display: flex; align-items: center; justify-content: center; margin: 0 auto 10px;">💳</div>
                        <h5 style="margin: 0; color: #495057;">Expenses</h5>
                    </div>

                    <div style="border: 1px solid #dee2e6; border-radius: 8px; padding: 15px; text-align: center; cursor: pointer;" onclick="frappe.set_route('List', 'Yawlitinvoice')">
                        <div style="width: 40px; height: 40px; background: #6f42c1; color: white; border-radius: 8px; display: flex; align-items: center; justify-content: center; margin: 0 auto 10px;">📄</div>
                        <h5 style="margin: 0; color: #495057;">Invoices</h5>
                    </div>

                    <div style="border: 1px solid #dee2e6; border-radius: 8px; padding: 15px; text-align: center; cursor: pointer;" onclick="frappe.set_route('List', 'Customerpayment')">
                        <div style="width: 40px; height: 40px; background: #28a745; color: white; border-radius: 8px; display: flex; align-items: center; justify-content: center; margin: 0 auto 10px;">💸</div>
                        <h5 style="margin: 0; color: #495057;">Payments</h5>
                    </div>

                </div>
            </div>

        </div>
    `);
    
    console.log('Migration Control Center loaded successfully!');
};
