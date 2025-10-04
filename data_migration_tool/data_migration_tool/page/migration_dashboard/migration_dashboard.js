frappe.pages['migration_dashboard'].on_page_load = function(wrapper) {
    try {
        var page = frappe.ui.make_app_page({
            parent: wrapper,
            title: 'Data Migration Dashboard',
            single_column: true
        });
        
        // Simple, working dashboard
        page.main.html(`
            <div class="dashboard-wrapper">
                <!-- Header -->
                <div class="dashboard-header">
                    <div class="header-content">
                        <div class="header-left">
                            <h1 class="dashboard-title">
                                <span style="font-size: 2rem;">🚀</span>
                                Data Migration Control Center
                            </h1>
                            <p class="dashboard-subtitle">Comprehensive migration management for all your data sources</p>
                        </div>
                        <div class="header-right">
                            <div class="status-badge">
                                <span style="display: inline-block; width: 8px; height: 8px; background: #4caf50; border-radius: 50%; margin-right: 8px;"></span>
                                System Active
                            </div>
                            <button class="refresh-btn" onclick="window.location.reload()">
                                <span>🔄</span> Refresh
                            </button>
                        </div>
                    </div>
                </div>

                <!-- Quick Actions -->
                <div class="quick-actions">
                    <h3>⚡ Quick Actions</h3>
                    <div class="action-buttons">
                        <button class="action-btn primary" onclick="frappe.set_route('List', 'Migration Settings')">
                            <span>📊</span> CSV Import
                        </button>
                        <button class="action-btn success" onclick="frappe.new_doc('Product')">
                            <span>📦</span> Add Product
                        </button>
                        <button class="action-btn warning" onclick="frappe.set_route('List', 'DocType Creation Request', {status: 'Pending'})">
                            <span>⏳</span> Pending Requests
                        </button>
                    </div>
                </div>

                <!-- Stats Cards 
                <div class="stats-section">
                    <div class="stats-grid">
                        <div class="stat-card">
                            <div class="stat-icon" style="color: #017e84;">⚙️</div>
                            <div class="stat-content">
                                <div class="stat-number">--</div>
                                <div class="stat-label">Migration Settings</div>
                            </div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-icon" style="color: #4caf50;">📦</div>
                            <div class="stat-content">
                                <div class="stat-number">--</div>
                                <div class="stat-label">Products</div>
                            </div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-icon" style="color: #ff9800;">📋</div>
                            <div class="stat-content">
                                <div class="stat-number">--</div>
                                <div class="stat-label">DocType Requests</div>
                            </div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-icon" style="color: #2196f3;">💾</div>
                            <div class="stat-content">
                                <div class="stat-number">--</div>
                                <div class="stat-label">Buffer Records</div>
                            </div>
                        </div>
                    </div>
                </div>
-->
            

                <!-- Master Data Section -->
                <div class="section-card">
                    <div class="section-header">
                        <h2><span style="margin-right: 12px;">🏬</span>Master Data</h2>
                        <p>Core business entities and catalog management</p>
                    </div>
                    <div class="feature-grid">
                        <div class="feature-card" onclick="frappe.set_route('List', 'Product')">
                            <div class="feature-header">
                                <div class="feature-icon" style="background: #4caf50;">📦</div>
                                <h3>Products</h3>
                            </div>
                            <p>Manage your complete product catalog and inventory</p>
                            <div class="feature-actions">
                                <button class="btn-primary" onclick="event.stopPropagation(); frappe.new_doc('Product')">New Product</button>
                                <button class="btn-secondary" onclick="event.stopPropagation(); frappe.set_route('List', 'Product')">View All</button>
                            </div>
                        </div>

                        <div class="feature-card" onclick="frappe.set_route('List', 'Addon')">
                            <div class="feature-header">
                                <div class="feature-icon" style="background: #017e84;">⚡</div>
                                <h3>Addons</h3>
                            </div>
                            <p>Standalone addon components and features</p>
                            <div class="feature-actions">
                                <button class="btn-primary" onclick="event.stopPropagation(); frappe.new_doc('Addon')">New Addon</button>
                                <button class="btn-secondary" onclick="event.stopPropagation(); frappe.set_route('List', 'Addon')">View All</button>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Classifications Section -->
                <div class="section-card">
                    <div class="section-header">
                        <h2><span style="margin-right: 12px;">🏷️</span>Classifications</h2>
                        <p>Organize data with categories and types</p>
                    </div>
                    <div class="feature-grid three-col">
                        <div class="feature-card" onclick="frappe.set_route('List', 'Service Category')">
                            <div class="feature-header">
                                <div class="feature-icon" style="background: #673ab7;">🏷️</div>
                                <h3>Service Categories</h3>
                            </div>
                            <p>Categorize services for better organization</p>
                            <div class="feature-actions">
                                <button class="btn-primary" onclick="event.stopPropagation(); frappe.new_doc('Service Category')">New Category</button>
                                <button class="btn-secondary" onclick="event.stopPropagation(); frappe.set_route('List', 'Service Category')">View All</button>
                            </div>
                        </div>

                        <div class="feature-card" onclick="frappe.set_route('List', 'Service Type')">
                            <div class="feature-header">
                                <div class="feature-icon" style="background: #9c27b0;">🔧</div>
                                <h3>Service Types</h3>
                            </div>
                            <p>Define different types of services offered</p>
                            <div class="feature-actions">
                                <button class="btn-primary" onclick="event.stopPropagation(); frappe.new_doc('Service Type')">New Type</button>
                                <button class="btn-secondary" onclick="event.stopPropagation(); frappe.set_route('List', 'Service Type')">View All</button>
                            </div>
                        </div>

                        <div class="feature-card" onclick="frappe.set_route('List', 'Vehicle Type')">
                            <div class="feature-header">
                                <div class="feature-icon" style="background: #ff9800;">🚗</div>
                                <h3>Vehicle Types</h3>
                            </div>
                            <p>Classify vehicles by type and characteristics</p>
                            <div class="feature-actions">
                                <button class="btn-primary" onclick="event.stopPropagation(); frappe.new_doc('Vehicle Type')">New Type</button>
                                <button class="btn-secondary" onclick="event.stopPropagation(); frappe.set_route('List', 'Vehicle Type')">View All</button>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Business Data Section -->
                <div class="section-card">
                    <div class="section-header">
                        <h2><span style="margin-right: 12px;">💼</span>Business Data</h2>
                        <p>Customer, vendor, and financial data management</p>
                    </div>
                    <div class="business-grid">
                        <div class="compact-card" onclick="frappe.set_route('List', 'Yawlitquote')">
                            <div class="compact-icon" style="background: #2196f3;">💰</div>
                            <h4>Quotes</h4>
                        </div>
                        <div class="compact-card" onclick="frappe.set_route('List', 'Yawlitvendor')">
                            <div class="compact-icon" style="background: #4caf50;">🏢</div>
                            <h4>Vendors</h4>
                        </div>
                        <div class="compact-card" onclick="frappe.set_route('List', 'Yawlitcontacts')">
                            <div class="compact-icon" style="background: #017e84;">👥</div>
                            <h4>Contacts</h4>
                        </div>
                        <div class="compact-card" onclick="frappe.set_route('List', 'Yawlitexpense')">
                            <div class="compact-icon" style="background: #ff9800;">💳</div>
                            <h4>Expenses</h4>
                        </div>
                        <div class="compact-card" onclick="frappe.set_route('List', 'Yawlitaddress')">
                            <div class="compact-icon" style="background: #9c27b0;">📍</div>
                            <h4>Addresses</h4>
                        </div>
                        <div class="compact-card" onclick="frappe.set_route('List', 'Yawlitinvoice')">
                            <div class="compact-icon" style="background: #673ab7;">📄</div>
                            <h4>Invoices</h4>
                        </div>
                        <div class="compact-card" onclick="frappe.set_route('List', 'Customerpayment')">
                            <div class="compact-icon" style="background: #4caf50;">💸</div>
                            <h4>Payments</h4>
                        </div>
                        <div class="compact-card" onclick="frappe.set_route('List', 'Chartofaccounts')">
                            <div class="compact-icon" style="background: #2196f3;">📊</div>
                            <h4>Chart of Accounts</h4>
                        </div>
                    </div>
                </div>
            </div>


			    <!-- Migration Management Section -->
                <div class="section-card">
                    <div class="section-header">
                        <h2><span style="margin-right: 12px;">🔄</span>Migration Management</h2>
                        <p>Core migration tools and settings</p>
                    </div>
                    <div class="feature-grid">
                        <div class="feature-card" onclick="frappe.set_route('List', 'Migration Settings')">
                            <div class="feature-header">
                                <div class="feature-icon" style="background: #017e84;">⚙️</div>
                                <h3>Migration Settings</h3>
                            </div>
                            <p>Configure data sources, connection parameters, and migration rules</p>
                            <div class="feature-actions">
                                <button class="btn-primary" onclick="event.stopPropagation(); frappe.new_doc('Migration Settings')">New Setting</button>
                                <button class="btn-secondary" onclick="event.stopPropagation(); frappe.set_route('List', 'Migration Settings')">View All</button>
                            </div>
                        </div>

                        <div class="feature-card" onclick="frappe.set_route('List', 'Migration Data Buffer')">
                            <div class="feature-header">
                                <div class="feature-icon" style="background: #2196f3;">💾</div>
                                <h3>Migration Buffer</h3>
                            </div>
                            <p>Temporary storage for data during migration processes</p>
                            <div class="feature-actions">
                                <button class="btn-primary" onclick="event.stopPropagation(); frappe.set_route('List', 'Migration Data Buffer')">View Buffer</button>
                                <button class="btn-secondary" onclick="event.stopPropagation(); frappe.set_route('Report', 'Migration Data Buffer')">Reports</button>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- DocType Management Section -->
                <div class="section-card">
                    <div class="section-header">
                        <h2><span style="margin-right: 12px;">📄</span>DocType Management</h2>
                        <p>Dynamic DocType creation and schema management</p>
                    </div>
                    <div class="feature-grid">
                        <div class="feature-card" onclick="frappe.set_route('List', 'DocType Creation Request')">
                            <div class="feature-header">
                                <div class="feature-icon" style="background: #ff9800;">📋</div>
                                <h3>DocType Requests</h3>
                            </div>
                            <p>Handle dynamic DocType creation requests and approvals</p>
                            <div class="feature-actions">
                                <button class="btn-primary" onclick="event.stopPropagation(); frappe.new_doc('DocType Creation Request')">New Request</button>
                                <button class="btn-secondary" onclick="event.stopPropagation(); frappe.set_route('List', 'DocType Creation Request')">View All</button>
                            </div>
                        </div>

                        <div class="feature-card" onclick="frappe.set_route('List', 'CSV Schema Registry')">
                            <div class="feature-header">
                                <div class="feature-icon" style="background: #9c27b0;">📊</div>
                                <h3>CSV Schema Registry</h3>
                            </div>
                            <p>Define CSV import/export mappings and data transformations</p>
                            <div class="feature-actions">
                                <button class="btn-primary" onclick="event.stopPropagation(); frappe.new_doc('CSV Schema Registry')">New Schema</button>
                                <button class="btn-secondary" onclick="event.stopPropagation(); frappe.set_route('List', 'CSV Schema Registry')">View All</button>
                            </div>
                        </div>
                    </div>
                </div>
            <style>
                .dashboard-wrapper {
                    width: 100%;
                    padding: 15px;
                    background-color: #f8f9fa;
                    min-height: 100vh;
                    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Roboto", sans-serif;
                }

                .dashboard-header {
                    background: #ffffff;
                    border: 1px solid #d1d8dd;
                    border-radius: 8px;
                    padding: 20px;
                    margin-bottom: 20px;
                    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
                }

                .header-content {
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    flex-wrap: wrap;
                    gap: 15px;
                }

                .dashboard-title {
                    display: flex;
                    align-items: center;
                    gap: 12px;
                    margin: 0 0 8px 0;
                    font-size: 1.75rem;
                    font-weight: 600;
                    color: #36414c;
                }

                .dashboard-subtitle {
                    margin: 0;
                    font-size: 0.95rem;
                    color: #8d99a6;
                }

                .status-badge {
                    display: inline-flex;
                    align-items: center;
                    background: #d1f7c4;
                    color: #2e7d32;
                    padding: 8px 16px;
                    border-radius: 20px;
                    font-size: 0.875rem;
                    font-weight: 500;
                    margin-bottom: 8px;
                }

                .refresh-btn {
                    display: inline-flex;
                    align-items: center;
                    gap: 8px;
                    background: #ffffff;
                    color: #36414c;
                    border: 1px solid #d1d8dd;
                    padding: 8px 16px;
                    border-radius: 6px;
                    cursor: pointer;
                    font-size: 0.875rem;
                    transition: all 0.2s ease;
                }

                .refresh-btn:hover {
                    background: #f8f9fa;
                    border-color: #8d99a6;
                }

                .quick-actions {
                    background: #ffffff;
                    border: 1px solid #d1d8dd;
                    border-radius: 8px;
                    padding: 20px;
                    margin-bottom: 20px;
                    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
                }

                .quick-actions h3 {
                    margin: 0 0 16px 0;
                    font-size: 1.1rem;
                    font-weight: 600;
                    color: #36414c;
                }

                .action-buttons {
                    display: flex;
                    flex-wrap: wrap;
                    gap: 12px;
                }

                .action-btn {
                    display: inline-flex;
                    align-items: center;
                    gap: 8px;
                    padding: 10px 20px;
                    border: none;
                    border-radius: 6px;
                    cursor: pointer;
                    font-size: 0.875rem;
                    font-weight: 500;
                    transition: all 0.2s ease;
                }

                .action-btn.primary {
                    background: #017e84;
                    color: #ffffff;
                }

                .action-btn.primary:hover {
                    background: #015a5f;
                    transform: translateY(-1px);
                }

                .action-btn.success {
                    background: #4caf50;
                    color: #ffffff;
                }

                .action-btn.success:hover {
                    background: #45a049;
                    transform: translateY(-1px);
                }

                .action-btn.warning {
                    background: #ff9800;
                    color: #ffffff;
                }

                .action-btn.warning:hover {
                    background: #f57c00;
                    transform: translateY(-1px);
                }

                .stats-section {
                    margin-bottom: 24px;
                }

                .stats-grid {
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
                    gap: 16px;
                }

                .stat-card {
                    background: #ffffff;
                    border: 1px solid #d1d8dd;
                    border-radius: 8px;
                    padding: 20px;
                    display: flex;
                    align-items: center;
                    gap: 16px;
                    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
                    transition: all 0.2s ease;
                }

                .stat-card:hover {
                    transform: translateY(-2px);
                    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
                }

                .stat-icon {
                    font-size: 2.5rem;
                }

                .stat-number {
                    font-size: 2rem;
                    font-weight: 700;
                    color: #36414c;
                    line-height: 1;
                    margin-bottom: 4px;
                }

                .stat-label {
                    font-size: 0.875rem;
                    color: #8d99a6;
                    font-weight: 500;
                }

                .section-card {
                    background: #ffffff;
                    border: 1px solid #d1d8dd;
                    border-radius: 8px;
                    padding: 24px;
                    margin-bottom: 24px;
                    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
                }

                .section-header {
                    text-align: center;
                    margin-bottom: 24px;
                }

                .section-header h2 {
                    margin: 0 0 8px 0;
                    font-size: 1.5rem;
                    font-weight: 600;
                    color: #36414c;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                }

                .section-header p {
                    margin: 0;
                    color: #8d99a6;
                    font-size: 0.95rem;
                }

                .feature-grid {
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
                    gap: 20px;
                }

                .feature-grid.three-col {
                    grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
                }

                .feature-card {
                    background: #ffffff;
                    border: 1px solid #d1d8dd;
                    border-radius: 8px;
                    padding: 20px;
                    cursor: pointer;
                    transition: all 0.2s ease;
                }

                .feature-card:hover {
                    transform: translateY(-3px);
                    box-shadow: 0 8px 25px rgba(0, 0, 0, 0.15);
                    border-color: #b3e5fc;
                }

                .feature-header {
                    display: flex;
                    align-items: center;
                    gap: 14px;
                    margin-bottom: 12px;
                }

                .feature-icon {
                    width: 48px;
                    height: 48px;
                    border-radius: 8px;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    font-size: 1.5rem;
                    color: #ffffff;
                    font-weight: 600;
                    flex-shrink: 0;
                }

                .feature-header h3 {
                    margin: 0;
                    font-size: 1.2rem;
                    font-weight: 600;
                    color: #36414c;
                }

                .feature-card p {
                    color: #8d99a6;
                    font-size: 0.9rem;
                    line-height: 1.5;
                    margin: 0 0 16px 0;
                }

                .feature-actions {
                    display: flex;
                    gap: 10px;
                }

                .btn-primary, .btn-secondary {
                    padding: 8px 16px;
                    border-radius: 6px;
                    border: none;
                    font-weight: 500;
                    font-size: 0.875rem;
                    cursor: pointer;
                    transition: all 0.2s ease;
                    flex: 1;
                }

                .btn-primary {
                    background: #017e84;
                    color: #ffffff;
                }

                .btn-primary:hover {
                    background: #015a5f;
                    transform: translateY(-1px);
                }

                .btn-secondary {
                    background: #ffffff;
                    color: #36414c;
                    border: 1px solid #d1d8dd;
                }

                .btn-secondary:hover {
                    background: #f8f9fa;
                    border-color: #8d99a6;
                }

                .business-grid {
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
                    gap: 16px;
                }

                .compact-card {
                    background: #ffffff;
                    border: 1px solid #d1d8dd;
                    border-radius: 8px;
                    padding: 16px;
                    text-align: center;
                    cursor: pointer;
                    transition: all 0.2s ease;
                }

                .compact-card:hover {
                    transform: translateY(-2px);
                    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
                    border-color: #b3e5fc;
                }

                .compact-icon {
                    width: 40px;
                    height: 40px;
                    border-radius: 8px;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    font-size: 1.25rem;
                    color: #ffffff;
                    margin: 0 auto 12px;
                }

                .compact-card h4 {
                    margin: 0;
                    font-size: 0.875rem;
                    font-weight: 500;
                    color: #36414c;
                }

                /* Responsive Design */
                @media (max-width: 768px) {
                    .dashboard-wrapper {
                        padding: 10px;
                    }

                    .header-content {
                        flex-direction: column;
                        text-align: center;
                    }

                    .dashboard-title {
                        font-size: 1.5rem;
                    }

                    .stats-grid {
                        grid-template-columns: 1fr;
                    }

                    .feature-grid,
                    .feature-grid.three-col {
                        grid-template-columns: 1fr;
                    }

                    .action-buttons {
                        flex-direction: column;
                    }

                    .action-btn {
                        width: 100%;
                        justify-content: center;
                    }

                    .business-grid {
                        grid-template-columns: repeat(2, 1fr);
                    }
                }

                @media (max-width: 480px) {
                    .business-grid {
                        grid-template-columns: 1fr;
                    }
                }
            </style>
        `);
        
        console.log('Migration Dashboard loaded successfully');
        
    } catch (error) {
        console.error('Error loading dashboard:', error);
        page.main.html('<div style="padding: 20px; text-align: center; color: red;">Error loading dashboard. Please check console for details.</div>');
    }
};
