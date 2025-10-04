// Copyright (c) 2025, Ayush Raj and contributors
// For license information, please see license.txt

// frappe.ui.form.on("Product", {
// 	refresh(frm) {

// 	},
// });


frappe.ui.form.on('Product', {
    refresh: function(frm) {
        // Add custom buttons
        if (!frm.is_new()) {
            frm.add_custom_button(__('Calculate Total Price'), function() {
                frappe.call({
                    method: 'get_total_service_price',
                    doc: frm.doc,
                    callback: function(r) {
                        if (r.message) {
                            frappe.msgprint(__('Total Price (including addons): ₹{0}', [r.message]));
                        }
                    }
                });
            });
            
            frm.add_custom_button(__('Add Recommended Addons'), function() {
                add_category_specific_addons(frm);
            });
        }
    },
    
    service_category: function(frm) {
        // Filter addons based on service category
        update_addon_filters(frm);
    }
});

function add_category_specific_addons(frm) {
    // Auto-add recommended addons based on service category
    let recommended_addons = {
        'Wash': ['Interior Vacuuming', 'Dashboard Polish', 'Tyre Polish'],
        'Polishing': ['Engine Bay Cleaning', 'Dashboard Polish', 'Tyre Polish'],
        'Detailing': ['Seat Detailing', 'Door Panel Cleaning', 'Ceiling Cleaning']
    };
    
    let category = frm.doc.service_category;
    let addons = recommended_addons[category] || [];
    
    addons.forEach(addon_name => {
        let child = frm.add_child('addons');
        frappe.model.set_value(child.doctype, child.name, 'addon', addon_name);
        frappe.model.set_value(child.doctype, child.name, 'quantity', 1);
        frappe.model.set_value(child.doctype, child.name, 'is_required', 0);
    });
    
    frm.refresh_field('addons');
}
