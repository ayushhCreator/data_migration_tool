# Zoho connector stub
import frappe
import requests
from typing import Dict, List, Any, Optional
import json
from datetime import datetime, timedelta

class ZohoConnector:
    def __init__(self, logger):
        self.logger = logger
        self.base_url = "https://www.zohoapis.com/crm/v2"
        self.access_token = None
        self.refresh_token = None
        self.client_id = None
        self.client_secret = None
        self.load_credentials()
    
    def load_credentials(self):
        """Load Zoho credentials from site config or DocType"""
        try:
            # Try to get from site config first
            site_config = frappe.get_site_config()
            zoho_config = site_config.get('zoho_integration', {})
            
            if zoho_config:
                self.client_id = zoho_config.get('client_id')
                self.client_secret = zoho_config.get('client_secret')
                self.refresh_token = zoho_config.get('refresh_token')
            else:
                # Fallback to custom settings DocType
                settings = frappe.get_single('Migration Settings')
                self.client_id = settings.zoho_client_id
                self.client_secret = settings.zoho_client_secret
                self.refresh_token = settings.zoho_refresh_token
                
        except Exception as e:
            self.logger.log_error(e, {"context": "loading_zoho_credentials"})
            raise
    
    def authenticate(self) -> bool:
        """Authenticate with Zoho API and get access token"""
        try:
            auth_url = "https://accounts.zoho.com/oauth/v2/token"
            
            data = {
                'refresh_token': self.refresh_token,
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'grant_type': 'refresh_token'
            }
            
            response = requests.post(auth_url, data=data)
            response.raise_for_status()
            
            auth_data = response.json()
            self.access_token = auth_data.get('access_token')
            
            if not self.access_token:
                raise Exception("Failed to get access token from Zoho")
            
            self.logger.logger.info("✅ Zoho authentication successful")
            return True
            
        except Exception as e:
            self.logger.log_error(e, {"context": "zoho_authentication"})
            return False
    
    def get_headers(self) -> Dict[str, str]:
        """Get request headers with authorization"""
        return {
            'Authorization': f'Zoho-oauthtoken {self.access_token}',
            'Content-Type': 'application/json'
        }
    
    def fetch_records(self, module: str, modified_since: Optional[datetime] = None, 
                     page_size: int = 200) -> List[Dict[str, Any]]:
        """Fetch records from Zoho CRM module"""
        
        if not self.access_token and not self.authenticate():
            raise Exception("Failed to authenticate with Zoho")
        
        try:
            url = f"{self.base_url}/{module}"
            params = {'per_page': page_size}
            
            if modified_since:
                params['If-Modified-Since'] = modified_since.isoformat()
            
            all_records = []
            page = 1
            
            while True:
                params['page'] = page
                
                response = requests.get(url, headers=self.get_headers(), params=params)
                
                if response.status_code == 401:  # Token expired
                    if self.authenticate():
                        continue
                    else:
                        raise Exception("Failed to re-authenticate with Zoho")
                
                response.raise_for_status()
                data = response.json()
                
                if 'data' not in data:
                    break
                
                records = data['data']
                all_records.extend(records)
                
                self.logger.logger.info(f"📥 Fetched page {page}: {len(records)} records from {module}")
                
                # Check if there are more pages
                info = data.get('info', {})
                if not info.get('more_records', False):
                    break
                
                page += 1
            
            self.logger.logger.info(f"🎯 Total records fetched from {module}: {len(all_records)}")
            return all_records
            
        except Exception as e:
            self.logger.log_error(e, {
                "context": "fetching_zoho_records",
                "module": module,
                "page": page
            })
            raise
    
    def get_available_modules(self) -> List[str]:
        """Get list of available modules from Zoho CRM"""
        try:
            url = f"{self.base_url}/settings/modules"
            response = requests.get(url, headers=self.get_headers())
            response.raise_for_status()
            
            data = response.json()
            modules = [module['module_name'] for module in data.get('modules', [])]
            
            self.logger.logger.info(f"📋 Available Zoho modules: {modules}")
            return modules
            
        except Exception as e:
            self.logger.log_error(e, {"context": "getting_zoho_modules"})
            return []
    
    def test_connection(self) -> Dict[str, Any]:
        """Test connection to Zoho API"""
        try:
            if not self.authenticate():
                return {"status": "failed", "message": "Authentication failed"}
            
            # Test with a simple API call
            url = f"{self.base_url}/users"
            response = requests.get(url, headers=self.get_headers())
            
            if response.status_code == 200:
                return {
                    "status": "success", 
                    "message": "Connection successful",
                    "user_info": response.json()
                }
            else:
                return {
                    "status": "failed", 
                    "message": f"API call failed with status {response.status_code}"
                }
                
        except Exception as e:
            self.logger.log_error(e, {"context": "testing_zoho_connection"})
            return {"status": "error", "message": str(e)}
