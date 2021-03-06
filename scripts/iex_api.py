import requests
import configparser
import json

class IEXmanager:
    '''
    Helper Class to request data from IEX Cloud API
    '''
    def __init__(self, config_file):
        config = configparser.ConfigParser()
        config.read(config_file)
        
        self.token   = config.get("API", 'TOKEN')
        self.domain  = config.get('API', 'DOMAIN')
        self.version = config.get('API', 'VERSION')
        self.api_types = ['company', 'stats']
    
    def api_type_exists(self, api_type):
        return api_type in self.api_types
    
    def get_symbol_data(self, symbol, api_type):
        SUCCESS_CODE = 200
        if self.api_type_exists(api_type):
            api_https = f"https://{self.domain}/{self.version}/stock/{symbol}/{api_type}?token={self.token}"
            response = requests.get(api_https)
            
            if response.status_code == SUCCESS_CODE:
                json_data = json.loads(response.text)
                if api_type == 'stats':
                    json_data['symbol'] = symbol
                elif api_type == 'company':
                    del json_data['tags'] # Removed this value because it is not currently desired
            else:
                raise requests.RequestException(f"Request failed with the following status code {response.status_code}")
            
            return json_data
        
        else:
            raise TypeError(f"{api_type} is not currently supported.\nPlease choose one of the following API types:\n{', '.join(self.api_types)}.")