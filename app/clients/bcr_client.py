from typing import Dict, Any
from datetime import datetime
from app.clients.base_client import BaseAPIClient
from app.utils.dates import get_date_range


class BCRClient(BaseAPIClient):
    def fetch_data(
        self, 
        series_code: str, 
        start_date: str = None, 
        end_date: str = None,
        format: str = "json"
    ) -> Dict[str, Any]:
        """
        Fetch data from BCR API
        
        Args:
            series_code: BCR series code (e.g., PD04637PD)
            start_date: Start date in YYYY-M-D format
            end_date: End date in YYYY-M-D format
            format: Output format (json, xml, csv)
        """
        if not start_date or not end_date:
            start_date, end_date = get_date_range(days_back=30)
        
        endpoint = f"{series_code}/{format}/{start_date}/{end_date}"
        
        return self._make_request(endpoint)
    
    def fetch_multiple_series(
        self,
        series_codes: list,
        start_date: str = None,
        end_date: str = None,
        format: str = "json"
    ) -> Dict[str, Any]:
        """Fetch multiple series in one request"""
        codes = "-".join(series_codes)
        
        if not start_date or not end_date:
            start_date, end_date = get_date_range(days_back=30)
        
        endpoint = f"{codes}/{format}/{start_date}/{end_date}"
        
        return self._make_request(endpoint)
