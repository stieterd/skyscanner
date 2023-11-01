import requests
import json
import datetime
import time


class BaseScraper:

    headers = {
                'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0",
                # 'Connection': "keep-alive"
    }

    longitude_km_ratio = 111
    
    base_url: str
    api_url: str

    def __init__(self, base_url, headers, api_url=None) -> None:
        
        self.headers = self.headers.update(headers)

        self.base_url = base_url
        self.api_url = api_url

    def get_api_url(self, *requests:list[str], **q_strings:dict[str, str]) -> str:
        '''
        
        '''
        query_params = '?'
        for key, value in q_strings.items():
            query_params += f'{key}={value}&'
        return f"{self.api_url}/{'/'.join(requests)}" + query_params[:-1]
    
    def km_to_long(self, km:float) -> float:
        return km/self.longitude_km_ratio
    
    def km_to_lat(self, km:float) -> float:
        return km/self.longitude_km_ratio
    
    def compare_strings(self, string1:str, string2:str) -> bool:
        return string1.lower().strip() == string2.lower().strip()