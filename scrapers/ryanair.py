import requests
import json
import datetime
from scrapers.BaseScraper import BaseScraper
from Request import Request
from Flight import Flight
from Exceptions import CityNotFoundException, CountryNotFoundException, TimeNotAvailableException, DateNotAvailableException
import pandas as pd
import asyncio
import concurrent.futures

class RyanAir(BaseScraper):

    base_url = "https://www.ryanair.com"
    api_url = "https://www.ryanair.com/api"

    ryanair_headers = {
        'Alt-Used': 'www.ryanair.com',
        'newrelic': 'eyJ2IjpbMCwxXSwiZCI6eyJ0eSI6IkJyb3dzZXIiLCJhYyI6IjY0NjgzMiIsImFwIjoiMzg5MjUxMzk3IiwiaWQiOiI3ZGI3Njc4N2U3NDA2NGVlIiwidHIiOiJhYzA3YjRjMTBlMjM5MWFjYTYxYTQyZGI3MWUyYmIwMCIsInRpIjoxNjk4Nzg5Nzk0MTkyfX0=',
        'X-NewRelic-ID': 'undefined'
    }

    def __init__(self) -> None:
        '''
        '''
        
        super().__init__(self.base_url, self.ryanair_headers, self.api_url)

    def _get_city_codes(self):
        '''
        Gets all the important data for all available ryanair airports:

        iataCode, name, seoName, aliases[], coordinates[latitude], coordinates[longitude], base, countryCode,
        regionCode, cityCode, currencyCode, routes[], seasonalRoutes[], categories[], priority, timeZone
        '''
        url = super().get_api_url('api', 'views', 'locate', '3', 'airports', 'en', 'active')
        re = requests.get(url, headers=super().headers)
        re.json()