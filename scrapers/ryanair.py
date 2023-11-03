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
        'X-NewRelic-ID': 'undefined',
        'Cookie': 'rid=ae238289-8716-4078-96f6-a3ab6b2fc20d; rid.sig=jyna6R42wntYgoTpqvxHMK7H+KyM6xLed+9I3KsvYZaVt7P36AL6zp9dGFPu5uVxaIiFpNXrszr+LfNCdY3IT3oCSYLeNv/ujtjsDqOzkY66AL3V6kH2vsK+au12X21HkZ4S8GaG8CoBmm/m0rLsOKYkxtw+U3+ejBaPc15jJjKXnc3owMBg82SNbqyKjVd6Z6qcsoE25p3RmlcaHuHC3GBf1yIGtlqeQun3Mj0vmSWJV8jwIw2wASJ789/CrRqZD7ygs/wfcb8hBBqVoMqjESkGsL/S7dk/EwM5eijnZV/nQWCwG90fqwg160peojGgPoiDiywWDoENQk0YV09rYSADa7R/3NbavlLjUKgw7Vqr2EkRoOQgMUdmDHgAfxJKsZrWodQFk9xEBzjuHPfocfN5yR5jxsTO/bmZ88KXBnt/S3VNMJYg+Yq0skSEsUdqtvj7UADUgM7dyNK9VH0+pLIknNJG+FGLdx1qeLyeDkQyPQmUxRR+3JCLvMPmo0oKaGvvnT9iQ6WZj/Dg9+196dlEt0ODD2dOmctVx3imVQFAXMzbRiIBmV9aRQSmk1zZ1GPPEYm9b9wRW+/fG6/QowErOT5b8/uISrGzpkFvFyMxiOw4SBIhxbYpEvgdURJBjsujq7N85xdC6U0MhAv7WfJVZwMnbKLgPLo1oDu3+7N3tagyw0B6LLNPVVMgYGZWWM1R7JFZRBE1AYT9w9LFkuuwlS/4F67Xnqe/MLjnUoRQDbo+uo2sDGnNTJ2KVG8UGLPZbbhbajWmQiI0hsgfxFjTZfHis1vyv1fv/yhICmF9ojk+S+1GY5/G1XpsO/qW/gl7UcXIXlPG3mmrc4vrfA==; mkt=/gb/en/;'
    }

    def __init__(self) -> None:
        '''
        Initializes the RyanAir object and its super BaseScraper
        '''
        
        super().__init__(self.base_url, self.ryanair_headers, self.api_url)

        city_codes = self._get_city_codes()
        country_codes = self._get_country_codes()

        countries_df = pd.json_normalize(country_codes, max_level=1)
        
        self.connections = pd.json_normalize(city_codes, max_level=1)
        # self.connections['routes'] = self.connections['routes'].str.split(',', expand=False)
        self.connections = self.connections.explode('routes')
        self.connections = self.connections[self.connections["routes"].str.contains("airport:") == True] 
        self.connections['routes'] = self.connections['routes'].str.replace('airport:', '')
        
        self.countries = countries_df.groupby(['country.code', 'country.iso3code', 'country.name', 'country.currency', 'country.defaultAirportCode', 'country.schengen']).size().reset_index(name='count').drop(columns='count')
        self.countries.columns = ['code', 'iso3code', 'name', 'currency', 'defaultAirportCode', 'schengen']

        self.cities = countries_df.groupby(['city.name', 'city.code', 'country.code', 'country.iso3code', 'country.name', 'country.currency','coordinates.latitude', 'coordinates.longitude', 'country.defaultAirportCode', 'country.schengen']).size().reset_index(name='count').drop(columns='count')
        self.cities.columns = ['name', 'code', 'country.code', 'country.iso3code', 'country.name', 'currency', 'latitude', 'longitude', 'country.defaultAirportCode', 'schengen']

    def _get_city_codes(self):
        '''
        Gets all the important data for all available ryanair airports:

        iataCode, name, seoName, aliases[], coordinates[latitude], coordinates[longitude], base, countryCode,
        regionCode, cityCode, currencyCode, routes[], seasonalRoutes[], categories[], priority, timeZone
        '''
        url = super().get_api_url('views', 'locate', '3', 'airports', 'en', 'active')
        re = requests.get(url, headers=super().headers)
        return re.json()

    def _get_country_codes(self):
        '''
        Gets all the important data for all available ryanair airports:

        code, name, seoName, aliases[], base, city[name], city[code], region[name], region[code], 
        country[code], country[iso3code], country[name], country[currency], country[defaultAirportCode], schengen,
        coordinates[latitude], coordinates[longitude], timeZone
        '''

        url = super().get_api_url('views', 'locate', '5', 'airports', 'en', 'active')
        re = requests.get(url, headers=super().headers)
        return re.json()
    
    def _get_country_code_from_name(self, country_name: str) -> str:
        '''
        Gets used country code for specific country
        '''
        result = self.countries[self.countries['name'] == country_name]['code'].values
        if len(result) == 0:
            raise CountryNotFoundException()
        return result[0]
    
    def _get_country_code_from_airport_code(self, airport_code:str) -> str:
        '''
        Gets used country code from its city airport code
        '''
        result = self.cities[self.cities['code'] == airport_code]['country.code'].values
        if len(result) == 0:
            raise CityNotFoundException()
        
        return result[0]
    
    def _get_airports_by_country(self, request:Request) -> Request:
        '''
        Grabs all the cities in the country of original departure city
        '''
        country_code = self._get_country_code_from_name(request.departure_country)
        departure_cities = self.connections[self.connections['countryCode'] == country_code]
        departure_cities_json = json.loads(departure_cities.to_json(orient='records'))
        if len(departure_cities_json) == 0:
            raise CityNotFoundException()
        
        request.departure_locations.extend(departure_cities_json)
        
        return request
    
    def _get_airports_by_radius(self, request: Request) -> Request:
        '''
        Grabs all the cities within a certain radius of original departure city
        '''
        
        departure_city = self.connections[self.connections['name'] == request.departure_city]

        if len(departure_city) == 0:
            raise CityNotFoundException()
        
        if request.airport_radius > 0:
            lat_range = (departure_city['coordinates.latitude'].values[0] - super().km_to_lat(request.airport_radius), departure_city['latitude'].values[0] + super().km_to_lat(request.airport_radius))
            long_range = (departure_city['longitude'].values[0] - super().km_to_long(request.airport_radius), departure_city['longitude'].values[0] + super().km_to_long(request.airport_radius))

            departure_locations = self.connections[lat_range[0] < self.connections['coordinates.latitude'] < lat_range[1] & long_range[0] < self.connections['coordinates.longitude'] < long_range[1]]
            departure_locations_json = json.loads(departure_locations.to_json(orient='records'))
            request.departure_locations.extend(departure_locations_json)

            return request
        
        else:
            request.departure_locations.extend(departure_city)
            return request

    def get_possible_flight(self, departure_location:dict, departure_date:str, arrival_date:str, request:Request) -> dict:
        
        url = super().get_api_url('booking', 
                                  'v4', 
                                  'en-gb', 
                                  'availability', 
                                  ADT=request.adult_count,
                                  TEEN=0,
                                  CHD=request.child_count,
                                  INF=request.infant_count,
                                  ORIGIN=departure_location['iataCode'],
                                  Destination=departure_location['routes'],
                                  promoCode='',
                                  IncludeConnectingFlights='false',
                                  Disc=0,
                                  DateOut=departure_date,
                                  FlexDaysBeforeOut=4,
                                  FlexDaysOut=2,
                                  DateIn=arrival_date,
                                  FlexDaysBeforeIn=4,
                                  FlexDaysIn=2,
                                  RoundTrip='true',
                                  ToUs='AGREED'
                                  )
        
        re = requests.get(url, headers=super().headers)
        try:
            result = re.json()
            outbound_flights = pd.json_normalize(result['trips'][0]['dates'], 'flights', max_level=4)
            return_flights = pd.json_normalize(result['trips'][1]['dates'], 'flights', max_level=4)
            return Flight(outbound_flights, return_flights)
        
        except Exception as e:
            print(re.text)
            return None    
        
    def get_possible_flights(self, request: Request) -> list:
        '''
        Gets the possible flighttimes and their prices according to request argument
        '''
        # TODO: called method be dependent on if radius or country of departure is chosen
        request = self._get_airports_by_country(request)

        results = []
        cur_date_departure = request.departure_date_first
        cur_date_arrival = request.arrival_date_first

        with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
            threads = []
            # The api of ryanair starts looking 4 days in the past and 2 days in the future from requested day
            cur_date_departure = cur_date_departure + datetime.timedelta(days=4)
            cur_date_arrival = cur_date_arrival + datetime.timedelta(days=4)
            
            while cur_date_departure + datetime.timedelta(days=2) < request.departure_date_last and cur_date_arrival + datetime.timedelta(days=2) < request.arrival_date_last:
                
                

                if cur_date_departure + datetime.timedelta(days=2) < request.departure_date_last:
                    cur_date_departure += datetime.timedelta(days=1)
                
                if cur_date_arrival + datetime.timedelta(days=2) < request.arrival_date_last:
                    cur_date_arrival += datetime.timedelta(days=1)

            
                for departure_location in request.departure_locations:
                    threads.append(executor.submit(self.get_possible_flight, departure_location, cur_date_departure, cur_date_arrival, request))

                for idx, future in enumerate(concurrent.futures.as_completed(threads)):
                    result = future.result()
                    results.append(result)

        return results