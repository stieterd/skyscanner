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

class WizzAir(BaseScraper):

    url = "https://be.wizzair.com"
    api_url = "https://be.wizzair.com/19.3.0/Api"
    
    wizz_headers = {
        'cookie': "RequestVerificationToken=10aa42acaa2d4bea88be9818666639b3",
        'Host': "be.wizzair.com",
        'Accept-Language': "en-US,en;q=0.5",
        'X-RequestVerificationToken': "10aa42acaa2d4bea88be9818666639b3",
        'TE': "trailers"
        }

    cities: list
    countries: list

    def __init__(self) -> None:
        '''
        Initializes the Wizzair object and its super BaseScraper

        Variable "connections" represents all the available destinations for wizzair airplanes
        Variable "cities" represents all cities that can be traveled to with wizzair
        Variable "countries" represents all countries that can be traveled to with wizzair
        '''
        super().__init__(self.url, self.wizz_headers, api_url=self.api_url)
        
        cities = self._get_city_codes()
        self.connections = pd.json_normalize(cities, record_path='connections', meta=['iata', 'longitude', 'currencyCode', 'latitude', 'shortName', 'countryName', 'countryCode', 'aliases', 'isExcludedFromGeoLocation', 'rank', 'categories', 'isFakeStation'], record_prefix='connection_')
        self.cities = pd.json_normalize(cities, record_path=None)

        # self.cities_df.columns = ['iata', 'longitude', 'currencyCode', 'latitude', 'shortName', 'countryName', 'countryCode', 'aliases', 'isExcludedFromGeoLocation', 'rank', 'categories', 'isFakeStation']
        self.countries = pd.json_normalize(self._get_country_codes(), meta=['code', 'name', 'isEu', 'isSchengen', 'phonePrefix'])
        
    
    def _get_country_codes(self):
        '''
        Gets the lettercodes for all available wizzair countries and also the phoneNumber prefix

        index, code, name, isEu, isSchengen, phonePrefix 
        '''
        url = super().get_api_url('asset', 'country', languageCode='en-gb')
        re = requests.get(url, headers=super().headers)
        
        return re.json()['countries']

    def _get_city_codes(self):
        '''
        Gets all the important data for all available wizzair cities:

        iata, longitude, currencyCode, latitude, shortName, countryName, countryCode, 
        connections, aliases, isExcludedFromGeoLocation, rank, categories, isFakeStation
        '''
        url = super().get_api_url('asset', 'map', languageCode='en-gb')
        re = requests.get(url, headers=super().headers)
        return re.json()['cities']
    
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
        result = self.cities[self.cities['iata'] == airport_code]['countryCode'].values
        if len(result) == 0:
            raise CityNotFoundException()
        
        return result[0]


    def _get_airports_by_country(self, request:Request) -> Request:
        '''
        Grabs all the cities in the country of original departure city
        '''
        country_code = self._get_country_code_from_name(request.departure_country)
        departure_cities = self.cities[self.cities['countryCode'] == country_code]
        departure_cities_json = json.loads(departure_cities.to_json(orient='records'))
        if len(departure_cities_json) == 0:
            raise CityNotFoundException()
        
        request.departure_locations.extend(departure_cities_json)
        
        return request

    def _get_airports_by_radius(self, request: Request) -> Request:
        '''
        Grabs all the cities within a certain radius of original departure city
        '''
        
        departure_city = self.cities[self.cities['shortName'] == request.departure_city]

        if len(departure_city) == 0:
            raise CityNotFoundException()
        
        if request.airport_radius > 0:
            lat_range = (departure_city['latitude'].values[0] - super().km_to_lat(request.airport_radius), departure_city['latitude'].values[0] + super().km_to_lat(request.airport_radius))
            long_range = (departure_city['longitude'].values[0] - super().km_to_long(request.airport_radius), departure_city['longitude'].values[0] + super().km_to_long(request.airport_radius))

            departure_locations = self.cities[lat_range[0] < self.cities['latitude'] < lat_range[1] & long_range[0] < self.cities['longitude'] < long_range[1]]
            departure_locations_json = json.loads(departure_locations.to_json(orient='records'))
            request.departure_locations.extend(departure_locations_json)

            return request
        
        else:
            request.departure_locations.extend(departure_city)
            return request
    
    def get_possible_flight(self, connection:dict, departure_location:dict, request: Request) -> Flight:
        '''
        Gets possible flights from the departure location through the connection that is given for all available dates
        '''
        
        departure_country_code = departure_location['countryCode'] 
        departure_city_code = departure_location['iata']
        arrival_city_code = connection['iata']
        
        if request.departure_date_first == None or request.departure_date_last == None or request.arrival_date_first == None or request.arrival_date_last == None:
            raise DateNotAvailableException("No date was passed as argument for departure and/or arrival")

        pl = {  "flightList":
                [
                    {   
                        "departureStation":departure_city_code,
                        "arrivalStation":arrival_city_code,
                        "from":request.departure_date_first.strftime("%Y-%m-%d"),
                        "to":request.departure_date_last.strftime("%Y-%m-%d")
                    },
                    {
                        "departureStation":arrival_city_code,
                        "arrivalStation":departure_city_code,
                        "from":request.arrival_date_first.strftime("%Y-%m-%d"),
                        "to":request.arrival_date_last.strftime("%Y-%m-%d")
                    }
                ],
                "priceType":"regular",
                "adultCount":request.adult_count,
                "childCount":request.child_count,
                "infantCount":request.infant_count
            }

        url = super().get_api_url('search', 'timetable')
        re = requests.post(url, headers=super().headers, json=pl)

        try:
            result = re.json()
            outbound_flights = pd.json_normalize(result['outboundFlights'], max_level=1)
            inbound_flights = pd.json_normalize(result['returnFlights'], max_level=1)

            outbound_flights['departureDate'] = pd.to_datetime(outbound_flights['departureDate'])
            inbound_flights['departureDate'] = pd.to_datetime(inbound_flights['departureDate'])

            return Flight(outbound_flights, inbound_flights)
            
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
        for departure_location in request.departure_locations:
            connections = departure_location['connections'] 
            with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
                threads = []
                for connection in connections:
                    threads.append(executor.submit(self.get_possible_flight, connection, departure_location, request))

            for idx, future in enumerate(concurrent.futures.as_completed(threads)):
                result = future.result()
                results.append(result)

        return results

    ### BELOW ARE UNUSED FUNCTIONS, ALSO NOT WORKING PROBABLY ###

    def old_get_travel_time(self, departure_date:str, connection:dict, departure_location:dict, request: Request):
        '''
        Gets travel times for certain connection on certain dates

        outboundFlights[], returnFlights[], 
        '''


        url = super().get_api_url('search', 'search')
        departure_country_code = departure_location['countryCode'] 
        departure_city_code = departure_location['iata']
        arrival_city_code = connection['iata']
        
        if request.departure_date_first == None or request.departure_date_last == None or request.arrival_date_first == None or request.arrival_date_last == None:
            raise DateNotAvailableException("No date was passed as argument for departure and/or arrival")


        pl = {
                "isFlightChange":False,
                "flightList":[
                    {
                        "departureStation":departure_city_code,
                        "arrivalStation":arrival_city_code,
                        "departureDate":departure_date
                    }
                ],
                "adultCount":request.adult_count,
                "childCount":request.child_count,
                "infantCount":request.infant_count,
                "wdc":True}
        
        re = requests.post(url, headers=super().headers, json=pl)
        return re.json()