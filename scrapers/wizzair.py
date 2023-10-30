import requests
import json
import datetime
from scrapers.BaseScraper import BaseScraper
from Request import Request
from Exceptions import CityNotFoundException, CountryNotFoundException, TimeNotAvailableException, DateNotAvailableException

class WizzAir(BaseScraper):

    url = "https://be.wizzair.com"
    api_url = "https://be.wizzair.com/19.2.0/Api"
    
    wizz_headers = {
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0",
        'X-RequestVerificationToken': "b368cb5ee8db4a65872b5ef92ed82f85",
        'Origin': "https://wizzair.com",
        'Cookie': "RequestVerificationToken=b368cb5ee8db4a65872b5ef92ed82f85",
        }

    cities: list
    countries: list

    def __init__(self) -> None:
        '''
        Initializes the Wizzair object and its super BaseScraper

        Variable "cities" represents all the available destinations for wizzair airplanes
        '''
        super().__init__(self.url, self.wizz_headers, api_url=self.api_url)
        self.cities = self._get_city_codes()
        self.countries = self._get_country_codes()

    def get_possible_flights(self, request: Request) -> list[dict[str,str]]:
        '''
        Gets the possible flighttimes and their prices for request param
        '''

        request = self._get_airports_by_country(request)
        for departure_location in request.departure_locations:
            connections = departure_location['connections'] 
            departure_country_code = departure_location['countryCode'] 
            departure_city_code = departure_location['iata']
            for connection in connections:
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
                yield re.json()
    
    def configureRequest(self, request: Request):
        '''
        Configure the request so it uses the right country and city codes
        '''

        # TODO: add the radius slider
        available_cities = []
        for city in self.cities:
            if city["countryName"] == request.departure_country:
                available_cities.append(city)

        

    def _get_country_codes(self):
        '''
        Gets the lettercodes for all available wizzair countries and also the phoneNumber prefix
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
        result = None
        for country in self.countries:
            if ',' in country['name']:
                if super().compare_strings(country['name'].split(",")[0], country_name):
                    result = country['code']
                    break
            else:
                if super().compare_strings(country['name'], country_name):
                    result = country['code']
                    break
        if result == None:
            raise CountryNotFoundException()
        return result 

    def _get_airports_by_country(self, request:Request) -> Request:
        '''
        Grabs all the cities in the country of original departure city
        '''
        country_code = self._get_country_code_from_name(request.departure_country)
        for city in self.cities:
            if self.compare_strings(city['countryCode'], country_code):
                request.departure_locations.append(city)
                departure_city = city

        if departure_city == None:
            raise CityNotFoundException()
        
        return request

    def _get_airports_by_radius(self, request: Request) -> Request:
        '''
        Grabs all the cities within a certain radius of original departure city
        '''
        
        departure_city = None
        for city in self.cities:
            if self.compare_strings(city['shortName'], request.departure_city):
                request.departure_locations.append(city)
                departure_city = city
                break

        if departure_city == None:
            raise CityNotFoundException()
        
        if request.airport_radius > 0:
            lat_range = (departure_city['latitude'] - super().km_to_lat(request.airport_radius), departure_city['latitude'] + super().km_to_lat(request.airport_radius))
            long_range = (departure_city['longitude'] - super().km_to_long(request.airport_radius), departure_city['longitude'] + super().km_to_long(request.airport_radius))

            for city in self.cities:
                if self.compare_strings(city['shortName'], request.departure_city):
                    continue
                
                if lat_range[0] < city['latitude'] < lat_range[1] and long_range[0] < city['longitude'] < long_range[1]:
                    request.departure_locations.append(city)

        return request

                

        