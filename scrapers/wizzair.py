import requests
import json
import datetime
from scrapers.BaseScraper import BaseScraper
from Request import Request
from Flight import Flight
from Exceptions import CityNotFoundException, CountryNotFoundException, TimeNotAvailableException, \
    DateNotAvailableException
import pandas as pd
import asyncio
import concurrent.futures


class WizzAir(BaseScraper):
    url = "https://be.wizzair.com"
    api_url = "https://be.wizzair.com/19.4.0/Api"

    headers = {
        'cookie': "RequestVerificationToken=10aa42acaa2d4bea88be9818666639b3",
        'Host': "be.wizzair.com",
        'Accept-Language': "en-US,en;q=0.5",
        'X-RequestVerificationToken': "10aa42acaa2d4bea88be9818666639b3",
        'TE': "trailers",
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0"
    }

    airports: pd.DataFrame
    countries: pd.DataFrame

    company_name = 'wizzair'

    def __init__(self) -> None:
        """
        Initializes the Wizzair object and its super BaseScraper

        Variable "connections" represents all the available destinations for wizzair airplanes
        Variable "cities" represents all cities that can be traveled to with wizzair
        Variable "countries" represents all countries that can be traveled to with wizzair
        """

        cities = self._get_city_codes()
        self.connections = pd.json_normalize(cities, record_path='connections',
                                             meta=['iata', 'longitude', 'currencyCode', 'latitude', 'shortName',
                                                   'countryName', 'countryCode', 'aliases', 'isExcludedFromGeoLocation',
                                                   'rank', 'categories', 'isFakeStation'], record_prefix='connection_')
        self.airports = pd.json_normalize(cities, record_path=None)

        # self.cities_df.columns = ['iata', 'longitude', 'currencyCode', 'latitude', 'shortName', 'countryName', 'countryCode', 'aliases', 'isExcludedFromGeoLocation', 'rank', 'categories', 'isFakeStation']
        self.countries = pd.json_normalize(self._get_country_codes(),
                                           meta=['code', 'name', 'isEu', 'isSchengen', 'phonePrefix'])

        super().__init__(self.url, self.headers, self.airports, self.countries, api_url=self.api_url)

    def _get_country_codes(self):
        """
        Gets the lettercodes for all available wizzair countries and also the phoneNumber prefix

        index, code, name, isEu, isSchengen, phonePrefix
        """
        url = super().get_api_url('asset', 'country', languageCode='en-gb')
        re = requests.get(url, headers=self.headers)

        return re.json()['countries']

    def _get_city_codes(self):
        """
        Gets all the important data for all available wizzair cities:

        iata, longitude, currencyCode, latitude, shortName, countryName, countryCode,
        connections, aliases, isExcludedFromGeoLocation, rank, categories, isFakeStation
        """
        url = super().get_api_url('asset', 'map', languageCode='en-gb')
        re = requests.get(url, headers=self.headers)
        return re.json()['cities']

    def get_possible_flight(self, connection: dict, departure_location: dict, request: Request) -> Flight:
        """
        Gets possible flights from the departure location through the connection that is given for all available dates
        """

        departure_country_code = departure_location['countryCode']
        departure_city_code = departure_location['iata']
        arrival_city_code = connection['iata']

        if request.departure_date_first is None or request.departure_date_last is None or request.arrival_date_first is None or request.arrival_date_last is None:
            raise DateNotAvailableException("No date was passed as argument for departure and/or arrival")

        pl = {"flightList": [
                {
                    "departureStation": departure_city_code,
                    "arrivalStation": arrival_city_code,
                    "from": request.departure_date_first.strftime("%Y-%m-%d"),
                    "to": request.departure_date_last.strftime("%Y-%m-%d")
                },
                {
                    "departureStation": arrival_city_code,
                    "arrivalStation": departure_city_code,
                    "from": request.arrival_date_first.strftime("%Y-%m-%d"),
                    "to": request.arrival_date_last.strftime("%Y-%m-%d")
                }
            ],
                "priceType": "regular",
                "adultCount": request.adult_count,
                "childCount": request.child_count,
                "infantCount": request.infant_count
        }

        url = super().get_api_url('search', 'timetable')
        re = requests.post(url, headers=self.headers, json=pl)

        try:
            result = re.json()
            outbound_flights = pd.json_normalize(result['outboundFlights'], max_level=1)
            return_flights = pd.json_normalize(result['returnFlights'], max_level=1)

            outbound_flights = outbound_flights.explode('departureDates')
            return_flights = return_flights.explode('departureDates')

            outbound_flights = outbound_flights[outbound_flights['priceType'] != "checkPrice"].reset_index(drop=True)
            return_flights = return_flights[return_flights['priceType'] != "checkPrice"].reset_index(drop=True)

            outbound_flights = outbound_flights.drop(columns=['hasMacFlight', 'originalPrice.amount', 'originalPrice.currencyCode', 'departureDate', 'priceType'])
            return_flights = return_flights.drop(columns=['hasMacFlight', 'originalPrice.amount', 'originalPrice.currencyCode', 'departureDate', 'priceType'])

            outbound_flights['arrivalDate'] = None
            return_flights['arrivalDate'] = None

            outbound_flights = outbound_flights.rename(columns={'price.amount': 'price', 'price.currencyCode': 'currencyCode', 'departureDates': 'departureDate'})
            return_flights = return_flights.rename(columns={'price.amount': 'price', 'price.currencyCode': 'currencyCode', 'departureDates': 'departureDate'})

            outbound_flights['company'] = self.company_name
            return_flights['company'] = self.company_name

            outbound_flights['departureDate'] = pd.to_datetime(outbound_flights['departureDate'])
            return_flights['departureDate'] = pd.to_datetime(return_flights['departureDate'])

            return Flight(outbound_flights, return_flights)

        except Exception as e:
            print(re.text)
            return Flight.empty_flight()

    def get_possible_flights(self, request: Request) -> list:
        """
        Gets the possible flight times and their prices according to request argument
        """
        request.departure_locations = self.airports
        # TODO: called method be dependent on if radius or country of departure is chosen
        if request.departure_country is not None:
            request = super().filter_departure_airports_by_country(request)
        if request.departure_city is not None:
            request = super().filter_departure_airports_by_radius(request)
        request = super().finalize_departure_locations(request)

        results = []
        for departure_location in request.departure_locations:
            # TODO: following 3 lines unnecessary if request.departure_locations keeps being a dataframe
            connections = departure_location['connections']
            connections_df = pd.json_normalize(connections, max_level=1)
            connections_df = super().filter_arrival_airports_by_country(request, connections_df)

            with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
                threads = []
                for idx, connection in connections_df.iterrows():
                    threads.append(executor.submit(self.get_possible_flight, connection, departure_location, request))

            for idx, future in enumerate(concurrent.futures.as_completed(threads)):
                result = future.result()
                results.append(result)

        return results

    ### BELOW ARE UNUSED FUNCTIONS, ALSO NOT WORKING PROBABLY ###

    def old_get_travel_time(self, departure_date: str, connection: dict, departure_location: dict, request: Request):
        """
        Gets travel times for certain connection on certain dates

        outboundFlights[], returnFlights[],
        """

        url = super().get_api_url('search', 'search')
        departure_country_code = departure_location['countryCode']
        departure_city_code = departure_location['iata']
        arrival_city_code = connection['iata']

        if request.departure_date_first is None or request.departure_date_last is None or \
                request.arrival_date_first is None or request.arrival_date_last is None:
            raise DateNotAvailableException("No date was passed as argument for departure and/or arrival")

        pl = {
            "isFlightChange": False,
            "flightList": [
                {
                    "departureStation": departure_city_code,
                    "arrivalStation": arrival_city_code,
                    "departureDate": departure_date
                }
            ],
            "adultCount": request.adult_count,
            "childCount": request.child_count,
            "infantCount": request.infant_count,
            "wdc": True
        }

        re = requests.post(url, headers=self.headers, json=pl)
        return re.json()
