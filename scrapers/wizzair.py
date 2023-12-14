import requests
import json
import datetime
from scrapers.BaseScraper import BaseScraper
from Request import Request
from Flight import Flight
from Exceptions import CityNotFoundException, CountryNotFoundException, TimeNotAvailableException, \
    DateNotAvailableException, WizzairApiVersionNotFoundException
import pandas as pd
import asyncio
import concurrent.futures
from dateutil.relativedelta import relativedelta
import re

class WizzAir(BaseScraper):
    url = "https://be.wizzair.com"
    # api_url = "https://be.wizzair.com/19.5.0/Api"

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

        self.api_url = f"https://be.wizzair.com/{self.detect_api_version()}/Api"

        cities = self._get_city_codes()
        self.connections = pd.json_normalize(cities, record_path='connections',
                                             meta=['iata', 'longitude', 'currencyCode', 'latitude', 'shortName',
                                                   'countryName', 'countryCode', 'aliases', 'isExcludedFromGeoLocation',
                                                   'rank', 'categories', 'isFakeStation'], record_prefix='connection_')
        self.connections = self.connections.rename(columns={'connection_iata': 'routes'})

        self.airports = pd.json_normalize(cities, record_path=None)

        # self.cities_df.columns = ['iata', 'longitude', 'currencyCode', 'latitude', 'shortName', 'countryName', 'countryCode', 'aliases', 'isExcludedFromGeoLocation', 'rank', 'categories', 'isFakeStation']
        self.countries = pd.json_normalize(self._get_country_codes(),
                                           meta=['code', 'name', 'isEu', 'isSchengen', 'phonePrefix'])

        super().__init__(self.url, self.headers, self.airports, self.countries, api_url=self.api_url)

    def detect_api_version(self) -> str:
        r = requests.get("https://wizzair.com/buildnumber", headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0"})
        pattern = r'\bhttps://be\.wizzair\.com/(\d+\.\d+\.\d+)\b'
        match = re.search(pattern, r.text)
        # Extract the desired substring
        if match:
            result = match.group(1)
            return result
        else:
            print("No match found.")
            raise WizzairApiVersionNotFoundException("Wizzair api version not found")



    def _get_country_codes(self):
        """
        Gets the lettercodes for all available wizzair countries and also the phoneNumber prefix

        index, code, name, isEu, isSchengen, phonePrefix
        """
        url = super().get_api_url('asset', 'country', languageCode='en-gb')
        r = requests.get(url, headers=self.headers)

        return r.json()['countries']

    def _get_city_codes(self):
        """
        Gets all the important data for all available wizzair cities:

        iata, longitude, currencyCode, latitude, shortName, countryName, countryCode,
        connections, aliases, isExcludedFromGeoLocation, rank, categories, isFakeStation
        """
        url = super().get_api_url('asset', 'map', languageCode='en-gb')
        r = requests.get(url, headers=self.headers)
        return r.json()['cities']

    def get_possible_flight(self, connection: dict, departure_location: dict, request: Request) -> Flight:
        """
        Gets possible flights from the departure location through the connection that is given for all available dates
        """

        departure_country_code = departure_location['countryCode']

        departure_city_code = departure_location['iata']
        arrival_city_code = connection['iata']

        if request.departure_date_first is None or request.departure_date_last is None or request.arrival_date_first is None or request.arrival_date_last is None:
            raise DateNotAvailableException("No date was passed as argument for departure and/or arrival")

        cur_departure_date1, cur_departure_date2 = super().find_first_and_last_day(request.departure_date_first)
        cur_arrival_date1, cur_arrival_date2 = super().find_first_and_last_day(request.arrival_date_first)
        payloads = []

        while cur_departure_date1 < request.departure_date_last or cur_arrival_date1 < request.arrival_date_last:
            pl = {"flightList": [
                    {
                        "departureStation": departure_city_code,
                        "arrivalStation": arrival_city_code,
                        "from": cur_departure_date1.strftime("%Y-%m-%d"),
                        "to": cur_departure_date2.strftime("%Y-%m-%d")
                    },
                    {
                        "departureStation": arrival_city_code,
                        "arrivalStation": departure_city_code,
                        "from": cur_arrival_date1.strftime("%Y-%m-%d"),
                        "to": cur_arrival_date2.strftime("%Y-%m-%d")
                    }
                ],
                    "priceType": "regular",
                    "adultCount": request.adult_count,
                    "childCount": request.child_count,
                    "infantCount": request.infant_count
            }
            payloads.append(pl)
            cur_departure_date1, cur_departure_date2 = super().find_first_and_last_day(cur_departure_date1 + relativedelta(months=1))
            cur_arrival_date1, cur_arrival_date2 = super().find_first_and_last_day(cur_arrival_date1 + relativedelta(months=1))

        fares_outbound = []
        fares_return = []
        for pl in payloads:
            url = super().get_api_url('search', 'timetable')
            r = requests.post(url, headers=self.headers, json=pl)
            try:
                fares_outbound.extend(r.json()['outboundFlights'])
                fares_return.extend(r.json()['returnFlights'])
            except Exception as e:
                print(r.text)
                print(e)
                print()

        try:
            # result = re.json()
            outbound_flights = pd.json_normalize(fares_outbound, max_level=1)
            return_flights = pd.json_normalize(fares_return, max_level=1)

            outbound_flights = outbound_flights.explode('departureDates')
            return_flights = return_flights.explode('departureDates')

            outbound_flights = outbound_flights[outbound_flights['priceType'] != "checkPrice"].reset_index(drop=True)
            return_flights = return_flights[return_flights['priceType'] != "checkPrice"].reset_index(drop=True)

            outbound_flights = outbound_flights.drop(columns=['hasMacFlight', 'originalPrice.amount', 'originalPrice.currencyCode', 'departureDate', 'priceType'])
            return_flights = return_flights.drop(columns=['hasMacFlight', 'originalPrice.amount', 'originalPrice.currencyCode', 'departureDate', 'priceType'])


            outbound_flights = outbound_flights.rename(columns={'price.amount': 'price', 'price.currencyCode': 'currencyCode', 'departureDates': 'departureDate'})
            return_flights = return_flights.rename(columns={'price.amount': 'price', 'price.currencyCode': 'currencyCode', 'departureDates': 'departureDate'})

            outbound_flights['company'] = self.company_name
            return_flights['company'] = self.company_name

            outbound_flights['departureDate'] = pd.to_datetime(outbound_flights['departureDate'])
            return_flights['departureDate'] = pd.to_datetime(return_flights['departureDate'])

            outbound_flights['arrivalDate'] = outbound_flights['departureDate'] + datetime.timedelta(hours=3)
            return_flights['arrivalDate'] = return_flights['departureDate'] + datetime.timedelta(hours=3)

            try:
                outbound_flights['departureCountryCode'] = outbound_flights.apply(
                    lambda x: self.get_countrycode_from_airport_code(x['departureStation']), axis=1)
                outbound_flights['arrivalCountryCode'] = outbound_flights.apply(
                    lambda x: self.get_countrycode_from_airport_code(x['arrivalStation']), axis=1)
            except Exception as e:
                print(e)
                pass

            try:
                return_flights['departureCountryCode'] = return_flights.apply(
                    lambda x: self.get_countrycode_from_airport_code(x['departureStation']), axis=1)
                return_flights['arrivalCountryCode'] = return_flights.apply(
                    lambda x: self.get_countrycode_from_airport_code(x['arrivalStation']), axis=1)
            except Exception as e:
                print(e)
                pass

            return Flight(outbound_flights, return_flights)

        except Exception as e:
            # print(re.text)
            print(e)
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

        r = requests.post(url, headers=self.headers, json=pl)
        return r.json()
