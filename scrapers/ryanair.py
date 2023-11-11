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


class RyanAir(BaseScraper):
    base_url = "https://www.ryanair.com"
    api_url = "https://www.ryanair.com/api"

    headers = {
        'Alt-Used': 'www.ryanair.com',
        'newrelic': 'eyJ2IjpbMCwxXSwiZCI6eyJ0eSI6IkJyb3dzZXIiLCJhYyI6IjY0NjgzMiIsImFwIjoiMzg5MjUxMzk3IiwiaWQiOiI3ZGI3Njc4N2U3NDA2NGVlIiwidHIiOiJhYzA3YjRjMTBlMjM5MWFjYTYxYTQyZGI3MWUyYmIwMCIsInRpIjoxNjk4Nzg5Nzk0MTkyfX0=',
        'X-NewRelic-ID': 'undefined',
        'Cookie': 'rid=ae238289-8716-4078-96f6-a3ab6b2fc20d; rid.sig=jyna6R42wntYgoTpqvxHMK7H+KyM6xLed+9I3KsvYZaVt7P36AL6zp9dGFPu5uVxaIiFpNXrszr+LfNCdY3IT3oCSYLeNv/ujtjsDqOzkY66AL3V6kH2vsK+au12X21HkZ4S8GaG8CoBmm/m0rLsOKYkxtw+U3+ejBaPc15jJjKXnc3owMBg82SNbqyKjVd6Z6qcsoE25p3RmlcaHuHC3GBf1yIGtlqeQun3Mj0vmSWJV8jwIw2wASJ789/CrRqZD7ygs/wfcb8hBBqVoMqjESkGsL/S7dk/EwM5eijnZV/nQWCwG90fqwg160peojGgPoiDiywWDoENQk0YV09rYSADa7R/3NbavlLjUKgw7Vqr2EkRoOQgMUdmDHgAfxJKsZrWodQFk9xEBzjuHPfocfN5yR5jxsTO/bmZ88KXBnt/S3VNMJYg+Yq0skSEsUdqtvj7UADUgM7dyNK9VH0+pLIknNJG+FGLdx1qeLyeDkQyPQmUxRR+3JCLvMPmo0oKaGvvnT9iQ6WZj/Dg9+196dlEt0ODD2dOmctVx3imVQFAXMzbRiIBmV9aRQSmk1zZ1GPPEYm9b9wRW+/fG6/QowErOT5b8/uISrGzpkFvFyMxiOw4SBIhxbYpEvgdURJBjsujq7N85xdC6U0MhAv7WfJVZwMnbKLgPLo1oDu3+7N3tagyw0B6LLNPVVMgYGZWWM1R7JFZRBE1AYT9w9LFkuuwlS/4F67Xnqe/MLjnUoRQDbo+uo2sDGnNTJ2KVG8UGLPZbbhbajWmQiI0hsgfxFjTZfHis1vyv1fv/yhICmF9ojk+S+1GY5/G1XpsO/qW/gl7UcXIXlPG3mmrc4vrfA==; mkt=/gb/en/;',
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0"
    }

    company_name = 'ryanair'

    def __init__(self) -> None:
        """
        Initializes the RyanAir object and its super BaseScraper
        """
        city_codes = self._get_city_codes()
        country_codes = self._get_country_codes()

        countries_df = pd.json_normalize(country_codes, max_level=1)

        self.connections = pd.json_normalize(city_codes, max_level=1)
        # self.connections['routes'] = self.connections['routes'].str.split(',', expand=False)
        self.connections = self.connections.explode('routes')
        self.connections = self.connections[self.connections["routes"].str.contains("airport:") == True]
        self.connections['routes'] = self.connections['routes'].str.replace('airport:', '')
        self.connections = self.connections.rename(columns={'iataCode': 'iata'})

        self.countries = countries_df.groupby(
            ['country.code', 'country.iso3code', 'country.name', 'country.currency', 'country.defaultAirportCode',
             'country.schengen']).size().reset_index(name='count').drop(columns='count')
        self.countries.columns = ['code', 'iso3code', 'name', 'currency', 'defaultAirportCode', 'schengen']

        self.airports = countries_df.groupby(
            ['city.name', 'city.code', 'country.code', 'country.iso3code', 'country.name', 'country.currency',
             'coordinates.latitude', 'coordinates.longitude', 'country.defaultAirportCode',
             'country.schengen']).size().reset_index(name='count').drop(columns='count')
        self.airports.columns = ['name', 'iata', 'country.badcode', 'countryCode', 'country.name', 'currency',
                                 'latitude', 'longitude', 'country.defaultAirportCode', 'schengen']


        super().__init__(self.base_url, self.headers, self.connections, self.countries, self.api_url)

    def _get_city_codes(self):
        """
        Gets all the important data for all available ryanair airports:

        iataCode, name, seoName, aliases[], coordinates[latitude], coordinates[longitude], base, countryCode,
        regionCode, cityCode, currencyCode, routes[], seasonalRoutes[], categories[], priority, timeZone
        """
        url = super().get_api_url('views', 'locate', '3', 'airports', 'en', 'active')
        re = requests.get(url, headers=self.headers)
        return re.json()

    def _get_country_codes(self):
        """
        Gets all the important data for all available ryanair airports:

        code, name, seoName, aliases[], base, city[name], city[code], region[name], region[code],
        country[code], country[iso3code], country[name], country[currency], country[defaultAirportCode], schengen,
        coordinates[latitude], coordinates[longitude], timeZone
        """

        url = super().get_api_url('views', 'locate', '5', 'airports', 'en', 'active')
        re = requests.get(url, headers=self.headers)
        return re.json()

    # def _get_country_code_from_name(self, country_name: str) -> str:
    #     """
    #     Gets used country code for specific country
    #     """
    #     result = self.countries[self.countries['name'] == country_name]['code'].values
    #     if len(result) == 0:
    #         raise CountryNotFoundException()
    #     return result[0]
    #
    # def _get_country_code_from_airport_code(self, airport_code: str) -> str:
    #     """
    #     Gets used country code from its city airport code
    #     """
    #     result = self.airports[self.airports['code'] == airport_code]['country.code'].values
    #     if len(result) == 0:
    #         raise CityNotFoundException()
    #
    #     return result[0]
    #
    # def _get_airports_by_country(self, request: Request) -> Request:
    #     """
    #     Grabs all the cities in the country of original departure city
    #     """
    #     country_code = self._get_country_code_from_name(request.departure_country)
    #     departure_cities = self.connections[self.connections['countryCode'] == country_code]
    #     departure_cities_json = json.loads(departure_cities.to_json(orient='records'))
    #     if len(departure_cities_json) == 0:
    #         raise CityNotFoundException()
    #
    #     request.departure_locations.extend(departure_cities_json)
    #
    #     return request
    #
    # def _get_airports_by_radius(self, request: Request) -> Request:
    #     """
    #     Grabs all the cities within a certain radius of original departure city
    #     """
    #
    #     departure_city = self.connections[self.connections['name'] == request.departure_city]
    #
    #     if len(departure_city) == 0:
    #         raise CityNotFoundException()
    #
    #     if request.airport_radius > 0:
    #         lat_range = (departure_city['coordinates.latitude'].values[0] - super().km_to_lat(request.airport_radius),
    #                      departure_city['latitude'].values[0] + super().km_to_lat(request.airport_radius))
    #         long_range = (departure_city['longitude'].values[0] - super().km_to_long(request.airport_radius),
    #                       departure_city['longitude'].values[0] + super().km_to_long(request.airport_radius))
    #
    #         departure_locations = self.connections[
    #             lat_range[0] < self.connections['coordinates.latitude'] < lat_range[1] & long_range[0] <
    #             self.connections['coordinates.longitude'] < long_range[1]]
    #         departure_locations_json = json.loads(departure_locations.to_json(orient='records'))
    #         request.departure_locations.extend(departure_locations_json)
    #
    #         return request
    #
    #     else:
    #         request.departure_locations.extend(departure_city)
    #         return request

    def get_possible_flight(self, departure_location: dict, request: Request) -> Flight:
        # url = "https://www.ryanair.com/api/farfnd/v4/oneWayFares/DUB/AMS/cheapestPerDay?outboundMonthOfDate=2024-01-02&currency=EUR"
        url_outbound = super().get_api_url("farfnd",
                                           "v4",
                                           "oneWayFares",
                                           f"{departure_location['iata']}",
                                           f"{departure_location['routes']}",
                                           "cheapestPerDay",
                                           outboundMonthOfDate=request.departure_date_first.strftime("%Y-%m-%d"),
                                           currency="EUR")

        url_return = super().get_api_url("farfnd",
                                         "v4",
                                         "oneWayFares",
                                         f"{departure_location['routes']}",
                                         f"{departure_location['iata']}",
                                         "cheapestPerDay",
                                         outboundMonthOfDate=request.arrival_date_first.strftime("%Y-%m-%d"),
                                         currency="EUR")

        re_outbound = requests.get(url_outbound, headers=self.headers)
        re_return = requests.get(url_return, headers=self.headers)

        try:
            result_outbound = re_outbound.json()
            result_return = re_return.json()
            outbound_flights = pd.json_normalize(result_outbound['outbound']['fares'], max_level=4)
            return_flights = pd.json_normalize(result_return['outbound']['fares'], max_level=4)

            outbound_flights = outbound_flights[(outbound_flights['unavailable'] == False) & (outbound_flights['soldOut'] == False)].reset_index(drop=True)
            return_flights = return_flights[(return_flights['unavailable'] == False) & (outbound_flights['soldOut'] == False)].reset_index(drop=True)

            outbound_flights = outbound_flights.drop(columns=['day', 'unavailable', 'soldOut', 'price.valueMainUnit', 'price.valueFractionalUnit', 'price.currencySymbol'])
            return_flights = return_flights.drop(columns=['day', 'unavailable', 'soldOut', 'price.valueMainUnit', 'price.valueFractionalUnit', 'price.currencySymbol'])
            try:
                outbound_flights = outbound_flights.drop(columns=['price'])
                return_flights = return_flights.drop(columns=['price'])
            except Exception as e:
                pass


            outbound_flights['departureStation'] = departure_location['iata']
            outbound_flights['arrivalStation'] = departure_location['routes']

            return_flights['departureStation'] = departure_location['routes']
            return_flights['arrivalStation'] = departure_location['iata']

            outbound_flights['departureDate'] = pd.to_datetime(outbound_flights['departureDate'])
            return_flights['departureDate'] = pd.to_datetime(return_flights['departureDate'])

            outbound_flights = outbound_flights.rename(columns={'price.value': 'price', 'price.currencyCode': 'currencyCode'})
            return_flights = return_flights.rename(columns={'price.value': 'price', 'price.currencyCode': 'currencyCode'})

            outbound_flights['company'] = self.company_name
            return_flights['company'] = self.company_name

            return Flight(outbound_flights, return_flights)

        except Exception as e:
            print(e)
            print(re_outbound.text)
            print(re_return.text)
            return Flight.empty_flight()

    def get_possible_flights(self, request: Request) -> list:
        """
        Gets the possible flight times and their prices according to request argument
        """
        # TODO: called method be dependent on if radius or country of departure is chosen
        request.departure_locations = self.airports
        if request.departure_country is not None:
            request = super().filter_departure_airports_by_country(request)
        if request.departure_city is not None:
            request = super().filter_departure_airports_by_radius(request)
        request = super().finalize_departure_locations(request)

        results = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
            threads = []
            # WOMP WOMP
            for departure_location in request.departure_locations:
                if request.arrival_country is not None and super().get_countrycode_from_name(request.arrival_country) != super().get_countrycode_from_airport_code(departure_location['routes']):
                    continue
                if request.arrival_city is not None and request.arrival_city != departure_location['routes']:
                    continue
                threads.append(executor.submit(self.get_possible_flight, departure_location, request))

            for idx, future in enumerate(concurrent.futures.as_completed(threads)):
                result = future.result()
                results.append(result)

        return results


    ### OLD FUNCTIONS MAY NOT WORK
    def get_possible_flight_old(self, departure_location: dict, departure_date: str, arrival_date: str,
                                request: Request) -> Flight:

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

        re = requests.get(url, headers=self.headers)
        try:
            result = re.json()
            outbound_flights = pd.json_normalize(result['trips'][0]['dates'], 'flights', max_level=4)
            return_flights = pd.json_normalize(result['trips'][1]['dates'], 'flights', max_level=4)
            return Flight(outbound_flights, return_flights)

        except Exception as e:
            print(re.text)
            return Flight.empty_flight()
