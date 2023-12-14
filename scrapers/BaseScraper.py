import json
import datetime
import time

import pandas as pd

from Exceptions import CityNotFoundException, CountryNotFoundException, TimeNotAvailableException, \
    DateNotAvailableException

import Request


class BaseScraper:

    longitude_km_ratio = 111

    base_url: str
    api_url: str
    headers: dict

    def __init__(self, base_url, headers, airports, countries, api_url=None) -> None:

        self.base_url = base_url
        self.api_url = api_url

        self.airports = airports
        self.countries = countries

    def get_headers(self):
        return self.headers
    def get_api_url(self, *api_calls: str, **queries: any) -> str:
        """
        Generates a proper api url string using the arguments and query arguments passed
        """
        query_params = '?'
        for key, value in queries.items():
            query_params += f'{key}={value}&'
        return f"{self.api_url}/{'/'.join(api_calls)}" + query_params[:-1]

    def km_to_long(self, km: float) -> float:
        return km / self.longitude_km_ratio

    def km_to_lat(self, km: float) -> float:
        return km / self.longitude_km_ratio

    def compare_strings(self, string1: str, string2: str) -> bool:
        return string1.lower().strip() == string2.lower().strip()

    def get_countrycode_from_name(self, country_name: str) -> str:
        result = self.countries[self.countries['name'] == country_name]['code'].values
        if len(result) == 0:
            raise CountryNotFoundException()
        return result[0]

    def get_countrycode_from_airport_code(self, airport_code: str) -> str:
        """
        Gets used country code from its city airport code
        """
        result = self.airports[self.airports['iata'] == airport_code]['countryCode'].values
        if len(result) == 0:
            raise CityNotFoundException()

        return result[0]

    def filter_arrival_airports_by_country(self, request: Request, connections_df) -> pd.DataFrame:
        if request.arrival_country is not None:
            connections_df['arrivalCountryCode'] = connections_df.apply(
                lambda x: self.get_countrycode_from_airport_code(x['iata']), axis=1)
            connections_df = connections_df[
                connections_df['arrivalCountryCode'] == self.get_countrycode_from_name(
                    request.arrival_country)].reset_index(drop=True)
        return connections_df

    def filter_departure_airports_by_country(self, request: Request) -> Request:
        """
        Grabs all the cities in the country of original departure city
        """

        if request.departure_country is not None:
            country_code = self.get_countrycode_from_name(request.departure_country)
            departure_cities = request.departure_locations[request.departure_locations['countryCode'] == country_code]
            # departure_cities_json = json.loads(departure_cities.to_json(orient='records'))
            if len(departure_cities) == 0:
                raise CityNotFoundException()

            # request.departure_locations.extend(departure_cities_json)
            request.departure_locations = departure_cities

        return request

    def filter_departure_airports_by_radius(self, request: Request):
        """
        Grabs all the cities within a certain radius of original departure city
        """

        departure_city = request.departure_locations[request.departure_locations.shortName.str.lower() == request.departure_city.lower()]

        if len(departure_city) == 0:
            raise CityNotFoundException()

        if request.airport_radius > 0:
            lat_range = (departure_city['latitude'].values[0] - self.km_to_lat(request.airport_radius),
                         departure_city['latitude'].values[0] + self.km_to_lat(request.airport_radius))
            long_range = (departure_city['longitude'].values[0] - self.km_to_long(request.airport_radius),
                          departure_city['longitude'].values[0] + self.km_to_long(request.airport_radius))

            departure_locations = request.departure_locations[
                (lat_range[0] < request.departure_locations['latitude']) & (request.departure_locations['latitude'] < lat_range[1]) &
                (long_range[0] < request.departure_locations['longitude']) & (request.departure_locations['longitude'] < long_range[1])
                ]

            # return_locations = pd.DataFrame()
            # for city in departure_locations.iata.unique():
            #     pd.concat([return_locations, request.departure_locations[request.departure_locations['routes'] == city]])

            # departure_locations_json = json.loads(departure_locations.to_json(orient='records'))
            # request.departure_locations.extend(departure_locations_json)
            request.departure_locations = departure_locations
            return request

        else:
            request.departure_locations = departure_city
            return request

    def finalize_departure_locations(self, request: Request):
        request.departure_locations = json.loads(request.departure_locations.to_json(orient='records'))
        return request

    def diff_month(self, d1: datetime.date, d2: datetime.date):
        return (d1.year - d2.year) * 12 + d1.month - d2.month

    def find_first_and_last_day(self, input_date: datetime.date) -> [datetime.date, datetime.date]:
        # Find the first day of the month
        first_day = datetime.date(input_date.year, input_date.month, 1)

        # Find the last day of the month
        next_month = datetime.date(input_date.year + (input_date.month // 12), ((input_date.month % 12) + 1), 1)
        last_day = next_month - datetime.timedelta(days=1)

        return first_day, last_day