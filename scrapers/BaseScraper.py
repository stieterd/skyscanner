import json
import datetime
import random
import time

import pandas as pd

from Airport import Airport
from Exceptions import CityNotFoundException, CountryNotFoundException, TimeNotAvailableException, \
    DateNotAvailableException

import Request
import itertools


class BaseScraper:

    longitude_km_ratio = 111

    base_url: str
    api_url: str
    headers: dict

    MAX_WORKERS = 80
    MAX_TEST_WORKERS = 1

    LANGUAGE = "nl"
    COUNTRY = 'nl'

    proxies = itertools.cycle(["http://31.204.3.112:5432:rpxod:ki2ag7xw", "http://31.204.3.252:5432:rpxod:ki2ag7xw",
               "http://213.209.140.106:5432:rpxod:ki2ag7xw", "http://s2t8v:jfr6jj57@89.19.33.120:5432"])

    def __init__(self, base_url, headers, api_url=None) -> None:

        self.base_url = base_url
        self.api_url = api_url

        self.cur_proxy = next(self.proxies)

    def get_headers(self):
        return self.headers

    def next_proxy(self):
        self.cur_proxy = next(self.proxies)

    def get_proxy(self):
        # return {"http": proxy, "https": proxy}
        return {
                  'http': self.cur_proxy,
                  'https': self.cur_proxy
                }

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

    def finalize_departure_locations(self, request: Request):
        request.departure_locations = json.loads(request.departure_locations.to_json(orient='records'))
        return request

    def diff_month(self, d1: datetime.date, d2: datetime.date):
        return (d1.year - d2.year) * 12 + d1.month - d2.month

    def add_country_codes(self, flights_df: pd.DataFrame) -> pd.DataFrame:
        try:

            flights_df = flights_df.merge(Airport.all_airports_df[['iata', 'country']],
                                                      left_on='departureStation',
                                                      right_on='iata', how='left')
            flights_df = flights_df.rename(columns={'country': 'departureCountryCode'})

            flights_df = flights_df.merge(Airport.all_airports_df[['iata', 'country']],
                                                      left_on='arrivalStation',
                                                      right_on='iata', how='left')
            flights_df = flights_df.rename(columns={'country': 'arrivalCountryCode'})

        except Exception as e:
            print(e)
            pass

        unnecessary_vars = ['iata_y', 'iata_x']
        flights_df = flights_df.drop(columns=unnecessary_vars)

        return flights_df

    def find_first_and_last_day(self, input_date: datetime.date) -> [datetime.date, datetime.date]:
        # Find the first day of the month
        first_day = datetime.date(input_date.year, input_date.month, 1)

        # Find the last day of the month
        next_month = datetime.date(input_date.year + (input_date.month // 12), ((input_date.month % 12) + 1), 1)
        last_day = next_month - datetime.timedelta(days=1)

        return first_day, last_day
