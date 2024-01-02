from Airport import Airport
from Exceptions import DateNotAvailableException
from Flight import Flight
from Request import Request
from scrapers.BaseScraper import BaseScraper
import requests
import re
import json
import pandas as pd
from dateutil.relativedelta import relativedelta
import datetime
import concurrent.futures


class Vueling(BaseScraper):
    base_url = "https://apiwww.vueling.com/"
    api_url = "https://apiwww.vueling.com/api/"

    company_name = 'vueling'

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    }

    def __init__(self):

        df_list = []

        for key, value in self._get_city_codes().items():
            for item in value:
                item.update({"Airport": key})
                df_list.append(item)

        self.airports = pd.DataFrame(df_list)
        super().__init__(self.base_url, self.headers, api_url=self.api_url)

    def _get_city_codes(self):
        """
        Gets all the important data for all available vueling airports:
        """
        # https://apiwww.vueling.com/api/Markets/GetAllMarketsSearcher
        url = super().get_api_url('Markets', 'GetAllMarketsSearcher')
        r = requests.get(url, headers=self.headers)
        return r.json()

    def get_possible_flight(self, arrival_iata: str, departure_iata: str, request: Request) -> Flight:
        """
        Gets possible flights from the departure location through the connection that is given for all available dates
        """
        # FlightPrice/GetAllFlights?originCode=BCN&destinationCode=AMS&year=2023&month=12&currencyCode=EUR&monthsRange=12
        departure_city_code = departure_iata
        arrival_city_code = arrival_iata

        if request.departure_date_first is None or request.departure_date_last is None or request.arrival_date_first is None or request.arrival_date_last is None:
            raise DateNotAvailableException("No date was passed as argument for departure and/or arrival")

        fares_outbound = []
        fares_return = []

        departure_url = super().get_api_url(
            'FlightPrice',
            'GetAllFlights',
            originCode=departure_city_code,
            destinationCode=arrival_city_code,
            year=request.departure_date_first.year,
            month=request.departure_date_first.month,
            day=request.departure_date_first.day,
            currencyCode="EUR",
            monthsrange=15
        )

        arrival_url = super().get_api_url(
            'FlightPrice',
            'GetAllFlights',
            originCode=arrival_city_code,
            destinationCode=departure_city_code,
            year=request.arrival_date_first.year,
            month=request.arrival_date_first.month,
            day=request.arrival_date_first.day,
            currencyCode="EUR",
            monthsrange=15
        )

        r_departure = requests.get(departure_url, headers=self.headers)
        r_arrival = requests.get(arrival_url, headers=self.headers)

        try:
            fares_outbound = r_departure.json()
        except Exception as e:
            print(r_departure.text)
            print(e)
            print()
            fares_outbound = []

        try:
            fares_return = r_arrival.json()
        except Exception as e:
            print(r_arrival.text)
            print(e)
            print()
            fares_return = []

        try:
            # result = re.json()
            outbound_flights = pd.json_normalize(fares_outbound, max_level=1)
            return_flights = pd.json_normalize(fares_return, max_level=1)

            outbound_flights = outbound_flights[~outbound_flights['IsInvalidPrice']].reset_index(drop=True)
            return_flights = return_flights[~return_flights['IsInvalidPrice']].reset_index(drop=True)

            outbound_flights = outbound_flights.drop(
                columns=['Availability', 'ClassOfService', 'Created', 'Fare',
                         'FlightID', 'ProductClass', 'Sort', 'Tax', 'IsInvalidPrice'])
            return_flights = return_flights.drop(
                columns=['Availability', 'ClassOfService', 'Created', 'Fare',
                         'FlightID', 'ProductClass', 'Sort', 'Tax', 'IsInvalidPrice'])

            outbound_flights = outbound_flights.rename(
                columns={'ArrivalDate': 'arrivalDate', 'DepartureDate': 'departureDate',
                         'ArrivalStation': 'arrivalStation', 'DepartureStation': 'departureStation',
                         'Price': 'price'})
            return_flights = return_flights.rename(
                columns={'ArrivalDate': 'arrivalDate', 'DepartureDate': 'departureDate',
                         'ArrivalStation': 'arrivalStation', 'DepartureStation': 'departureStation',
                         'Price': 'price'})

            outbound_flights['currencyCode'] = 'EUR'
            return_flights['currencyCode'] = 'EUR'

            outbound_flights['company'] = self.company_name
            return_flights['company'] = self.company_name

            outbound_flights['departureDate'] = pd.to_datetime(outbound_flights['departureDate'], utc=True)
            return_flights['departureDate'] = pd.to_datetime(return_flights['departureDate'], utc=True)

            outbound_flights['arrivalDate'] = pd.to_datetime(outbound_flights['arrivalDate'], utc=True)
            return_flights['arrivalDate'] = pd.to_datetime(return_flights['arrivalDate'], utc=True)

            try:

                outbound_flights = outbound_flights.merge(Airport.all_airports_df[['iata', 'country']], left_on='departureStation',
                                       right_on='iata', how='left')
                outbound_flights = outbound_flights.rename(columns={'country': 'departureCountryCode'})

                outbound_flights = outbound_flights.merge(Airport.all_airports_df[['iata', 'country']],
                                                          left_on='arrivalStation',
                                                          right_on='iata', how='left')
                outbound_flights = outbound_flights.rename(columns={'country': 'arrivalCountryCode'})

            except Exception as e:
                print(e)
                pass

            try:
                return_flights = return_flights.merge(Airport.all_airports_df[['iata', 'country']],
                                                          left_on='departureStation',
                                                          right_on='iata', how='left')
                return_flights = return_flights.rename(columns={'country': 'departureCountryCode'})

                return_flights = return_flights.merge(Airport.all_airports_df[['iata', 'country']],
                                                          left_on='arrivalStation',
                                                          right_on='iata', how='left')
                return_flights = return_flights.rename(columns={'country': 'arrivalCountryCode'})
            except Exception as e:
                print(e)
                pass

            unnecessary_vars = ['iata_y', 'iata_x']
            outbound_flights = outbound_flights.drop(columns=unnecessary_vars)
            return_flights = return_flights.drop(columns=unnecessary_vars)

            return Flight(outbound_flights, return_flights)

        except Exception as e:
            # print(re.text)
            print(e)
            return Flight.empty_flight()

    def get_possible_flights(self, request: Request) -> list:
        """
        Gets the possible flight times and their prices according to request argument
        """

        # TODO: called method be dependent on if radius or country of departure is chosen

        departure_airports_df = request.get_requested_departure_airports_df()

        connections_df = self.airports[self.airports['Airport'].isin(departure_airports_df['iata'])].reset_index(drop=True)
        # TODO: Dropping all the flights with transfer rn (Pretty much all transfers are through Barcelona), have to change this later
        connections_df = connections_df[connections_df['Connection'].isnull() | connections_df['Connection'].str.strip().eq('')]

        results = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
            threads = []
            for idx, connection_row in connections_df.iterrows():

                connection = connection_row['DestinationCode']

                if request.arrival_city and not Airport.airports_in_radius(connection, request.arrival_city, request.airport_radius):
                    continue

                threads.append(executor.submit(self.get_possible_flight, connection, connection_row['Airport'], request))

            for idx, future in enumerate(concurrent.futures.as_completed(threads)):
                result = future.result()
                results.append(result)

        return results
