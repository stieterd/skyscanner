import concurrent.futures
import traceback

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


class EasyJet(BaseScraper):

    base_url = "https://www.easyjet.com/"
    api_url = "https://www.easyjet.com/api"

    company_name = 'easyjet'

    headers = {
        "Host": "www.easyjet.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "DNT": "1",
        "Sec-GPC": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-User": "?1"
    }

    def __init__(self):

        # https://www.easyjet.com/ejavailability/api/v76/fps/lowestdailyfares?ArrivalIata=LPL&Currency=EUR&DateFrom=2023-12-30&DateTo=2025-12-30&DepartureIata=AMS&InboundFares=true
        r_json = self._get_city_codes()
        self.airports = pd.json_normalize(r_json, record_path=["Connections"], meta=['CityIata'])
        self.airports = self.airports.rename(columns={0: "connection", "CityIata": "iata"})
        self.airports = self.airports[~self.airports['iata'].isnull() & ~self.airports['iata'].str.strip().eq('') & ~self.airports['connection'].str.contains('*', regex=False)].reset_index(drop=True)
        super().__init__(self.base_url, self.headers, api_url=self.api_url)

    def _get_city_codes(self):
        """
        Gets all the important data for all available ryanair airports:

        iataCode, name, seoName, aliases[], coordinates[latitude], coordinates[longitude], base, countryCode,
        regionCode, cityCode, currencyCode, routes[], seasonalRoutes[], categories[], priority, timeZone
        """
        url = "https://www.easyjet.com/nl/"
        r = requests.get(url, headers=self.headers)
        pattern = pattern = r'angularEjModule\.constant\("Sitecore_RoutesData",\s*(.*?)\s*\);'
        matches = re.search(pattern, r.text, re.DOTALL)
        if matches:
            extracted_data = matches.group(1)
            return json.loads(extracted_data)['Airports']
        else:
            return {}

    def get_possible_flight(self, arrival_iata: str, departure_iata: str, request: Request) -> Flight:
        """
        Gets possible flights from the departure location through the connection that is given for all available dates
        ---
        IMPORTANT, maximum interval time of a week
        """

        headers = {
            "Host": "www.easyjet.com",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
            "X-Requested-With": "XMLHttpRequest",
            "X-Transaction-Id": "BF8FC686-3CDD-E777-DACF-3C49095D1B49",
            "Connection": "keep-alive",
            "Referer": "https://www.easyjet.com/nl/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin"
        }

        departure_city_code = departure_iata
        arrival_city_code = arrival_iata

        if request.departure_date_first is None or request.departure_date_last is None or request.arrival_date_first is None or request.arrival_date_last is None:
            raise DateNotAvailableException("No date was passed as argument for departure and/or arrival")

        cur_departure_date = request.departure_date_first.replace(day=1)
        cur_arrival_date = cur_departure_date + datetime.timedelta(weeks=1)

        outbound_urls = []
        return_urls = []

        while cur_departure_date < request.arrival_date_last:
            outbound_url = super().get_api_url(
                'routepricing',
                'v3',
                'searchfares',
                'GetAllFaresByDate',
                departureAirport=departure_city_code,
                arrivalAirport=arrival_city_code,
                currency="EUR",
                departureDateFrom=cur_departure_date,
                departureDateTo=cur_arrival_date
            )

            return_url = super().get_api_url(
                'routepricing',
                'v3',
                'searchfares',
                'GetAllFaresByDate',
                departureAirport=arrival_city_code,
                arrivalAirport=departure_city_code,
                currency="EUR",
                departureDateFrom=cur_departure_date,
                departureDateTo=cur_arrival_date
            )

            outbound_urls.append(outbound_url)
            return_urls.append(return_url)

            cur_departure_date = cur_departure_date + datetime.timedelta(weeks=1)
            cur_arrival_date = cur_arrival_date + datetime.timedelta(weeks=1)

        fares_outbound = []
        fares_return = []

        for url in outbound_urls:
            r = requests.get(url, headers=headers)
            try:
                fares_outbound.extend(r.json())
            except Exception as e:
                print(r.text)
                print(e)
                print()

        for url in return_urls:
            r = requests.get(url, headers=headers)
            try:
                fares_return.extend(r.json())
            except Exception as e:
                print(r.text)
                print(e)
                print()

        try:
            outbound_flights = pd.json_normalize(fares_outbound, max_level=4)
            return_flights = pd.json_normalize(fares_return, max_level=4)
            if not outbound_flights.empty:
                try:

                    outbound_flights = outbound_flights.drop(
                        columns=['flightNumber', 'departureAirport', 'arrivalAirport', 'arrivalCountry', 'returnPrice', 'serviceError'])
                    outbound_flights = outbound_flights.rename(columns={"outboundPrice": 'price', "departureDateTime": 'departureDate', "arrivalDateTime": 'arrivalDate'})

                    outbound_flights['departureStation'] = departure_iata
                    outbound_flights['arrivalStation'] = arrival_iata

                    outbound_flights['departureDate'] = pd.to_datetime(outbound_flights['departureDate'], utc=True)
                    outbound_flights['arrivalDate'] = pd.to_datetime(outbound_flights['arrivalDate'], utc=True)

                    outbound_flights['company'] = self.company_name
                    outbound_flights['currencyCode'] = "EUR"

                    outbound_flights = super().add_country_codes(outbound_flights)

                except Exception as e:
                    outbound_flights = pd.DataFrame()
                    print(e)
                    print()

            if not return_flights.empty:
                try:

                    return_flights = return_flights.drop(
                        columns=['flightNumber', 'departureAirport', 'arrivalAirport', 'arrivalCountry', 'returnPrice',
                                 'serviceError'])
                    return_flights = return_flights.rename(
                        columns={"outboundPrice": 'price', "departureDateTime": 'departureDate',
                                 "arrivalDateTime": 'arrivalDate'})

                    return_flights['departureStation'] = arrival_iata
                    return_flights['arrivalStation'] = departure_iata

                    return_flights['departureDate'] = pd.to_datetime(return_flights['departureDate'], utc=True)
                    return_flights['arrivalDate'] = pd.to_datetime(return_flights['arrivalDate'], utc=True)

                    return_flights['company'] = self.company_name
                    return_flights['currencyCode'] = "EUR"

                    return_flights = super().add_country_codes(return_flights)

                except Exception as e:
                    return_flights = pd.DataFrame()
                    print(e)
                    print()

            return Flight(outbound_flights, return_flights)

        except Exception as e:
            # print(re.text)
            print(traceback.format_exc())
            print(e)
            return Flight.empty_flight()

    def get_possible_flights(self, request: Request) -> list:
        """
        Gets the possible flight times and their prices according to request argument
        """

        # TODO: called method be dependent on if radius or country of departure is chosen

        departure_airports_df = request.get_requested_departure_airports_df()
        connections_df = self.airports[self.airports['iata'].isin(departure_airports_df['iata'])].reset_index(drop=True)

        results = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=super().MAX_WORKERS) as executor:
            threads = []
            for idx, connection_row in connections_df.iterrows():

                connection = connection_row['connection']

                if request.arrival_city and not Airport.airports_in_radius(connection, request.arrival_city,
                                                                           request.airport_radius):
                    continue

                threads.append(
                    executor.submit(self.get_possible_flight, connection, connection_row['iata'], request))

            for idx, future in enumerate(concurrent.futures.as_completed(threads)):
                result = future.result()
                results.append(result)

        return results