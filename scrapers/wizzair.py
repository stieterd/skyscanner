import requests
import json
import datetime
from scrapers.BaseScraper import BaseScraper
from Request import Request
from Flight import Flight
from Exceptions import CityNotFoundException, CountryNotFoundException, TimeNotAvailableException, \
    DateNotAvailableException, WizzairApiVersionNotFoundException
from Airport import Airport

import pandas as pd
import asyncio
import concurrent.futures
from dateutil.relativedelta import relativedelta
import re


class WizzAir(BaseScraper):
    base_url = "https://be.wizzair.com"
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

    ticket_url = "https://wizzair.com/{language}-{country}#/booking/select-flight/{departure_iata}/{arrival_iata}/{date}/null/1/0/0/null"

    def __init__(self) -> None:
        """
        Initializes the Wizzair object and its super BaseScraper

        Variable "connections" represents all the available destinations for wizzair airplanes
        Variable "cities" represents all cities that can be traveled to with wizzair
        Variable "countries" represents all countries that can be traveled to with wizzair
        """

        self.api_url = f"https://be.wizzair.com/{self.detect_api_version()}/Api"
        super().__init__(self.base_url, self.headers, api_url=self.api_url)

        cities = self._get_city_codes()
        self.airports = pd.json_normalize(cities, record_path=None)

    def detect_api_version(self) -> str:
        proxy = super().get_proxy()
        r = requests.get("https://wizzair.com/buildnumber", proxies=proxy, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0"})
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
        proxy = super().get_proxy()
        r = requests.get(url, proxies=proxy, headers=self.headers)

        return r.json()['countries']

    def _get_city_codes(self):
        """
        Gets all the important data for all available wizzair cities:

        iata, longitude, currencyCode, latitude, shortName, countryName, countryCode,
        connections, aliases, isExcludedFromGeoLocation, rank, categories, isFakeStation
        """
        url = super().get_api_url('asset', 'map', languageCode='en-gb')
        proxy = super().get_proxy()
        r = requests.get(url, proxies=proxy, headers=self.headers)
        return r.json()['cities']

    def get_possible_flight(self, arrival_iata: str, departure_iata: str, request: Request) -> Flight:
        """
        Gets possible flights from the departure location through the connection that is given for all available dates
        """

        departure_city_code = departure_iata
        arrival_city_code = arrival_iata

        if request.departure_date_first is None or request.departure_date_last is None or request.arrival_date_first is None or request.arrival_date_last is None:
            raise DateNotAvailableException("No date was passed as argument for departure and/or arrival")

        cur_departure_date1, cur_departure_date2 = super().find_first_and_last_day(request.departure_date_first)
        if cur_departure_date1 <= datetime.datetime.now().date():
            cur_departure_date1 = datetime.datetime.now().date()

        cur_arrival_date1, cur_arrival_date2 = super().find_first_and_last_day(request.arrival_date_first)
        if cur_arrival_date1 <= datetime.datetime.now().date():
            cur_arrival_date1 = datetime.datetime.now().date()
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
            cur_departure_date1, cur_departure_date2 = super().find_first_and_last_day(
                cur_departure_date1 + relativedelta(months=1))
            cur_arrival_date1, cur_arrival_date2 = super().find_first_and_last_day(
                cur_arrival_date1 + relativedelta(months=1))

        fares_outbound = []
        fares_return = []
        for pl in payloads:
            url = super().get_api_url('search', 'timetable')
            proxy = super().get_proxy()
            r = requests.post(url, proxies=proxy, headers=self.headers, json=pl)
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

            if not outbound_flights.empty:
                try:
                    outbound_flights = outbound_flights.explode('departureDates')
                    outbound_flights = outbound_flights[outbound_flights['priceType'] != "checkPrice"].reset_index(drop=True)
                    outbound_flights = outbound_flights.drop(
                        columns=['hasMacFlight', 'originalPrice.amount', 'originalPrice.currencyCode', 'departureDate',
                                 'priceType'])
                    outbound_flights = outbound_flights.rename(
                        columns={'price.amount': 'price', 'price.currencyCode': 'currencyCode',
                                 'departureDates': 'departureDate'})
                    outbound_flights['company'] = self.company_name
                    outbound_flights['departureDate'] = pd.to_datetime(outbound_flights['departureDate'], utc=True)
                    outbound_flights['arrivalDate'] = outbound_flights['departureDate'] + datetime.timedelta(hours=3)
                    outbound_flights = super().add_country_codes(outbound_flights)

                    ticket_url = f"https://wizzair.com/{super().LANGUAGE}-{super().COUNTRY}/#/booking/select-flight/{departure_iata}/{arrival_iata}/"
                    outbound_flights['ticketUrl'] = ticket_url + outbound_flights['departureDate'].dt.strftime('%Y-%m-%d').astype(str) + "/null/1/0/0/null"

                    # {date}/null/1/0/0/null"


                except Exception as e:
                    outbound_flights = pd.DataFrame()
                    print(e)
                    print()

            if not return_flights.empty:
                try:
                    return_flights = return_flights.explode('departureDates')
                    return_flights = return_flights[return_flights['priceType'] != "checkPrice"].reset_index(drop=True)

                    return_flights = return_flights.drop(
                        columns=['hasMacFlight', 'originalPrice.amount', 'originalPrice.currencyCode', 'departureDate',
                                 'priceType'])
                    return_flights = return_flights.rename(
                        columns={'price.amount': 'price', 'price.currencyCode': 'currencyCode',
                                 'departureDates': 'departureDate'})

                    return_flights['company'] = self.company_name
                    return_flights['departureDate'] = pd.to_datetime(return_flights['departureDate'], utc=True)

                    return_flights['arrivalDate'] = return_flights['departureDate'] + datetime.timedelta(hours=3)
                    return_flights = super().add_country_codes(return_flights)

                    ticket_url = f"https://wizzair.com/{super().LANGUAGE}-{super().COUNTRY}/#/booking/select-flight/{arrival_iata}/{departure_iata}/"
                    return_flights['ticketUrl'] = ticket_url + return_flights['departureDate'].dt.strftime('%Y-%m-%d').astype(str) + "/null/1/0/0/null"

                except Exception as e:
                    return_flights = pd.DataFrame()
                    print(e)
                    print()

            outbound_flights.drop_duplicates().reset_index(drop=True)
            return_flights.drop_duplicates().reset_index(drop=True)

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
        connections_df = self.airports[self.airports['iata'].isin(departure_airports_df['iata'])].reset_index(drop=True)

        results = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=super().MAX_WORKERS) as executor:
            threads = []
            for idx, connection_row in connections_df.iterrows():
                connections = [connection['iata'] if 'ROM' not in connection['iata'] else 'FCO' for connection in
                               connection_row['connections']]

                if request.arrival_city:
                    connections = [connection for connection in filter(
                        lambda x: Airport.airports_in_radius(x, request.arrival_city, request.airport_radius),
                        connections)]

                for connection in connections:
                    threads.append(
                        executor.submit(self.get_possible_flight, connection, connection_row['iata'], request))

            for idx, future in enumerate(concurrent.futures.as_completed(threads)):
                result = future.result()
                results.append(result)

        return results
