import datetime

import requests
import pandas as pd

from Airport import Airport
from Exceptions import DateNotAvailableException
from Flight import Flight
from Request import Request
from scrapers.BaseScraper import BaseScraper
import concurrent.futures
from dateutil.relativedelta import relativedelta

class Transavia(BaseScraper):

    # https://www.transavia.com/en-EU/api/SearchPanelDestinations/?departureAirport=EIN&selfConnect=true
    base_url = "https://www.transavia.com/en-EU"
    api_url = "https://www.transavia.com/en-EU/api"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0"
    }

    company_name = 'transavia'

    def __init__(self):

        self.airports = pd.json_normalize(self._get_city_codes()['Airports'], max_level=1)

        super().__init__(self.base_url, self.headers, api_url=self.api_url)

    def _get_city_codes(self):
        url = super().get_api_url("airports", selfConnect="true")
        r = requests.get(url, headers=self.headers)
        return r.json()

    def get_possible_flight(self, arrival_iata: str, departure_iata: str, request: Request) -> Flight:
        """
        Gets possible flights from the departure location through the connection that is given for all available dates
        """

        departure_city_code = departure_iata
        arrival_city_code = arrival_iata

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
            cur_departure_date1, cur_departure_date2 = super().find_first_and_last_day(
                cur_departure_date1 + relativedelta(months=1))
            cur_arrival_date1, cur_arrival_date2 = super().find_first_and_last_day(
                cur_arrival_date1 + relativedelta(months=1))

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

            outbound_flights = outbound_flights.drop(
                columns=['hasMacFlight', 'originalPrice.amount', 'originalPrice.currencyCode', 'departureDate',
                         'priceType'])
            return_flights = return_flights.drop(
                columns=['hasMacFlight', 'originalPrice.amount', 'originalPrice.currencyCode', 'departureDate',
                         'priceType'])

            outbound_flights = outbound_flights.rename(
                columns={'price.amount': 'price', 'price.currencyCode': 'currencyCode',
                         'departureDates': 'departureDate'})
            return_flights = return_flights.rename(
                columns={'price.amount': 'price', 'price.currencyCode': 'currencyCode',
                         'departureDates': 'departureDate'})

            outbound_flights['company'] = self.company_name
            return_flights['company'] = self.company_name

            outbound_flights['departureDate'] = pd.to_datetime(outbound_flights['departureDate'], utc=True)
            return_flights['departureDate'] = pd.to_datetime(return_flights['departureDate'], utc=True)

            outbound_flights['arrivalDate'] = outbound_flights['departureDate'] + datetime.timedelta(hours=3)
            return_flights['arrivalDate'] = return_flights['departureDate'] + datetime.timedelta(hours=3)

            outbound_flights = super().add_country_codes(outbound_flights)
            return_flights = super().add_country_codes(return_flights)

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

        with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
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
