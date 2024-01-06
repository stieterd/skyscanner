import json

from Airport import Airport
from Exceptions import DateNotAvailableException
from Flight import Flight
from Request import Request
from scrapers.BaseScraper import BaseScraper
import requests
import pandas as pd
import concurrent.futures
import traceback

class Volotea(BaseScraper):

    base_url = "https://www.volotea.com/"
    api_url = "https://www.volotea.com/api/v1/"

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0"}

    company_name = 'volotea'

    def __init__(self):

        df_list = []

        for key, value in self._get_city_codes().items():
            culture_data = value.get("Culture", {}).get("en-GB", {})
            markets_data = value.get("Markets", {})

            for market_key, market_value in markets_data.items():
                price_data = market_value.get("Prices", {})
                weather_data = value.get("Weather", {}).get("current", {})

                row_data = {
                    "StationCode": key,
                    "MarketCode": market_key,
                    "Name": culture_data.get("Name", ""),
                    "FullName": culture_data.get("FullName", ""),
                    "MarketPrice": market_value.get("Price", 0.0),
                    "Enabled": value.get("Enabled", False),
                    "Lat": value.get("Lat", 0.0),
                    "Long": value.get("Long", 0.0),
                    "CurrentTemperature": weather_data.get("current", 0),
                    "MinTemperature": weather_data.get("min", 0),
                    "MaxTemperature": weather_data.get("max", 0),
                    "WeatherIcon": weather_data.get("icon", ""),
                    "CreatedDate": pd.to_datetime(value.get("CreatedDate", "")),
                }

                df_list.append(row_data)

            # df_list.append(row_data)

        self.airports = pd.DataFrame(df_list)

        super().__init__(self.base_url, self.headers, self.api_url)

    def _get_city_codes(self):
        url = "https://json.volotea.com/dist/stations/stations.json?v=1"
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

        fares_outbound = []
        fares_return = []

        url = f'https://json.volotea.com/dist/schedule/{departure_city_code}-{arrival_city_code}_schedule.json'

        r = requests.get(url, headers=self.headers)

        try:
            data = json.loads(r.content.decode('utf-8-sig'))
            fares_outbound = data[f'{departure_city_code}-{arrival_city_code}']
        except Exception as e:
            # print(r.text)
            print(e)
            # print(traceback.format_exc())
            print()
            fares_outbound = []

        try:
            data = json.loads(r.content.decode('utf-8-sig'))
            fares_return = data[f'{arrival_city_code}-{departure_city_code}']
        except Exception as e:
            # print(r.text)
            print(e)
            # print(traceback.format_exc())
            print()
            fares_return = []

        if len(fares_return) == 0 and len(fares_outbound) == 0:
            return Flight.empty_flight()

        try:
            # result = re.json()
            # TODO: Include FlightNumber and Terminal and AvailableSeats and OperatingCarrier in a future version
            outbound_flights = pd.json_normalize(fares_outbound, record_path='Prices', meta=['Departure', 'Arrival'], max_level=3)
            return_flights = pd.json_normalize(fares_return, record_path='Prices', meta=['Departure', 'Arrival'], max_level=3)

            if not outbound_flights.empty:
                try:
                    outbound_flights['departureStation'] = departure_iata
                    outbound_flights['arrivalStation'] = arrival_iata

                    outbound_flights = outbound_flights.drop(
                        columns=['FareType', 'PriceWithFee'])
                    outbound_flights = outbound_flights.rename(
                        columns={'Arrival': 'arrivalDate', 'Departure': 'departureDate',
                                 'Price': 'price'})

                    outbound_flights['currencyCode'] = 'EUR'
                    outbound_flights['company'] = self.company_name

                    outbound_flights['departureDate'] = pd.to_datetime(outbound_flights['departureDate'],
                                                                       format='%Y%m%d%H%M', utc=True)
                    outbound_flights['arrivalDate'] = pd.to_datetime(outbound_flights['arrivalDate'], format='%Y%m%d%H%M',
                                                                     utc=True)
                    outbound_flights = super().add_country_codes(outbound_flights)
                except Exception as e:
                    outbound_flights = pd.DataFrame()
                    print(e)
                    print()

            if not return_flights.empty:
                try:
                    return_flights['departureStation'] = arrival_iata
                    return_flights['arrivalStation'] = departure_iata

                    return_flights = return_flights.drop(
                        columns=['FareType', 'PriceWithFee'])
                    return_flights = return_flights.rename(
                        columns={'Arrival': 'arrivalDate', 'Departure': 'departureDate',
                                 'Price': 'price'})

                    return_flights['currencyCode'] = 'EUR'
                    return_flights['company'] = self.company_name

                    return_flights['departureDate'] = pd.to_datetime(return_flights['departureDate'],
                                                                     format='%Y%m%d%H%M', utc=True)
                    return_flights['arrivalDate'] = pd.to_datetime(return_flights['arrivalDate'], format='%Y%m%d%H%M',
                                                                   utc=True)
                    return_flights = super().add_country_codes(return_flights)
                except Exception as e:
                    return_flights = pd.DataFrame()
                    print(e)
                    print()

            return Flight(outbound_flights, return_flights)

        except Exception as e:
            # print(re.text)
            print(e)
            print(traceback.format_exc())
            return Flight.empty_flight()

    def get_possible_flights(self, request: Request) -> list:
        """
        Gets the possible flight times and their prices according to request argument
        """

        # TODO: called method be dependent on if radius or country of departure is chosen

        departure_airports_df = request.get_requested_departure_airports_df()
        connections_df = self.airports[self.airports['StationCode'].isin(departure_airports_df['iata'])].reset_index(drop=True)

        results = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=super().MAX_WORKERS) as executor:
            threads = []
            for idx, connection_row in connections_df.iterrows():

                connection = connection_row['MarketCode']

                if request.arrival_city and not Airport.airports_in_radius(connection, request.arrival_city,
                                                                           request.airport_radius):
                    continue

                threads.append(
                    executor.submit(self.get_possible_flight, connection, connection_row['StationCode'], request))

            for idx, future in enumerate(concurrent.futures.as_completed(threads)):
                result = future.result()
                results.append(result)

        return results

