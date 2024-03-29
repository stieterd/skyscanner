import pandas
import requests
import time
from Flight import Flight
import datetime


class FlightRadar:
    LIMIT = 100

    def __init__(self):
        self.headers = {
            'Host': 'api.flightradar24.com',
            'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0"
        }

    def get_route_data(self, departure_iata: str):
        i = 0
        all_departures_df = pandas.DataFrame()
        all_arrivals_df = pandas.DataFrame()
        tod = datetime.datetime.now()

        for i in range(-1, 2):
            if i == 0:
                continue

            # cur_date = tod - datetime.timedelta(days=i)

            departure_url = ''.join((f"https://api.flightradar24.com/common/v1/airport.json?",
                                     f"code={departure_iata}",
                                     f"&plugin[]=",
                                     f"&plugin-setting[schedule][mode]={'departures'}",
                                     f"&plugin-setting[schedule][timestamp]={int(tod.timestamp())}"
                                     f"&page={i}"
                                     f"&limit={self.LIMIT}"
                                     f"&fleet=&"
                                     f"token="
                                     ))

            arrival_url = ''.join((f"https://api.flightradar24.com/common/v1/airport.json?",
                                   f"code={departure_iata}",
                                   f"&plugin[]=",
                                   f"&plugin-setting[schedule][mode]={'arrivals'}",
                                   f"&plugin-setting[schedule][timestamp]={int(tod.timestamp())}"
                                   f"&page={i}"
                                   f"&limit={self.LIMIT}"
                                   f"&fleet=&"
                                   f"token="
                                   ))

            re_departure = requests.get(departure_url, headers=self.headers)
            re_arrival = requests.get(arrival_url, headers=self.headers)

            def flatten_dict(d, parent_key='', sep='.'):
                items = []
                for k, v in d.items():
                    new_key = parent_key + sep + k if parent_key else k
                    if isinstance(v, dict):
                        items.extend(flatten_dict(v, new_key, sep=sep).items())
                    else:
                        items.append((new_key, v))
                return dict(items)

            # Flatten JSON data and convert it to DataFrame
            flat_departure = [flatten_dict(item) for item in re_departure.json()['result']['response']['airport']['pluginData']['schedule']['departures']['data']]
            flat_arrival = [flatten_dict(item) for item in re_departure.json()['result']['response']['airport']['pluginData']['schedule']['departures']['data']]
            departure_df = pandas.DataFrame(flat_departure)
            arrival_df = pandas.DataFrame(flat_arrival)

            all_departures_df = pandas.concat([departure_df, all_departures_df]).reset_index(drop=True).dropna(
                axis=1, how='all').reset_index(drop=True)
            all_arrivals_df = pandas.concat([arrival_df, all_arrivals_df]).reset_index(drop=True).dropna(
                axis=1, how='all').reset_index(drop=True)

        all_arrivals_df.drop_duplicates(subset=None, keep="first", inplace=True)
        all_departures_df.drop_duplicates(subset=None, keep="first", inplace=True)

        all_arrivals_df = all_arrivals_df[~all_arrivals_df["flight.time.real.arrival"].isnull()].reset_index(drop=True)
        all_departures_df = all_departures_df[~all_departures_df["flight.time.real.arrival"].isnull()].reset_index(drop=True)

        column_mapping = {
            'flight.airport.origin.code.iata': 'departureStation',
            'flight.airport.destination.code.iata': 'arrivalStation',
            'flight.time.scheduled.departure': 'expectedDepartureDate',
            'flight.time.real.departure': 'realDepartureDate',
            'flight.time.scheduled.arrival': 'expectedArrivalDate',
            'flight.time.real.arrival': 'realArrivalDate',
            'flight.airline.name': 'company',
            'flight.identification.number.default': 'flightNumber',
            'flight.airport.destination.info.terminal': 'terminal',
            'flight.airport.destination.info.gate': 'gate',
            'flight.aircraft.availability.available': 'availableSeats',
            'flight.aircraft.model.code': 'carrier'
        }

        # Rename the columns
        all_arrivals_df = all_arrivals_df.rename(columns=column_mapping)

        # Select only the desired columns
        columns_to_drop = [col for col in all_arrivals_df.columns if col not in column_mapping.values()]

        # Drop the columns
        all_arrivals_df = all_arrivals_df.drop(columns=columns_to_drop)

        print(all_arrivals_df)
        print(all_departures_df)
