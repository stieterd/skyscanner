import datetime

import pandas as pd

from Airport import Airport
from Request import Request
from scrapers.BaseScraper import BaseScraper


class Flight:
    """
    Organizes the flight dataframes
    """

    return_flights: pd.DataFrame
    outbound_flights: pd.DataFrame

    airport = Airport()

    def __init__(self, outbound_flights: pd.DataFrame, inbound_flights: pd.DataFrame) -> None:

        if not outbound_flights.empty:
            outbound_flights['departureDate'] = pd.to_datetime(outbound_flights['departureDate'])
            outbound_flights['arrivalDate'] = pd.to_datetime(outbound_flights['arrivalDate'])
            outbound_flights['departureDay'] = outbound_flights['departureDate'].dt.date
            outbound_flights['departureDay'] = pd.to_datetime(outbound_flights['departureDay'])

            outbound_flights['scrapeDate'] = datetime.datetime.now()
            outbound_flights['scrapeDate'] = pd.to_datetime(outbound_flights['scrapeDate'])

            outbound_flights['hash'] = outbound_flights.apply(lambda x: hash(
                tuple(x[['arrivalDate', 'departureDate', 'departureStation', 'arrivalStation', 'company']])), axis=1)

        if not inbound_flights.empty:
            inbound_flights['departureDate'] = pd.to_datetime(inbound_flights['departureDate'])
            inbound_flights['arrivalDate'] = pd.to_datetime(inbound_flights['arrivalDate'])
            inbound_flights['departureDay'] = inbound_flights['departureDate'].dt.date
            inbound_flights['departureDay'] = pd.to_datetime(inbound_flights['departureDay'])

            inbound_flights['scrapeDate'] = datetime.datetime.now()
            inbound_flights['scrapeDate'] = pd.to_datetime(inbound_flights['scrapeDate'])

            inbound_flights['hash'] = inbound_flights.apply(lambda x: hash(
                tuple(x[['arrivalDate', 'departureDate', 'departureStation', 'arrivalStation', 'company']])), axis=1)

        self.return_flights = inbound_flights.reset_index(drop=True)
        self.outbound_flights = outbound_flights.reset_index(drop=True)

    def __add__(self, other: 'Flight'):
        if isinstance(other, Flight):
            outbound_flights = pd.concat([self.outbound_flights, other.outbound_flights]).reset_index(drop=True).dropna(
                axis=1, how='all')
            return_flights = pd.concat([self.return_flights, other.return_flights]).reset_index(drop=True).dropna(
                axis=1, how='all')
            return Flight(outbound_flights, return_flights)
        elif isinstance(other, int):
            return self
        return NotImplemented

    def filter_flights(self, request: Request) -> "Flight":

        if len(self.outbound_flights) == 0 or len(self.return_flights) == 0:
            print("Nothing was found")
            return Flight.empty_flight()

        outbound_flights = self.outbound_flights
        return_flights = self.return_flights

        # TODO: fix this shit!!! WE NEED TO BE ABLE TO SCAN FOR A RADIUS OF ARRIVALSTATIONS
        if request.arrival_city is not None:
            outbound_flights = outbound_flights[outbound_flights['arrivalStation'].isin(
                request.get_requested_arrival_airports_df()['iata'])].reset_index(drop=True)
            return_flights = return_flights[return_flights['departureStation'].isin(
                request.get_requested_arrival_airports_df()['iata'])].reset_index(drop=True)

        if request.departure_city is not None:
            outbound_flights = outbound_flights[outbound_flights['departureStation'].isin(
                request.get_requested_departure_airports_df()['iata'])].reset_index(drop=True)
            return_flights = return_flights[return_flights['arrivalStation'].isin(
                request.get_requested_departure_airports_df()['iata'])].reset_index(drop=True)

        # sort by price
        outbound_flights.sort_values('price', inplace=True)
        return_flights.sort_values('price', inplace=True)

        # filter out price
        cheap_outbound_flights = outbound_flights[
            outbound_flights['price'] < request.max_price_per_flight].drop_duplicates().reset_index(drop=True)
        cheap_return_flights = return_flights[
            return_flights['price'] < request.max_price_per_flight].drop_duplicates().reset_index(drop=True)

        # get correct dates
        cheap_outbound_flights = cheap_outbound_flights[
            (request.departure_date_first <= cheap_outbound_flights['departureDate'].dt.date) & (
                    cheap_outbound_flights['departureDate'].dt.date <= request.departure_date_last)].reset_index(
            drop=True)
        cheap_return_flights = cheap_return_flights[
            (request.arrival_date_first <= cheap_return_flights['departureDate'].dt.date) & (
                    cheap_return_flights['departureDate'].dt.date <= request.arrival_date_last)].reset_index(
            drop=True)

        # add day of week as columns
        cheap_outbound_flights = cheap_outbound_flights[
            cheap_outbound_flights['departureDate'].dt.dayofweek.isin(request.available_departure_weekdays)]
        cheap_outbound_flights['weekday'] = cheap_outbound_flights['departureDate'].dt.day_name()
        cheap_return_flights['weekday'] = cheap_return_flights['departureDate'].dt.day_name()

        return Flight(cheap_outbound_flights, cheap_return_flights)

    def get_possible_return_flights(self, idx, request: Request):

        iata_airport = Airport.get_airports_by_iata(self.outbound_flights['arrivalStation'].iloc[idx])
        airports_radius_df = Airport.get_airports_by_radius(
            iata_airport['lon'].iloc[0],
            iata_airport['lat'].iloc[0],
            request.airport_radius
        )

        self.return_flights['travel_days'] = (
                self.return_flights['departureDay'] - self.outbound_flights['departureDay'].iloc[idx]).dt.days

        returnfl = self.return_flights[
            (self.return_flights['departureStation'].isin(airports_radius_df['iata'])) &
            (self.return_flights['departureDate'] > pd.to_datetime(self.outbound_flights['departureDate'].iloc[idx]))
            ]

        returnfl = returnfl[returnfl['departureDate'].dt.dayofweek.isin(request.available_arrival_weekdays)]

        returnfl = returnfl[returnfl['travel_days'] >= 0].reset_index(
            drop=True)

        if request.min_days_stay:
            returnfl = returnfl[returnfl['travel_days'] >= request.min_days_stay].reset_index(
                drop=True)

        if request.max_days_stay:
            returnfl = returnfl[returnfl['travel_days'] < request.max_days_stay].reset_index(
                drop=True)

        return returnfl

    def get_possible_return_flights_df(self, request: Request):

        result_df = pd.merge(self.outbound_flights, self.return_flights, left_on='arrivalStation',
                             right_on="departureStation")
        result_df['travel_days'] = (result_df['departureDay_x'] - result_df['departureDay_y']).dt.days
        result_df = result_df[result_df['departureDate_x'] > result_df['departureDate_y']]
        result_df = result_df[
            result_df['departureDate_x'].dt.dayofweek.isin(request.available_departure_weekdays) & result_df[
                'departureDate_y'].dt.dayofweek.isin(request.available_arrival_weekdays)]

        result_df = result_df[(result_df['travel_days'] >= 0) & (result_df['travel_days'] >= request.min_days_stay) & (
                    result_df['travel_days'] < request.max_days_stay)].reset_index(drop=True)
        result_df['total_cost'] = result_df['price_x'] + result_df['price_y']

        result_df.sort_values('total_cost', inplace=True)

        return result_df

    # TODO: I dont know what to do with this so I guess Ill just leave it here
    @staticmethod
    def date_json_encoder(value: datetime.datetime) -> str:
        if isinstance(value, (datetime.date, datetime.datetime)):
            return value.isoformat()

    @classmethod
    def empty_flight(cls):
        return cls(pd.DataFrame(), pd.DataFrame())
