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

        if not inbound_flights.empty:
            inbound_flights['departureDate'] = pd.to_datetime(inbound_flights['departureDate'])
            inbound_flights['arrivalDate'] = pd.to_datetime(inbound_flights['arrivalDate'])
            inbound_flights['departureDay'] = inbound_flights['departureDate'].dt.date
            inbound_flights['departureDay'] = pd.to_datetime(inbound_flights['departureDay'])

        self.return_flights = inbound_flights.reset_index(drop=True)
        self.outbound_flights = outbound_flights.reset_index(drop=True)

    def __add__(self, other: 'Flight'):
        if isinstance(other, Flight):
            outbound_flights = pd.concat([self.outbound_flights, other.outbound_flights]).reset_index(drop=True).dropna(axis=1, how='all')
            return_flights = pd.concat([self.return_flights, other.return_flights]).reset_index(drop=True).dropna(axis=1, how='all')
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
        cheap_outbound_flights = cheap_outbound_flights[cheap_outbound_flights['departureDate'].dt.dayofweek.isin(request.available_departure_weekdays)]
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

    def get_possible_return_flights1(self, row, request: Request):
        # Airport.arrival_station_radius_lambda(row, request.airport_radius)
        pass

    @classmethod
    def empty_flight(cls):
        return cls(pd.DataFrame(), pd.DataFrame())
