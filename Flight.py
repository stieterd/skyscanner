import pandas as pd
from Request import Request
from scrapers.BaseScraper import BaseScraper

class Flight:
    """
    Organizes the flight dataframes
    """

    return_flights: pd.DataFrame
    outbound_flights: pd.DataFrame

    def __init__(self, outbound_flights: pd.DataFrame, inbound_flights: pd.DataFrame) -> None:

        if len(outbound_flights) > 0 and len(inbound_flights) > 0:

            outbound_flights['departureDate'] = pd.to_datetime(outbound_flights['departureDate'])
            inbound_flights['departureDate'] = pd.to_datetime(inbound_flights['departureDate'])

            outbound_flights['arrivalDate'] = pd.to_datetime(outbound_flights['arrivalDate'])
            inbound_flights['arrivalDate'] = pd.to_datetime(inbound_flights['arrivalDate'])

            outbound_flights['departureDay'] = outbound_flights['departureDate'].dt.date
            inbound_flights['departureDay'] = inbound_flights['departureDate'].dt.date

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
        cheap_outbound_flights['weekday'] = cheap_outbound_flights['departureDate'].dt.day_name()
        cheap_return_flights['weekday'] = cheap_return_flights['departureDate'].dt.day_name()

        # filter flights that don't have a return flight
        cheap_outbound_flights = cheap_outbound_flights[
            cheap_outbound_flights['arrivalStation'].isin(cheap_return_flights['departureStation'])]
        cheap_return_flights = cheap_return_flights[
            cheap_return_flights['departureStation'].isin(cheap_outbound_flights['arrivalStation'])]

        return Flight(cheap_outbound_flights, cheap_return_flights)

    def get_possible_return_flights(self, idx, request: Request):
        self.return_flights['travel_days'] = (
                self.return_flights['departureDate'] - self.outbound_flights['departureDate'].values[idx]).dt.days
        returnfl = self.return_flights[
            (self.return_flights['departureStation'] == self.outbound_flights['arrivalStation'].values[idx]) &
            (self.return_flights['departureDate'] > pd.to_datetime(
                self.outbound_flights['departureDate'].values[idx]))
            ]
        returnfl = returnfl[returnfl['travel_days'] >= 0].reset_index(
            drop=True)

        if request.days_stay:
            returnfl = returnfl[returnfl['travel_days'] >= request.days_stay].reset_index(
            drop=True)

        return returnfl

    @classmethod
    def empty_flight(cls):
        return cls(pd.DataFrame(), pd.DataFrame())
