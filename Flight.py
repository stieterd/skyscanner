import pandas as pd


class Flight:
    """
    Organizes the flight dataframes
    """

    return_flights: pd.DataFrame
    outbound_flights: pd.DataFrame

    def __init__(self, outbound_flights: pd.DataFrame, inbound_flights: pd.DataFrame) -> None:
        self.return_flights = inbound_flights.reset_index(drop=True)
        self.outbound_flights = outbound_flights.reset_index(drop=True)

    def __add__(self, other: 'Flight'):
        if isinstance(other, Flight):
            outbound_flights = pd.concat([self.outbound_flights, other.outbound_flights]).reset_index(drop=True)
            return_flights = pd.concat([self.return_flights, other.return_flights]).reset_index(drop=True)
            return Flight(outbound_flights, return_flights)
        elif isinstance(other, int):
            return self
        return NotImplemented

    @classmethod
    def empty_flight(cls):
        return cls(pd.DataFrame(), pd.DataFrame())
