import pandas as pd



class Flight:
    '''
    Organizes the flight dataframes
    '''

    return_flights: pd.DataFrame
    outbound_flights: pd.DataFrame

    def __init__(self, oubound_flights:pd.DataFrame, inbound_flights:pd.DataFrame) -> None:
        self.return_flights = inbound_flights
        self.outbound_flights = oubound_flights