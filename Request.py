import datetime
import json
import copy

import pandas

from Airport import Airport


class Request:
    """
    Stores all the needed data for an api request
    """

    adult_count: int
    child_count: int
    infant_count: int

    departure_country: str
    departure_city: str
    departure_locations: list[str]

    arrival_country: str
    arrival_city: str
    arrival_locations: list[str]

    departure_date_first: datetime.date
    departure_date_last: datetime.date

    arrival_date_first: datetime.date
    arrival_date_last: datetime.date

    days_stay: int

    departure_airport_radius: float  # in kilometers
    arrival_airport_radius: float  # in kilometers

    max_travel_time: datetime.time
    earliest_travel_time: datetime.time
    latest_travel_time: datetime.time

    available_departure_weekdays: tuple[int, ...]
    available_arrival_weekdays: tuple[int, ...]

    max_price_per_flight: int

    def __init__(self,
                 adult_count: int = 1,
                 child_count: int = 0,
                 infant_count: int = 0,

                 departure_country: str = None,
                 departure_city: str = None,
                 arrival_country: str = None,
                 arrival_city: str = None,

                 departure_date_first: datetime.date = None,
                 departure_date_last: datetime.date = None,
                 arrival_date_first: datetime.date = None,
                 arrival_date_last: datetime.date = None,

                 min_days_stay: int = 0,
                 max_days_stay: int = 99999999,
                 available_departure_weekdays: tuple[int, ...] = (0, 1, 2, 3, 4, 5, 6),
                 available_arrival_weekdays: tuple[int, ...] = (0, 1, 2, 3, 4, 5, 6),

                 departure_airport_radius: float = 0,
                 arrival_airport_radius: float = 0,

                 max_travel_time: datetime.time = None,  # TODO: add this one
                 earliest_travel_time: datetime.time = None,  # TODO: add this one
                 latest_travel_time: datetime.time = None,  # TODO: add this one
                 max_price_per_flight: int = 99999999,

                 ) -> None:
        """
        Initializes the class.
        """

        self.adult_count = adult_count
        self.child_count = child_count
        self.infant_count = infant_count

        self.departure_country = departure_country
        self.departure_city = departure_city
        self.departure_iata = None
        self.departure_icao = None

        self.arrival_country = arrival_country
        self.arrival_city = arrival_city
        self.arrival_iata = None
        self.arrival_icao = None

        self.departure_date_first = departure_date_first
        self.departure_date_last = departure_date_last
        self.arrival_date_first = arrival_date_first
        self.arrival_date_last = arrival_date_last

        if min_days_stay is None:
            self.min_days_stay = 0
        else:
            self.min_days_stay = min_days_stay

        if max_days_stay is None:
            self.max_days_stay = 99999999
        else:
            self.max_days_stay = max_days_stay

        self.available_departure_weekdays = available_departure_weekdays
        self.available_arrival_weekdays = available_arrival_weekdays

        if departure_airport_radius is None:
            self.departure_airport_radius = 0
        else:
            self.departure_airport_radius = departure_airport_radius

        if arrival_airport_radius is None:
            self.arrival_airport_radius = 0
        else:
            self.arrival_airport_radius = arrival_airport_radius

        self.max_travel_time = max_travel_time
        self.earliest_travel_time = earliest_travel_time
        self.latest_travel_time = latest_travel_time

        if max_price_per_flight is None:
            self.max_price_per_flight = 99999999
        else:
            self.max_price_per_flight = max_price_per_flight

    def __str__(self):
        """
        Returns a json string of the object
        """
        return json.dumps(vars(self), default=self._date_json_encoder)

    def get_requested_departure_airports_df(self) -> pandas.DataFrame:
        """
        @return: Dataframe containing available departure airports
        """

        if self.departure_city != None:

            iata = self.departure_city
            departure_airports_df = Airport.get_airports_by_iata(iata)

            if self.departure_airport_radius > 0:
                lat = departure_airports_df.at[0, 'lat']
                long = departure_airports_df.at[0, 'lon']
                return Airport.get_airports_by_radius(long, lat, self.departure_airport_radius)

            else:
                return departure_airports_df

        if self.departure_country != None:
            departure_airports_df = Airport.get_airports_by_country(self.departure_country)
            return departure_airports_df

        else:
            return Airport.all_airports_df

    def get_requested_arrival_airports_df(self) -> pandas.DataFrame:
        """
        @return: Dataframe containing available departure airports
        """

        if self.arrival_city != None:

            iata = self.arrival_city
            departure_airports_df = Airport.get_airports_by_iata(iata)

            if self.departure_airport_radius > 0:
                lat = departure_airports_df.at[0, 'lat']
                long = departure_airports_df.at[0, 'lon']
                return Airport.get_airports_by_radius(long, lat, self.departure_airport_radius)

            else:
                return departure_airports_df

        if self.arrival_country != None:
            departure_airports_df = Airport.get_airports_by_country(self.arrival_country)
            return departure_airports_df

        else:
            return Airport.all_airports_df

    def get_requested_arrival_airports_df(self) -> pandas.DataFrame:
        """
        @return: Dataframe containing available departure airports
        """

        if self.arrival_city != None:

            iata = self.arrival_city
            arrival_airports_df = Airport.get_airports_by_iata(iata)

            if self.arrival_airport_radius > 0:
                lat = arrival_airports_df.at[0, 'lat']
                long = arrival_airports_df.at[0, 'lon']
                return Airport.get_airports_by_radius(long, lat, self.departure_airport_radius)

            else:
                return arrival_airports_df

        if self.arrival_country != None:
            arrival_airports_df = Airport.get_airports_by_country(self.arrival_country)
            return arrival_airports_df

    def split_up_for_layovers(self) -> ["Request", "Request"]:
        kwargs = vars(self)
        del kwargs['departure_locations']
        del kwargs['arrival_locations']

        request1 = Request(**kwargs)
        request2 = Request(**kwargs)

        request1.arrival_country = None
        request2.departure_country = request2.arrival_country
        request2.arrival_country = None

        return request1, request2

    @staticmethod
    def _date_json_encoder(value: datetime.datetime) -> str:
        if isinstance(value, (datetime.date, datetime.datetime)):
            return value.isoformat()


if __name__ == "__main__":
    r = Request(airport_radius=20, adult_count=20)
    print(r)
    print(r.departure_city)
