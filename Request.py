import datetime
import json
import copy


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

    airport_radius: float  # in kilometers

    max_travel_time: datetime.time
    earliest_travel_time: datetime.time
    latest_travel_time: datetime.time

    max_price_per_flight: int

    def __init__(self, adult_count: int = 1, child_count: int = 0, infant_count: int = 0,
                 departure_country: str = None, departure_city: str = None, arrival_country: str = None,
                 arrival_city: str = None,
                 departure_date_first: datetime.date = None, departure_date_last: datetime.date = None,
                 arrival_date_first: datetime.date = None, arrival_date_last: datetime.date = None,
                 min_days_stay: int = None, max_days_stay: int = None, airport_radius: float = 0,
                 max_travel_time: datetime.time = None, earliest_travel_time: datetime.time = None,
                 latest_travel_time: datetime.time = None,
                 max_price_per_flight: int = 99999999
                 ) -> None:
        """
        Initializes the class.
        """
        self.adult_count = adult_count
        self.child_count = child_count
        self.infant_count = infant_count

        self.departure_country = departure_country
        self.departure_city = departure_city
        self.arrival_country = arrival_country
        self.arrival_city = arrival_city

        self.departure_date_first = departure_date_first
        self.departure_date_last = departure_date_last
        self.arrival_date_first = arrival_date_first
        self.arrival_date_last = arrival_date_last

        self.min_days_stay = min_days_stay
        self.max_days_stay = max_days_stay
        self.airport_radius = airport_radius

        self.max_travel_time = max_travel_time
        self.earliest_travel_time = earliest_travel_time
        self.latest_travel_time = latest_travel_time

        self.departure_locations = []
        self.arrival_locations = []

        self.max_price_per_flight = max_price_per_flight

    def __str__(self):
        """
        Returns a json string of the object
        """
        return json.dumps(vars(self), default=self._date_json_encoder)

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
