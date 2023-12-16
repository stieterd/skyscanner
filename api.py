from fastapi import FastAPI
import pandas as pd
import json
from pydantic import BaseModel, field_validator
import datetime
from Request import Request

from scrapers.wizzair import WizzAir
from scrapers.ryanair import RyanAir
from scrapers.easyjet import EasyJet

from Request import Request
import datetime
from Flight import Flight

app = FastAPI()

class FlightRequest(BaseModel):
    departure_city: str
    departure_date_first: datetime.date
    departure_date_last: datetime.date
    arrival_date_first: datetime.date
    arrival_date_last: datetime.date
    min_days_stay: int | None = None
    max_days_stay: int | None = None
    airport_radius: int | None = None
    max_price_per_flight: int | None = 99999999

    @field_validator("departure_date_first", "departure_date_last", "arrival_date_first", "arrival_date_last")
    def string_to_date(cls, v: object) -> object:
        if isinstance(v, str):
            return datetime.datetime.strptime(v, cls.date_format).date()
        return v

@app.post("/outbound/")
async def get_outbound_flights(flight: FlightRequest):
    request = Request(**vars(flight))
    wa = WizzAir()
    result_flight_wa = sum(wa.get_possible_flights(request), start=Flight.empty_flight())

    ra = RyanAir()
    possible_flights_ra = ra.get_possible_flights(request)
    result_flight_ra = sum(possible_flights_ra, start=Flight.empty_flight())

    ### direct flights ###

    flight = result_flight_wa + result_flight_ra
    filtered_flight = flight.filter_flights(request)
    filtered_flight.outbound_flights['n_returnflights'] = filtered_flight.outbound_flights.apply(
        lambda row: len(filtered_flight.get_possible_return_flights(row.name, request)), axis=1)
    filtered_flight.outbound_flights = filtered_flight.outbound_flights[
        filtered_flight.outbound_flights['n_returnflights'] > 0].reset_index(drop=True)

    filtered_flight.outbound_flights['total_cost'] = filtered_flight.outbound_flights.apply(
        lambda row: row['price'] + (filtered_flight.get_possible_return_flights(row.name, request))['price'].values[0],
        axis=1)


    return filtered_flight.outbound_flights.to_json(orient="records")