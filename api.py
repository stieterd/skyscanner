import os

import pandas
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

import threading
import time

import uvicorn

app = FastAPI()

outbound_df = pandas.DataFrame()
return_df = pandas.DataFrame()

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


class BackgroundTasks(threading.Thread):
    def run(self, *args, **kwargs):
        global outbound_df
        global return_df
        while True:
            outputs = os.listdir("output_data")
            outbound_files = [file for file in outputs if file.startswith('outbound')]
            return_files = [file for file in outputs if file.startswith('return')]
            # Find the latest outbound file
            latest_outbound = max(outbound_files, key=lambda x: datetime.datetime.strptime(x.split('_')[1], '%Y-%m-%d-%H.csv'))
            # Find the latest return file
            latest_return = max(return_files, key=lambda x: datetime.datetime.strptime(x.split('_')[1], '%Y-%m-%d-%H.csv'))
            outbound_df = pandas.read_csv(latest_outbound)
            return_df = pandas.read_csv(latest_return)
            time.sleep(60)


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

if __name__ == "__main__":
    t = BackgroundTasks()
    t.start()
    uvicorn.run(app, host="0.0.0.0", port=8000)
