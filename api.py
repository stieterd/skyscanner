import os

from fastapi import HTTPException, Depends

import pandas
from fastapi import FastAPI, Form, Request
import pandas as pd
import json
from pydantic import BaseModel, field_validator
import datetime
from fastapi.responses import HTMLResponse
from scrapers.wizzair import WizzAir
from scrapers.ryanair import RyanAir
from scrapers.easyjet import EasyJet

from Request import Request as RQ
import datetime
from Flight import Flight

import threading
import time

import uvicorn
from fastapi.templating import Jinja2Templates

app = FastAPI()
templates = Jinja2Templates(directory="templates")

flight = Flight.empty_flight()
OUTPUT_DIR = "output_data"


class FlightRequest(BaseModel):
    departure_city: str
    departure_date_first: str
    departure_date_last: str
    arrival_date_first: str
    arrival_date_last: str
    min_days_stay: str
    max_days_stay: str
    airport_radius: str
    max_price_per_flight: str

    @field_validator("departure_date_first", "departure_date_last", "arrival_date_first", "arrival_date_last")
    def string_to_date(cls, v: object) -> object:
        if isinstance(v, str):
            return datetime.datetime.strptime(v, cls.date_format).date()
        return v


class Item(BaseModel):
    departure_city: str | None = None
    departure_date_first: str | None = None
    departure_date_last: str | None = None
    arrival_date_first: str | None = None
    arrival_date_last: str | None = None
    min_days_stay: str | None = None
    max_days_stay: str | None = None
    airport_radius: str | None = None
    max_price_per_flight: str | None = None


class BackgroundTasks(threading.Thread):
    def run(self, *args, **kwargs):
        global flight
        while True:
            outputs = os.listdir(OUTPUT_DIR)
            outbound_files = [file for file in outputs if file.startswith('outbound')]
            return_files = [file for file in outputs if file.startswith('return')]
            # Find the latest outbound file
            latest_outbound = max(outbound_files,
                                  key=lambda x: datetime.datetime.strptime(x.split('_')[1], '%Y-%m-%d-%H.csv'))
            # Find the latest return file
            latest_return = max(return_files,
                                key=lambda x: datetime.datetime.strptime(x.split('_')[1], '%Y-%m-%d-%H.csv'))
            flight = Flight(pandas.read_csv(f"{OUTPUT_DIR}/{latest_outbound}"),
                            pandas.read_csv(f"{OUTPUT_DIR}/{latest_return}"))
            time.sleep(5 * 60)


@app.get("/outbound/", response_class=HTMLResponse)
async def read_form(request: Request):
    # request = Request(
    #     departure_city="EIN",
    #     # arrival_city="ALC",
    #     departure_date_first=datetime.date(2024, 3, 1),
    #     departure_date_last=datetime.date(2024, 3, 6),
    #     arrival_date_first=datetime.date(2024, 3, 1),
    #     arrival_date_last=datetime.date(2024, 3, 6),
    #     # departure_date_first=datetime.date(2024, 3, 1),
    #     # departure_date_last=datetime.date(2024, 3, 30),
    #     # arrival_date_first=datetime.date(2024, 3, 1),
    #     # arrival_date_last=datetime.date(2024, 3, 30),
    #     min_days_stay=2,
    #     airport_radius=100,
    #     # available_departure_weekdays=(4, 5),
    #     # available_arrival_weekdays=(6, 0, 1)
    #     # max_price_per_flight=25
    # )
    # pl = str(request)
    # filtered_flight = flight.filter_flights(request)
    # return_flights_data = filtered_flight.outbound_flights.apply(
    #     lambda row: filtered_flight.get_possible_return_flights(row.name, request), axis=1)
    #
    # # Add a new column 'return_flights_data' to the filtered_flight DataFrame
    # # filtered_flight.outbound_flights['return_flights_data'] = return_flights_data
    #
    # # Calculate the number of return flights and filter outbound flights
    # filtered_flight.outbound_flights['n_returnflights'] = return_flights_data.apply(lambda df: len(df))
    # filtered_flight.outbound_flights = filtered_flight.outbound_flights[
    #     filtered_flight.outbound_flights['n_returnflights'] > 0].reset_index(drop=True)
    #
    # # Calculate total cost using the precomputed return flights
    # # filtered_flight.outbound_flights['total_cost'] = filtered_flight.outbound_flights.apply(
    # #     lambda row: row['price'] + row['return_flights_data']['price'].min(), axis=1
    # # )
    #
    # print(filtered_flight.outbound_flights)
    # return filtered_flight.outbound_flights.to_html()
    return templates.TemplateResponse("form.html", {"request": request})


@app.post("/outbound/", response_class=HTMLResponse)
async def get_outbound_flights(request: Request,
                               departure_city: str = Form(),
                               arrival_city: str = Form(),
                               departure_date_first: str = Form(),
                               departure_date_last: str = Form(),
                               arrival_date_first: str = Form(),
                               arrival_date_last: str = Form(),
                               min_days_stay: str = Form(),
                               max_days_stay: str = Form(),
                               airport_radius: str = Form(),
                               max_price_per_flight: str = Form(),
                               ):
    request = RQ(departure_city=departure_city,
                 arrival_city=arrival_city,
                 departure_date_first=datetime.datetime.strptime(departure_date_first, "%Y-%m-%d").date(),
                 departure_date_last=datetime.datetime.strptime(departure_date_last, "%Y-%m-%d").date(),
                 arrival_date_first=datetime.datetime.strptime(arrival_date_first, "%Y-%m-%d").date(),
                 arrival_date_last=datetime.datetime.strptime(arrival_date_last, "%Y-%m-%d").date(),
                 min_days_stay=int(min_days_stay),
                 max_days_stay=int(max_days_stay),
                 airport_radius=float(airport_radius),
                 max_price_per_flight=int(max_price_per_flight)
                 )

    # request = RQ(**vars(item.model_dump_json()))
    # wa = WizzAir()
    # result_flight_wa = sum(wa.get_possible_flights(request), start=Flight.empty_flight())
    #
    # ra = RyanAir()
    # possible_flights_ra = ra.get_possible_flights(request)
    # result_flight_ra = sum(possible_flights_ra, start=Flight.empty_flight())
    #
    # ### direct flights ###
    #
    # flight = result_flight_wa + result_flight_ra
    # filtered_flight = flight.filter_flights(request)
    # filtered_flight.outbound_flights['n_returnflights'] = filtered_flight.outbound_flights.apply(
    #     lambda row: len(filtered_flight.get_possible_return_flights(row.name, request)), axis=1)
    # filtered_flight.outbound_flights = filtered_flight.outbound_flights[
    #     filtered_flight.outbound_flights['n_returnflights'] > 0].reset_index(drop=True)
    #
    # filtered_flight.outbound_flights['total_cost'] = filtered_flight.outbound_flights.apply(
    #     lambda row: row['price'] + (filtered_flight.get_possible_return_flights(row.name, request))['price'].values[0],
    #     axis=1)

    filtered_flight = flight.filter_flights(request)
    return_flights_data = filtered_flight.outbound_flights.apply(
        lambda row: filtered_flight.get_possible_return_flights(row.name, request), axis=1)

    # Add a new column 'return_flights_data' to the filtered_flight DataFrame
    # filtered_flight.outbound_flights['return_flights_data'] = return_flights_data

    filtered_flight.outbound_flights['n_returnflights'] = return_flights_data.apply(lambda df: len(df))
    filtered_flight.outbound_flights = filtered_flight.outbound_flights[
        filtered_flight.outbound_flights['n_returnflights'] > 0].reset_index(drop=True)

    # Calculate total cost using the precomputed return flights
    # filtered_flight.outbound_flights['total_cost'] = filtered_flight.outbound_flights.apply(
    #     lambda row: row['price'] + row['return_flights_data']['price'].min(), axis=1
    # )

    # filtered_flight.outbound_flights = filtered_flight.outbound_flights.drop(columns=['return_flights_data'])

    return filtered_flight.outbound_flights.to_html()


if __name__ == "__main__":
    t = BackgroundTasks()
    t.start()
    uvicorn.run(app, host="0.0.0.0", port=8000)
