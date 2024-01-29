from Airport import Airport
from scrapers.volotea import Volotea

from scrapers.vueling import Vueling
from scrapers.wizzair import WizzAir
from scrapers.ryanair import RyanAir
from scrapers.easyjet import EasyJet
from scrapers.transavia import Transavia

from Request import Request
import datetime
import pandas as pd
from Flight import Flight
import asyncio

from pandas.io import sql

from concurrent.futures import ThreadPoolExecutor
import concurrent
import threading
import json
import time

# Function to encode python datetime objects to convert into the .json file

def testing():
    start_time = time.time()

    request = Request(
        departure_city="EIN",
        arrival_city="ALC",
        departure_date_first=datetime.date(2024, 8, 19),
        departure_date_last=datetime.date(2024, 8, 26),
        arrival_date_first=datetime.date(2024, 8, 19),
        arrival_date_last=datetime.date(2024, 8, 26),
        # departure_date_first=datetime.date(2024, 3, 1),
        # departure_date_last=datetime.date(2024, 3, 30),
        # arrival_date_first=datetime.date(2024, 3, 1),
        # arrival_date_last=datetime.date(2024, 3, 30),
        min_days_stay=6,
        departure_airport_radius=100,
        arrival_airport_radius=10,
        # available_departure_weekdays=(4, 5),
        # available_arrival_weekdays=(6, 0, 1)
        # max_price_per_flight=25
    )

    # tv = Transavia()

    wa = WizzAir()
    result_flight_wa = sum(wa.get_possible_flights(request), start=Flight.empty_flight())
    print(time.time() - start_time)
    print("Wizzair done scraping")
    print()

    ra = RyanAir()
    result_flight_ra = sum(ra.get_possible_flights(request), start=Flight.empty_flight())

    print(time.time() - start_time)
    print("Ryanair done scraping")
    print()

    ej = EasyJet()
    result_flight_ej = sum(ej.get_possible_flights(request), start=Flight.empty_flight())
    # result_flight_ej = Flight.empty_flight()
    print(time.time() - start_time)
    print("Easyjet done scraping")
    print()


    vu = Vueling()
    result_flight_vu = sum(vu.get_possible_flights(request), start=Flight.empty_flight())
    print(time.time() - start_time)
    print("Vueling done scraping")
    print()

    vt = Volotea()
    result_flight_vt = sum(vt.get_possible_flights(request), start=Flight.empty_flight())
    print(time.time() - start_time)
    print("Volotea done scraping")
    print()

    ## layover flights ##

    # request_departure, request_arrival = request.split_up_for_layovers()
    #
    # result_layover_flight_wa_dep = sum(wa.get_possible_flights(request_departure), start=Flight.empty_flight())
    # result_layover_flight_wa_arrival = sum(wa.get_possible_flights(request_arrival), start=Flight.empty_flight())
    #
    # result_layover_flight_ra_dep = sum(ra.get_possible_flights(request_departure), start=Flight.empty_flight())
    # result_layover_flight_ra_arrival = sum(ra.get_possible_flights(request_arrival), start=Flight.empty_flight())
    #
    # result_layover_flight_ra_dep = result_layover_flight_ra_dep + result_layover_flight_wa_dep
    # result_layover_flight_ra_arrival = result_layover_flight_ra_arrival + result_layover_flight_wa_arrival
    #
    # result_layover_flight_ra_dep = result_layover_flight_ra_dep.filter_flights(request_departure)
    # result_layover_flight_ra_arrival = result_layover_flight_ra_arrival.filter_flights(request_arrival)
    #
    # flight_layover_ra_return = Flight(result_layover_flight_ra_arrival.outbound_flights, result_layover_flight_ra_dep.return_flights)
    # flight_layover_ra_outbound = Flight(result_layover_flight_ra_dep.outbound_flights, result_layover_flight_ra_arrival.return_flights)
    #
    #
    # try:
    #     return_layover_first = pd.merge(flight_layover_ra_return.outbound_flights, flight_layover_ra_return.return_flights, left_on=['arrivalStation', 'departureDay'], right_on=['departureStation', 'departureDay'], how='inner')
    #     outbound_layover_first = pd.merge(flight_layover_ra_outbound.outbound_flights, flight_layover_ra_outbound.return_flights, left_on=['arrivalStation', 'departureDay'], right_on=['departureStation', 'departureDay'], how='inner')
    #
    #     return_layover_first = return_layover_first[return_layover_first['arrivalDate_x'] < return_layover_first['departureDate_y']].reset_index(drop=True)
    #     outbound_layover_first = outbound_layover_first[
    #         outbound_layover_first['arrivalDate_x'] < outbound_layover_first['departureDate_y']].reset_index(drop=True)
    # except Exception as e:
    #     print("empty flight records")
    #
    #
    # while True:
    #
    #     idx = int(input("Give me the index"))
    #
    #     returnfl = return_layover_first[
    #         (return_layover_first['departureStation_x'] == outbound_layover_first['arrivalStation_y'].values[idx]) &
    #         (return_layover_first['departureDate_x'] > pd.to_datetime(
    #             outbound_layover_first['departureDate_x'].values[idx]))
    #         ]
    #
    #     print(returnfl)

    ### direct flights ###

    flight = result_flight_wa + result_flight_ra + result_flight_vu + result_flight_vt + result_flight_ej
    # flight = Flight(pd.read_csv("output_data/outbound_2024-01-16-13.csv"), pd.read_csv("output_data/return_2024-01-16-13.csv"))
    filtered_flight = flight.filter_flights(request)
    print(time.time() - start_time)
    print("flights filtered")
    print()


    return_flights_data = filtered_flight.outbound_flights.apply(
        lambda row: filtered_flight.get_possible_return_flights(row.name, request), axis=1)

    # Add a new column 'return_flights_data' to the filtered_flight DataFrame
    filtered_flight.outbound_flights['return_flights_data'] = return_flights_data

    # Calculate the number of return flights and filter outbound flights
    filtered_flight.outbound_flights['n_returnflights'] = return_flights_data.apply(lambda df: len(df))
    filtered_flight.outbound_flights = filtered_flight.outbound_flights[
        filtered_flight.outbound_flights['n_returnflights'] > 0].reset_index(drop=True)

    # Calculate total cost using the precomputed return flights
    filtered_flight.outbound_flights['total_cost'] = filtered_flight.outbound_flights.apply(
        lambda row: row['price'] + row['return_flights_data']['price'].min(), axis=1
    )

    filtered_flight.outbound_flights = filtered_flight.outbound_flights.drop(columns=['return_flights_data'])

    print(time.time() - start_time)
    print("n_return + total_cost flights finished")
    print()

    # filtered_flight.outbound_flights['n_returnflights'] = filtered_flight.outbound_flights.apply(lambda row: len(filtered_flight.get_possible_return_flights(row.name, request)), axis=1)
    # filtered_flight.outbound_flights = filtered_flight.outbound_flights[
    #     filtered_flight.outbound_flights['n_returnflights'] > 0].reset_index(drop=True)
    #
    # print(time.time() - start_time)
    # print("n_return flights fixed")
    # print()
    #
    # filtered_flight.outbound_flights['total_cost'] = filtered_flight.outbound_flights.apply(lambda row: row['price'] + (filtered_flight.get_possible_return_flights(row.name, request))['price'].values[0], axis=1)
    # print(time.time() - start_time)
    # print("Finished everything")
    # print()


    while True:

        idx = int(input("Give me the index"))
        # row = cheap_outbound_flights.loc[[idx]]

        returnfl = filtered_flight.get_possible_return_flights(idx, request)
        print(returnfl)


    print(time.time() - start_time)

if __name__ == "__main__":
    testing()

    # https://www.ryanair.com/api/farfnd/v4/oneWayFares/DUB/AMS/cheapestPerDay?outboundMonthOfDate=2024-01-02&currency=EUR

    # https://www.ryanair.com/api/farfnd/v4/oneWayFares/AMS/DUB/cheapestPerDay?outboundMonthOfDate=2024-01-02&currency=EUR
