from Airport import Airport
from scrapers.volotea import Volotea

from scrapers.vueling import Vueling
from scrapers.wizzair import WizzAir
from scrapers.ryanair import RyanAir
from scrapers.easyjet import EasyJet
from scrapers.transavia import Transavia

from Request import Request
import pandas as pd
from Flight import Flight
from Proxy import Proxy

import time
import datetime
from dateutil.relativedelta import relativedelta

DIRECTORY = "output_data"

def get_flights():
    request = Request(
        departure_city="EIN",
        departure_date_first=datetime.datetime.now().date(),
        departure_date_last=datetime.datetime.now().date() + relativedelta(months=+9),
        arrival_date_first=datetime.datetime.now().date(),
        arrival_date_last=datetime.datetime.now().date() + relativedelta(months=+9),
        airport_radius=100
        # max_price_per_flight=25
    )

    start_time = time.time()
    # companies = [RyanAir(), EasyJet(), WizzAir(), Vueling(), Volotea()]
    print("start")

    ej = EasyJet()
    result_flight_ej = sum(ej.get_possible_flights(request), start=Flight.empty_flight())

    # result_flight_ej = Flight.empty_flight()
    print(time.time() - start_time)
    print("Easyjet done scraping")
    print()

    ra = RyanAir()
    result_flight_ra = sum(ra.get_possible_flights(request), start=Flight.empty_flight())

    print(time.time() - start_time)
    print("Ryanair done scraping")
    print()

    wa = WizzAir()
    result_flight_wa = sum(wa.get_possible_flights(request), start=Flight.empty_flight())

    print(time.time() - start_time)
    print("Wizzair done scraping")
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

    print(f"Done scraping companies: {time.time() - start_time}")
    result_flights = result_flight_wa + result_flight_ra + result_flight_vu + result_flight_vt + result_flight_ej
    print(f"Finished {time.time() - start_time}")

    return result_flights


if __name__ == "__main__":
    Proxy.next_proxy()
    begin_time = 0
    while True:
        if time.time() - begin_time > 60 * 60:
            begin_time = time.time()
            flights: Flight = get_flights()

            cur_time_str = datetime.datetime.now().strftime("%Y-%m-%d-%H")

            flights.outbound_flights.to_csv(f"{DIRECTORY}/outbound_{cur_time_str}.csv")
            flights.return_flights.to_csv(f"{DIRECTORY}/return_{cur_time_str}.csv")

            Proxy.next_proxy()
