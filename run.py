from scrapers.wizzair import WizzAir
from Request import Request
import datetime
import pandas as pd
from Flight import Flight
import asyncio

from concurrent.futures import ThreadPoolExecutor
import concurrent
import threading

import time


start_time = time.time()

wa = WizzAir()
request = Request(  
        departure_country="France", 
        departure_date_first=datetime.date(2024, 1, 1), 
        departure_date_last=datetime.date(2024, 1, 25),
        arrival_date_first=datetime.date(2024, 1, 15),
        arrival_date_last=datetime.date(2024, 2, 15),
        airport_radius=50
        )

i = 0

outbound_flights = pd.DataFrame()
return_flights = pd.DataFrame()

for flight in wa.get_possible_flights(request):
    try:
        outbound_flights = pd.concat([outbound_flights, flight.outbound_flights])
        return_flights = pd.concat([return_flights, flight.return_flights])
        
    except Exception as e:
        pass

    i +=1 

print(outbound_flights)
outbound_flights.sort_values('price.amount', inplace=True)
return_flights.sort_values('price.amount', inplace=True)

print(time.time() - start_time)

# for idx, possible_flight in enumerate(wa.get_possible_flights(request)):
#     print(possible_flight)

# print(idx)

# print(wa._get_country_code_from_name("Netherlands"))
# for loc in (wa._get_airports_by_country(request).departure_locations):
#     print(loc['aliases'])
#     print(loc['shortName'])