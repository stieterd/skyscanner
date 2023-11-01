from scrapers.wizzair import WizzAir
from scrapers.ryanair import RyanAir

from Request import Request
import datetime
import pandas as pd
from Flight import Flight
import asyncio

from concurrent.futures import ThreadPoolExecutor
import concurrent
import threading

import time

def ryanair():
    start_time = time.time()

    wa = RyanAir()
    request = Request(  
            departure_country="Netherlands", 
            departure_date_first=datetime.date(2024, 1, 1), 
            departure_date_last=datetime.date(2024, 1, 25),
            arrival_date_first=datetime.date(2024, 1, 15),
            arrival_date_last=datetime.date(2024, 2, 15),
            airport_radius=50
            )
    
    # print(wa._get_city_codes())
    # wa.get_possible_flights(request)
    print(wa.get_possible_flight('2023-12-20', '2023-12-26', request)['trips'][0]['dates'][0]['dateOut'])
    print(wa.get_possible_flight('2023-12-20', '2023-12-26', request)['trips'][0]['dates'][-1]['dateOut'])

    print(wa.get_possible_flight('2023-12-20', '2023-12-26', request)['trips'][-1]['dates'][0]['dateOut'])
    print(wa.get_possible_flight('2023-12-20', '2023-12-26', request)['trips'][-1]['dates'][-1]['dateOut'])


def wizzair():
    start_time = time.time()

    wa = WizzAir()
    request = Request(  
            departure_country="Netherlands", 
            departure_date_first=datetime.date(2024, 1, 1), 
            departure_date_last=datetime.date(2024, 1, 25),
            arrival_date_first=datetime.date(2024, 1, 15),
            arrival_date_last=datetime.date(2024, 2, 15),
            airport_radius=50,
            days_stay=2
            )

    outbound_flights = pd.DataFrame()
    return_flights = pd.DataFrame()

    for flight in wa.get_possible_flights(request):
        try:
            outbound_flights = pd.concat([outbound_flights, flight.outbound_flights])
            return_flights = pd.concat([return_flights, flight.return_flights])
            
        except Exception as e:
            pass

    
    outbound_flights = outbound_flights[outbound_flights['priceType'] != "checkPrice"]
    return_flights = return_flights[return_flights['priceType'] != "checkPrice"]

    outbound_flights['departureCountryCode'] = outbound_flights.apply(lambda x: wa._get_country_code_from_airport_code(x['departureStation']), axis=1)
    outbound_flights['arrivalCountryCode'] = outbound_flights.apply(lambda x: wa._get_country_code_from_airport_code(x['arrivalStation']), axis=1)

    return_flights['departureCountryCode'] = return_flights.apply(lambda x: wa._get_country_code_from_airport_code(x['departureStation']), axis=1)
    return_flights['arrivalCountryCode'] = return_flights.apply(lambda x: wa._get_country_code_from_airport_code(x['arrivalStation']), axis=1)


    outbound_flights.sort_values('price.amount', inplace=True)
    print(outbound_flights)
    return_flights.sort_values('price.amount', inplace=True)
    print(type(outbound_flights['departureDate'].iloc[0]))
    print(return_flights.dtypes)

    return_flights['departureDate'] = pd.to_datetime(return_flights['departureDate'])
    #  datetime.strptime(outbound_flights['departureDate'].values[0])
    return_flight = return_flights[(return_flights['departureStation'] == outbound_flights['arrivalStation'].values[0]) & (return_flights['departureDate'] > pd.to_datetime(outbound_flights['departureDate'].values[0]))]
    return_flight = return_flights[(return_flights['departureDate'])]
    print(return_flight)

    print(time.time() - start_time)

    # for idx, possible_flight in enumerate(wa.get_possible_flights(request)):
    #     print(possible_flight)

    # print(idx)

    # print(wa._get_country_code_from_name("Netherlands"))
    # for loc in (wa._get_airports_by_country(request).departure_locations):
    #     print(loc['aliases'])
    #     print(loc['shortName'])

if __name__ == "__main__":
    wizzair()