from scrapers.wizzair import WizzAir
from scrapers.ryanair import RyanAir
from scrapers.easyjet import EasyJet

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
        arrival_country="Italy",
        departure_date_first=datetime.date(2024, 1, 1),
        departure_date_last=datetime.date(2024, 1, 25),
        arrival_date_first=datetime.date(2024, 1, 15),
        arrival_date_last=datetime.date(2024, 2, 15),
        airport_radius=50
    )

    w = wa.get_possible_flights(request)
    # print(wa._get_city_codes())
    result_flights = sum(w, start=Flight.empty_flight())
    outbound_flights = result_flights.outbound_flights
    return_flights = result_flights.return_flights

    outbound_flights['departureCountryCode'] = outbound_flights.apply(
        lambda x: wa.get_countrycode_from_airport_code(x['departureStation']), axis=1)
    outbound_flights['arrivalCountryCode'] = outbound_flights.apply(
        lambda x: wa.get_countrycode_from_airport_code(x['arrivalStation']), axis=1)

    return_flights['departureCountryCode'] = return_flights.apply(
        lambda x: wa.get_countrycode_from_airport_code(x['departureStation']), axis=1)
    return_flights['arrivalCountryCode'] = return_flights.apply(
        lambda x: wa.get_countrycode_from_airport_code(x['arrivalStation']), axis=1)

    print(result_flights)
    outbound_flights.sort_values('price', inplace=True)
    print(outbound_flights)
    return_flights.sort_values('price', inplace=True)
    print(type(outbound_flights['departureDate'].iloc[0]))
    print(return_flights.dtypes)
    #  datetime.strptime(outbound_flights['departureDate'].values[0])

    cheap_outbound_flights = outbound_flights[outbound_flights['price'] < 25].reset_index(drop=True)
    cheap_return_flights = return_flights[return_flights['price'] < 25].reset_index(drop=True)

    for index, row in cheap_outbound_flights.iterrows():

        cheap_return_flights['travel_days'] = (
                    cheap_return_flights['departureDate'] - cheap_outbound_flights['departureDate'].values[
                index]).dt.days
        returnfl = cheap_return_flights[
            (cheap_return_flights['departureStation'] == cheap_outbound_flights['arrivalStation'].values[index]) &
            (cheap_return_flights['departureDate'] > pd.to_datetime(
                cheap_outbound_flights['departureDate'].values[index]))
            ]
        returnfl = returnfl[returnfl['travel_days'] < 5]

        if len(returnfl) == 0:
            continue

        returnfl.sort_values('travel_days')
        returnfl = returnfl.iloc[0]

        print(f"{row['departureStation']}-{row['arrivalStation']}: {row['price']} at {row['departureDate']}")
        print(wa.get_countrycode_from_airport_code(row['arrivalStation']))
        print(
            f"{returnfl['departureStation']}-{returnfl['arrivalStation']}: {returnfl['price']} at {returnfl['departureDate']}")
        print()

    print(time.time() - start_time)

    # print(wa.get_possible_flight('2023-12-20', '2023-12-26', request)['trips'][0]['dates'][0]['dateOut'])
    # print(wa.get_possible_flight('2023-12-20', '2023-12-26', request)['trips'][0]['dates'][-1]['dateOut'])

    # print(wa.get_possible_flight('2023-12-20', '2023-12-26', request)['trips'][-1]['dates'][0]['dateOut'])
    # print(wa.get_possible_flight('2023-12-20', '2023-12-26', request)['trips'][-1]['dates'][-1]['dateOut'])

def wizzair():
    start_time = time.time()

    wa = WizzAir()
    request = Request(
        departure_country="Netherlands",
        arrival_country="Italy",
        departure_date_first=datetime.date(2024, 1, 1),
        departure_date_last=datetime.date(2024, 1, 28),
        arrival_date_first=datetime.date(2024, 1, 1),
        arrival_date_last=datetime.date(2024, 1, 28),
        days_stay=9
    )

    # flights = wa.get_possible_flights(request)
    # test = flights[0] + flights[1]

    result_flights = sum(wa.get_possible_flights(request), start=Flight.empty_flight())

    outbound_flights = result_flights.outbound_flights
    return_flights = result_flights.return_flights

    outbound_flights['departureCountryCode'] = outbound_flights.apply(
        lambda x: wa.get_countrycode_from_airport_code(x['departureStation']), axis=1)
    outbound_flights['arrivalCountryCode'] = outbound_flights.apply(
        lambda x: wa.get_countrycode_from_airport_code(x['arrivalStation']), axis=1)

    return_flights['departureCountryCode'] = return_flights.apply(
        lambda x: wa.get_countrycode_from_airport_code(x['departureStation']), axis=1)
    return_flights['arrivalCountryCode'] = return_flights.apply(
        lambda x: wa.get_countrycode_from_airport_code(x['arrivalStation']), axis=1)

    outbound_flights.sort_values('price.amount', inplace=True)
    print(outbound_flights)
    return_flights.sort_values('price.amount', inplace=True)
    print(type(outbound_flights['departureDate'].iloc[0]))
    print(return_flights.dtypes)
    #  datetime.strptime(outbound_flights['departureDate'].values[0])

    cheap_outbound_flights = outbound_flights[outbound_flights['price.amount'] < 25].reset_index(drop=True)
    cheap_return_flights = return_flights[return_flights['price.amount'] < 25].reset_index(drop=True)

    for index, row in cheap_outbound_flights.iterrows():

        cheap_return_flights['travel_days'] = (cheap_return_flights['departureDate'] - cheap_outbound_flights['departureDate'].values[index]).dt.days
        returnfl = cheap_return_flights[
            (cheap_return_flights['departureStation'] == cheap_outbound_flights['arrivalStation'].values[index]) &
            (cheap_return_flights['departureDate'] > pd.to_datetime(cheap_outbound_flights['departureDate'].values[index]))
            ]
        returnfl = returnfl[returnfl['travel_days'] < 5]

        if len(returnfl) == 0:
            continue

        returnfl.sort_values('travel_days')
        returnfl = returnfl.iloc[0]

        print(f"{row['departureStation']}-{row['arrivalStation']}: {row['price.amount']} at {row['departureDates']}")
        print(wa.get_countrycode_from_airport_code(row['arrivalStation']))
        print(
            f"{returnfl['departureStation']}-{returnfl['arrivalStation']}: {returnfl['price.amount']} at {returnfl['departureDates']}")
        print()

    print(time.time() - start_time)

    # for idx, possible_flight in enumerate(wa.get_possible_flights(request)):
    #     print(possible_flight)

    # print(idx)

    # print(wa._get_country_code_from_name("Netherlands"))
    # for loc in (wa._get_airports_by_country(request).departure_locations):
    #     print(loc['aliases'])
    #     print(loc['shortName'])

def easyjet():
    pass

def testing():
    start_time = time.time()

    ra = RyanAir()

    request = Request(
        departure_country="Netherlands",
        arrival_country="Spain",
        departure_date_first=datetime.date(2024, 1, 1),
        departure_date_last=datetime.date(2024, 1, 30),
        arrival_date_first=datetime.date(2024, 1, 1),
        arrival_date_last=datetime.date(2024, 1, 30),
        days_stay=9,
        max_price_per_flight=60
    )

    possible_ryanair_flights = ra.get_possible_flights(request)

    result_flights_ra = sum(possible_ryanair_flights, start=Flight.empty_flight())

    outbound_flights_ra = result_flights_ra.outbound_flights
    return_flights_ra = result_flights_ra.return_flights

    try:
        outbound_flights_ra['departureCountryCode'] = outbound_flights_ra.apply(
            lambda x: ra.get_countrycode_from_airport_code(x['departureStation']), axis=1)
        outbound_flights_ra['arrivalCountryCode'] = outbound_flights_ra.apply(
            lambda x: ra.get_countrycode_from_airport_code(x['arrivalStation']), axis=1)
    except Exception as e:
        pass

    try:
        return_flights_ra['departureCountryCode'] = return_flights_ra.apply(
            lambda x: ra.get_countrycode_from_airport_code(x['departureStation']), axis=1)
        return_flights_ra['arrivalCountryCode'] = return_flights_ra.apply(
            lambda x: ra.get_countrycode_from_airport_code(x['arrivalStation']), axis=1)
    except Exception as e:
        pass

    wa = WizzAir()
    result_flights_wa = sum(wa.get_possible_flights(request), start=Flight.empty_flight())

    outbound_flights_wa = result_flights_wa.outbound_flights
    return_flights_wa = result_flights_wa.return_flights
    try:
        outbound_flights_wa['departureCountryCode'] = outbound_flights_wa.apply(
            lambda x: wa.get_countrycode_from_airport_code(x['departureStation']), axis=1)
        outbound_flights_wa['arrivalCountryCode'] = outbound_flights_wa.apply(
            lambda x: wa.get_countrycode_from_airport_code(x['arrivalStation']), axis=1)
    except Exception as e:
        pass

    try:
        return_flights_wa['departureCountryCode'] = return_flights_wa.apply(
            lambda x: wa.get_countrycode_from_airport_code(x['departureStation']), axis=1)
        return_flights_wa['arrivalCountryCode'] = return_flights_wa.apply(
            lambda x: wa.get_countrycode_from_airport_code(x['arrivalStation']), axis=1)
    except Exception as e:
        pass

    final_flights = Flight(outbound_flights_ra, return_flights_ra) + Flight(outbound_flights_wa, return_flights_wa)
    outbound_flights = final_flights.outbound_flights
    return_flights = final_flights.return_flights

    if len(outbound_flights) == 0 or len(return_flights) == 0:
        print("Nothing was found")
        return

    outbound_flights.sort_values('price', inplace=True)
    print(outbound_flights)
    return_flights.sort_values('price', inplace=True)
    print(type(outbound_flights['departureDate'].iloc[0]))
    print(return_flights.dtypes)
    #  datetime.strptime(outbound_flights['departureDate'].values[0])

    cheap_outbound_flights = outbound_flights[outbound_flights['price'] < request.max_price_per_flight].drop_duplicates().reset_index(drop=True)
    cheap_return_flights = return_flights[return_flights['price'] < request.max_price_per_flight].drop_duplicates().reset_index(drop=True)
    cheap_outbound_flights = cheap_outbound_flights[(request.departure_date_first <= cheap_outbound_flights['departureDate'].dt.date) & (cheap_outbound_flights['departureDate'].dt.date <= request.departure_date_last)].reset_index(drop=True)
    cheap_return_flights = cheap_return_flights[(request.arrival_date_first <= cheap_return_flights['departureDate'].dt.date) & (cheap_return_flights['departureDate'].dt.date <= request.arrival_date_last)].reset_index(drop=True)
    cheap_outbound_flights['weekday'] = cheap_outbound_flights['departureDate'].dt.day_name()
    cheap_return_flights['weekday'] = cheap_return_flights['departureDate'].dt.day_name()

    # for index, row in cheap_outbound_flights.iterrows():
    #     cheap_return_flights['travel_days'] = (
    #             cheap_return_flights['departureDate'] - cheap_outbound_flights['departureDate'].values[
    #         index]).dt.days
    #     returnfl = cheap_return_flights[
    #         (cheap_return_flights['departureStation'] == cheap_outbound_flights['arrivalStation'].values[index]) &
    #         (cheap_return_flights['departureDate'] > pd.to_datetime(cheap_outbound_flights['departureDate'].values[index]))
    #         ]
    #     # returnfl = returnfl[returnfl['travel_days'] < 5]
    #
    #     if len(returnfl) == 0:
    #         continue
    #
    #     returnfl.sort_values('travel_days')
    #     returnfl = returnfl.iloc[0]
    #
    #     print(f"{row['departureStation']}-{row['arrivalStation']}: {row['price']} at {row['departureDate']} by {row['company']}")
    #     try:
    #         print(wa.get_countrycode_from_airport_code(row['arrivalStation']))
    #     except Exception as e:
    #         print(ra.get_countrycode_from_airport_code(row['arrivalStation']))
    #     print(
    #         f"{returnfl['departureStation']}-{returnfl['arrivalStation']}: {returnfl['price']} at {returnfl['departureDate']} by {returnfl['company']}")
    #     print()

    cheap_outbound_flights = cheap_outbound_flights[cheap_outbound_flights['arrivalStation'].isin(cheap_return_flights['departureStation'])]
    cheap_return_flights = cheap_return_flights[cheap_return_flights['departureStation'].isin(cheap_outbound_flights['arrivalStation'])]

    while True:

        idx = int(input("Give me the index"))
        # row = cheap_outbound_flights.loc[[idx]]

        cheap_return_flights['travel_days'] = (
                cheap_return_flights['departureDate'] - cheap_outbound_flights['departureDate'].values[idx]).dt.days
        returnfl = cheap_return_flights[
            (cheap_return_flights['departureStation'] == cheap_outbound_flights['arrivalStation'].values[idx]) &
            (cheap_return_flights['departureDate'] > pd.to_datetime(
                cheap_outbound_flights['departureDate'].values[idx]))
            ]
        returnfl = returnfl[returnfl['travel_days'] >= 0].reset_index(
            drop=True)

        print(returnfl)


    print(time.time() - start_time)

if __name__ == "__main__":
    testing()

    # https://www.ryanair.com/api/farfnd/v4/oneWayFares/DUB/AMS/cheapestPerDay?outboundMonthOfDate=2024-01-02&currency=EUR

    # https://www.ryanair.com/api/farfnd/v4/oneWayFares/AMS/DUB/cheapestPerDay?outboundMonthOfDate=2024-01-02&currency=EUR
