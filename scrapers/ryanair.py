import requests
import json
import datetime
from scrapers.BaseScraper import BaseScraper
from Request import Request
from Flight import Flight
from Airport import Airport
from Exceptions import CityNotFoundException, CountryNotFoundException, TimeNotAvailableException, \
    DateNotAvailableException, EmptyDataframeException
import pandas as pd
import asyncio
import concurrent.futures
from dateutil.relativedelta import relativedelta
import traceback


class RyanAir(BaseScraper):

    base_url = "https://www.ryanair.com"
    api_url = "https://www.ryanair.com/api"

    headers = {
        'Alt-Used': 'www.ryanair.com',
        'newrelic': 'eyJ2IjpbMCwxXSwiZCI6eyJ0eSI6IkJyb3dzZXIiLCJhYyI6IjY0NjgzMiIsImFwIjoiMzg5MjUxMzk3IiwiaWQiOiI3ZGI3Njc4N2U3NDA2NGVlIiwidHIiOiJhYzA3YjRjMTBlMjM5MWFjYTYxYTQyZGI3MWUyYmIwMCIsInRpIjoxNjk4Nzg5Nzk0MTkyfX0=',
        'X-NewRelic-ID': 'undefined',
        'Cookie': 'rid=ae238289-8716-4078-96f6-a3ab6b2fc20d; rid.sig=jyna6R42wntYgoTpqvxHMK7H+KyM6xLed+9I3KsvYZaVt7P36AL6zp9dGFPu5uVxaIiFpNXrszr+LfNCdY3IT3oCSYLeNv/ujtjsDqOzkY66AL3V6kH2vsK+au12X21HkZ4S8GaG8CoBmm/m0rLsOKYkxtw+U3+ejBaPc15jJjKXnc3owMBg82SNbqyKjVd6Z6qcsoE25p3RmlcaHuHC3GBf1yIGtlqeQun3Mj0vmSWJV8jwIw2wASJ789/CrRqZD7ygs/wfcb8hBBqVoMqjESkGsL/S7dk/EwM5eijnZV/nQWCwG90fqwg160peojGgPoiDiywWDoENQk0YV09rYSADa7R/3NbavlLjUKgw7Vqr2EkRoOQgMUdmDHgAfxJKsZrWodQFk9xEBzjuHPfocfN5yR5jxsTO/bmZ88KXBnt/S3VNMJYg+Yq0skSEsUdqtvj7UADUgM7dyNK9VH0+pLIknNJG+FGLdx1qeLyeDkQyPQmUxRR+3JCLvMPmo0oKaGvvnT9iQ6WZj/Dg9+196dlEt0ODD2dOmctVx3imVQFAXMzbRiIBmV9aRQSmk1zZ1GPPEYm9b9wRW+/fG6/QowErOT5b8/uISrGzpkFvFyMxiOw4SBIhxbYpEvgdURJBjsujq7N85xdC6U0MhAv7WfJVZwMnbKLgPLo1oDu3+7N3tagyw0B6LLNPVVMgYGZWWM1R7JFZRBE1AYT9w9LFkuuwlS/4F67Xnqe/MLjnUoRQDbo+uo2sDGnNTJ2KVG8UGLPZbbhbajWmQiI0hsgfxFjTZfHis1vyv1fv/yhICmF9ojk+S+1GY5/G1XpsO/qW/gl7UcXIXlPG3mmrc4vrfA==; mkt=/gb/en/;',
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0"
    }

    company_name = 'ryanair'

    def __init__(self) -> None:
        """
        Initializes the RyanAir object and its super BaseScraper
        """
        city_codes = self._get_city_codes()
        # country_codes = self._get_country_codes()
        # countries_df = pd.json_normalize(country_codes, max_level=1)

        self.airports = pd.json_normalize(city_codes, max_level=1)

        super().__init__(self.base_url, self.headers, self.api_url)

    def _get_city_codes(self):
        """
        Gets all the important data for all available ryanair airports:

        iataCode, name, seoName, aliases[], coordinates[latitude], coordinates[longitude], base, countryCode,
        regionCode, cityCode, currencyCode, routes[], seasonalRoutes[], categories[], priority, timeZone
        """
        proxy = super().get_proxy()
        url = super().get_api_url('views', 'locate', '3', 'airports', 'en', 'active')

        re = requests.get(url, proxies=proxy, headers=self.headers, verify=False)
        return re.json()

    def _get_country_codes(self):
        """
        Gets all the important data for all available ryanair airports:

        code, name, seoName, aliases[], base, city[name], city[code], region[name], region[code],
        country[code], country[iso3code], country[name], country[currency], country[defaultAirportCode], schengen,
        coordinates[latitude], coordinates[longitude], timeZone
        """
        proxy = super().get_proxy()
        url = super().get_api_url('views', 'locate', '5', 'airports', 'en', 'active')
        re = requests.get(url, proxies=proxy, headers=self.headers)
        return re.json()

    def last_day_of_month(self, any_day: datetime.date):
        # The day 28 exists in every month. 4 days later, it's always next month
        next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
        # subtracting the number of the current day brings us back one month
        return next_month - datetime.timedelta(days=next_month.day)

    def get_possible_flight(self, arrival_iata: str, departure_iata: str, request: Request) -> Flight:

        if request.departure_date_first is None or request.departure_date_last is None or request.arrival_date_first is None or request.arrival_date_last is None:
            raise DateNotAvailableException("No date was passed as argument for departure and/or arrival")

        cur_departure_date = request.departure_date_first.replace(day=1)
        cur_arrival_date = self.last_day_of_month(request.departure_date_first)

        urls = []

        proxy = super().get_proxy()

        while cur_departure_date < request.arrival_date_last:
            url = super().get_api_url("farfnd",
                                      "v4",
                                      "roundTripFares",
                                      departure_iata,
                                      arrival_iata,
                                      "cheapestPerDay",
                                      ToUs='AGREED',
                                      inboundMonthOfDate=cur_arrival_date.strftime("%Y-%m-%d"),
                                      market='nl-nl',
                                      outboundMonthOfDate=cur_departure_date.strftime("%Y-%m-%d")
                                      )

            urls.append(url)
            cur_departure_date = cur_departure_date + relativedelta(months=1)
            cur_arrival_date = cur_arrival_date + relativedelta(months=1)

        fares_outbound = []
        fares_return = []

        for url in urls:
            re = requests.get(url, proxies=proxy, headers=self.headers)
            try:
                fares_outbound.extend(re.json()['outbound']['fares'])
            except Exception as e:
                print(re.text)
                print(e)
                print()

            try:
                fares_return.extend(re.json()['inbound']['fares'])
            except Exception as e:
                print(re.text)
                print(e)
                print()

        try:

            outbound_flights = pd.json_normalize(fares_outbound, max_level=4)
            return_flights = pd.json_normalize(fares_return, max_level=4)
            if not outbound_flights.empty:
                try:
                    outbound_flights = outbound_flights.loc[~outbound_flights.unavailable].loc[
                        ~outbound_flights.soldOut].reset_index(drop=True)

                    if outbound_flights.empty:
                        raise EmptyDataframeException("empty dataframe outbound_flights")

                    outbound_flights = outbound_flights.drop(
                        columns=['day', 'unavailable', 'soldOut', 'price.valueMainUnit', 'price.valueFractionalUnit',
                                 'price.currencySymbol'])
                    try:
                        outbound_flights = outbound_flights.drop(columns=['price'])
                    except Exception as e:
                        pass
                    outbound_flights['departureStation'] = departure_iata
                    outbound_flights['arrivalStation'] = arrival_iata

                    outbound_flights['departureDate'] = pd.to_datetime(outbound_flights['departureDate'], utc=True)
                    outbound_flights['arrivalDate'] = pd.to_datetime(outbound_flights['arrivalDate'], utc=True)

                    outbound_flights = outbound_flights.rename(
                        columns={'price.value': 'price', 'price.currencyCode': 'currencyCode'})
                    outbound_flights['company'] = self.company_name

                    outbound_flights = super().add_country_codes(outbound_flights)

                    ticket_url = f"https://www.ryanair.com/{super().LANGUAGE}/{super().COUNTRY}/trip/flights/select?adults=1&teens=0&children=0&infants=0&dateOut="
                    outbound_flights['ticketUrl'] = ticket_url + outbound_flights['departureDate'].dt.strftime(
                        '%Y-%m-%d').astype(
                        str) + f"&dateIn=&isConnectedFlight=false&discount=0&promoCode=&isReturn=false&originIata={departure_iata}&destinationIata={arrival_iata}&tpAdults=1&tpTeens=0&tpChildren=0&tpInfants=0&tpStartDate=" + \
                                                    outbound_flights['departureDate'].dt.strftime('%Y-%m-%d').astype(
                                                        str) + f"&tpEndDate=&tpDiscount=0&tpPromoCode=&tpOriginIata={departure_iata}&tpDestinationIata={arrival_iata}"

                except EmptyDataframeException:
                    return_flights = pd.DataFrame()

                except Exception as e:

                    print(traceback.format_exc())
                    print(e)
                    print()
                    outbound_flights = pd.DataFrame()

            if not return_flights.empty:
                try:
                    return_flights = return_flights.loc[~return_flights.unavailable].loc[
                        ~return_flights.soldOut].reset_index(
                        drop=True)

                    if return_flights.empty:
                        raise EmptyDataframeException("empty dataframe return_flights")

                    return_flights = return_flights.drop(
                        columns=['day', 'unavailable', 'soldOut', 'price.valueMainUnit', 'price.valueFractionalUnit',
                                 'price.currencySymbol'])
                    try:
                        return_flights = return_flights.drop(columns=['price'])
                    except Exception as e:
                        # print(e)
                        pass

                    return_flights['departureStation'] = arrival_iata
                    return_flights['arrivalStation'] = departure_iata

                    return_flights['departureDate'] = pd.to_datetime(return_flights['departureDate'], utc=True)
                    return_flights['arrivalDate'] = pd.to_datetime(return_flights['arrivalDate'], utc=True)

                    return_flights = return_flights.rename(
                        columns={'price.value': 'price', 'price.currencyCode': 'currencyCode'})

                    return_flights['company'] = self.company_name
                    return_flights = super().add_country_codes(return_flights)

                    ticket_url = f"https://www.ryanair.com/{super().LANGUAGE}/{super().COUNTRY}/trip/flights/select?adults=1&teens=0&children=0&infants=0&dateOut="
                    return_flights['ticketUrl'] = ticket_url + return_flights['departureDate'].dt.strftime(
                        '%Y-%m-%d').astype(
                        str) + f"&dateIn=&isConnectedFlight=false&discount=0&promoCode=&isReturn=false&originIata={departure_iata}&destinationIata={arrival_iata}&tpAdults=1&tpTeens=0&tpChildren=0&tpInfants=0&tpStartDate=" + \
                                                  return_flights['departureDate'].dt.strftime('%Y-%m-%d').astype(
                                                      str) + f"&tpEndDate=&tpDiscount=0&tpPromoCode=&tpOriginIata={departure_iata}&tpDestinationIata={arrival_iata}"

                except EmptyDataframeException:
                    return_flights = pd.DataFrame()

                except Exception as e:
                    return_flights = pd.DataFrame()
                    # print(traceback.format_exc())
                    print(e)
                    print()

            return Flight(outbound_flights, return_flights)

        except Exception as e:
            print(e)
            return Flight.empty_flight()

    def get_possible_flights(self, request: Request) -> list:
        """
        Gets the possible flight times and their prices according to request argument
        """
        # TODO: called method be dependent on if radius or country of departure is chosen
        departure_airports_df = request.get_requested_departure_airports_df()
        connections_df = self.airports[self.airports['iataCode'].isin(departure_airports_df['iata'])]

        results = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=super().MAX_WORKERS) as executor:
            threads = []
            # WOMP WOMP
            for idx, connection_row in connections_df.iterrows():
                routes = [x.split('airport:')[1] for x in filter(lambda x: 'airport' in x, connection_row['routes'])]
                if request.arrival_city:
                    routes = [connection for connection in filter(
                        lambda x: Airport.airports_in_radius(x, request.arrival_city, request.airport_radius), routes)]

                for connection in routes:
                    threads.append(
                        executor.submit(self.get_possible_flight, connection, connection_row['iataCode'], request))

            for idx, future in enumerate(concurrent.futures.as_completed(threads)):
                result = future.result()
                results.append(result)

        return results
