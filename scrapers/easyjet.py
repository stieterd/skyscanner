from Airport import Airport
from Exceptions import DateNotAvailableException
from Flight import Flight
from Request import Request
from scrapers.BaseScraper import BaseScraper
import requests
import re
import json
import pandas as pd
from dateutil.relativedelta import relativedelta
import datetime

class EasyJet(BaseScraper):

    base_url = "https://www.easyjet.com/ejavailability"
    api_url = "https://www.easyjet.com/ejavailability/api"

    company_name = 'easyjet'

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "X-Transaction-Id": "FAB75F99-D06D-4480-6200-136EF68105AB",
        "X-RBK-XSRF": "CB616C74DEBEF9D2AD49D0E25B3F3473E6149891",
        "ADRUM": "isAjax:true"
    }
    def __init__(self):
        airports = None
        countries = None
        # https://www.easyjet.com/ejavailability/api/v76/fps/lowestdailyfares?ArrivalIata=LPL&Currency=EUR&DateFrom=2023-12-30&DateTo=2025-12-30&DepartureIata=AMS&InboundFares=true
        self.cities = pd.json_normalize(self._get_city_codes())

        super().__init__(self.base_url, self.headers, api_url=self.api_url)

    def _get_city_codes(self):
        """
        Gets all the important data for all available ryanair airports:

        iataCode, name, seoName, aliases[], coordinates[latitude], coordinates[longitude], base, countryCode,
        regionCode, cityCode, currencyCode, routes[], seasonalRoutes[], categories[], priority, timeZone
        """
        url = "https://www.easyjet.com/nl/"
        r = requests.get(url, headers=self.headers)
        pattern = r'<script>\s*var easyjetWorldwideRoutesData = angular.fromJson\((.*?)\);\s*</script>'
        matches = re.search(pattern, r.text, re.DOTALL)
        if matches:
            extracted_data = matches.group(1)
            return json.loads(extracted_data)['Airports']
        else:
            return {}


    def _get_country_codes(self):
        """
        Gets all the important data for all available ryanair airports:

        code, name, seoName, aliases[], base, city[name], city[code], region[name], region[code],
        country[code], country[iso3code], country[name], country[currency], country[defaultAirportCode], schengen,
        coordinates[latitude], coordinates[longitude], timeZone
        """

        url = super().get_api_url('views', 'locate', '5', 'airports', 'en', 'active')
        r = requests.get(url, headers=self.headers)
        return r.json()

    def get_possible_flight(self, arrival_iata: str, departure_iata: str, request: Request) -> Flight:
        """
        Gets possible flights from the departure location through the connection that is given for all available dates
        """
        # v76/fps/lowestdailyfares?ArrivalIata=LPL&Currency=EUR&DateFrom=2023-12-30&DateTo=2025-12-30&DepartureIata=AMS&InboundFares=true
        departure_city_code = departure_iata
        arrival_city_code = arrival_iata

        if request.departure_date_first is None or request.departure_date_last is None or request.arrival_date_first is None or request.arrival_date_last is None:
            raise DateNotAvailableException("No date was passed as argument for departure and/or arrival")

        cur_departure_date1, cur_departure_date2 = super().find_first_and_last_day(request.departure_date_first)
        cur_arrival_date1, cur_arrival_date2 = super().find_first_and_last_day(request.arrival_date_first)
        payloads = []

        while cur_departure_date1 < request.departure_date_last or cur_arrival_date1 < request.arrival_date_last:
            pl = {"flightList": [
                    {
                        "departureStation": departure_city_code,
                        "arrivalStation": arrival_city_code,
                        "from": cur_departure_date1.strftime("%Y-%m-%d"),
                        "to": cur_departure_date2.strftime("%Y-%m-%d")
                    },
                    {
                        "departureStation": arrival_city_code,
                        "arrivalStation": departure_city_code,
                        "from": cur_arrival_date1.strftime("%Y-%m-%d"),
                        "to": cur_arrival_date2.strftime("%Y-%m-%d")
                    }
                ],
                    "priceType": "regular",
                    "adultCount": request.adult_count,
                    "childCount": request.child_count,
                    "infantCount": request.infant_count
            }
            payloads.append(pl)
            cur_departure_date1, cur_departure_date2 = super().find_first_and_last_day(cur_departure_date1 + relativedelta(months=1))
            cur_arrival_date1, cur_arrival_date2 = super().find_first_and_last_day(cur_arrival_date1 + relativedelta(months=1))

        fares_outbound = []
        fares_return = []
        for pl in payloads:
            url = super().get_api_url('search', 'timetable')
            r = requests.post(url, headers=self.headers, json=pl)
            try:
                fares_outbound.extend(r.json()['outboundFlights'])
                fares_return.extend(r.json()['returnFlights'])
            except Exception as e:
                print(r.text)
                print(e)
                print()

        try:
            # result = re.json()
            outbound_flights = pd.json_normalize(fares_outbound, max_level=1)
            return_flights = pd.json_normalize(fares_return, max_level=1)

            outbound_flights = outbound_flights.explode('departureDates')
            return_flights = return_flights.explode('departureDates')

            outbound_flights = outbound_flights[outbound_flights['priceType'] != "checkPrice"].reset_index(drop=True)
            return_flights = return_flights[return_flights['priceType'] != "checkPrice"].reset_index(drop=True)

            outbound_flights = outbound_flights.drop(columns=['hasMacFlight', 'originalPrice.amount', 'originalPrice.currencyCode', 'departureDate', 'priceType'])
            return_flights = return_flights.drop(columns=['hasMacFlight', 'originalPrice.amount', 'originalPrice.currencyCode', 'departureDate', 'priceType'])


            outbound_flights = outbound_flights.rename(columns={'price.amount': 'price', 'price.currencyCode': 'currencyCode', 'departureDates': 'departureDate'})
            return_flights = return_flights.rename(columns={'price.amount': 'price', 'price.currencyCode': 'currencyCode', 'departureDates': 'departureDate'})

            outbound_flights['company'] = self.company_name
            return_flights['company'] = self.company_name

            outbound_flights['departureDate'] = pd.to_datetime(outbound_flights['departureDate'])
            return_flights['departureDate'] = pd.to_datetime(return_flights['departureDate'])

            outbound_flights['arrivalDate'] = outbound_flights['departureDate'] + datetime.timedelta(hours=3)
            return_flights['arrivalDate'] = return_flights['departureDate'] + datetime.timedelta(hours=3)

            try:
                outbound_flights['departureCountryCode'] = outbound_flights.apply(
                    lambda x: Airport.get_countrycode_from_iata(x['departureStation']), axis=1)
                outbound_flights['arrivalCountryCode'] = outbound_flights.apply(
                    lambda x: Airport.get_countrycode_from_iata(x['arrivalStation']), axis=1)
            except Exception as e:
                print(e)
                pass

            try:
                return_flights['departureCountryCode'] = return_flights.apply(
                    lambda x: Airport.get_countrycode_from_iata(x['departureStation']), axis=1)
                return_flights['arrivalCountryCode'] = return_flights.apply(
                    lambda x: Airport.get_countrycode_from_iata(x['arrivalStation']), axis=1)
            except Exception as e:
                print(e)
                pass

            return Flight(outbound_flights, return_flights)

        except Exception as e:
            # print(re.text)
            print(e)
            return Flight.empty_flight()
