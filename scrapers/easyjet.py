import concurrent.futures
import traceback
import random

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

    # worldwide.easyjet

    base_url = "https://www.easyjet.com/"
    api_url = "https://www.easyjet.com/api"

    company_name = 'easyjet'

    headers = {
        "Host": "www.easyjet.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "DNT": "1",
        "Sec-GPC": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-User": "?1"
    }

    timeout = 5

    def __init__(self):

        # https://www.easyjet.com/ejavailability/api/v76/fps/lowestdailyfares?ArrivalIata=LPL&Currency=EUR&DateFrom=2023-12-30&DateTo=2025-12-30&DepartureIata=AMS&InboundFares=true
        r_json = self._get_city_codes()
        self.airports = pd.json_normalize(r_json, record_path=["Connections"], meta=['CityIata'])
        self.airports = self.airports.rename(columns={0: "connection", "CityIata": "iata"})
        self.airports = self.airports[~self.airports['iata'].isnull() & ~self.airports['iata'].str.strip().eq('') & ~self.airports['connection'].str.contains('*', regex=False)].reset_index(drop=True)
        super().__init__(self.base_url, self.headers, api_url=self.api_url)

    def _get_city_codes(self):
        """
        Gets all the important data for all available ryanair airports:

        iataCode, name, seoName, aliases[], coordinates[latitude], coordinates[longitude], base, countryCode,
        regionCode, cityCode, currencyCode, routes[], seasonalRoutes[], categories[], priority, timeZone
        """
        url = "https://www.easyjet.com/en/"

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1"
        }

        proxy = super().get_proxy()
        print(proxy)
        r = requests.get(url, proxies=proxy, headers=headers, timeout=5)
        print(r.status_code)
        pattern = pattern = r'angularEjModule\.constant\("Sitecore_RoutesData",\s*(.*?)\s*\);'
        matches = re.search(pattern, r.text, re.DOTALL)
        if matches:
            print("matched")
            extracted_data = matches.group(1)
            return json.loads(extracted_data)['Airports']
        else:
            print("Bad")
            return {}

    def get_possible_flight(self, arrival_iata: str, departure_iata: str, request: Request):

        if request.departure_date_first is None or request.departure_date_last is None or request.arrival_date_first is None or request.arrival_date_last is None:
            raise DateNotAvailableException("No date was passed as argument for departure and/or arrival")

        headers = {
            "Host": "gateway.prod.dohop.net",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
            'Referer': "https://worldwide.easyjet.com/",
            'Content-Type': "application/json",
            'Origin': "https://worldwide.easyjet.com"
        }

        url = f'https://gateway.prod.dohop.net/api/graphql?query= query getAvailability($partner: Partner!, $origins: [String]!, $destinations: [String]!, $currencyCode: CurrencyCode!) {{ availability( partner: $partner origins: $origins destinations: $destinations currencyCode: $currencyCode ) {{ homebound {{ ...AvailabilityNode }} outbound {{ ...AvailabilityNode }} }}}} fragment AvailabilityNode on AvailabilityNode {{ date type lowestFare}} &variables={{"partner":"easyjet","currencyCode":"EUR","origins":["{departure_iata}"],"destinations":["{arrival_iata}"]}}'

        proxy = super().get_proxy()


        try:
            r = requests.get(url, proxies=proxy, headers=headers, timeout=self.timeout)
            availability_outbound = pd.DataFrame(r.json()['data']['availability']['outbound'])
            availability_outbound['date'] = pd.to_datetime(availability_outbound['date'])
            availability_return = pd.DataFrame(r.json()['data']['availability']['homebound'])
            availability_return['date'] = pd.to_datetime(availability_return['date'])
        except Exception as e:
            print(e)
            print(r.text)
            return Flight.empty_flight()

        availability_outbound = availability_outbound[availability_outbound['type'].str.contains("DIRECT", case=False)]
        availability_outbound = availability_outbound[(request.departure_date_first <= availability_outbound['date'].dt.date) & (availability_outbound['date'].dt.date <= request.departure_date_last)]

        availability_return = availability_return[availability_return['type'].str.contains("DIRECT", case=False)]
        availability_return = availability_return[(request.departure_date_first <= availability_return['date'].dt.date) & (availability_return['date'].dt.date <= request.departure_date_last)]

        outbound_urls = []
        return_urls = []

        for date in availability_outbound['date'].values:
            date = pd.to_datetime(str(date))
            departure_url = f'https://gateway.prod.dohop.net/api/graphql?query= query searchOutbound($partner: Partner!, $origin: String!, $destination: String!, $passengerAges: [PositiveInt!]!, $metadata: Metadata!, $departureDateString: String!, $returnDateString: String, $sort: Sort, $limit: PositiveInt, $filters: OfferFiltersInput, $utmSource: String) {{ searchOutbound( partner: $partner origin: $origin destination: $destination passengerAges: $passengerAges metadata: $metadata departureDateString: $departureDateString returnDateString: $returnDateString sort: $sort limit: $limit filters: $filters utmSource: $utmSource ) {{ offers {{ ...Offer }} offersFilters {{ ...OffersFilters }} }}}} fragment Offer on Offer {{ id journeyId price pricePerPerson outboundPricePerPerson homeboundPricePerPerson currency transferURL duration itinerary {{ ...Itinerary }}}} fragment Itinerary on Itinerary {{ outbound {{ ...Route }} homebound {{ ...Route }}}} fragment Route on Route {{ id origin {{ code name city country }} destination {{ code name city country }} departure arrival duration operatingCarrier {{ name code flightNumber }} marketingCarrier {{ name code flightNumber }} legs {{ ...Leg }}}} fragment Leg on Leg {{ id duration origin {{ code name city country }} destination {{ code name city country }} departure arrival carrierType operatingCarrier {{ name code flightNumber }} marketingCarrier {{ name code flightNumber }}}} fragment OffersFilters on OfferFilters {{ overnightStay overnightFlight maxNumberOfStops carrierCodes connectionTime {{ min max }} landing {{ outbound {{ min max }} homebound {{ min max }} }} takeoff {{ outbound {{ min max }} homebound {{ min max }} }}}} &variables={{"partner":"easyjet","metadata":{{"language":"en","currency":"EUR","country":"GB"}},"origin":"{departure_iata}","destination":"{arrival_iata}","departureDateString":"{date.strftime("%Y-%m-%d")}","returnDateString":null,"passengerAges":[16],"filters":{{"overnightStay":true,"overnightFlight":true,"maxNumberOfStops":0,"carrierCodes":[]}},"sort":"CHEAPEST","limit":25,"utmSource":""}}'
            outbound_urls.append(departure_url)

        for date in availability_return['date'].values:
            date = pd.to_datetime(str(date))
            arrival_url = f'https://gateway.prod.dohop.net/api/graphql?query= query searchOutbound($partner: Partner!, $origin: String!, $destination: String!, $passengerAges: [PositiveInt!]!, $metadata: Metadata!, $departureDateString: String!, $returnDateString: String, $sort: Sort, $limit: PositiveInt, $filters: OfferFiltersInput, $utmSource: String) {{ searchOutbound( partner: $partner origin: $origin destination: $destination passengerAges: $passengerAges metadata: $metadata departureDateString: $departureDateString returnDateString: $returnDateString sort: $sort limit: $limit filters: $filters utmSource: $utmSource ) {{ offers {{ ...Offer }} offersFilters {{ ...OffersFilters }} }}}} fragment Offer on Offer {{ id journeyId price pricePerPerson outboundPricePerPerson homeboundPricePerPerson currency transferURL duration itinerary {{ ...Itinerary }}}} fragment Itinerary on Itinerary {{ outbound {{ ...Route }} homebound {{ ...Route }}}} fragment Route on Route {{ id origin {{ code name city country }} destination {{ code name city country }} departure arrival duration operatingCarrier {{ name code flightNumber }} marketingCarrier {{ name code flightNumber }} legs {{ ...Leg }}}} fragment Leg on Leg {{ id duration origin {{ code name city country }} destination {{ code name city country }} departure arrival carrierType operatingCarrier {{ name code flightNumber }} marketingCarrier {{ name code flightNumber }}}} fragment OffersFilters on OfferFilters {{ overnightStay overnightFlight maxNumberOfStops carrierCodes connectionTime {{ min max }} landing {{ outbound {{ min max }} homebound {{ min max }} }} takeoff {{ outbound {{ min max }} homebound {{ min max }} }}}} &variables={{"partner":"easyjet","metadata":{{"language":"en","currency":"EUR","country":"GB"}},"origin":"{arrival_iata}","destination":"{departure_iata}","departureDateString":"{date.strftime("%Y-%m-%d")}","returnDateString":null,"passengerAges":[16],"filters":{{"overnightStay":true,"overnightFlight":true,"maxNumberOfStops":0,"carrierCodes":[]}},"sort":"CHEAPEST","limit":25,"utmSource":""}}'
            return_urls.append(arrival_url)

        fares_outbound = []
        fares_return = []

        for url in outbound_urls:
            proxy = super().get_proxy()
            def run(flip=False):

                try:
                    r = requests.get(url, proxies=proxy, headers=headers, timeout=self.timeout)
                    fares_outbound.extend(r.json()['data']['searchOutbound']['offers'])
                except Exception as e:
                    if flip:
                        # print(r.text)
                        print(e)
                        print()
                    else:
                        run(True)

            run()

        for url in return_urls:
            print("Easyjet url")
            proxy = super().get_proxy()
            def run(flip=False):

                try:
                    r = requests.get(url, proxies=proxy, headers=headers, timeout=self.timeout)
                    fares_return.extend(r.json()['data']['searchOutbound']['offers'])
                except Exception as e:
                    if flip:
                        # print(r.text)
                        print(e)
                        print()
                    else:
                        run(True)

            run()

        outbound_flights = pd.json_normalize(fares_outbound, ['itinerary', ['outbound']], ['outboundPricePerPerson', 'transferURL'])
        return_flights = pd.json_normalize(fares_return, ['itinerary', ['outbound']], ['outboundPricePerPerson', 'transferURL'])

        if not outbound_flights.empty:
            try:
                outbound_flights = outbound_flights.drop(columns=['id', 'duration', 'legs', 'origin.name', 'origin.city', 'origin.country', 'destination.name', 'destination.city', 'destination.country', 'operatingCarrier.name', 'operatingCarrier.code', 'operatingCarrier.flightNumber', 'marketingCarrier.name', 'marketingCarrier.code', 'marketingCarrier.flightNumber'])
                outbound_flights = outbound_flights.rename(columns={"departure": 'departureDate', 'arrival': 'arrivalDate', 'origin.code': 'departureStation', 'destination.code': 'arrivalStation', 'outboundPricePerPerson': 'price', 'transferURL': 'ticketUrl'})

                outbound_flights['currencyCode'] = "EUR"
                outbound_flights['company'] = self.company_name
                outbound_flights = super().add_country_codes(outbound_flights)

            except Exception as e:
                print(traceback.format_exc())
                print(e)
                print()
                outbound_flights = pd.DataFrame()

        if not return_flights.empty:
            try:
                return_flights = return_flights.drop(
                    columns=['id', 'duration', 'legs', 'origin.name', 'origin.city', 'origin.country', 'destination.name', 'destination.city', 'destination.country', 'operatingCarrier.name', 'operatingCarrier.code', 'operatingCarrier.flightNumber', 'marketingCarrier.name', 'marketingCarrier.code', 'marketingCarrier.flightNumber'])
                return_flights = return_flights.rename(
                    columns={"departure": 'departureDate', 'arrival': 'arrivalDate', 'origin.code': 'departureStation', 'destination.code': 'arrivalStation', 'outboundPricePerPerson': 'price', 'transferURL': 'ticketUrl'})

                return_flights['currencyCode'] = "EUR"
                return_flights['company'] = self.company_name
                return_flights = super().add_country_codes(return_flights)

            except Exception as e:
                print(traceback.format_exc())
                print(e)
                print()
                return_flights = pd.DataFrame()

        return Flight(outbound_flights, return_flights)

    def get_possible_flights(self, request: Request) -> list:
        """
        Gets the possible flight times and their prices according to request argument
        """

        # TODO: called method be dependent on if radius or country of departure is chosen

        departure_airports_df = request.get_requested_departure_airports_df()
        connections_df = self.airports[self.airports['iata'].isin(departure_airports_df['iata'])].reset_index(drop=True)

        results = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=super().MAX_WORKERS) as executor:
            threads = []
            for idx, connection_row in connections_df.iterrows():

                connection = connection_row['connection']

                if request.arrival_city and not Airport.airports_in_radius(connection, request.arrival_city,
                                                                           request.airport_radius):
                    continue

                threads.append(
                    executor.submit(self.get_possible_flight, connection, connection_row['iata'], request))

            for idx, future in enumerate(concurrent.futures.as_completed(threads)):
                result = future.result()
                results.append(result)

        return results