from scrapers.BaseScraper import BaseScraper
import requests

class EasyJet(BaseScraper):

    base_url = "https://www.easyjet.com/ejavailability"
    api_url = "https://www.easyjet.com/ejavailability/api"

    headers = {
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
        'X-Transaction-Id': "845010C9-82B1-967A-328F-CECDF65DDE90",
        'X-Requested-With': "XMLHttpRequest",
        'ADRUM': "isAjax:true",
    }
    def __init__(self):
        airports = None
        countries = None
        url = "https://www.easyjet.com/ejavailability/api/v74/fps/lowestdailyfares?ArrivalIata=BER&Currency=EUR&DateFrom=2023-11-11&DateTo=2025-11-11&DepartureIata=AMS&InboundFares=true"
        re = requests.get(url, self.headers)
        print(re.json())
        super().__init__(self.base_url, self.headers, airports, countries, api_url=self.api_url)

    def _get_city_codes(self):
        """
        Gets all the important data for all available ryanair airports:

        iataCode, name, seoName, aliases[], coordinates[latitude], coordinates[longitude], base, countryCode,
        regionCode, cityCode, currencyCode, routes[], seasonalRoutes[], categories[], priority, timeZone
        """
        url = super().get_api_url('views', 'locate', '3', 'airports', 'en', 'active')
        re = requests.get(url, headers=self.headers)
        return re.json()

    def _get_country_codes(self):
        """
        Gets all the important data for all available ryanair airports:

        code, name, seoName, aliases[], base, city[name], city[code], region[name], region[code],
        country[code], country[iso3code], country[name], country[currency], country[defaultAirportCode], schengen,
        coordinates[latitude], coordinates[longitude], timeZone
        """

        url = super().get_api_url('views', 'locate', '5', 'airports', 'en', 'active')
        re = requests.get(url, headers=self.headers)
        return re.json()