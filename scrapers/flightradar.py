import requests
import time

class FlightRadar:

    def __init__(self):
        headers = {
            'Host': 'api.flightradar24.com',
            'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0"
        }

        modes = ['arrivals', 'departures']

        # url = f"https://api.flightradar24.com/common/v1/airport.json?code={city_code}&plugin[]=&plugin-setting[schedule][mode]={mode}&plugin-setting[schedule][timestamp]={int(time.time())}&page=-{i}&limit=100&fleet=&token="
