from scrapers.wizzair import WizzAir
from Request import Request
import datetime

wa = WizzAir()
request = Request(  
            departure_country="France", 
            departure_date_first=datetime.date(2023, 11, 1), 
            departure_date_last=datetime.date(2023, 11, 25),
            arrival_date_first=datetime.date(2023, 11, 15),
            arrival_date_last=datetime.date(2023, 12, 15),
            airport_radius=50
            )

for idx, possible_flight in enumerate(wa.get_possible_flights(request)):
    print(possible_flight)

print(idx)

# print(wa._get_country_code_from_name("Netherlands"))
# for loc in (wa._get_airports_by_country(request).departure_locations):
#     print(loc['aliases'])
#     print(loc['shortName'])