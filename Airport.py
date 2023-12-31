import pandas
import pandas as pd
from Exceptions import CityNotFoundException, CountryNotFoundException
import json

class Airport:

    LONG_KM_RATIO = 111
    LAT_KM_RATIO = 111


    all_airports_df = pandas.read_csv('popular_airports.csv', sep=',')
    all_airports_df[['lon', 'lat']] = all_airports_df[['lon', 'lat']].astype(float)
    all_airports_df = all_airports_df[~all_airports_df["icao"].isnull()].reset_index(drop=True)
    all_airports_df = all_airports_df[~all_airports_df["iata"].isnull()].reset_index(drop=True)
    # all_airports_df = all_airports_df[~all_airports_df["city"].isnull()].reset_index(drop=True)
    # all_airports_df = all_airports_df[~all_airports_df["country"].isnull()].reset_index(drop=True)

    @staticmethod
    def get_iata_from_icao(icao):

        result = Airport.all_airports_df[Airport.all_airports_df["icao"].str.contains(icao, case=False)]["iata"]
        if len(result) > 0:
            return result.iloc[0]
        raise CityNotFoundException(f"requested icao {icao} not found in our database")

    @staticmethod
    def get_countrycode_from_iata(iata: str) -> str:

        result = Airport.all_airports_df[Airport.all_airports_df["iata"].str.contains(iata, case=False)]["country"]
        if len(result) > 0:
            return result.iloc[0]
        raise CityNotFoundException(f"requested iata {iata} not found in our database")

    @staticmethod
    def get_iata_from_city(city_name) -> str:

        result = Airport.all_airports_df[Airport.all_airports_df["city"].str.contains(city_name, case=False)]["iata"]
        if len(result) > 0:
            return result.iloc[0]
        raise CityNotFoundException(f"requested city {city_name} not found in our database")

    @staticmethod
    def get_city_from_iata(iata) -> str:

        result = Airport.all_airports_df[Airport.all_airports_df["iata"].str.contains(iata, case=False)][
            "city"]
        if len(result) > 0:
            return result.iloc[0]
        raise CityNotFoundException(f"requested iata {iata} not found in our database")

    @staticmethod
    def get_airports_by_iata(iata: str) -> pd.DataFrame:
        """
        @param iata: this is the city code of a given city
        @return: all airports and their properties that are in this city
        """
        return Airport.all_airports_df[Airport.all_airports_df['iata'].str.contains(iata, case=False)].reset_index(drop=True)

    @staticmethod
    def get_airports_by_country(country_code: str):
        """
        @param country_code: this is the country code of a given country
        @return: all airports and their properties that are in this country
        """
        return Airport.all_airports_df[Airport.all_airports_df['country'].contains(country_code, case=False)].reset_index(drop=True)

    @staticmethod
    def get_airport_by_icao(icao: str) -> pd.DataFrame:
        """"
        @param icao: this is an airport code
        @return: the airport and its properties
        """
        return Airport.all_airports_df[icao == Airport.all_airports_df['icao']].reset_index(drop=True)

    @staticmethod
    def km_to_long(km: float) -> float:
        return km / Airport.LONG_KM_RATIO

    @staticmethod
    def km_to_lat(km: float) -> float:
        return km / Airport.LAT_KM_RATIO

    @staticmethod
    def arrival_station_radius_lambda(row: pd.Series, radius: float) -> list:
        """
        @param row: row of cheap_outbound_flights dataframe coming from the 'FLIGHT' class
        @param radius: float parameter indicating how big the radius is
        @return: list of iata codes
        """
        ## TODO: This function doesnt really fit here but don't really know where to fit it elsewhere
        iata_airports = Airport.get_airports_by_iata(row['arrivalStation'])
        lat = iata_airports['lat'].iloc[0]
        long = iata_airports['lon'].iloc[0]

        return Airport.get_airports_by_radius(long, lat, radius)['iata'].values

    @staticmethod
    def airports_in_radius(iata1: str, iata2: str, rad: float) -> bool:
        """
        @param iata1: This is the base, we are going to check if the 2nd airport is in radius of this airport
        @param iata2: This is the checked airport, we are going to check if this airport is in radius of airport1
        @param rad: this is the radius in which the airports are allowed to be
        @return: True, if airports in radius else False
        """

        airport1 = Airport.get_airports_by_iata(iata1)
        airport2 = Airport.get_airports_by_iata(iata2)

        # print(airport2['lat'].iloc[0])
        if airport2['iata'].iloc[0] in Airport.get_airports_by_radius(airport1['lon'].iloc[0], airport1['lat'].iloc[0], rad)['iata'].values:
            return True
        return False

    @staticmethod
    def get_airports_by_radius(long: float, lat: float, rad: float) -> pd.DataFrame:
        """
        @param long: lon of certain point
        @param lat: lat of certain point
        @param rad: the radius along which we want to scan for airports
        @return: dataframe of all airports and their properties that are in the given radius of the given point
        """
        if rad > 0:
            lat_range = (lat - Airport.km_to_lat(rad), lat + Airport.km_to_lat(rad))
            long_range = (long - Airport.km_to_long(rad), long + Airport.km_to_long(rad))

            airports_df = Airport.all_airports_df[
                (lat_range[0] <= Airport.all_airports_df['lat']) & (Airport.all_airports_df['lat'] <= lat_range[1]) &
                (long_range[0] <= Airport.all_airports_df['lon']) & (Airport.all_airports_df['lon'] <= long_range[1])
                ].reset_index(drop=True)
            return airports_df

        else:
            return pd.DataFrame()

if __name__ == "__main__":
    print(Airport.get_iata_from_city("rotterdam"))
    iata_airports = Airport.get_airports_by_iata("STN")
    print(iata_airports)