from scrapers.BaseScraper import BaseScraper


class AirSerbia(BaseScraper):

    base_url = ""
    api_url = ""

    headers = {

    }

    def __init__(self):

        super().__init__(self.base_url, self.headers, self.api_url)
