import itertools


class Proxy:
    # proxies_list = ["http://rpxod:ki2ag7xw@31.204.3.112:5432", "http://rpxod:ki2ag7xw@31.204.3.252:5432", "http://rpxod:ki2ag7xw@213.209.140.106:5432"]
    proxies_list = []

    proxies = itertools.cycle(proxies_list)

    if len(proxies_list) > 0:
        this_proxy = next(proxies)
    else:
        this_proxy = None

    @staticmethod
    def next_proxy():
        Proxy.this_proxy = next(Proxy.proxies)
        return Proxy.this_proxy
