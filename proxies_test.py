import requests

url_to_check = 'https://www.ryanair.com/api/views/locate/3/airports/en/active'  # Replace with the actual URL you want to check

with open("proxies.txt", 'r') as reader:
    t = reader.read().splitlines()

headers = {
        'cookie': "RequestVerificationToken=10aa42acaa2d4bea88be9818666639b3",
        'Host': "be.wizzair.com",
        'Accept-Language': "en-US,en;q=0.5",
        'X-RequestVerificationToken': "10aa42acaa2d4bea88be9818666639b3",
        'TE': "trailers",
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0"
    }

print(t)

for proxy in t:
    try:
        response = requests.get(url_to_check, headers=headers, proxies={"http": proxy, "https": proxy})
        if response.status_code == 200:
            # print(response.text)
            print(f"Proxy {proxy} is working")
            pass
        else:
            print(f"Proxy {proxy} is not working (Status code: {response.status_code})")
    except requests.RequestException as e:
        print(f"Proxy {proxy} is not working (Error: {e})")
