import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup


class PlayCrawl:
    def __init__(self):
        self.data = set()

    def fetch_data(self, alphabet_list):

        useragent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) " \
                    "Chrome/82.0.4085.2 Safari/537.36"
        session = requests.Session()
        session.headers['User-Agent'] = useragent
        for i in alphabet_list:
            print(f'Searching with the keyword: {i}')
            try:
                url = "https://play.google.com/store/search?q=" + i + "&c=apps&hl=en_US"
                response = session.get(url)
                soup = BeautifulSoup(response.text, "html.parser")
                items = soup.find_all('div', class_='Vpfmgd')

                for links in items:
                    links_with_text = [a['href'] for a in links.find_all('a', href=True) if a.text]
                    # print(links_with_text)
                    my_id = links_with_text[0].split('=')[1]
                    self.data.add(my_id)
            except Exception as e:
                print("An exception occurred: ", e)
                continue

    def crawl_all(self):  # 1
        search_key = ['a', 'b', 'c', 'd', 'e', 'f', 'g',
                      'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
                      't', 'u', 'v', 'w', 'x', 'y', 'z']
        self.fetch_data(search_key)

    def save(self):
        result = list(self.data)
        result = np.array(result)
        pd.DataFrame(result).to_csv("result.csv")


if __name__ == "__main__":
    print(f"The Bot starts!\n")
    app = PlayCrawl()
    app.crawl_all()
    app.save()
