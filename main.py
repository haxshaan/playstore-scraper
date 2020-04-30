import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from time import process_time
from configparser import ConfigParser
from mysql.connector import connect, Error


class PlayCrawl:
    def __init__(self):
        self.data = set()

    def init_db(self, hostname, username, passw, db_name):
        print("Initializing Database connection, please wait.")
        try:
            self.connection = connect(host=hostname, user=username, passwd=passw, db=db_name)
            # engine = create_engine(f"mysql+mysqlconnector://{user}:{passwd}@{host}:{port}/{database}")
            self.cursor = self.connection.cursor()
        except Error as ex:
            print("Can't connect to database!, error received: ", ex)
            raise SystemExit(0)

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
                    my_id = links_with_text[0].split('=')[1]
                    self.data.add(my_id)

            except Exception as ex:
                print("An exception occurred: ", ex)
                continue
        print(f"\nFetched {len(self.data)} package names.")

    def crawl_all(self, keys):
        bad_chars = ["'", "(", ")", "[", "]", '"']
        my_key = ''.join(i for i in keys if i not in bad_chars)
        my_key.replace(', ', ',').replace(' ,', ',')
        keywords = my_key.replace(', ', ',').replace(' ,', ',').split(',')
        self.fetch_data(keywords)

    def save_to_csv(self):
        print(f"\nSaving into CSV file")
        result = list(self.data)
        result = np.array(result)
        pd.DataFrame(result).to_csv("result.csv")

    def save_to_mysql(self, table, column):
        print(f"\nSaving into MYSQL DATABASE")
        table_fetch = f"SHOW TABLES FROM {database}"
        self.cursor.execute(table_fetch)
        table_query = [item for item in self.cursor.fetchall()[0]]
        tables = [i.decode() for i in table_query]

        if table in tables:
            self.cursor.execute(f"SELECT * FROM {table}")
            current_data = set([i[0] for i in self.cursor.fetchall()])
            final_data = [j for j in list(self.data) if j not in current_data]

            if final_data:
                print(f"\nFound {len(self.data) - len(current_data)} duplicates, discarding.")
                try:
                    for i in list(final_data):
                        insert_statement = f"INSERT INTO {table} ({column}) VALUES ('{i}')"
                        self.cursor.execute(insert_statement)

                except Error as ex:
                    print('Error: ', ex)

                finally:
                    self.connection.commit()
                    print(f"\nSuccessfully saved {len(final_data)} number of new records into database")
                    print("\nClosing MySQL connection")
            else:
                print("No new record found!")
            self.cursor.close()
            self.connection.close()

        else:
            print(f"\nTable named {table} does not exist")


if __name__ == "__main__":

    parser = ConfigParser()
    parser.read('config.ini')
    if parser['MYSQL'] and parser['SAVE'] and parser['CRAWLER']:
        try:
            host = parser['MYSQL']['host']
            port = parser['MYSQL']['port']
            user = parser['MYSQL']['user']
            passwd = parser['MYSQL']['password']
            database = parser['MYSQL']['database']
            table_name = parser['MYSQL']['table']
            column_name = parser['MYSQL']['column']
            method = parser['SAVE']['method']
            key = parser['CRAWLER']['keywords']
        except Exception as e:
            print(f"Config file is broken! Please check. Error: {e}")
            raise SystemExit(0)
    else:
        print("Config file is broken! Please check.")
        raise SystemExit(0)

    print("The Play Store crawler Bot has started!\n")
    moment = process_time()
    app = PlayCrawl()
    app.crawl_all(keys=key)
    print(f"\nTime taken: {process_time() - moment} seconds")

    if method == 'csv':
        app.save_to_csv()
    elif method == 'database':
        app.init_db(host, user, passwd, database)
        app.save_to_mysql(table_name, column_name)
    else:
        print(f"Unknown SAVE method found : {method}, please check config file.")
    print("\nThanks for using Play store crawler!")
