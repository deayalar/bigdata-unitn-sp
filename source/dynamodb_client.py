%reload_ext autoreload
%autoreload 2

import pandas as pd
import requests
import json
import time
import logging
from dynamo_dao import DynamoDAO

from bs4 import BeautifulSoup
from io import StringIO
from datetime import timedelta, date

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', filename="load.log", level=logging.INFO)
logger = logging.getLogger('insert')

class ChartsLoader():
    def __init__(self, base_url='https://spotifycharts.com', timeframe="weekly", local=True, retry_unprocessed=True):
        if not (timeframe == "weekly" or timeframe == "daily"):
            raise ValueError("timeframe must be 'daily' or 'weekly'")
        self.BASE_URL = base_url
        self.timeframe = timeframe
        self.DOWNLOAD_URL_PATTERN = self.BASE_URL + "/regional/{country}/{timeframe}/{day}/download"
        self.dao = DynamoDAO(local)
        self.dao.create_charts_table(self.timeframe)
        self.retry_unprocessed =retry_unprocessed
        self.unprocessed_items = []
        self.exclude=["ad", "cy", "mc"] #Countries to exclude because don't have daily charts

    def scrape_selects(self):
        page = requests.get(self.BASE_URL + '/regional/global/' + self.timeframe + '/latest')
        soup = BeautifulSoup(page.content, 'html.parser')
        country_li = soup.find('div',{'data-type':'country'}).find('ul').find_all('li')
        days_li = soup.find('div',{'data-type':'date'}).find('ul').find_all('li')
        countries = [j for j in [i['data-value'] for i in country_li] if j not in self.exclude]
        days = [i['data-value'] for i in days_li]
        return countries, days

    def get_chart_item(self, country, day):
        url = self.DOWNLOAD_URL_PATTERN.replace("{country}", country).replace("{timeframe}", self.timeframe).replace("{day}", day)
        try:
            csv_chart = requests.get(url)
            if csv_chart.status_code == 200:
                csv_io = StringIO(csv_chart.content.decode('utf-8'))
                chart_df = pd.read_csv(csv_io, sep=",", encoding='UTF-8', skiprows=1, usecols=['Position', 'Streams', "URL"])
                chart_df = chart_df.fillna("None")
                chart_df['URL'] = chart_df['URL'].apply(func=lambda x: x.replace('https://open.spotify.com/track/',''))
                chart_df = chart_df.rename(columns={'Position': 'pos', 'Streams': 's', 'URL': 'id'})
                songs = chart_df.to_dict('records')
                if self.timeframe == "weekly":
                    d_arr = [int(i) for i in (day.split("--")[1]).split("-")] 
                    day = str(date(d_arr[0],d_arr[1],d_arr[2]) - timedelta(days=1))
                chart = {"country": country, "day": day, "songs": songs}
                return chart
            else:
                logger.error("Cannot get csv %s, %s url: %s" % (country, day, url))
        except Exception as e:
            print(e)
            logger.error("Cannot get csv %s, %s url: %s" % (country, day, url))

    def get_tuple_batches(self, countries, days, batch_size=25):
        tuples_list = [(c,d) for c in countries for d in days]
        batches = [tuples_list[i:i + batch_size] for i in range(0, len(tuples_list), batch_size)]  
        return  batches

    def save_batch(self, countries, days, batch_size=25):
        logger.info("------STARTING BATCH INSERT--------")
        batches = self.get_tuple_batches(countries, days, batch_size)
        charts_to_save = []
        self.unprocessed_items = []
        for batch in batches:
            start = time.time()
            for country, day in batch:
                item_tuple = (country, day)
                chart = self.get_chart_item(country, day)
                if chart:
                    charts_to_save.append(chart)
                else:
                    self.unprocessed_items.append(item_tuple)
                try:
                    self.dao.save_batch(charts_to_save)
                except Exception as e:
                    logger.error("Cannot save in db %s, %s" % item_tuple)
                    self.unprocessed_items.extend(batch)
            end = time.time()
            logger.info("Elapsed %.3f secs Last %s %s" % ((end - start), country, day))
            charts_to_save = []

    def handle_unprocessed(self):
        new_unprocessed = []
        for c, d in self.unprocessed_items:
            chart = self.get_chart_item(c, d)
            if not chart:
                new_unprocessed.append((c,d))
            else:
                self.dao.save_item(data=chart)
        logger.info("Unsuccesful inserts count: %d, charts: %s" % (len(new_unprocessed), str(new_unprocessed)))
        self.unprocessed_items = new_unprocessed


    def load(self):
        self.countries, self.days = self.scrape_selects()
        t_start = time.time()
        self.save_batch(self.countries, self.days)
        t_end = time.time()
        logger.info("FINISHED Total time %.3f secs" % (t_end - t_start))
        if self.retry_unprocessed:
            self.handle_unprocessed()

cl = ChartsLoader(timeframe="weekly", retry_unprocessed=False, local=True)
cl.load()
#response = cl.dao.dynamodb.Table('weekly').scan()
#print(response['Items'])

response = cl.dao.get_chart_by_id("global", "2020-04-30")
response