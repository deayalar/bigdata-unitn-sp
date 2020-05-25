%reload_ext autoreload
%autoreload 2

import pandas as pd
import numpy
import requests
import json
import time
import logging
import concurrent.futures

from dynamo_dao import DynamoDAO
from sqs_client import SqsClient
from bs4 import BeautifulSoup
from io import StringIO
from datetime import timedelta, date

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', filename="load.log", level=logging.INFO)
logger = logging.getLogger('insert')


class Scrapper():
    def __init__(self, base_url='https://spotifycharts.com'):
        self.BASE_URL = base_url
        self.DOWNLOAD_URL_PATTERN = self.BASE_URL + "/regional/{country}/{timeframe}/{day}/download"
        self.exclude = ["ad", "cy", "mc"]  # Countries to exclude because don't have daily charts

    def scrape_selects(self, timeframe):
        page = requests.get(self.BASE_URL + '/regional/global/' + timeframe + '/latest')
        soup = BeautifulSoup(page.content, 'html.parser')
        country_li = soup.find('div', {'data-type': 'country'}).find('ul').find_all('li')
        days_li = soup.find('div', {'data-type': 'date'}).find('ul').find_all('li')
        countries = [j for j in [i['data-value'] for i in country_li] if j not in self.exclude]
        days = [i['data-value'] for i in days_li]
        return countries, days

    def get_download_url(self, country, timeframe, day):
        return self.DOWNLOAD_URL_PATTERN.replace("{country}", country).replace("{timeframe}", timeframe).replace("{day}", day)

class ChartsLoader():
    def __init__(self, timeframe="weekly", local=True, retry_unprocessed=True, 
                 threads=None, batch_size=25):
        if not (timeframe == "weekly" or timeframe == "daily"):
            raise ValueError("timeframe must be 'daily' or 'weekly'")
        self.timeframe = timeframe
        self.dao = DynamoDAO(local)
        self.sqs = SqsClient()
        self.retry_unprocessed = retry_unprocessed
        if threads:
            self.threads = threads
        else:
            self.threads = 1
        self.failed_results = []
        self.batch_size = batch_size
        self.scrapper = Scrapper()
        self.dao.get_or_create(self.timeframe)

    def get_chart_item(self, country, day, thread_id=1):
        url = self.scrapper.get_download_url(country, self.timeframe, day)
        ids = []
        chart = {}
        try:
            csv_chart = requests.get(url)
            if csv_chart.status_code == 200:
                csv_io = StringIO(csv_chart.content.decode('utf-8'))
                chart_df = pd.read_csv(csv_io, sep=",", encoding='UTF-8', skiprows=1, usecols=['Position', 'Streams', "URL"])
                chart_df = chart_df.fillna("None")
                chart_df['URL'] = chart_df['URL'].apply(func=lambda x: x.replace('https://open.spotify.com/track/', ''))
                ids = chart_df['URL'].tolist()
                chart_df = chart_df.rename(columns={'Position': 'pos', 'Streams': 's', 'URL': 'id'})
                songs = chart_df.to_dict('records')
                if self.timeframe == "weekly":
                    d_arr = [int(i) for i in (day.split("--")[1]).split("-")]
                    day = str(date(d_arr[0], d_arr[1], d_arr[2]) - timedelta(days=1))
                chart = {"country": country, "day": day, "songs": songs}
            else:
                logger.error("Cannot get csv 404 | thread %d: [%s, %s] url: %s" % (thread_id, country, day, url))
        except Exception as e:
            logger.error("Cannot get csv | thread %d:[%s, %s] url: %s" % (thread_id, country, day, url))
        finally:
            return chart, ids

    def get_tuple_batches(self, tuples_list):
        batches = [tuples_list[i:i + self.batch_size] for i in range(0, len(tuples_list), self.batch_size)]
        return batches

    def save_batch(self, tuples_list, thread_id=1):
        batches = self.get_tuple_batches(tuples_list)
        charts_to_save = []
        ids_to_send = set()
        unprocessed_items = []
        for batch in batches:
            start = time.time()
            for country, day in batch:
                item_tuple = (country, day)
                # if country in ["il", "th", "ro", "vn", "za", "jp", "in"]:
                chart, ids = self.get_chart_item(country, day, thread_id)
                if chart:
                    charts_to_save.append(chart)
                    ids_to_send.update(ids)
                else:
                    unprocessed_items.append(item_tuple)
            try:
                ids_string = ",".join(ids_to_send)
                self.sqs.send(ids_string)
                #  self.dao.save_batch(charts_to_save)
            except Exception as e:
                print(e)
                logger.error("Cannot save in db thread %d [%s, %s]" % (thread_id, item_tuple))
                unprocessed_items.extend(batch)
            end = time.time()
            logger.info("Elapsed by thread %d: %.3f secs Last saved chart [%s, %s], song id count %d" % (thread_id, (end - start), country, day, len(list(ids_to_send))))
            charts_to_save = []
            ids_to_send = set()

        if self.retry_unprocessed:
            unprocessed_items = self.handle_unprocessed(unprocessed_items, thread_id)
        return unprocessed_items

    def handle_unprocessed(self, unprocessed_items, thread_id=1):
        logger.info("---Handling unprocessed charts thread " + str(thread_id) + " count:" + str(len(unprocessed_items)))
        new_unprocessed = []
        for c, d in unprocessed_items:
            chart, ids = self.get_chart_item(c, d, thread_id)
            if not chart:
                new_unprocessed.append((c, d))
            else:
                try:
                    logger.info("Saving [%s, %s]" % (c, d))
                    ids_string = ",".join(ids)
                    self.sqs.send(ids_string, is_batch=False)
                    self.dao.save_item(data=chart)
                except Exception as e:
                    logger.info("Error [%s, %s]" % (c, d))
                    new_unprocessed.append((c, d))
        return new_unprocessed

    def load(self):
        logger.info("------STARTING BATCH INSERT--------")
        self.countries, self.days = self.scrapper.scrape_selects(self.timeframe)
        t_start = time.time()
        tuples_list = [(c, d) for c in self.countries for d in self.days]
        if self.threads:
            logger.info("Processing in %d threads" % self.threads)
            thread_batches = [a.tolist() for a in numpy.array_split(tuples_list, self.threads)]
            failed_results = []  # Joined unprocessed items
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.threads) as executor:
                future_batch = {executor.submit(self.save_batch, tb, idx): tb for idx, tb in enumerate(thread_batches)}
                for future in concurrent.futures.as_completed(future_batch):
                    failed_results.append(future.result())
            self.failed_results = failed_results
        else:
            self.failed_results = self.save_batch(tuples_list)
        t_end = time.time()
        logger.info("Unsuccesful inserts, charts: %s" % str(self.failed_results))
        logger.info("FINISHED Total time %.3f secs" % (t_end - t_start))


# cl = ChartsLoader(timeframe="weekly", retry_unprocessed=False, local=True)
cl = ChartsLoader(timeframe="weekly", retry_unprocessed=True,
                  local=False, batch_size=25, threads=10)
cl.load()
cl.failed_results