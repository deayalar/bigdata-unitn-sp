#%reload_ext autoreload
#%autoreload 2

import pandas as pd
import requests
import json
import time
import logging
from dynamo_dao import DynamoDAO

from bs4 import BeautifulSoup
from io import StringIO

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', filename="load.log", level=logging.INFO)
logger = logging.getLogger('insert')

BASE_URL = 'https://spotifycharts.com' #TODO: Move this to an environment variable
URL_PATTERN = BASE_URL + "/regional/{country}/daily/{date}/download"
dao = DynamoDAO(local=False)
dao.create_charts_table()
unprocessed = []

def scrape_selects():
    page = requests.get(BASE_URL + '/regional/global/daily/latest')
    soup = BeautifulSoup(page.content, 'html.parser')
    country_li = soup.find('div',{'data-type':'country'}).find('ul').find_all('li')
    dates_li = soup.find('div',{'data-type':'date'}).find('ul').find_all('li')
    exclude=["ad", "cy", "mc"] #Countries to exclude because don't have daily charts
    countries = [i['data-value'] for i in country_li if i not in exclude]
    dates = [i['data-value'] for i in dates_li]
    return countries, dates

def get_chart_item(country, date):
    id = country + "_" + date
    try:
        url = URL_PATTERN.replace("{country}", country).replace("{date}", date)
        csv_chart = requests.get(url)
        if csv_chart.status_code == 200:
            data = StringIO(csv_chart.content.decode('utf-8'))
            chart_df = pd.read_csv(data, sep=",", encoding='UTF-8', skiprows=1, usecols=['Position', 'Streams', "URL"])
            chart_df = chart_df.fillna("No Name")
            chart_df['URL'] = chart_df['URL'].apply(func=lambda x: x.replace('https://open.spotify.com/track/',''))
            chart_df = chart_df.rename(columns={'Position': 'pos', 'Streams': 'streams', 'URL': 'spotify_id'})
            songs = chart_df.to_dict('records')
            chart = {"id": id, "date": date, "songs": songs}
            return chart
    except Exception as e:
        logger.error("Cannot get csv %s, %s" % (country, date))

def get_tuple_batches(countries, dates, batch_size):
    tuples_list = [(c,d) for c in countries[:10] for d in dates]
    batches = [tuples_list[i:i + batch_size] for i in range(0, len(tuples_list), batch_size)]  
    return  batches

def save_batch(countries, dates, batch_size=25):
    logger.info("------STARTING BATCH INSERT--------")
    unprocessed = []
    batches = get_tuple_batches(countries, dates, batch_size)
    charts_to_save = []
    for batch in batches:
        start = time.time()
        for country, date in batch:
            item_tuple = (country, date)
            chart = get_chart_item(country, date)
            if chart:
                charts_to_save.append(chart)
            else:
                unprocessed.append(item_tuple)
        try:
            dao.save_batch(charts_to_save)
        except Exception as e:
            logger.error("Cannot save in ddb %s, %s" % item_tuple)
            unprocessed.extend(batch)
        end = time.time()
        logger.info("Elapsed %.3f secs Last %s %s" % ((end - start), country, date))
        charts_to_save = []
    return unprocessed

def retry_unprocessed(unprocessed):
    new_unprocessed = []
    for c, d in unprocessed:
        chart = get_chart_item(c, d)
        if not chart:
            new_unprocessed.append((c,d))
        else:
            dao.save_item(data=chart)
    logger.info("Unsuccesful inserts count: %d, charts: %s" % (len(new_unprocessed), str(new_unprocessed)))
    unprocessed = new_unprocessed

countries, dates = scrape_selects()
t_start = time.time()
unprocessed = save_batch(countries, dates)
t_end = time.time()
logger.info("FINISHED Total time %.3f secs" % (t_end - t_start))
retry_unprocessed(unprocessed)

response = dao.get_chart_by_id("global_2020-04-28")
print(response)

len(dates) #1213
len(countries) #63