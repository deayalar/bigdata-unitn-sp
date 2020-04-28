import pandas as pd
import datetime
import requests
import json

from google.cloud import firestore
from bs4 import BeautifulSoup
from io import StringIO

def get_json_chart():
    BASE_URL = 'https://spotifycharts.com' #TODO: Move this to an environment variable
    URL_PATTERN = BASE_URL + "/regional/{country}/daily/{day}/download"
    db = firestore.Client()
    page = requests.get(BASE_URL + '/regional/global/daily/latest')
    soup = BeautifulSoup(page.content, 'html.parser')
    dates_list = soup.find('div',{'data-type':'date'}).find('ul').find_all('li')
    country_list = soup.find('div',{'data-type':'country'}).find('ul').find_all('li')
    
    for item_country in country_list[:1]:
        country = item_country['data-value']
        for item_date in dates_list:
            day = item_date['data-value']
            url = URL_PATTERN.replace("{country}", country).replace("{day}", day)
            csv_chart = requests.get(url)
            if csv_chart.status_code == 200:
                data = StringIO(csv_chart.content.decode('utf-8'))
                chart_df = pd.read_csv(data, sep=",", encoding='UTF-8', skiprows=1)
                chart_df['URL'] = chart_df['URL'].apply(func=lambda x: x.replace('https://open.spotify.com/track/',''))
                chart_df = chart_df.rename(columns={'Position': 'pos', 'Track Name': 'track', 'Artist': 'artist', 'Streams': 'streams', 'URL': 'spotify_id'})
                songs = chart_df.to_dict('records')
                
                data = {'songs': songs, 'day': day, 'day_time': datetime.datetime.fromisoformat(day)}
                db.collection(country).document(day).set(data)

def hello_world(request):
    chart = get_json_chart()
    #for doc in db.collection('global').stream():
    #    print(u'{} => {}'.format(doc.id, doc.to_dict()))