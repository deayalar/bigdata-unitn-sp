import requests
import json
from bs4 import BeautifulSoup
from google.cloud import bigquery

def scrape(path):
    BASE_URL = 'https://spotifycharts.com' #TODO: Move this to an environment variable
    page = requests.get(BASE_URL + path)
    soup = BeautifulSoup(page.content, 'html.parser')

    chart_table = soup.find('table',{'class':'chart-table'}).find('tbody')
    tr = chart_table.find_all('tr')
    regional = path.startswith("/regional")
    
    json_rows = []

    #TODO: Include position and date of the chart
    for table_row in tr:
        track = table_row.find('td',{'class':'chart-table-track'}).find('strong').string
        artist = table_row.find('td',{'class':'chart-table-track'}).find('span').string[3:]
        spotify_id = table_row.find('td',{'class':'chart-table-image'}).find('a')['href'] 
        spotify_id = spotify_id[len("https://open.spotify.com/track/"):]
        json_row = {'spotify_id': spotify_id, 'track': track, 'artist': artist}
        if regional:
            json_row["streams"] = table_row.find('td',{'class':'chart-table-streams'}).string.replace(",",".")
        json_rows.append(json_row)

    return json_rows

def store_in_bq(rows_list):
    client = bigquery.Client()
    #TODO: Update table name according to country
    bq_output = client.insert_rows_json('bigdata-unitn.spotify.chart_test', rows_list)
    return bq_output

def execute(request):
    path = "/regional/global/daily/latest"  #Change to scrape all the needed charts 
    json_rows = scrape(path)
    result = store_in_bq(json_rows)
    return str(result)
