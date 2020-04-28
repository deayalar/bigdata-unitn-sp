import requests
import csv
import sys
from bs4 import BeautifulSoup
import time


def scrape(path):
    BASE_URL = 'https://spotifycharts.com'
    page = requests.get(BASE_URL + path)
    soup = BeautifulSoup(page.content, 'html.parser')
    chart_table = soup.find('table',{'class':'chart-table'}).find('tbody')
    tr = chart_table.find_all('tr')
    regional = path.startswith("/regional")
    with open("chart" + path.replace("/", "_") + '.csv', mode='w') as csv_file:
        fieldnames = ['track', 'artist', 'spotify_id']
        if regional:
            fieldnames.append('streams')
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for row in tr:
            track = row.find('td',{'class':'chart-table-track'}).find('strong').string
            artist = row.find('td',{'class':'chart-table-track'}).find('span').string[3:]
            spotify_id = row.find('td',{'class':'chart-table-image'}).find('a')['href'] 
            spotify_id = spotify_id[len("https://open.spotify.com/track/"):]
            if regional:
                streams = row.find('td',{'class':'chart-table-streams'}).string.replace(",",".")
                writer.writerow({'track': track, 'artist': artist, 'streams': streams, 'spotify_id': spotify_id})
            else:
                writer.writerow({'track': track, 'artist': artist, 'spotify_id': spotify_id})


if __name__ == "__main__":
    """
        -v or -r : Viral or Regional
        -c : code of the country
        -d : date
        Execution: python web_scraping.py -r -c ar -d 2020-03-31
    """
    opts = [opt for opt in sys.argv[1:] if opt.startswith("-")]
    args = [arg for arg in sys.argv[1:] if not arg.startswith("-")]

    url = ""

    if "-v" in opts:
        url = url + "/viral"
    elif "-r" in opts:
        url = url + "/regional"

    if "-c" in opts:
         url = url + "/" + args[0]
    else:
         url = url + "/global"
    
    url = url + "/daily"

    if "-d" in opts:
         url = url + "/" + args[1]
    else:
         url = url + "/latest"

    start = time.time()
    print("Scraping " + url)
    scrape(url)
    end = time.time()
    elapsed_time = end - start
    print("Done! {:.2} ms".format(elapsed_time))