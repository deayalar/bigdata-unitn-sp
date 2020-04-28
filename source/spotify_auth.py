import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

credentials = SpotifyClientCredentials(client_id="02ba908522894e4c9b2663cea4cd1418", client_secret="25228e86fc7c42cc87034617c33f2ea7")
sp = spotipy.Spotify(client_credentials_manager=credentials)

results = sp.search(q='Shape of you', limit=20)
results
for idx, track in enumerate(results['tracks']['items']):
    print(track['artists'][0]['name'], track['name'], track['popularity'])


from spotipy.oauth2 import SpotifyClientCredentials
import json
import spotipy
import time
import sys

if len(sys.argv) > 1:
    tid = sys.argv[1]
else:
    tid = 'spotify:track:4TTV7EcfroSLWzXRY6gLv6'

start = time.time()
analysis = sp.audio_analysis(tid)
delta = time.time() - start
print(json.dumps(analysis, indent=4))
print("analysis retrieved in %.2f seconds" % (delta,))


import numpy