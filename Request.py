"""
Step1: retrieve the ID from the db
Step2: define the function to make the requests to spotify API
Step3: Save the info into a new table in the db (?)

"""
import json
import requests


Base URL =  "https://api.spotify.com/v1"
Endpoint = " /v1/audio-features" #up to 100 IDs,retrieve features info(comma separated list)

id = [songsID]
spotifyID = []#Todo
spotify_token = [token] #Todo
uri = [urisongs]


class Get_Songs_Features:
id = [AuthenticationID]
token = [token_requested]    
    def __init__(self):
        self.user_id = spotify_user_id

#Step1: retrieve the ID from the db
    def get_id_from_db(self):
        pass


#Step2: define the function to make the requests to spotify API
    def get_spotify_data(self,id):
        
        request_body = json.dumps(
            "duration_ms" : "",
            "key" : "",
            "mode" : "",
            "time_signature" : "",
            "acousticness" : "",
            "danceability" : "",
            "energy" : "",
            "instrumentalness" : "",
            "liveness" : "",
            "loudness" : "",
            "speechiness" : "",
            "valence" : "",
            "tempo" : "",
            "id" : "{}".format(id),
            "uri" : "{}".format(uri),
            "track_href" : "https://api.spotify.com/v1/tracks/06AKEBrKUckW0KREUWRnvT",
            "analysis_url" : "",
            "type" : ""
        )


        query = "https://api.spotify.com/v1/audio-features?ids={}".format(id)
        response = response.post(
            query,
            data = request_body,
            headers ={
                "Content-Type": "application/json"
                "Authorization":"Bearer {}".format(spotify_token)
            }
        )

        response_json = response.json()

        return response_json("features")
        
#Step3: Save the info into a new table in the db (?)
    def save_info(self):
        pass


