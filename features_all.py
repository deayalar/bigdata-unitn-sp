import json
import redis
from Spotify_Api_ALL import SpotifyAPI
import time
import boto3

client = boto3.client('sqs')    #call the client
r = redis.Redis(host='127.0.0.1', port=6379, db=0) #standard port (be aware that the server must be working) #db goes from 0 to 16
client_id = "c08a019d1f3b432eb2d81c880a1f8342"
client_secret= "TO_INSERT" #to insert

spotify = SpotifyAPI(client_id,client_secret)

queue = client.get_queue_url(
    QueueName='spotify_ids',
)   #assign the queue we want to the var
queue = queue['QueueUrl']
client_db = boto3.resource("dynamodb")
songs_table = client_db.Table("songs_feature")

names={
    "danceability":"d",
    "energy":"e",
    "key":"k",
    "loudness":"l",
    "mode":"m",
    "speechiness":"s",
    "acousticness": "a",
    "instrumentalness": "i",
    "liveness": "li",
    "valence": "v",
    "tempo": "t",
    "time_signature":"ts",
    "duration_ms":"dm"
}

def send_to_db(batch):
    with songs_table.batch_writer() as batch_w:
                for c in batch:
                    batch_w.put_item(
                        Item={
                                "s_id": c["id"],
                                names["danceability"]: str(c["danceability"]),
                                names["energy"]: str(c["energy"]),
                                names["key"]: str(c["key"]),
                                names["loudness"]: str(c["loudness"]),
                                names["mode"]: str(c["mode"]),
                                names["speechiness"]: str(c["speechiness"]),
                                names["acousticness"]: str(c["acousticness"]),
                                names["instrumentalness"]: str(c["instrumentalness"]),
                                names["liveness"]: str(c["liveness"]),
                                names["valence"]: str(c["valence"]),
                                names["tempo"]: str(c["tempo"]),
                                names["time_signature"]: str(c["time_signature"]),
                                names["duration_ms"]: str(c["duration_ms"])
                            }
                        )
                        
ids_list = []  #empty list to store message.body
lines=[]    #to get ids_spotify easily
ask_api=[] #list of ids to ask api
features =[] #features list
con = 0
fail_ids=[]

while True:
    message = client.receive_message(QueueUrl = queue) #using a maxnumberofmessages give a problem to get the body and delete the message, should range(0,max(body))
    message_body= message['Messages'][0]['Body']
    ids_list.append(message_body)
    lines = ids_list[0]
    ids_spotify = lines.split(",")  #store the ids in the variable ids_spotify
    for ids in ids_spotify:
        if not r.exists(ids):
            ask_api.append(ids)
    batches = [ask_api[i:i + 100] for i in range(0, len(ask_api), 100)]
    for ids in batches:
        to_join= ",".join(ids)
        batch_features = spotify.base_search(to_join)
        for elem in batch_features['audio_features']:
            if elem is None:
                fail_ids.append(elem)
        batch_features= [i for i in batch_features['audio_features'] if i is not None]
        features.append(batch_features)    
    for f in features:
        batch_divide =[f[i:i + 25 ] for i in range(0, len(f), 25)]
        for batch in batch_divide:
            send_to_db(batch)
            for b in batch:
                r.set(b['id'],1)
    ask_api=[]
    features=[]
    lines=[]
    ids_list=[]
    con +=1
    client.delete_message(
    QueueUrl=queue,
    ReceiptHandle=message['Messages'][0]["ReceiptHandle"])
    print(con)
    time.sleep(1)
#100