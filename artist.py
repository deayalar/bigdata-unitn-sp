import json
import redis
from Spotify_Api_ALL import SpotifyAPI
import time
import boto3

client = boto3.client('sqs')    #call the client
r = redis.Redis(host='127.0.0.1', port=6379, db=2) #standard port (be aware that the server must be working) #db goes from 0 to 16
client_id = "c08a019d1f3b432eb2d81c880a1f8342"
client_secret= "to insert" #to insert

spotify = SpotifyAPI(client_id,client_secret)

queue = client.get_queue_url(
    QueueName='spotify_ids',
)   #assign the queue we want to the var
queue = queue['QueueUrl']
client_db = boto3.resource("dynamodb")
artist_table = client_db.Table("artists")

names={
  "album": {
    "album_type": "single",
    "artists": [
      {
        "external_urls": {
        },
        "id": "a_id",
        "name": "n"
      }
    ]}}

def send_to_db(batch):
    with artist_table.batch_writer() as batch_w:
                for c in batch:
                    batch_w.put_item(
                        Item={
                                "s_id": c["id"],
                                names["album"]["artists"][0]['id']: c["album"]["artists"][0]['id'],
                                names["album"]["artists"][0]['name']: c["album"]["artists"][0]['name']
                            }
                        )
                        
ids_list = []  #empty list to store message.body
lines=[]    #to get ids_spotify easily
ask_api=[] #list of ids to ask api
artists_name =[] #features list
con = 0
fail_ids=[]


#Maximum 50 ids. 
while True:
    message = client.receive_message(QueueUrl = queue) #using a maxnumberofmessages give a problem to get the body and delete the message, should range(0,max(body))
    message_body= message['Messages'][0]['Body']
    ids_list.append(message_body)
    lines = ids_list[0]
    ids_spotify = lines.split(",")
    print(len(ids_spotify))  #store the ids in the variable ids_spotify
    for ids in ids_spotify:     #look at each id
        if not r.exists(ids):   #if it is in redis
            ask_api.append(ids) #if not it appends it in the list ask_api
    batches = [ask_api[i:i + 50] for i in range(0, len(ask_api), 50)]     #it creates a batch of 100 ids
    for ids in batches:     
        to_join= ",".join(ids)      #create a single string
        batch_artists = spotify.get_tracks(to_join)       #we call the spotify api and assign the result to batch_artists(50)
        for elem in batch_artists['tracks']:       #
            if elem is None:
                fail_ids.append(elem)
        batch_artists= [i for i in batch_artists["tracks"] if i is not None]   #riassegno batch_artists a una lista con i nomi degli artisti
        artists_name.append(batch_artists)    
    for f in artists_name:
        batch_divide =[f[i:i + 25 ] for i in range(0, len(f), 25)]
        for batch in batch_divide:
            send_to_db(batch)
            for b in batch:
                r.set(b['id'],1)
    ids_list = []  #empty list to store message.body
    ines=[]    #to get ids_spotify easily
    ask_api=[] #list of ids to ask api
    artists_name =[] #features list
    con +=1
    client.delete_message(
    QueueUrl=queue,
    ReceiptHandle=message['Messages'][0]["ReceiptHandle"])
    print(con)
    time.sleep(1)