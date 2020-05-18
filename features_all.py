#Step1: Get ids from the Queue
import boto3
import json
import redis
from Spotify_Api_ALL import *

client = boto3.client('sqs')    #call the client
r = redis.Redis(host='127.0.0.1', port=6379) #standard port (be aware that the server must be working)
spotify = SpotifyAPI(client_id,client_secret)



queue = client.get_queue_url(
    QueueName='test_standard',
)   #assign the queue we want to the var
queue = queue['QueueUrl']



ids_list = []  #empty list to store message.body
lines=[]    #to get ids_spotify easily
ask_api=[] #list of ids to ask api
features =[] #features list
ask_one_hundred=[] #to get 100 ids in a batch
con = 0 #try to make it work w/o a while True

while con != 3:
    con +=1

    message = client.receive_message(QueueUrl = queue) #using a maxnumberofmessages give a problem to get the body and delete the message, should range(0,max(body))
    message_body= message['Messages'][0]['Body']
    client.delete_message(
    QueueUrl=queue,
    ReceiptHandle=message['Messages'][0]["ReceiptHandle"])
    ids_list.append(message_body)
    lines = ids_list[0]
    ids_spotify = lines.split(",")  #store the ids in the variable ids_spotify
    while len(ids_spotify) != 0:
        for ids in ids_spotify:
            if r.exists(ids):
                ids_spotify.remove(ids)
            else:
                r.set(ids,'1')
                ask_api.append(ids)
                ids_spotify.remove(ids)
        while len(ask_api) != 0:  #Step3/Step5: call the Spotify api &  Update redis with the processed id
            for ids in ask_api:
                r.set(ids,'1')
                ask_one_hundred.append(ids)
                if len(ask_one_hundred) == 100:
                    features.append(spotify.get_feature(ids))          #i ask one by one, i can ask in batch, MAX 100
                    ask_api.remove(ids)
                    ask_one_hundred =[]

ids_spotify
type(features)
len(features)
#Step4: Send data to dynamo db

songs =["7ouMYWpwJ422jRcDASZB7P","4VqPOruhp5EdPBeR92t6lQ","2takcwOaAZWiXQijPHIx7B"]
poppy=[]
spotify.base_search(songs) #trying to solve the get list of ids
#---------------------------------------------------

#Problems&questions
#1 Spotify_api wasn't working, so i called the original Spotify_api_all
#2 watch out to flushdb in redis to have it empty
#3 batching from sqs? possible it seems i need to resolve the look up to get the several messages.body
#4 last point is about, batching from spotify, possible? yes, ofc, there is the api endpoint, working? f***** no -.- 
