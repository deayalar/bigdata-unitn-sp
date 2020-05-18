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

songs =["4JpKVNYnVcJ8tuMKjAj50A","2NRANZE9UCmPAS5XVbXL40","24JygzOLM0EmRQeGtFcIcG"]
poppy=[]
spotify.base_search(songs) #trying to solve the get list of ids
#---------------------------------------------------


#1 put it in a while loop
#2 max ids in a list is 100 to ask the api
#3 i downloaded the aws cli and configure it, so there shouldn't be anymore the credential error
#5 i modify the step3, look if it works
#6 the final output to send to dynamo must be a list of dictionaries
#6