#Step1: Get ids from the Queue
import boto3

#there's another way to set credentials, it works with a file credentials, but it does not work for me
sqs = boto3.resource('sqs', #set credentials to connect with sqs
region_name="us-west-2")

queue = sqs.get_queue_by_name(QueueName='test_standard') #assign queue to var

ids_list = []   #empty list to store message.body

def return_message():
    for message in queue.receive_messages(WaitTimeSeconds = 10):    #waittimeseconds may be wrong for batches
        ids_list.append(message.body)
        return ids_list
        #message.delete()  #to activate later when it is working
       # if message:
       #     stampa_message()

return_message() #ritorna una stringa

lines = ids_list[0]
ids_spotify = lines.split(",")  #store the ids in the variable ids_spotify
len(ids_spotify) #to check


#Step2: Set if ids are in Redis
import redis

r = redis.Redis(host='127.0.0.1', port=6379) #standard port (be aware that the server must be working)

ask_api=[] #list of ids to ask api
def check_ids(ids_spotify):   #x is ids_spotify
    for ids in ids_spotify:
        if r.exists(ids):
            pass
        else:
            r.set(ids,'1')
            ask_api.append(ids)
    return ask_api

check_ids(ids_spotify)

#Step3: Those that are not in redis -> call the sp api
from sp_api import SpotifyAPI

var = SpotifyAPI(sp_api.client_id,sp_api.client_secret) #is it correct? it is working but i feel like i am reassigning self

features=[]
def retrieve_feature(ask_api):
    for ids in ask_api:
        features.append(SpotifyAPI.get_feature(var,ids))
    return features

retrieve_feature(ask_api)
ask_api

"""
DICTIONARY WAY
features={}
def retrieve_features(ask_api):
    for ids in ask_api:
        features[ids] = SpotifyAPI.get_feature(var,ids)
    return features
"""
#Step4: Send data to dynamo db




#Step5 Update redis with the processed ids
ask_api

for ids in ask_api:
    r.set(ids,'1')
    
#Step6: start again

