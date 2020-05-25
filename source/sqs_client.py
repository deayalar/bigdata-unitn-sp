import boto3
import time
import secrets

class SqsClient():
    def __init__(self, region='us-west-2'):
        self.queue_name = "spotify_ids"
        self.url = "https://sqs.us-west-2.amazonaws.com/585318406306/" + self.queue_name
        self.sqs = boto3.client('sqs')
        self.maxBatchSize = 10
        self.entries = []

    def send(self, message, is_batch=True):
        entry = {"Id": secrets.token_hex(24), "MessageBody": message}
        self.entries.append(entry)
        if not is_batch:
            response = self.sqs.send_message(QueueUrl=self.url, MessageBody=message)
        if len(self.entries) == self.maxBatchSize:
            response = self.sqs.send_message_batch(QueueUrl=self.url, Entries=self.entries)
            self.entries = []

    def get(self):
        response = self.sqs.receive_message(QueueUrl=self.url, MaxNumberOfMessages=self.maxBatchSize)
        return response['Messages']

def test():
    sqs_client = SqsClient()
    #  sqs_client.send("abc,def,ghi")
    #  sqs_client.send("qwe,rty,uio")
    #  sqs_client.send("asd,fgh,jkl")

    data = sqs_client.get()
    print(data)
#    for i in data:
#        print(i["Body"].split(","))
# test()