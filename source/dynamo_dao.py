import boto3
import json
import time
import decimal

from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

class DynamoDAO:

    def __init__(self, local=False, region='us-west-2'):
        self.local = local
        if self.local:
            self.dynamodb = boto3.resource('dynamodb', region_name=region, endpoint_url="http://localhost:8000")
        else:
            self.dynamodb = boto3.resource('dynamodb', region_name=region)
        self.table = self.dynamodb.Table('charts')

    def create_charts_table(self):
        try:
            self.table = self.dynamodb.create_table(
                TableName='charts',
                BillingMode= "PAY_PER_REQUEST",
                KeySchema=[
                    {
                        'AttributeName': 'id',
                        'KeyType': 'HASH'  #Partition key
                    }
#                    {
#                        'AttributeName': 'date',
#                        'KeyType': 'RANGE'  #Sort key
#                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'id',
                        'AttributeType': 'S'
                    }
#                   {
#                        'AttributeName': 'date',
#                        'AttributeType': 'S'
#                    }
                ]
            )
            if not self.local:
                time.sleep(10) #Give time to the remote dynamodb to create the table
            print("Table status:", self.table.table_status)
            print("Current tables:" + str(list(self.dynamodb.tables.all())))
        except ClientError:
            print("Table was not created")
    
    def save_item(self, data):
        self.table.put_item(
                        Item={
                            'id': data["id"],
                            'date': data["date"],
                            'songs': data["songs"],
                            }
                        )

    def get_chart_by_id(self, id):
        try:
            response = self.table.get_item(
                Key={
                    'id': id
                    #'date': date
                }
            )
        except ClientError as e:
            print(e.response['Error']['Message'])
        return response['Item']
    
    def save_batch(self, charts_to_save):
        with self.table.batch_writer() as batch:
            for c in charts_to_save:
                batch.put_item(
                    Item={
                            'id': c["id"],
                            'date': c["date"],
                            'songs': c["songs"],
                        }
                    )