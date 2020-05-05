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
            self.dynamodb = boto3.resource('dynamodb', region_name='us-west-2', endpoint_url="http://localhost:8000")
        else:
            self.dynamodb = boto3.resource('dynamodb', region_name=region)

    def create_charts_table(self, table_name):
        if table_name in [t.name for t in list(self.dynamodb.tables.all())]:
            print("Table was already created")
            self.table = self.dynamodb.Table(table_name)
        else:
            try:
                self.table = self.dynamodb.create_table(
                    TableName=table_name,
                    BillingMode= "PAY_PER_REQUEST",
                    KeySchema=[
                        {
                            'AttributeName': 'country',
                            'KeyType': 'HASH'  #Partition key
                        },
                        {
                            'AttributeName': 'day',
                            'KeyType': 'RANGE'  #Sort key
                        }
                    ],
                    AttributeDefinitions=[
                        {
                            'AttributeName': 'country',
                            'AttributeType': 'S'
                        },
                        {
                            'AttributeName': 'day',
                            'AttributeType': 'S'
                        }
                    ]
                )
                if not self.local:
                    time.sleep(10) #Give time to the remote dynamodb to create the table
                print("Table status:", self.table.table_status)
            except ClientError:
                print("Table was not created")
    
    def save_item(self, data):
        self.table.put_item(
                        Item={
                            'country': data["country"],
                            'day': data["day"],
                            'songs': data["songs"],
                            }
                        )

    def get_chart_by_id(self, country, day):
        try:
            response = self.table.get_item(
                Key={
                    'country': country,
                    'day': day
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
                            'country': c["country"],
                            'day': c["day"],
                            'songs': c["songs"],
                        }
                    )