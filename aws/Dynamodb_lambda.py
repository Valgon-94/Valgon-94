import base64
import json
import boto3
import decimal
import time

def lambda_handler(event, context):
    item = None
    dynamo_db = boto3.client('dynamodb')


    if event:
            try:

                response = dynamo_db.create_table(
                AttributeDefinitions=[
                    {
                        'AttributeName': 'File_name',
                        'AttributeType': 'S',
                    },
                    {
                        'AttributeName': 'Process_date',
                        'Process_date': 'S',
                    }
                ],
                KeySchema=[
                    {
                        'AttributeName': 'File_name',
                        'KeyType': 'HASH',
                    },
                    {
                        'AttributeName': 'Process_date',
                        'KeyType': 'RANGE',
                    },
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5,
                    },
                TableName='Documents_Detail'
            )
            except:
                 pass

            finally:

                time.sleep(10)
                dynamo_db_r = boto3.resource('dynamodb')
                tab = dynamo_db_r.Table('Documents_Detail')

                ''' Sample event object
                event = {'Records': [{'eventVersion': '2.1', 'eventSource': 'aws:s3', 'awsRegion': 'us-east-2',
                                  'eventTime': '2021-03-25T20:26:46.988Z', 'eventName': 'ObjectCreated:Put',
                                  'userIdentity': {'principalId': 'A2WPRUKYF1GBXP'},
                                  'requestParameters': {'sourceIPAddress': '183.82.189.95'},
                                  'responseElements': {'x-amz-request-id': 'K6VQNW5DA4YYWW7S',
                                                       'x-amz-id-2': 'wxKH1P9prVBm11eIP8pgxYUmboFrDv0CKvRztc/2yrSnxC8TxToAU7zeVHPb4VwVPhr86buGApFqkooIygtPQYA4rJwUFJES'},
                                  's3': {'s3SchemaVersion': '1.0',
                                         'configurationId': '2923d5c6-c1de-482c-a4fe-dede08fb00c4',
                                         'buc ket': {'name': 'ocr-source-1',
                                                     'ownerIdentity': {'principalId': 'A2WPRUKYF1GBXP'},
                                                     'arn': 'arn:aws:s3:::ocr-source-1'},
                                         'object': {'key': 'dummy.png', 'size': 16755,
                                                    'eTag': 'cb21f381a89f14eac0bfa6da98b60f98',
                                                    'sequencer': '00605CF20CC5A508C7'}}}]}
                 '''
                file_name = str(event["Records"][0]["s3"]["object"]["key"])
                file_size = str(event["Records"][0]["s3"]["object"]["size"])
                file_extension = str(event["Records"][0]["s3"]["object"]["key"])[int(event["Records"][0]["s3"]["object"]["key"].index(".")) + 1::]
                process_date = str(event["Records"][0]["eventTime"])

                dataset = {'File_name': file_name, 'File_size': file_size, 'File_extension': file_extension,
                    'Process_date': process_date}

                response = tab.put_item(Item=dataset)


