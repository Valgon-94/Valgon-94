import re
import json
import boto3
import time
import base64
import decimal
import pdf_ocr as po

'''########################################################################################################################################
Sample project 

Objective: 
(i)Get the back up of the newly uploaded document  in source path S3 bucket server-feed to the backup folder document_bkp in bucket: document-bkp(PNG/JPG/PDF).
(ii)Create a database/catalog on AWS dynamodb to keep track of the files that are used for OCR (Table name : Documents_detail)
(iii) Apply OCR on the backed up  files and store the editable format (txt/csv/tsv) and store in Bucket: ocr-documents-bkp

################################################################################################################################################'''

'''
event = {'Records': [{'eventVersion': '2.1', 'eventSource': 'aws:s3', 'awsRegion': 'us-east-2',
                              'eventTime': '2021-03-25T20:26:46.988Z', 'eventName': 'ObjectCreated:Put',
                              'userIdentity': {'principalId': 'A2WPRUKYF1GBXP'},
                              'requestParameters': {'sourceIPAddress': '183.82.189.95'},
                              'responseElements': {'x-amz-request-id': 'K6VQNW5DA4YYWW7S',
                                                   'x-amz-id-2': 'wxKH1P9prVBm11eIP8pgxYUmboFrDv0CKvRztc/2yrSnxC8TxToAU7zeVHPb4VwVPhr86buGApFqkooIygtPQYA4rJwUFJES'},
                              's3': {'s3SchemaVersion': '1.0',
                                     'configurationId': '2923d5c6-c1de-482c-a4fe-dede08fb00c4',
                                     'bucket': {'name': 'ocr-source-1',
                                                 'ownerIdentity': {'principalId': 'A2WPRUKYF1GBXP'},
                                                 'arn': 'arn:aws:s3:::ocr-source-1'},
                                     'object': {'key': 'sample_pdf.pdf', 'size': 16755,
                                                'eTag': 'cb21f381a89f14eac0bfa6da98b60f98',
                                                'sequencer': '00605CF20CC5A508C7'}}}]}
'''


def lambda_handler(event, context):
    s3 = boto3.client("s3")
    s3r = boto3.resource("s3")
    textract = boto3.client("textract")
    dynamo_db = boto3.client('dynamodb')
    item = None

    if event:  # Once event triggered get the file names from the event object
        # print("Event:  ",event)
        file_obj = event["Records"][0]
        file_name = str(file_obj['s3']['object']['key'])
        file_extension = str(event["Records"][0]["s3"]["object"]["key"])[
                         int(event["Records"][0]["s3"]["object"]["key"].index(".")) + 1::]
        # print("File_Name: ",file_name)

        # Getting backup of the original file to bucket : document-bkp
        source = {'Bucket': 'server-feed', 'Key': file_name}
        dest = s3r.Bucket('document-bkp')
        dest.copy(source, str(file_name + "_bkp_" + time.strftime("%Y%m%d-%H%M%S")))

        # database update starts
        try:
            response_create_tab = dynamo_db.create_table(
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

            file_name_obj = file_name
            file_size = str(event["Records"][0]["s3"]["object"]["size"])
            file_extension_obj = file_extension
            process_date = str(event["Records"][0]["eventTime"])
            status = 'Completed'

            dataset = {'File_name': file_name_obj, 'File_size': file_size, 'File_extension': file_extension_obj,
                       'Process_date': process_date, 'Status' : status}

            response_put = tab.put_item(Item=dataset)
            # database update completed

            # OCR process
            destData = str(file_name + time.strftime("%Y%m%d-%H%M%S.txt"))
            file_path = file_name
            bad_chars = [';', ':', '!', "*", "]", "[", "{", "}", '"', "'", ","]
            s3BucketName = "server-feed"
            # documentName = file_path

            # call aws textract
            if file_extension == 'pdf':
                jobId = po.startConversion(s3BucketName, file_path)
                print("Started job with id: {}".format(jobId))
                if (po.isJobComplete(jobId)):
                    response = po.getJobResults(jobId)

                    # print(response)

                    # Print detected text
                    for resultPage in response:
                        for item in resultPage["Blocks"]:
                            if item["BlockType"] == "LINE":
                                content = item["Text"]
                                object = s3r.Object('ocr-documents-bkp', destData)
                                object.put(Body=content)

            else:
                response_textract = textract.detect_document_text(
                    Document={
                        'S3Object': {
                            'Bucket': s3BucketName,
                            'Name': file_path
                        }
                    }
                )

                result = []
                processedresult = ""
                for item in response_textract["Blocks"]:
                    if item["BlockType"] == "WORD":
                        result.append(item["Text"])
                        element = item["Text"] + " "
                        processedresult += element

                        object = s3r.Object('ocr-documents-bkp', destData)
                        object.put(Body=processedresult)





