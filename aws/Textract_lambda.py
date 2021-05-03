import re
import json
import boto3
import time


def lambda_handler(event,context):
    s3 = boto3.client("s3")
    s3r  = boto3.resource("s3")
    textract = boto3.client("textract")

    if event: # Once event triggered get the file names from the event object
        #print("Event:  ",event)
        file_obj = event["Records"][0]
        file_name = str(file_obj['s3']['object']['key'])
        #print("File_Name: ",file_name)


        destData = str(file_name + time.strftime("%Y%m%d-%H%M%S.txt"))
        file_path=file_name
        bad_chars = [';',':','!',"*","]","[","{","}",'"',"'",","]
        s3BucketName ="ocr-source-1"
        documentName = file_path


        #call aws textract
        response = textract.detect_document_text(
            Document = {
                'S3Object': {
                'Bucket' :s3BucketName,
                'Name' :documentName
                }
            }
        )

    result = []
    processedresult =""
    for item in response["Blocks"]:
        if item["BlockType"] == "WORD":
            result.append(item["Text"])
            element = item["Text"] + " "
            processedresult += element

            object = s3r.Object('ocr-target-1',destData)
            object.put(Body=processedresult)





