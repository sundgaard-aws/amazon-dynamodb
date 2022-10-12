from datetime import datetime
import boto3
import aws_util
import os
import uuid


def main():
    swID=aws_util.startStopwatch()
    print("started at ["+str(datetime.now())+"]...")
    #writeMessages(100)
    writeMessagesBatch(1000)
    now=datetime.now()
    duration=aws_util.stopStopwatch(swID)
    print("ended at ["+str(datetime.now())+"].")
    print("duration was ["+str(duration)+"].")

def writeMessages(rows):
    print("started writeMessages() at ["+str(datetime.now())+"]...")
    dynamodb = boto3.resource('dynamodb', region_name="eu-west-1")
    trades=dynamodb.Table('trades')
    for i in range(rows):
        guid=str(uuid.uuid4())
        response=trades.put_item(
            Item={
                'trade_id': guid,
                'trade_amount': 999999,
                "trade_ccy": "SEK"
            }
        )
        i+=1
    print("ended writeMessages() at ["+str(datetime.now())+"]...")

def writeMessagesBatch(rows):
    print("started writeMessagesBatch() at ["+str(datetime.now())+"]...")
    dynamodb = boto3.resource('dynamodb', region_name="eu-west-1")
    trades=dynamodb.Table('trades')
    with trades.batch_writer() as batchWriter:
        for i in range(rows):
            guid=str(uuid.uuid4())
            batchWriter.put_item(
                Item={
                    'trade_id': guid,
                    'trade_amount': 999999,
                    "trade_ccy": "SEK"
                }
            )
    print("ended writeMessagesBatch() at ["+str(datetime.now())+"]...")            

main()