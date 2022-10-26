from datetime import datetime
import boto3
import aws_util
import json_util
import json
import uuid


def main():
    swID=aws_util.startStopwatch()
    print("started at ["+str(datetime.now())+"]...")
    #writeMessages(100)
    writeMessagesBatch(10)
    #describeTable("trades")
    #getItem("0b6777bc-44ec-429d-bd40-5ef46802b075")
    now=datetime.now()
    duration=aws_util.stopStopwatch(swID)
    print("ended at ["+str(datetime.now())+"].")
    print("duration was ["+str(duration)+"].")

def describeTable(tableName):
    dynamodbClient=boto3.client('dynamodb', region_name="eu-west-1")
    response=dynamodbClient.describe_table(TableName=tableName)  
    table=response["Table"] 
    print("TableName=["+table["TableName"]+"]")
    print("ItemCount=["+str(table["ItemCount"])+"]")

def getTradeTable():
    dynamodb = boto3.resource('dynamodb', region_name="eu-west-1")
    trades=dynamodb.Table('trades')
    return trades

def writeMessages(rows):
    print("started writeMessages() at ["+str(datetime.now())+"]...")
    trades=getTradeTable()
    for i in range(rows):
        guid=str(uuid.uuid4())
        response=trades.put_item(
            Item={
                'trade_id': guid,
                'trade_amount': 999999,
                "trade_ccy": "SEK"
            }
        )
    print("last GUID inserted was ["+guid+"]")
    print("ended writeMessages() at ["+str(datetime.now())+"]...")

def writeMessagesBatch(rows):
    print("started writeMessagesBatch() at ["+str(datetime.now())+"]...")
    trades=getTradeTable()
    with trades.batch_writer() as batchWriter:
        for i in range(rows):
            guid=str(uuid.uuid4())
            response=batchWriter.put_item(
                Item={
                    'trade_id': guid,
                    'trade_amount': 999999,
                    "trade_ccy": "SEK"
                }
            )
            print(batchWriter)
    print("last GUID inserted was ["+guid+"]")
    print("ended writeMessagesBatch() at ["+str(datetime.now())+"]...")          

def getItem(tradeId):
    trades=getTradeTable()
    response = trades.get_item(
        Key={
            'trade_id': tradeId,
        }
    )
    tradeItemDic=response['Item']
    print("*******RESPONSE**********")
    print(response)
    print("*******TRADE**********")
    print(tradeItemDic)
    #print(tradeItemDic["trade_amount"])

main()


# I want to read a CSV file in to a Pandas data frame