from datetime import datetime
import boto3
import aws_util
import os
import uuid


def main():
    swID=aws_util.startStopwatch()
    print("started at ["+str(datetime.now())+"]...")
    #sparkContext=SparkSession.builder.getOrCreate()
    #System.setProperty("hadoop.home.dir", "C:\\Users\\sundgaar\\apps\\hadoop-common-2.2.0-bin-master\\bin")
    #os.environ["hadoop.home.dir"] = "C:\\Users\\sundgaar\\apps\\hadoop-common-2.2.0-bin-master\\bin"
    #os.environ["HADOOP_HOME"] = "C:\\Users\\sundgaar\\apps\\hadoop-common-2.2.0-bin-master"
    #sparkContext = SparkSession.builder.master("local").appName('demo').getOrCreate()
    #sparkContext.setSystemProperty("hadoop.home.dir", "C:\\Users\\sundgaar\\apps\\hadoop-common-2.2.0-bin-master\\bin")
    #glueContext = GlueContext(sparkContext)
    writeMessages(1000)
    now=datetime.now()
    duration=aws_util.stopStopwatch(swID)
    print("ended at ["+str(datetime.now())+"].")
    print("duration was ["+str(duration)+"].")

def writeMessages(rows):
    print("started writeMessages() at ["+str(datetime.now())+"]...")
    dynamodb = boto3.resource('dynamodb', region_name="eu-west-1")
    trades=dynamodb.Table('trades')
    i=0
    rowsToGenerate=rows    
    while i<rowsToGenerate:
        guid=str(uuid.uuid4())
        response=trades.put_item(
            Item={
                'trade_id': guid,
                'datacount': 1,
                'trade_amount': 999999,
                "trade_ccy": "SEK"
            }
        )
        i+=1
    print("ended writeMessages() at ["+str(datetime.now())+"]...")

main()