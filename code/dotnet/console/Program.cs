namespace DynamoDB.Demo
{
    using System;
    // See https://aka.ms/new-console-template for more information
    using Amazon;
    using Amazon.DynamoDBv2;
    using Amazon.DynamoDBv2.DataModel;
    using Amazon.DynamoDBv2.DocumentModel;
    using Amazon.DynamoDBv2.Model;

    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("Started!");
            //TradeTest();
            StockPriceTest().GetAwaiter().GetResult();
            Console.WriteLine("Ended!");
        }

        private async static Task StockPriceTest()
        {
            var dac=new StockPriceDAC();
            //await dac.GeneratePrices(500*1);
            var symbol="3b39c11c-aa81-4f1a-a72a-1acee22b9202";
            await dac.GetPrice(symbol);
            await dac.GetPrice2(symbol);
            await dac.ScanPrices();
        }

        private static void TradeTest()
        {
            var dac = new TradeDAC();
            dac.SingleTableBatchWrite().GetAwaiter().GetResult();
            //dac.GetAllTrades().GetAwaiter().GetResult();
            var dateFrom=DateTime.UtcNow - TimeSpan.FromSeconds(10);
            dac.ScanTradesByDate(dateFrom).GetAwaiter().GetResult();
        }
    }
}