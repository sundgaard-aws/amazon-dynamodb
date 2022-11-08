namespace DynamoDB.Demo
{
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
            var dac = new DAC();
            dac.SingleTableBatchWrite().GetAwaiter().GetResult();
            //dac.GetAllTrades().GetAwaiter().GetResult();
            var dateFrom=DateTime.UtcNow - TimeSpan.FromSeconds(10);
            dac.ScanTradesByDate(dateFrom).GetAwaiter().GetResult();
            Console.WriteLine("Ended!");
        }
    }

    public class DAC
    {
        private DynamoDBContext context;
        public DAC() {
            var clientConfig = new AmazonDynamoDBConfig();
            clientConfig.RegionEndpoint = RegionEndpoint.EUWest1;
            var client = new AmazonDynamoDBClient(clientConfig);
            context = new DynamoDBContext(client);

        }

        public async Task SingleTableBatchWrite()
        {
            var trade1 = new Trade
            {
                TradeGUID=Guid.NewGuid().ToString(),
                TradeDate=DateTime.Now
            };

            Trade trade2 = new Trade
            {
                TradeGUID = Guid.NewGuid().ToString(),
                TradeDate=DateTime.Now
            };

            var TradeBatch = context.CreateBatchWrite<Trade>();
            TradeBatch.AddPutItems(new List<Trade> { trade1, trade2 });

            Console.WriteLine("Adding two trades to 'Trade' table.");
            await TradeBatch.ExecuteAsync();
        }

        public async Task MultiTableBatchWrite()
        {
            Trade newTrade = new Trade
            {
                TradeGUID=Guid.NewGuid().ToString(),
                TradeDate=DateTime.Now
            };
            var tradeBatch = context.CreateBatchWrite<Trade>();
            tradeBatch.AddPutItem(newTrade);

            DynamoDBOperationConfig config = new DynamoDBOperationConfig();
            config.SkipVersionCheck = true;
            var threadBatch = context.CreateBatchWrite<Trade>(config);
            threadBatch.AddPutItem(newTrade);
            threadBatch.AddDeleteKey("some partition key value", "some sort key value");

            var superBatch = new MultiTableBatchWrite(tradeBatch, threadBatch);
            Console.WriteLine("Performing batch write in MultiTableBatchWrite().");
            await superBatch.ExecuteAsync();
        }


        public async Task GetAllTrades()
        {
            var scanConditions=new List<ScanCondition>();
            var response = context.ScanAsync<Trade>(scanConditions);
            var tradeScanResult = await response.GetRemainingAsync();

            Console.WriteLine("\nTrades:");
            foreach (var trade in tradeScanResult)
            {
                Console.WriteLine($"{trade.TradeGUID}\t{trade.TradeAmount}\t{trade.TradeDate}");
            }
        }

        public async Task GetTrade(IDynamoDBContext context, string tradeGUID)
        {
            Trade tradeItem = await context.LoadAsync<Trade>(tradeGUID);
            Console.WriteLine($"TradeGUID: {tradeItem.TradeGUID} \n TradeType:{tradeItem.TradeAmount} \n TradeType: {tradeItem.TradeType}");
        }

        public async Task ScanTradesByDate(DateTime dateFrom)
        {
            var times = new List<object>();
            times.Add(dateFrom);

            var scanConditions=new List<ScanCondition>();
            var scanCondition=new ScanCondition("TradeDate", ScanOperator.GreaterThan, times.ToArray());
            scanConditions.Add(scanCondition);
            var response = context.ScanAsync<Trade>(scanConditions);
            var tradeScanResult = await response.GetRemainingAsync();

            Console.WriteLine("\nTrades created less than 1 hour ago:");
            foreach (var trade in tradeScanResult)
            {
                Console.WriteLine($"{trade.TradeGUID}\t{trade.TradeAmount}\t{trade.TradeDate}");
            }
        }        

        public async Task QueryTradesByDate(IDynamoDBContext context, string tradeGUID)
        {
            DateTime twoWeeksAgoDate = DateTime.UtcNow - TimeSpan.FromDays(15);
            List<object> times = new List<object>();
            times.Add(twoWeeksAgoDate);

            List<ScanCondition> scs = new List<ScanCondition>();
            var sc = new ScanCondition("TradeDate", ScanOperator.GreaterThan, times.ToArray());
            scs.Add(sc);

            var cfg = new DynamoDBOperationConfig
            {
                QueryFilter = scs,
            };

            var response = context.QueryAsync<Trade>(tradeGUID, cfg);
            var tradeScanResult = await response.GetRemainingAsync();

            Console.WriteLine("\nTrades:");
            foreach (var trade in tradeScanResult)
            {
                Console.WriteLine($"{trade.TradeGUID}\t{trade.TradeAmount}\t{trade.TradeDate}");
            }
        }
    }
}