namespace DynamoDB.Demo
{
    // See https://aka.ms/new-console-template for more information
    using Amazon;
    using Amazon.DynamoDBv2;
    using Amazon.DynamoDBv2.DataModel;
    using Amazon.DynamoDBv2.DocumentModel;
    using Amazon.DynamoDBv2.Model;

    public class StockPriceDAC
    {
        private AmazonDynamoDBClient client;
        private DynamoDBContext context;
        public StockPriceDAC() {
            var clientConfig = new AmazonDynamoDBConfig();
            clientConfig.RegionEndpoint = RegionEndpoint.EUWest1;
            client = new AmazonDynamoDBClient(clientConfig);
            context = new DynamoDBContext(client);

        }

        public async Task GeneratePrices(int rowsToGenerate) {
            var batch = context.CreateBatchWrite<StockPrice>();
            var random=new Random();
            for(var i=0;i<rowsToGenerate;i++) {
                var newStockPrice = new StockPrice
                {
                    Symbol=Guid.NewGuid().ToString(),
                    Price=new decimal(random.NextDouble()*1000),
                    Exchange="LSE",
                    LatestUpdateDate=DateTime.Now
                };
                batch.AddPutItem(newStockPrice);
            }

            DynamoDBOperationConfig config = new DynamoDBOperationConfig();
            config.SkipVersionCheck = true;
            //var threadBatch = context.CreateBatchWrite<StockPrice>(config);
            //threadBatch.AddPutItem(newStockPrice);
            //threadBatch.AddDeleteKey("some partition key value", "some sort key value");

            var superBatch = new MultiTableBatchWrite(batch);
            Console.WriteLine("Performing batch write in GeneratePrices().");
            await superBatch.ExecuteAsync();
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
            var newStockPrice = new StockPrice
            {
                Symbol=Guid.NewGuid().ToString(),
                LatestUpdateDate=DateTime.Now
            };
            var batch = context.CreateBatchWrite<StockPrice>();
            batch.AddPutItem(newStockPrice);

            DynamoDBOperationConfig config = new DynamoDBOperationConfig();
            config.SkipVersionCheck = true;
            var threadBatch = context.CreateBatchWrite<StockPrice>(config);
            threadBatch.AddPutItem(newStockPrice);
            threadBatch.AddDeleteKey("some partition key value", "some sort key value");

            var superBatch = new MultiTableBatchWrite(batch, threadBatch);
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

        public async Task GetPrice(string symbol)
        {
            var startTime=DateTime.Now;
            var price=await context.LoadAsync<StockPrice>(symbol);
            Console.WriteLine($"Symbol: {price.Symbol} \n Price:{price.Price} \n LatestUpdatedDate: {price.LatestUpdateDate}");
            var endTime=DateTime.Now;
            Console.WriteLine($"GetPrice() Duration was {(endTime-startTime).Milliseconds} ms");
        }

        public async Task GetPrice2(string symbol)
        {
            var startTime=DateTime.Now;
            var key = new Dictionary<string,AttributeValue>() { { "symbol", new AttributeValue { S = symbol } } };
            var response=await client.GetItemAsync(new GetItemRequest{ TableName="stock-prices", Key=key });
            var requestId=response.ResponseMetadata.RequestId;
            var item=response.Item;
            Console.WriteLine($"RequestId {requestId}");
            //var result=client.GetItemAsync(new GetItemRequest{ TableName="stock-prices", Key=key }).Result;
            
            //var price=result.Item.SingleOrDefault<StockPrice>();
            //Console.WriteLine($"Symbol: {price.Symbol} \n Price:{price.Price} \n LatestUpdatedDate: {price.LatestUpdateDate}");
            //var item=result.Item;
            Console.WriteLine($"Item symbol was {item["symbol"].S}");
            var endTime=DateTime.Now;
            Console.WriteLine($"GetPrice2() Duration was {(endTime-startTime).Milliseconds} ms");
        }        

        public async Task<List<StockPrice>> ScanPrices()
        {
            var startTime=DateTime.Now;            
            var scanConditions=new List<ScanCondition>();
            //var scanCondition=new ScanCondition("price", ScanOperator.Between, new int[100, 101]);
            //scanConditions.Add(scanCondition);
            var response = context.ScanAsync<StockPrice>(scanConditions);
            //var scanResult = await response.GetRemainingAsync();
            var cancellationToken=new CancellationToken();
            var scanResult = await response.GetNextSetAsync(cancellationToken);

            /*Console.WriteLine("\nPrices between 100 and 500:");
            foreach (var price in scanResult)
            {
                Console.WriteLine($"Symbol: {price.Symbol} \n Price:{price.Price} \n LatestUpdatedDate: {price.LatestUpdateDate}");
            }*/
            var endTime=DateTime.Now;
            Console.WriteLine($"ScanPrices() Duration was {(endTime-startTime).Milliseconds} ms");
            return scanResult.ToList();
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