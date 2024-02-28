// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX - License - Identifier: Apache - 2.0

namespace DynamoDB.Demo
{
    using System.ComponentModel.DataAnnotations.Schema;
    using Amazon.DynamoDBv2.DataModel;
    [DynamoDBTable("stock-prices")]
    public class StockPrice
    {
        [DynamoDBHashKey("symbol")] public string Symbol { get; set; } // Partition key
        [DynamoDBProperty("exchange")] public string? Exchange { get; set; }
        [DynamoDBProperty("price")] public decimal Price { get; set; }
        [DynamoDBProperty("latest-update-date")] public DateTime LatestUpdateDate { get; set; }
    }
}