// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX - License - Identifier: Apache - 2.0

namespace DynamoDB.Demo
{
    using Amazon.DynamoDBv2.DataModel;
    [DynamoDBTable("Trade")]
    public class Trade
    {
        [DynamoDBHashKey] // Partition key
        public string TradeGUID { get; set; }
        public string? TradeType { get; set; }
        public int TradeAmount { get; set; }
        public DateTime TradeDate { get; set; }
        public string? CounterpartyId { get; set; }
    }
}