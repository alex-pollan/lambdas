using Amazon.DynamoDBv2.DataModel;

namespace AWSServerless1.Models
{
    [DynamoDBTable("Works")]
    public class Work
    {
        [DynamoDBHashKey] //Partition key
        public string ConsumerId { get; set; }

        [DynamoDBRangeKey] //Range key
        public long CreationTimestamp { get; set; }

        [DynamoDBProperty]
        public string Name { get; set; }
    }
}
