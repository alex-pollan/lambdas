using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Lambda.DynamoDBEvents;
using Amazon.Lambda.TestUtilities;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace BackgroundLambdas.Tests
{
    public class CountWorksTest
    {
        [Fact]
        public async Task TestCountWorks()
        {
            DynamoDBEvent evnt = new DynamoDBEvent
            {
                Records = new List<DynamoDBEvent.DynamodbStreamRecord>
                {
                    new DynamoDBEvent.DynamodbStreamRecord
                    {
                        AwsRegion = "us-west-2",
                        Dynamodb = new StreamRecord
                        {
                            ApproximateCreationDateTime = DateTime.Now,
                            Keys = new Dictionary<string, AttributeValue> { {"id", new AttributeValue { S = "MyId" } } },
                            NewImage = new Dictionary<string, AttributeValue> { { "field1", new AttributeValue { S = "NewValue" } }, { "field2", new AttributeValue { S = "AnotherNewValue" } } },
                            OldImage = new Dictionary<string, AttributeValue> { { "field1", new AttributeValue { S = "OldValue" } }, { "field2", new AttributeValue { S = "AnotherOldValue" } } },
                            StreamViewType = StreamViewType.NEW_AND_OLD_IMAGES
                        }
                    }
                }
            };


            var context = new TestLambdaContext();
            var function = new CountWorks();

            await function.FunctionHandlerAsync(evnt, context);

            var testLogger = context.Logger as TestLambdaLogger;
            Assert.Contains("Stream processing complete", testLogger.Buffer.ToString());
        }
    }
}
