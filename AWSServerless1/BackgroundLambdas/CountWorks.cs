
using Amazon.Lambda.Core;
using Amazon.Lambda.DynamoDBEvents;
using System.Threading.Tasks;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace BackgroundLambdas
{
    public class CountWorks
    {
        public Task FunctionHandlerAsync(DynamoDBEvent dynamoEvent, ILambdaContext context)
        {
            context.Logger.LogLine($"Beginning to process {dynamoEvent.Records.Count} records...");

            foreach (var record in dynamoEvent.Records)
            {
                context.Logger.LogLine($"Event ID: {record.EventID}");
                context.Logger.LogLine($"Event Name: {record.EventName}");

                // TODO: Add business logic processing the record.Dynamodb object.
            }

            context.Logger.LogLine("Stream processing complete.");

            return Task.CompletedTask;
        }
    }
}