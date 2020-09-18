using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace DbTools
{
    class Program
    {
        private const string TableName = "Works";
        private const string KeyCreationTimestamp = "CreationTimestamp";
        private const string KeyConsumerId = "ConsumerId";
        private const string PathData = "data/works.json";

        private static readonly AmazonDynamoDBClient _client = new AmazonDynamoDBClient(
            new AmazonDynamoDBConfig
            {
                //ServiceURL = "http://localhost:8000"
                ServiceURL = "https://dynamodb.eu-west-1.amazonaws.com"
            });


        static async Task Main(string[] args)
        {
            try
            {
                await TryToDeleteTableAsync();
                await CreateTableAsync();
                await LoadDataAsync();

                Console.WriteLine("To continue, press Enter");
                Console.ReadLine();
            }
            catch (AmazonDynamoDBException e) { Console.WriteLine(e.Message); }
            catch (AmazonServiceException e) { Console.WriteLine(e.Message); }
            catch (Exception e) { Console.WriteLine(e.Message); }
        }

        private static async Task CreateTableAsync()
        {
            Console.WriteLine("\n*** Creating table ***");
            var request = new CreateTableRequest
            {
                AttributeDefinitions = new List<AttributeDefinition>()
                {
                    new AttributeDefinition
                    {
                        AttributeName = KeyConsumerId,
                        AttributeType = "S"
                    },
                    new AttributeDefinition
                    {
                        AttributeName = KeyCreationTimestamp,
                        AttributeType = "N"
                    }
                },
                KeySchema = new List<KeySchemaElement>
                {
                    new KeySchemaElement
                    {
                        AttributeName = KeyConsumerId,
                        KeyType = "HASH" //Partition key
                    },
                    new KeySchemaElement
                    {
                        AttributeName = KeyCreationTimestamp,
                        KeyType = "RANGE" //Sort key
                    }
                },
                ProvisionedThroughput = new ProvisionedThroughput
                {
                    ReadCapacityUnits = 5,
                    WriteCapacityUnits = 6
                },
                TableName = TableName
            };

            var response = await _client.CreateTableAsync(request);

            var tableDescription = response.TableDescription;
            Console.WriteLine("{1}: {0} \t ReadsPerSec: {2} \t WritesPerSec: {3}",
                      tableDescription.TableStatus,
                      tableDescription.TableName,
                      tableDescription.ProvisionedThroughput.ReadCapacityUnits,
                      tableDescription.ProvisionedThroughput.WriteCapacityUnits);

            string status = tableDescription.TableStatus;
            Console.WriteLine(TableName + " - " + status);

            await WaitUntilTableReadyAsync(TableName);
        }

        private static async Task LoadDataAsync()
        {
            var table = Table.LoadTable(_client, TableName);

            JArray items = await ReadJsonFileAsync(PathData);

            foreach (var item in items)
            {
                string itemJson = item.ToString();
                Document doc = Document.FromJson(itemJson);
                doc[KeyCreationTimestamp] = DateTime.UtcNow.Ticks;
                await table.PutItemAsync(doc);
            }
        }

        public static async Task<JArray> ReadJsonFileAsync(string path)
        {
            using var sr = new StreamReader(path);
            using var jtr = new JsonTextReader(sr);
            return (JArray)await JToken.ReadFromAsync(jtr);
        }

        private static async Task TryToDeleteTableAsync()
        {
            Console.WriteLine("\n*** Deleting table ***");

            if (await TryToGetTableStatusAsync(TableName) == null)
            {
                Console.WriteLine("Table does not exist...");
                return;
            }

            var request = new DeleteTableRequest
            {
                TableName = TableName
            };

            var response = await _client.DeleteTableAsync(request);

            Console.WriteLine("Table is being deleted...");
        }

        private static async Task WaitUntilTableReadyAsync(string tableName)
        {
            string status = null;
            // Let us wait until table is created. Call DescribeTable.
            do
            {
                System.Threading.Thread.Sleep(5000); // Wait 5 seconds.
                status = await TryToGetTableStatusAsync(tableName);
            } while (status != "ACTIVE");
        }

        private static async Task<string> TryToGetTableStatusAsync(string tableName)
        {
            try
            {
                var res = await _client.DescribeTableAsync(new DescribeTableRequest
                {
                    TableName = tableName
                });

                Console.WriteLine("Table name: {0}, status: {1}",
                          res.Table.TableName,
                          res.Table.TableStatus);
                return res.Table.TableStatus;
            }
            catch (ResourceNotFoundException)
            {
                // DescribeTable is eventually consistent. So you might
                // get resource not found. So we handle the potential exception.
            }

            return null;
        }
    }
}
