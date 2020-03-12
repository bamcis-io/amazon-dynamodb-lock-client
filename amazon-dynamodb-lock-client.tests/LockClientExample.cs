using Amazon.Runtime;
using BAMCIS.Util.Concurrent;
using System.Diagnostics;
using System.Threading.Tasks;
using Xunit;

namespace Amazon.DynamoDBv2.Tests
{
    /// <summary>
    /// Usage example listed in README.md. Start DynamoDB Local first on port 4567.
    /// </summary>
    public class LockClientExample
    {

        [Fact]
        public async Task UsageExample()
        {
            // ARRANGE

            // Inject client configuration to the builder like the endpoint and signing region
            IAmazonDynamoDB dynamodb = new AmazonDynamoDBClient(new BasicAWSCredentials("a", "a"), new AmazonDynamoDBConfig()
            {
                ServiceURL = "http://localhost:4567"
            });

            // Whether or not to create a heartbeating background thread
            bool createHeartbeatBackgroundThread = true;


            // Build the lock client
            IAmazonDynamoDBLockClient client = new AmazonDynamoDBLockClient(new AmazonDynamoDBLockClientOptions(dynamodb, "lockTable") { TimeUnit = TimeUnit.SECONDS, LeaseDuration = 10, HeartbeatPeriod = 3, CreateHeartbeatBackgroundThread = createHeartbeatBackgroundThread });

            try
            {
                // Create the table
                await client.CreateLockTableInDynamoDBAsync(new CreateDynamoDBTableOptions(dynamodb, "lockTable"));


                // ACT
                LockItem lockItem = await client.TryAcquireLockAsync(new AcquireLockOptions("Moe"));

                // ASSERT
                Assert.NotNull(lockItem);
                Debug.WriteLine("Acquired lock! If I die, my lock will expire in 10 seconds.");
                Debug.WriteLine("Otherwise, I will hold it until I stop heartbeating.");
                Assert.True(client.ReleaseLock(lockItem));

            }
            finally
            {
                await dynamodb.DeleteTableAsync("lockTable");
                client.Close();
            }
        }
    }
}
