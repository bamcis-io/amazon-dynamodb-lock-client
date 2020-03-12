using Amazon.DynamoDBv2.Model;
using BAMCIS.Util.Concurrent;
using Moq;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Amazon.DynamoDBv2.Tests
{
    public class AmazonDynamoDBLockClientOptionsTest
    {

        private static Mock<IAmazonDynamoDB> dynamodb = new Mock<IAmazonDynamoDB>();

        [Fact]
        public async Task Test_OwnerName_IsMaintained()
        {
            // ARRANGE
            Guid uuid = AmazonDynamoDBLockClientTest.SetOwnerNameToUuid();

            AmazonDynamoDBLockClientOptions options = new AmazonDynamoDBLockClientOptions(dynamodb.Object, "table")
            {
                LeaseDuration = 2,
                HeartbeatPeriod = 1,
                TimeUnit = TimeUnit.SECONDS,
                PartitionKeyName = "customer",
                OwnerName = uuid.ToString()
            };

            AmazonDynamoDBLockClient client = new AmazonDynamoDBLockClient(options);

            Dictionary<string, AttributeValue> previousLockItem = new Dictionary<string, AttributeValue>()
            {
                {"ownerName", new AttributeValue("foobar") },
                {"recordVersionNumber", new AttributeValue("oolala") },
                {"leaseDuration", new AttributeValue("1") }
            };

            dynamodb.Setup(x => x.GetItemAsync(It.IsAny<GetItemRequest>(), default(CancellationToken))).ReturnsAsync(new GetItemResponse() { Item = previousLockItem });

            // ACT
            LockItem @lock = await client.AcquireLockAsync(new AcquireLockOptions("asdf"));
            
            // ASSERT
            Assert.Equal(uuid.ToString(), @lock.OwnerName);
        }
    }
}
