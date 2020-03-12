using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2.Util;
using Amazon.Runtime;
using BAMCIS.Util.Concurrent;
using Moq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Amazon.DynamoDBv2.Tests
{
    [TestCaseOrderer("Amazon.DynamoDBv2.Tests.AlphabeticalOrderer", "dynamodb-lock-client.tests")]
    public class AmazonDynamoDBLockClientTest
    {
        private Mock<AmazonDynamoDBClient> dynamodbMock = new Mock<AmazonDynamoDBClient>()
        {
            CallBase = true
        };
        private readonly string PARTITION_KEY = "pk";
        private static readonly long TEN_THOUSAND = 10000;
        private static readonly long ONE_HUNDRED_THOUSAND = 100000;
        private static readonly long THREE_THOUSAND = 3000;
        private const string OWNER_NAME = "ownerName";


        [Fact]
        public async Task ReleaseLock_WhenRemoveKillSessionMonitorJoinInterrupted_SwallowsInterruptedException()
        {
            // ARRANGE         
            AmazonDynamoDBLockClient lockClient = this.GetLockClientMock().Object;

            Dictionary<string, AttributeValue> item = new Dictionary<string, AttributeValue>()
            {
                { "customer", new AttributeValue("customer1") },
                { "ownerName", new AttributeValue("foobar") },
                { "recordVersionNumber", new AttributeValue("oolala") },
                { "leaseDuration", new AttributeValue("1") }
            };

            dynamodbMock.Setup(x => x.GetItemAsync(It.IsAny<GetItemRequest>(), default(CancellationToken))).ReturnsAsync(new GetItemResponse() { Item = item });
            dynamodbMock.Setup(x => x.PutItemAsync(It.IsAny<PutItemRequest>(), default(CancellationToken))).ReturnsAsync(new PutItemResponse() { });
            dynamodbMock.Setup(x => x.DeleteItemAsync(It.IsAny<DeleteItemRequest>(), default(CancellationToken))).ReturnsAsync(new DeleteItemResponse() { });

            // ACT
            AcquireLockOptions options = new AcquireLockOptions(PARTITION_KEY)
            {
                TimeUnit = TimeUnit.MILLISECONDS
            };
            options.AddSessionMonitor(3001, () => Console.WriteLine("monitored"), TimeUnit.MILLISECONDS);

            LockItem lockItem = await lockClient.AcquireLockAsync(options);

            // ASSERT
            //cts.Cancel();
            lockClient.ReleaseLock(lockItem);
        }

        [Fact]
        public async Task LockTableExists_WhenTableIsUpdating_ReturnTrue()
        {
            // ARRANGE
            dynamodbMock.Setup(x => x.DescribeTableAsync(It.IsAny<DescribeTableRequest>(), default(CancellationToken))).ReturnsAsync(new DescribeTableResponse() { Table = new TableDescription() { TableStatus = TableStatus.UPDATING } });

            // ACT
            AmazonDynamoDBLockClient lockClient = this.GetLockClientMock().Object;

            // ASSERT
            Assert.True(await lockClient.LockTableExistsAsync());
        }

        [Fact]
        public async Task LockTableExists_WhenTableIsActive_ReturnTrue()
        {
            // ARRANGE
            dynamodbMock.Setup(x => x.DescribeTableAsync(It.IsAny<DescribeTableRequest>(), default(CancellationToken))).ReturnsAsync(new DescribeTableResponse() { Table = new TableDescription() { TableStatus = TableStatus.ACTIVE } });

            // ACT
            AmazonDynamoDBLockClient lockClient = this.GetLockClientMock().Object;

            // ASSERT
            Assert.True(await lockClient.LockTableExistsAsync());
        }

        [Fact]
        public async Task LockTableExists_WhenTableIsDeleting_ReturnFalse()
        {
            // ARRANGE
            dynamodbMock.Setup(x => x.DescribeTableAsync(It.IsAny<DescribeTableRequest>(), default(CancellationToken))).ReturnsAsync(new DescribeTableResponse() { Table = new TableDescription() { TableStatus = TableStatus.DELETING } });

            // ACT
            AmazonDynamoDBLockClient lockClient = this.GetLockClientMock().Object;

            // ASSERT
            Assert.False(await lockClient.LockTableExistsAsync());
        }

        [Fact]
        public async Task LockTableExists_WhenTableIsCreating_ReturnFalse()
        {
            // ARRANGE
            dynamodbMock.Setup(x => x.DescribeTableAsync(It.IsAny<DescribeTableRequest>(), default(CancellationToken))).ReturnsAsync(new DescribeTableResponse() { Table = new TableDescription() { TableStatus = TableStatus.CREATING } });

            // ACT
            AmazonDynamoDBLockClient lockClient = this.GetLockClientMock().Object;

            // ASSERT
            Assert.False(await lockClient.LockTableExistsAsync());
        }

        [Fact]
        public async Task AssertLockTableExists_WhenTableIsUpdating_ReturnTrue()
        {
            // ARRANGE
            dynamodbMock.Setup(x => x.DescribeTableAsync(It.IsAny<DescribeTableRequest>(), default(CancellationToken))).ReturnsAsync(new DescribeTableResponse() { Table = new TableDescription() { TableStatus = TableStatus.UPDATING } });
            dynamodbMock.Setup(x => x.DescribeTableAsync(It.IsAny<DescribeTableRequest>(), default(CancellationToken))).ThrowsAsync(new LockTableDoesNotExistException());

            // ACT
            AmazonDynamoDBLockClient lockClient = this.GetLockClientMock().Object;

            // ASSERT
            await Assert.ThrowsAsync<LockTableDoesNotExistException>(() => lockClient.AssertLockTableExistsAsync());
        }

        [Fact]
        public async Task AcquireLock_WhenLockAlreadyExists_ThrowLockNotGrantedException()
        {
            // ARRANGE
            AmazonDynamoDBLockClient lockClient = this.GetLockClientMock("foobar").Object;
            Dictionary<string, AttributeValue> item = new Dictionary<string, AttributeValue>()
            {
                { "ownerName", new AttributeValue("foobar") },
                { "recordVersionNumber", new AttributeValue("oolala") },
                { "leaseDuration", new AttributeValue("1") }
            };

            dynamodbMock.Setup(x => x.GetItemAsync(It.IsAny<GetItemRequest>(), default(CancellationToken))).ReturnsAsync(new GetItemResponse() { Item = item });
            dynamodbMock.Setup(x => x.PutItemAsync(It.IsAny<PutItemRequest>(), default(CancellationToken))).ThrowsAsync(new ConditionalCheckFailedException("item existed"));

            // ACT

            // ASSERT
            await Assert.ThrowsAsync<LockNotGrantedException>(() => lockClient.AcquireLockAsync(new AcquireLockOptions("asdf")));
        }

        [Fact]
        public async Task AcquireLock_WhenProvisionedThroughputExceeds_ThrowLockNotGrantedException()
        {
            // ARRANGE
            AmazonDynamoDBLockClient lockClient = this.GetLockClientMock("foobar").Object;
            Dictionary<string, AttributeValue> item = new Dictionary<string, AttributeValue>()
            {
                { "customer", new AttributeValue("customer1") },
                { "ownerName", new AttributeValue("foobar") },
                { "recordVersionNumber", new AttributeValue("oolala") },
                { "leaseDuration", new AttributeValue("1") }
            };

            dynamodbMock.Setup(x => x.GetItemAsync(It.IsAny<GetItemRequest>(), default(CancellationToken))).ReturnsAsync(new GetItemResponse() { Item = item });
            dynamodbMock.Setup(x => x.PutItemAsync(It.IsAny<PutItemRequest>(), default(CancellationToken))).ThrowsAsync(new ProvisionedThroughputExceededException("Provisioned Throughput for the table exceeded"));

            // ACT

            // ASSERT
            await Assert.ThrowsAsync<LockNotGrantedException>(() => lockClient.AcquireLockAsync(new AcquireLockOptions("asdf")));
        }

        [Fact]
        public async Task AcquireLock_WhenLockAlreadyExists_ThrowIllegalArgumentException()
        {
            // ARRANGE
            AmazonDynamoDBLockClient lockClient = this.GetLockClientWithSortKeyMock().Object;
            Dictionary<string, AttributeValue> additionalAttributes = new Dictionary<string, AttributeValue>()
            {
                { "sort", new AttributeValue("cool") }
            };

            // ACT

            // ASSERT
            await Assert.ThrowsAsync<ArgumentException>(() => lockClient.AcquireLockAsync(new AcquireLockOptions("asdf") { SortKey = "sort", AdditionalAttributes = additionalAttributes }));
        }

        [Fact]
        public async Task AcquireLock_WhenLockDoesNotExist_AndWhenAcquireOnlyIfLockAlreadyExistsTrue_ThrowLockNotGrantedException()
        {
            // ARRANGE
            AmazonDynamoDBLockClient lockClient = this.GetLockClientMock().Object;
            
            dynamodbMock.Setup(x => x.GetItemAsync(It.IsAny<GetItemRequest>(), default(CancellationToken))).ReturnsAsync(new GetItemResponse() { Item = null });

            // ACT

            // ASSERT
            await Assert.ThrowsAsync<LockNotGrantedException>(() => lockClient.AcquireLockAsync(new AcquireLockOptions("asdf") { AcquireOnlyIfLockAlreadyExists = true }));
        }

        [Fact]
        public async Task AcquireLock_WithAcquireOnlyIfLockAlreadyExistsTrue_ReleasedLockConditionalCheckFailure()
        {
            // ARRANGE
            AmazonDynamoDBLockClient lockClient = this.GetLockClientMock("foobar").Object;
            Dictionary<string, AttributeValue> item = new Dictionary<string, AttributeValue>()
            {
                { "customer", new AttributeValue("customer1") },
                { "ownerName", new AttributeValue("foobar") },
                { "recordVersionNumber", new AttributeValue("oolala") },
                { "leaseDuration", new AttributeValue("1") },
                { "isReleased", new AttributeValue() { BOOL = true } }
            };

            dynamodbMock.Setup(x => x.GetItemAsync(It.IsAny<GetItemRequest>(), default(CancellationToken))).ReturnsAsync(new GetItemResponse() { Item = item });
            dynamodbMock.Setup(x => x.PutItemAsync(It.IsAny<PutItemRequest>(), default(CancellationToken))).ThrowsAsync(new ConditionalCheckFailedException("item existed"));

            // ACT

            // ASSERT
            await Assert.ThrowsAsync<LockNotGrantedException>(() => lockClient.AcquireLockAsync(new AcquireLockOptions("asdf") { AcquireOnlyIfLockAlreadyExists = true }));
        }

        [Fact]
        public async Task AcquireLock_WithAcquireOnlyIfLockAlreadyExists_ReleasedLockGetsCreated()
        {
            // ARRANGE
            AmazonDynamoDBLockClient lockClient = this.GetLockClientMock("foobar").Object;
            Dictionary<string, AttributeValue> item = new Dictionary<string, AttributeValue>()
            {
                { "customer", new AttributeValue("customer1") },
                { "ownerName", new AttributeValue("foobar") },
                { "recordVersionNumber", new AttributeValue("oolala") },
                { "leaseDuration", new AttributeValue("1") },
                { "isReleased", new AttributeValue() { BOOL = true } }
            };

            dynamodbMock.Setup(x => x.GetItemAsync(It.IsAny<GetItemRequest>(), default(CancellationToken))).ReturnsAsync(new GetItemResponse() { Item = item });
            dynamodbMock.Setup(x => x.PutItemAsync(It.IsAny<PutItemRequest>(), default(CancellationToken))).ReturnsAsync(new PutItemResponse() { Attributes = item });

            // ACT
            LockItem lockItem = await lockClient.AcquireLockAsync(new AcquireLockOptions("asdf") { AcquireOnlyIfLockAlreadyExists = true });

            // ASSERT
            Assert.NotNull(lockItem);
            Assert.Equal("asdf", lockItem.PartitionKey);
        }

        [Fact]
        public async Task AcquireLock_WhenLockAlreadyExistsAndIsNotReleased_AndWhenHaveSleptForMinimumLeaseDurationTime_SkipsAddingLeaseDuration()
        {
            // ARRANGE
            AmazonDynamoDBLockClient lockClient = this.GetLockClientMock("foobar").Object;
            Dictionary<string, AttributeValue> item = new Dictionary<string, AttributeValue>()
            {
                { "customer", new AttributeValue("customer1") },
                { "ownerName", new AttributeValue("foobar") },
                { "recordVersionNumber", new AttributeValue("oolala") },
                { "leaseDuration", new AttributeValue("1") }
            };

            Dictionary<string, AttributeValue> differentItem = new Dictionary<string, AttributeValue>(item);
            differentItem["recordVersionNumber"] = new AttributeValue("a different uuid");


            dynamodbMock.Setup(x => x.PutItemAsync(It.IsAny<PutItemRequest>(), default(CancellationToken))).ReturnsAsync(new PutItemResponse() { Attributes = item });
            dynamodbMock.SetupSequence(x => x.GetItemAsync(It.IsAny<GetItemRequest>(), default(CancellationToken))).ReturnsAsync(new GetItemResponse() { Item = item }).ReturnsAsync(new GetItemResponse() { Item = differentItem }).ReturnsAsync(new GetItemResponse() { Item = differentItem } );

            // ACT
            LockItem lockItem = await lockClient.AcquireLockAsync(new AcquireLockOptions("customer1") { RefreshPeriod = 800, AdditionalTimeToWaitForLock = ONE_HUNDRED_THOUSAND, TimeUnit = TimeUnit.MILLISECONDS, DeleteLockOnRelease = false });

            // ASSERT
            Assert.NotNull(lockItem);
        }

        [Fact]
        public async Task AcquireLock_WithConsistentLockDataTrue_ReleasedLockConditionalCheckFailure()
        {
            // ARRANGE
            AmazonDynamoDBLockClient lockClient = this.GetLockClientMock("foobar").Object;
            Dictionary<string, AttributeValue> item = new Dictionary<string, AttributeValue>()
            {
                { "customer", new AttributeValue("customer1") },
                { "ownerName", new AttributeValue("foobar") },
                { "recordVersionNumber", new AttributeValue("oolala") },
                { "leaseDuration", new AttributeValue("1") },
                { "isReleased", new AttributeValue() { BOOL = true } }
            };

            dynamodbMock.Setup(x => x.GetItemAsync(It.IsAny<GetItemRequest>(), default(CancellationToken))).ReturnsAsync(new GetItemResponse() { Item = item });
            dynamodbMock.Setup(x => x.PutItemAsync(It.IsAny<PutItemRequest>(), default(CancellationToken))).ThrowsAsync(new ConditionalCheckFailedException("RVN constraint failed"));

            // ACT

            // ASSERT
            await Assert.ThrowsAsync<LockNotGrantedException>(() => lockClient.AcquireLockAsync(new AcquireLockOptions("asdf") { AcquireReleasedLocksConsistently = true }));
        }

        [Fact]
        public async Task AcquireLock_WithNotUpdateRecordAndConsistentLockDataTrue_ReleasedLockGetsCreated()
        {
            // ARRANGE
            AmazonDynamoDBLockClient lockClient = this.GetLockClientMock("foobar").Object;
            Dictionary<string, AttributeValue> item = new Dictionary<string, AttributeValue>()
            {
                { "customer", new AttributeValue("customer1") },
                { "ownerName", new AttributeValue("foobar") },
                { "recordVersionNumber", new AttributeValue("a specific rvn") },
                { "leaseDuration", new AttributeValue("1") },
                { "isReleased", new AttributeValue() { BOOL = true } }
            };

            List<PutItemRequest> requests = new List<PutItemRequest>();
            dynamodbMock.Setup(x => x.GetItemAsync(It.IsAny<GetItemRequest>(), default(CancellationToken))).ReturnsAsync(new GetItemResponse() { Item = item });
            dynamodbMock.Setup(x => x.PutItemAsync(Capture.In<PutItemRequest>(requests), default(CancellationToken))).ReturnsAsync(new PutItemResponse());

            // ACT
            LockItem lockItem = await lockClient.AcquireLockAsync(new AcquireLockOptions("asdf") { AcquireReleasedLocksConsistently = true, UpdateExistingLockRecord = false });

            // ASSERT
            Assert.NotNull(lockItem);
            Assert.Equal("asdf", lockItem.PartitionKey);

            PutItemRequest putItemRequest = requests.First();
            dynamodbMock.Verify(x => x.PutItemAsync(putItemRequest, default(CancellationToken)));
            
            Assert.Equal(typeof(AmazonDynamoDBLockClient).GetField("PK_EXISTS_AND_RVN_IS_THE_SAME_AND_IS_RELEASED_CONDITION", BindingFlags.NonPublic | BindingFlags.Static).GetValue(null), putItemRequest.ConditionExpression);
            Assert.Equal("a specific rvn", putItemRequest.ExpressionAttributeValues[typeof(AmazonDynamoDBLockClient).GetField("RVN_VALUE_EXPRESSION_VARIABLE", BindingFlags.NonPublic | BindingFlags.Static).GetValue(null) as string].S);
        }

        [Fact]
        public async Task AcquireLock_WithNotUpdateRecordAndConsistentLockDataTrue_ReleasedLockGetsCreated2()
        {
            // ARRANGE
            AmazonDynamoDBLockClient lockClient = this.GetLockClientMock("foobar").Object;
            Dictionary<string, AttributeValue> item = new Dictionary<string, AttributeValue>()
            {
                { "customer", new AttributeValue("customer1") },
                { "ownerName", new AttributeValue("foobar") },
                { "recordVersionNumber", new AttributeValue("a specific rvn") },
                { "leaseDuration", new AttributeValue("1") },
                { "isReleased", new AttributeValue() { BOOL = true } }
            };


            ArgumentCaptor<PutItemRequest> putItemCaptor = new ArgumentCaptor<PutItemRequest>();

            List<PutItemRequest> requests = new List<PutItemRequest>();
            dynamodbMock.Setup(x => x.GetItemAsync(It.IsAny<GetItemRequest>(), default(CancellationToken))).ReturnsAsync(new GetItemResponse() { Item = item });
            dynamodbMock.Setup(x => x.PutItemAsync(It.IsAny<PutItemRequest>(), default(CancellationToken))).ReturnsAsync(new PutItemResponse());

            // ACT
            LockItem lockItem = await lockClient.AcquireLockAsync(new AcquireLockOptions("asdf") { AcquireReleasedLocksConsistently = true, UpdateExistingLockRecord = false });

            // ASSERT
            Assert.NotNull(lockItem);
            Assert.Equal("asdf", lockItem.PartitionKey);
            dynamodbMock.Verify(x => x.PutItemAsync(putItemCaptor.Capture(), default(CancellationToken)));
            PutItemRequest putItemRequest = putItemCaptor.Value;

            Assert.Equal(typeof(AmazonDynamoDBLockClient).GetField("PK_EXISTS_AND_RVN_IS_THE_SAME_AND_IS_RELEASED_CONDITION", BindingFlags.NonPublic | BindingFlags.Static).GetValue(null), putItemRequest.ConditionExpression);
            Assert.Equal("a specific rvn", putItemRequest.ExpressionAttributeValues[typeof(AmazonDynamoDBLockClient).GetField("RVN_VALUE_EXPRESSION_VARIABLE", BindingFlags.NonPublic | BindingFlags.Static).GetValue(null) as string].S);
        }

        [Fact]
        public async Task AcquireLock_WithUpdateRecordAndConsistentLockDataTrue_ReleasedLockGetsCreated()
        {
            // ARRANGE
            AmazonDynamoDBLockClient lockClient = this.GetLockClientMock("foobar").Object;
            Dictionary<string, AttributeValue> item = new Dictionary<string, AttributeValue>()
            {
                { "customer", new AttributeValue("customer1") },
                { "ownerName", new AttributeValue("foobar") },
                { "recordVersionNumber", new AttributeValue("a specific rvn") },
                { "leaseDuration", new AttributeValue("1") },
                { "isReleased", new AttributeValue() { BOOL = true } }
            };

            List<UpdateItemRequest> requests = new List<UpdateItemRequest>();
            dynamodbMock.Setup(x => x.GetItemAsync(It.IsAny<GetItemRequest>(), default(CancellationToken))).ReturnsAsync(new GetItemResponse() { Item = item });
            dynamodbMock.Setup(x => x.UpdateItemAsync(Capture.In<UpdateItemRequest>(requests), default(CancellationToken))).ReturnsAsync(new UpdateItemResponse());

            // ACT
            LockItem lockItem = await lockClient.AcquireLockAsync(new AcquireLockOptions("asdf") { AcquireReleasedLocksConsistently = true, UpdateExistingLockRecord = true });

            // ASSERT
            Assert.NotNull(lockItem);
            Assert.Equal("asdf", lockItem.PartitionKey);

            UpdateItemRequest updateItemRequest = requests.First();
            dynamodbMock.Verify(x => x.UpdateItemAsync(updateItemRequest, default(CancellationToken)));

            Assert.Equal(typeof(AmazonDynamoDBLockClient).GetField("PK_EXISTS_AND_RVN_IS_THE_SAME_AND_IS_RELEASED_CONDITION", BindingFlags.NonPublic | BindingFlags.Static).GetValue(null), updateItemRequest.ConditionExpression);
            Assert.Equal("a specific rvn", updateItemRequest.ExpressionAttributeValues[typeof(AmazonDynamoDBLockClient).GetField("RVN_VALUE_EXPRESSION_VARIABLE", BindingFlags.NonPublic | BindingFlags.Static).GetValue(null) as string].S);
        }

        /// <summary>
        /// Test case that tests that the lock was successfully acquired when the lock does not exist in the table.
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task AcquireLock_WhenLockNotExists_AndSkipBlockingWaitIsTurnedOn()
        {
            // ARRANGE
            AmazonDynamoDBLockClient lockClient = this.GetLockClientMock().Object;
            dynamodbMock.Setup(x => x.GetItemAsync(It.IsAny<GetItemRequest>(), default(CancellationToken))).ReturnsAsync(new GetItemResponse());
            dynamodbMock.Setup(x => x.PutItemAsync(It.IsAny<PutItemRequest>(), default(CancellationToken))).ReturnsAsync(new PutItemResponse());

            // ACT
            LockItem lockItem = await lockClient.AcquireLockAsync(new AcquireLockOptions("customer1") { ShouldSkipBlockingWait = true, DeleteLockOnRelease = false });

            // ASSERT
            Assert.NotNull(lockItem);
        }

        /// <summary>
        /// Test case that tests that the lock was successfully acquired when the lock exist in the table and the lock has
        /// past the lease duration. This is for cases where the first owner(host) who acquired the lock abruptly died
        /// without releasing the lock before the expiry of the lease duration.
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task AcquireLock_WhenLockExistsAndIsExpired_AndSkipBlockingWaitIsTurnedOn()
        {
            // ARRANGE
            AmazonDynamoDBLockClient lockClient = this.GetLockClientMock("foobar").Object;
            Dictionary<string, AttributeValue> item = new Dictionary<string, AttributeValue>()
            {
                { "customer", new AttributeValue("customer1") },
                { "ownerName", new AttributeValue("foobar") },
                { "recordVersionNumber", new AttributeValue("a specific rvn") },
                { "leaseDuration", new AttributeValue("1") },
                { "isReleased", new AttributeValue() { BOOL = true } }
            };

            dynamodbMock.SetupSequence(x => x.GetItemAsync(It.IsAny<GetItemRequest>(), default(CancellationToken))).ReturnsAsync(new GetItemResponse() { Item = item }).ReturnsAsync(new GetItemResponse());
            dynamodbMock.Setup(x => x.PutItemAsync(It.IsAny<PutItemRequest>(), default(CancellationToken))).ReturnsAsync(new PutItemResponse());

            // ACT
            LockItem lockItem = await lockClient.AcquireLockAsync(new AcquireLockOptions("customer1") { ShouldSkipBlockingWait = true, DeleteLockOnRelease = false });

            // ASSERT
            Assert.NotNull(lockItem);          
        }

        [Fact]
        public async Task AcquireLock_WhenLockAlreadyExistsAndIsNotReleased_AndSkipBlockingWait_ThrowsAlreadyOwnedException()
        {
            // ARRANGE
            AmazonDynamoDBLockClient lockClient = this.GetLockClientMock("foobar").Object;
            Dictionary<string, AttributeValue> item = new Dictionary<string, AttributeValue>()
            {
                { "customer", new AttributeValue("customer1") },
                { "ownerName", new AttributeValue("foobar") },
                { "recordVersionNumber", new AttributeValue("a specific rvn") },
                { "leaseDuration", new AttributeValue(ONE_HUNDRED_THOUSAND.ToString()) }
            };

            dynamodbMock.SetupSequence(x => x.GetItemAsync(It.IsAny<GetItemRequest>(), default(CancellationToken))).ReturnsAsync(new GetItemResponse() { Item = item }).ReturnsAsync(new GetItemResponse());
            dynamodbMock.Setup(x => x.PutItemAsync(It.IsAny<PutItemRequest>(), default(CancellationToken))).ReturnsAsync(new PutItemResponse());

            // ACT

            // ASSERT
            await Assert.ThrowsAsync<LockCurrentlyUnavailableException>(() => lockClient.AcquireLockAsync(new AcquireLockOptions("customer1") { ShouldSkipBlockingWait = true, DeleteLockOnRelease = false }));
        }

        [Fact]
        public void SendHeartbeat_WhenDeleteDataTrueAndDataNotNull_ThrowsIllegalArgumentException()
        {
            // ARRANGE
            AmazonDynamoDBLockClient lockClient = this.GetLockClientMock().Object;
            LockItem lockItem = CreateLockItem(lockClient);

            // ACT

            // ASSERT
            Assert.Throws<ArgumentException>(() => lockClient.SendHeartbeat(new SendHeartbeatOptions(lockItem) { DeleteData = true, Data = new MemoryStream(Encoding.UTF8.GetBytes("data")) }));
        }

        [Fact]
        public void SendHeartbeat_WhenExpired_ThrowsLockNotGrantedException()
        {
            // ARRANGE
            AmazonDynamoDBLockClient lockClient = this.GetLockClientMock().Object;
            LockItem lockItem = CreateLockItem(lockClient);

            // ACT

            // ASSERT
            Assert.Throws<LockNotGrantedException>(() => lockClient.SendHeartbeat(new SendHeartbeatOptions(lockItem) { Data = new MemoryStream(Encoding.UTF8.GetBytes("data")) }));
        }

        [Fact]
        public void SendHeartbeat_WhenNotExpiredAndDifferentOwner_ThrowsLockNotGrantedException()
        {
            // ARRANGE
            AmazonDynamoDBLockClient lockClient = this.GetLockClientMock().Object;
            LockItem lockItem = CreateLockItem(lockClient, "differentOwner");

            // ACT

            // ASSERT
            Assert.Throws<LockNotGrantedException>(() => lockClient.SendHeartbeat(new SendHeartbeatOptions(lockItem) {  Data = new MemoryStream(Encoding.UTF8.GetBytes("data")) }));
        }

        [Fact]
        public void SendHeartbeat_WhenNotExpired_AndSameOwner_ReleasedTrue_ThrowsLockNotGrantedException()
        {
            // ARRANGE
            Guid owner = Guid.NewGuid();
            AmazonDynamoDBLockClient lockClient = this.GetLockClientMock(owner.ToString()).Object;
            LockItem lockItem = CreateReleasedLockItemWithMaxLastUpdatedTime(lockClient, owner.ToString());

            // ACT

            // ASSERT
            Assert.Throws<LockNotGrantedException>(() => lockClient.SendHeartbeat(new SendHeartbeatOptions(lockItem) { Data = new MemoryStream(Encoding.UTF8.GetBytes("data")) }));
        }

        [Fact]
        public void SendHeartbeat_WhenNotExpired_AndSameOwner_ReleasedFalse_SetsRequestMetricCollector()
        {
            // ARRANGE
            Guid owner = Guid.NewGuid();
            AmazonDynamoDBLockClient lockClient = this.GetLockClientMock(owner.ToString()).Object;
            LockItem lockItem = CreateLockItemWithMaxLastUpdatedTime(lockClient, owner.ToString());
            dynamodbMock.Setup(x => x.UpdateItemAsync(It.IsAny<UpdateItemRequest>(), default(CancellationToken))).ReturnsAsync(new UpdateItemResponse());

            // ACT
            lockClient.SendHeartbeat(new SendHeartbeatOptions(lockItem) { Data = new MemoryStream(Encoding.UTF8.GetBytes("data")) });

            // ASSERT
        }

        [Fact]
        public void SendHeartbeat_WhenServiceUnavailable_AndHoldLockOnServiceUnavailableFalse_ThenDoNotUpdateLookupTime()
        {
            // ARRANGE
            AmazonServiceException serviceUnavailableException = new AmazonServiceException("Service Unavailable.", ErrorType.Receiver, "1", "1", HttpStatusCode.ServiceUnavailable);
            Guid owner = Guid.NewGuid();
            dynamodbMock.Setup(x => x.UpdateItemAsync(It.IsAny<UpdateItemRequest>(), default(CancellationToken))).ThrowsAsync(serviceUnavailableException);

            AmazonDynamoDBLockClient lockClient = this.GetLockClientWithHoldOnServiceUnavailableMock(owner.ToString()).Object;

            long lastUpdatedTimeMilliseconds = LockClientUtils.MillisecondTime();
            LockItem lockItem = CreateLockItem(lockClient, TEN_THOUSAND, lastUpdatedTimeMilliseconds, owner.ToString());

            AmazonServiceException ex = null;

            // ACT
            try
            {
                lockClient.SendHeartbeat(new SendHeartbeatOptions(lockItem));
            }
            catch (AmazonServiceException e)
            {
                ex = e;
            }

            // ASSERT
            Assert.Equal(serviceUnavailableException, ex);
            Assert.Equal(lastUpdatedTimeMilliseconds, lockItem.LookupTime);
        }

        [Fact]
        public void SendHeartbeat_WhenServiceUnavailable_AndHoldLockOnServiceUnavailableTrue_ThenUpdateLookupTimeUsingUpdateLookUpTimeMethod()
        {
            // ARRANGE
            AmazonServiceException serviceUnavailableException = new AmazonServiceException("Service Unavailable.", ErrorType.Receiver, "1", "1", HttpStatusCode.ServiceUnavailable);
            Guid owner = Guid.NewGuid();
            dynamodbMock.Setup(x => x.UpdateItemAsync(It.IsAny<UpdateItemRequest>(), default(CancellationToken))).ThrowsAsync(serviceUnavailableException);

            AmazonDynamoDBLockClient lockClient = this.GetLockClientWithHoldOnServiceUnavailableMock(owner.ToString(), true).Object;

            long lastUpdatedTimeMilliseconds = LockClientUtils.MillisecondTime();
            long leaseDuration = TEN_THOUSAND;
            Mock<LockItem> lockItem = CreateLockItemMock(lockClient, leaseDuration, lastUpdatedTimeMilliseconds, owner.ToString());

            // ACT

            // This is to make sure that the lookup time has a higher value
            Thread.Sleep(1000);
            lockClient.SendHeartbeat(new SendHeartbeatOptions(lockItem.Object));

            // ASSERT
            Assert.True(lockItem.Object.LookupTime > lastUpdatedTimeMilliseconds);
            lockItem.Verify(x => x.UpdateLookupTime(It.IsAny<long>()), Times.Once);
            lockItem.Verify(x => x.UpdateRecordVersionNumber(It.IsAny<string>(), It.IsAny<long>(), It.IsAny<long>()), Times.Never);
        }

        #region Internal Methods

        public static Guid SetOwnerNameToUuid()
        {
            Guid guid = Guid.NewGuid();
            return guid;
        }

        private Mock<AmazonDynamoDBLockClient> GetLockClientMock(string ownerId = OWNER_NAME)
        {
            AmazonDynamoDBLockClientOptions options = new AmazonDynamoDBLockClientOptions(this.dynamodbMock.Object, "locks", ownerId)
            {
                HeartbeatPeriod = THREE_THOUSAND,
                LeaseDuration = TEN_THOUSAND,
                TimeUnit = TimeUnit.MILLISECONDS,
                PartitionKeyName = "customer",
                CreateHeartbeatBackgroundThread = false
            };

            return new Mock<AmazonDynamoDBLockClient>(options)
            {
                CallBase = true
            };
        }

        private Mock<AmazonDynamoDBLockClient> GetLockClientWithHoldOnServiceUnavailableMock(string ownerId = OWNER_NAME, bool holdLock = false)
        {
            AmazonDynamoDBLockClientOptions options = new AmazonDynamoDBLockClientOptions(this.dynamodbMock.Object, "locks", ownerId)
            {
                HeartbeatPeriod = THREE_THOUSAND,
                LeaseDuration = TEN_THOUSAND,
                TimeUnit = TimeUnit.MILLISECONDS,
                PartitionKeyName = "customer",
                CreateHeartbeatBackgroundThread = false,
                HoldLockOnServiceUnavailable = holdLock
            };

            return new Mock<AmazonDynamoDBLockClient>(options)
            {
                CallBase = true
            };
        }

        private Mock<AmazonDynamoDBLockClient> GetLockClientMock(CancellationTokenSource cts)
        {
            AmazonDynamoDBLockClientOptions options = new AmazonDynamoDBLockClientOptions(this.dynamodbMock.Object, "locks", OWNER_NAME)
            {
                HeartbeatPeriod = THREE_THOUSAND,
                LeaseDuration = TEN_THOUSAND,
                TimeUnit = TimeUnit.MILLISECONDS,
                PartitionKeyName = "customer",
                CreateHeartbeatBackgroundThread = false,
                BackgroundTaskToken = cts
            };

            return new Mock<AmazonDynamoDBLockClient>(options)
            {
                CallBase = true
            };
        }

        private Mock<AmazonDynamoDBLockClient> GetLockClientWithSortKeyMock()
        {
            AmazonDynamoDBLockClientOptions options = new AmazonDynamoDBLockClientOptions(this.dynamodbMock.Object, "locks", null)
            {
                HeartbeatPeriod = THREE_THOUSAND,
                LeaseDuration = TEN_THOUSAND,
                TimeUnit = TimeUnit.MILLISECONDS,
                PartitionKeyName = "customer",
                CreateHeartbeatBackgroundThread = false,
                SortKeyName = "sort"
            };

            return new Mock<AmazonDynamoDBLockClient>(options)
            {
                CallBase = true
            };
        }

        private static LockItem CreateLockItem(AmazonDynamoDBLockClient client, string owner = OWNER_NAME)
        {
            return CreateLockItem(client, 1, 2, owner);
        }

        private static LockItem CreateLockItem(AmazonDynamoDBLockClient client, long leaseDuration, long lastUpdatedTimeInMilliseconds, string owner = OWNER_NAME)
        {
            return (LockItem)Activator.CreateInstance(typeof(LockItem), BindingFlags.NonPublic | BindingFlags.Instance, null, new object[] {
                client,
                "a",
                String.Empty,
                new MemoryStream(Encoding.UTF8.GetBytes("data")),
                false,
                owner,
                leaseDuration, // Lease Duration
                lastUpdatedTimeInMilliseconds, // Last updated time in milliseconds
                Guid.NewGuid().ToString(),
                false, // Released
                null, //session monitor
                new Dictionary<string, AttributeValue>()
            }, null);
        }

        private static Mock<LockItem> CreateLockItemMock(AmazonDynamoDBLockClient client, long leaseDuration, long lastUpdatedTimeInMilliseconds, string owner = OWNER_NAME)
        {
            return new Mock<LockItem>(
                client,
                "a",
                String.Empty,
                new MemoryStream(Encoding.UTF8.GetBytes("data")),
                false,
                owner,
                leaseDuration, // Lease Duration
                lastUpdatedTimeInMilliseconds, // Last updated time in milliseconds
                Guid.NewGuid().ToString(),
                false, // Released
                null, //session monitor
                new Dictionary<string, AttributeValue>()
            )
            {
                CallBase = true
            };
        }

        private static LockItem CreateLockItemWithMaxLastUpdatedTime(AmazonDynamoDBLockClient client, string owner = OWNER_NAME)
        {
            return (LockItem)Activator.CreateInstance(typeof(LockItem), BindingFlags.NonPublic | BindingFlags.Instance, null, new object[] {
                client,
                "a",
                String.Empty,
                new MemoryStream(Encoding.UTF8.GetBytes("data")),
                false,
                owner,
                1, // Lease Duration
                long.MaxValue, // Last updated time in milliseconds
                Guid.NewGuid().ToString(),
                false, // Released
                null, //session monitor
                new Dictionary<string, AttributeValue>()
            }, null);
        }

        private static LockItem CreateReleasedLockItemWithMaxLastUpdatedTime(AmazonDynamoDBLockClient client, string owner = OWNER_NAME)
        {
            return (LockItem)Activator.CreateInstance(typeof(LockItem), BindingFlags.NonPublic | BindingFlags.Instance, null, new object[] {
                client,
                "a",
                String.Empty,
                new MemoryStream(Encoding.UTF8.GetBytes("data")),
                false,
                owner,
                1, // Lease Duration
                long.MaxValue, // Last updated time in milliseconds
                Guid.NewGuid().ToString(),
                true, // Released
                null, //session monitor
                new Dictionary<string, AttributeValue>()
            }, null);
        }

        #endregion
    }
}
