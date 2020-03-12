using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Amazon.DynamoDBv2.Tests
{
    [TestCaseOrderer("Amazon.DynamoDBv2.Tests.AlphabeticalOrderer", "dynamodb-lock-client.tests")]
    [InMemoryTester]
    [Collection("InMemoryTests")]
    public class GetAllLocksTests : InMemoryLockClientTester
    {
        /// <summary>
        /// 1 MB
        /// Dynamo DB Max Page Size Documentation
        /// http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScan.html#Pagination
        /// </summary>
        private static readonly long DYNAMODB_MAX_PAGE_SIZE_IN_BYTES = 1 << 20;

        /// <summary>
        /// 400 KB
        /// DynamoDB Max Item Size Documentation
        /// http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html#WorkingWithItems.BatchOperations
        /// </summary>
        private static readonly long DYNAMODB_MAX_ITEM_SIZE_IN_BYTES = 400 * (1 << 10);

        public GetAllLocksTests()
        {
        }

        [Fact]
        public void TestGetAllLocksFromDynamoDBNoLocks()
        {
            // ARRANGE
            bool deleteOnRelease = false;

            // ACT
            List<LockItem> allLocksFromDynamoDB = this.lockClient.GetAllLocksFromDynamoDB(deleteOnRelease).ToList();

            // ASSERT
            Assert.Empty(allLocksFromDynamoDB);
        }

        [Fact]
        public async Task TestGetAllLocksFromDynamoDBSingleLock()
        {
            // ARRANGE
            AcquireLockOptions options = new AcquireLockOptions("Test 1") { Data = TEST_DATA_STREAM };
            LockItem singleLock = await this.lockClient.AcquireLockAsync(options);
            bool deleteOnRelease = false;

            // ACT
            List<LockItem> allLocksFromDynamoDB = this.lockClient.GetAllLocksFromDynamoDB(deleteOnRelease).ToList();

            // ASSERT
            Assert.Single(allLocksFromDynamoDB);

            LockItem retrievedLock = allLocksFromDynamoDB.First();
            Assert.Equal(singleLock.PartitionKey, retrievedLock.PartitionKey);
            Assert.True(singleLock.Data.ToArray().SequenceEqual(retrievedLock.Data.ToArray()));
            Assert.Equal(singleLock.RecordVersionNumber, retrievedLock.RecordVersionNumber);

            LockItem item = await this.lockClient.GetLockAsync("Test 1");
            item.Close();

            allLocksFromDynamoDB = this.lockClient.GetAllLocksFromDynamoDB(deleteOnRelease).ToList();
            Assert.Empty(allLocksFromDynamoDB);                
        }

        [Fact]
        public async Task TestGetAllLocksFromDynamoDBMultipleLocks()
        {
            // ARRANGE
            AcquireLockOptions options1 = new AcquireLockOptions("Test 1") { Data = TEST_DATA_STREAM };
            LockItem firstLock = await this.lockClient.AcquireLockAsync(options1);

            AcquireLockOptions options2 = new AcquireLockOptions("Test 2") { Data = TEST_DATA_STREAM };
            LockItem secondLock = await this.lockClient.AcquireLockAsync(options2);

            bool deleteOnRelease = false;

            // ACT
            List<LockItem> allLocksFromDynamoDB = this.lockClient.GetAllLocksFromDynamoDB(deleteOnRelease).ToList();

            // ASSERT
            Assert.Equal(2, allLocksFromDynamoDB.Count);

            Dictionary<string, LockItem> lockItemsByKey = allLocksFromDynamoDB.ToDictionary(x => x.PartitionKey);


            LockItem retrievedLock = lockItemsByKey[firstLock.PartitionKey];
            Assert.Equal(firstLock.PartitionKey, retrievedLock.PartitionKey);
            Assert.True(firstLock.Data.ToArray().SequenceEqual(retrievedLock.Data.ToArray()));
            Assert.Equal(firstLock.RecordVersionNumber, retrievedLock.RecordVersionNumber);

            retrievedLock = lockItemsByKey[secondLock.PartitionKey];
            Assert.Equal(secondLock.PartitionKey, retrievedLock.PartitionKey);
            Assert.True(secondLock.Data.ToArray().SequenceEqual(retrievedLock.Data.ToArray()));
            Assert.Equal(secondLock.RecordVersionNumber, retrievedLock.RecordVersionNumber);

            firstLock.Close();

            allLocksFromDynamoDB = this.lockClient.GetAllLocksFromDynamoDB(deleteOnRelease).ToList();
            Assert.Single(allLocksFromDynamoDB);

            retrievedLock = allLocksFromDynamoDB.First();
            Assert.Equal(secondLock.PartitionKey, retrievedLock.PartitionKey);
            Assert.True(secondLock.Data.ToArray().SequenceEqual(retrievedLock.Data.ToArray()));
            Assert.Equal(secondLock.RecordVersionNumber, retrievedLock.RecordVersionNumber);

            secondLock.Close();

            allLocksFromDynamoDB = this.lockClient.GetAllLocksFromDynamoDB(deleteOnRelease).ToList();
            Assert.Empty(allLocksFromDynamoDB);
        }

        [Fact]
        public async Task TestGetAllLocksFromDynamoDBMultiplePagedResultSet()
        {
            // ARRANGE

            // Scan is paginated.
            // Scans with items that are larger than 1MB are paginated
            // and must be retrieved by performing multiple scans.
            // Make sure the client handles pagination correctly.
            // See
            // http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScan.html#Pagination

            long numBytesOfData = 0;
            byte[] data = new byte[(DYNAMODB_MAX_ITEM_SIZE_IN_BYTES * 9) / 10];
            long randomSeed = DateTime.Now.Ticks;

            Debug.WriteLine($"Random seed is: {randomSeed}");

            Dictionary<string, LockItem> acquiredLockItemsByKey = new Dictionary<string, LockItem>();

            while (numBytesOfData < DYNAMODB_MAX_PAGE_SIZE_IN_BYTES)
            {
                SECURE_RANDOM.NextBytes(data);

                using (MemoryStream stream = new MemoryStream(data))
                {
                    AcquireLockOptions options = new AcquireLockOptions(acquiredLockItemsByKey.Count.ToString()) { Data = stream, DeleteLockOnRelease = true };

                    LockItem acquiredLock = await this.lockClient.AcquireLockAsync(options);
                    acquiredLockItemsByKey.Add(acquiredLock.PartitionKey, acquiredLock);
                    numBytesOfData += acquiredLock.Data.Length;
                }
            }

            bool deleteOnRelease = false;
            
            // ACT
            List<LockItem> allLocksFromDynamoDB = this.lockClient.GetAllLocksFromDynamoDB(deleteOnRelease, true).ToList();

            // ASSERT

            Assert.Equal(acquiredLockItemsByKey.Count, allLocksFromDynamoDB.Count);
            Dictionary<string, LockItem> lockItemsByKey = allLocksFromDynamoDB.OrderBy(x => x.PartitionKey).ToDictionary(x => x.PartitionKey);
            Assert.True(acquiredLockItemsByKey.Keys.SequenceEqual(lockItemsByKey.Keys));

            foreach (LockItem acquiredLock in acquiredLockItemsByKey.Values)
            {
                acquiredLock.Close();
            }

            allLocksFromDynamoDB = this.lockClient.GetAllLocksFromDynamoDB(deleteOnRelease).ToList();
            Assert.Empty(allLocksFromDynamoDB);
        }
    }
}
