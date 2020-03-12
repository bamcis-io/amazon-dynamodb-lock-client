using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2.Util;
using Amazon.Runtime;
using BAMCIS.Util.Concurrent;
using Moq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Amazon.DynamoDBv2.Tests
{
    [TestCaseOrderer("Amazon.DynamoDBv2.Tests.AlphabeticalOrderer", "dynamodb-lock-client.tests")]
    [InMemoryTester]
    [Collection("InMemoryTests")]
    public class BasicLockClientTests : InMemoryLockClientTester//, IClassFixture<InMemoryLockClientFixture>
    {
        //private InMemoryLockClientFixture fixture;
        private static readonly string INTEGRATION_TESTER_2 = "integrationTester2";
        private static readonly string INTEGRATION_TESTER_3 = "integrationTester3";
        private static readonly int DEFAULT_LEASE_DURATION_SECONDS = 3;

        public BasicLockClientTests() : base(DEFAULT_LEASE_DURATION_SECONDS)
        {
            //this.fixture = new InMemoryLockClientFixture();
        }

        ~BasicLockClientTests()
        {
            //this.fixture.Dispose();
        }

        [Fact]
        public async Task TestAcquireBasicLock()
        {
            // ARRANGE
            await this.lockClient.AcquireLockAsync(new AcquireLockOptions("testKey1") { Data = new MemoryStream(Encoding.UTF8.GetBytes("data")) });

            // ACT
            LockItem lockItem = await this.lockClient.GetLockAsync("testKey1");

            // ASSERT
            Assert.Equal(INTEGRATION_TESTER, lockItem.OwnerName);
        }

        [Fact]
        public async Task TestAcquireBasicLockWithSortKey()
        {
            // ARRANGE
            await this.lockClientForRangeKeyTable.AcquireLockAsync(new AcquireLockOptions("testKey1") { SortKey = "1", Data = new MemoryStream(Encoding.UTF8.GetBytes("data")) });

            // ACT
            LockItem lockItem = await this.lockClientForRangeKeyTable.GetLockAsync("testKey1", "1");

            // ASSERT
            Assert.Equal(INTEGRATION_TESTER, lockItem.OwnerName);
        }

        [Fact]
        public async Task TestLockTableExists_Created()
        {
            Assert.True(await this.lockClient.LockTableExistsAsync());
        }

        [Fact]
        public async Task TestAssertLockTableExists_Created()
        {
            await this.lockClient.AssertLockTableExistsAsync();
        }

        [Fact]
        public async Task TestLockTableExists_NotCreated()
        {
            Assert.False(await this.lockClientNoTable.LockTableExistsAsync());
        }

        [Fact]
        public async Task TestAssertLockTableExists_NotCreated()
        {
            await Assert.ThrowsAsync<LockTableDoesNotExistException>(() => this.lockClientNoTable.AssertLockTableExistsAsync());
        }

        /// <summary>
        /// With a background thread, the lock does not expire
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task TestBackgroundThread()
        {
            // ARRANGE

            // ACT
            LockItem item = await this.lockClientWithHeartbeating.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1);

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);
            Assert.Null(await this.lockClientWithHeartbeating.TryAcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1));

            LockItem item2 = await this.lockClientWithHeartbeating.TryAcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_2);
            Assert.NotNull(item2);

            Thread.Sleep(5000);

            Assert.Null(await this.lockClientWithHeartbeating.TryAcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1));
            Assert.Null(await this.lockClientWithHeartbeating.TryAcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_2));

            item.Close();
            item2.Close();
        }

        [Fact]
        public async Task TestBackgroundThreadWithSortKey()
        {
            // ARRANGE

            // ACT
            LockItem item = await this.lockClientWithHeartbeatingForRangeKeyTable.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1_SORT_1);

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);
            Assert.Equal("1", item.SortKey);
            Assert.Null(await this.lockClientWithHeartbeatingForRangeKeyTable.TryAcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1_SORT_1));

            LockItem item2 = await this.lockClientWithHeartbeatingForRangeKeyTable.TryAcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1_SORT_2);
            Assert.NotNull(item2);
            Assert.Equal("testKey1", item2.PartitionKey);
            Assert.Equal("2", item2.SortKey);

            Thread.Sleep(5000);

            Assert.Null(await this.lockClientWithHeartbeatingForRangeKeyTable.TryAcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1_SORT_1));
            Assert.Null(await this.lockClientWithHeartbeatingForRangeKeyTable.TryAcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1_SORT_2));

            item.Close();
            item2.Close();
        }

        /// <summary>
        /// Without a background thread, the lock does expire
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task TestNoBackgroundThread()
        {
            // ARRANGE

            // ACT
            LockItem item = await this.shortLeaseLockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1);
            LockItem item2 = await this.shortLeaseLockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_2);

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);
            Assert.Equal("testKey2", item2.PartitionKey);

            Thread.Sleep(5000);

            Assert.NotNull(await this.shortLeaseLockClient.TryAcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1));
            Assert.NotNull(await this.shortLeaseLockClient.TryAcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_2));
        }

        [Fact]
        public async Task TestNoBackgroundThreadWithSortKey()
        {
            // ARRANGE

            // ACT
            LockItem item = await this.shortLeaseLockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1_SORT_1);
            LockItem item2 = await this.shortLeaseLockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1_SORT_2);

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);
            Assert.Equal("1", item.SortKey);
            Assert.Equal("testKey1", item2.PartitionKey);
            Assert.Equal("2", item2.SortKey);

            Thread.Sleep(5000);

            Assert.NotNull(await this.shortLeaseLockClient.TryAcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1_SORT_1));
            Assert.NotNull(await this.shortLeaseLockClient.TryAcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1_SORT_2));
        }

        [Fact]
        public async Task TestReleasingLock()
        {
            // ARRANGE
            LockItem item = await this.lockClientWithHeartbeating.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1);

            // ACT

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);
            Assert.Null(await this.lockClientWithHeartbeating.TryAcquireLockAsync(ACQUIRE_LOCK_OPTIONS_WITH_NO_WAIT));
            item.Close();
            Assert.NotNull(await this.lockClientWithHeartbeating.TryAcquireLockAsync(ACQUIRE_LOCK_OPTIONS_WITH_NO_WAIT));
        }

        [Fact]
        public async Task TestReleasingLockWithSortKey()
        {
            // ARRANGE
            LockItem item = await this.lockClientWithHeartbeatingForRangeKeyTable.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1_SORT_1);

            // ACT

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);
            Assert.Equal("1", item.SortKey);
            Assert.Null(await this.lockClientWithHeartbeatingForRangeKeyTable.TryAcquireLockAsync(ACQUIRE_LOCK_OPTIONS_WITH_NO_WAIT_SORT_1));
            item.Close();
            Assert.NotNull(await this.lockClientWithHeartbeatingForRangeKeyTable.TryAcquireLockAsync(ACQUIRE_LOCK_OPTIONS_WITH_NO_WAIT_SORT_1));
        }

        [Fact]
        public async Task TestAcquireLockLeaveData()
        {
            // ARRANGE
            string data = "testAcquireLockLeaveData";

            // ACT
            LockItem item = await this.lockClient.AcquireLockAsync(new AcquireLockOptions("testKey1") { Data = GetMemoryStream(data) });

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);
            this.lockClient.ReleaseLock(new ReleaseLockOptions(item) { DeleteLock = false });
            Assert.Null(await this.lockClient.GetLockAsync("testKey1"));
            LockItem item2 = await this.lockClient.GetLockFromDynamoDBAsync(new GetLockOptions("testKey1"));
            Assert.True(item2.IsReleased());

            item = await this.lockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE);

            Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));
            item.Close();
        }

        [Fact]
        public async Task TestAcquireLockLeaveData_RvnChanged()
        {
            // ARRANGE
            string data = "testAcquireLockLeaveData";

            // ACT
            LockItem item = await this.lockClient.AcquireLockAsync(new AcquireLockOptions("testKey1") { Data = GetMemoryStream(data) });

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);
            this.lockClient.ReleaseLock(new ReleaseLockOptions(item) { DeleteLock = false });
            Assert.Null(await this.lockClient.GetLockAsync("testKey1"));
            item = (await this.lockClient.GetLockFromDynamoDBAsync(new GetLockOptions("testKey1")));
            Assert.True(item.IsReleased());

            // Report a different RVN from what's in ddb
            item.UpdateRecordVersionNumber(Guid.NewGuid().ToString(), 0, 0);
            this.lockClientMock.Setup(x => x.GetLockFromDynamoDBAsync(It.IsAny<GetLockOptions>())).ReturnsAsync(item);

            try
            {
                item = await this.lockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE);

                Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));
               
            }
            catch (LockNotGrantedException)
            {
                throw new Exception("Lock should not fail to acquire dur to incorrect RVN - consistent data not enabled.");
            }
            finally
            {
                item.Close();
            }
        }

        [Fact]
        public async Task TestAcquireLockLeaveDataWithSortKey()
        {
            // ARRANGE
            string data = "testAcquireLockLeaveData";

            // ACT
            LockItem item = await this.lockClientForRangeKeyTable.AcquireLockAsync(new AcquireLockOptions("testKey1") { Data = GetMemoryStream(data), SortKey = "1" });

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);
            Assert.Equal("1", item.SortKey);

            this.lockClientForRangeKeyTable.ReleaseLock(new ReleaseLockOptions(item) { DeleteLock = false });

            Assert.Null(await this.lockClientForRangeKeyTable.GetLockAsync("testKey1", "1"));
            Assert.True((await this.lockClientForRangeKeyTable.GetLockFromDynamoDBAsync(new GetLockOptions("testKey1") { SortKey = "1" })).IsReleased());

            item = await this.lockClientForRangeKeyTable.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_SORT_1);

            Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));
            item.Close();
        }

        [Fact]
        public async Task TestAcquireLockLeaveDataWithSortKey_RvnChanged()
        {
            // ARRANGE
            string data = "testAcquireLockLeaveData";

            // ACT
            LockItem item = await this.lockClientForRangeKeyTable.AcquireLockAsync(new AcquireLockOptions("testKey1") { Data = GetMemoryStream(data), SortKey = "1" });

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);
            Assert.Equal("1", item.SortKey);

            this.lockClientForRangeKeyTable.ReleaseLock(new ReleaseLockOptions(item) { DeleteLock = false });
            Assert.Null(await this.lockClient.GetLockAsync("testKey1"));
            item = (await this.lockClientForRangeKeyTable.GetLockFromDynamoDBAsync(new GetLockOptions("testKey1") { SortKey = "1" }));
            Assert.True(item.IsReleased());

            // Report a different RVN from what's in ddb
            item.UpdateRecordVersionNumber(Guid.NewGuid().ToString(), 0, 0);
            this.lockClientForRangeKeyTableMock.Setup(x => x.GetLockFromDynamoDBAsync(It.IsAny<GetLockOptions>())).ReturnsAsync(item);

            try
            {
                item = await this.lockClientForRangeKeyTable.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_SORT_1);

                Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));
                item.Close();
            }
            catch (LockNotGrantedException)
            {
                throw new Exception("Lock should not fail to acquire dur to incorrect RVN - consistent data not enabled.");
            }
        }

        [Fact]
        public async Task TestAcquireLockLeaveDataConsistentData()
        {
            // ARRANGE
            string data = "testAcquireLockLeaveData";

            // ACT
            LockItem item = await this.lockClient.AcquireLockAsync(new AcquireLockOptions("testKey1") { Data = GetMemoryStream(data) });

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);
            this.lockClient.ReleaseLock(new ReleaseLockOptions(item) { DeleteLock = false });
            Assert.Null(await this.lockClient.GetLockAsync("testKey1"));
            LockItem item2 = await this.lockClient.GetLockFromDynamoDBAsync(new GetLockOptions("testKey1"));
            Assert.True(item2.IsReleased());

            item = await this.lockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_CONSISTENT_DATA_TRUE);

            Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));
            item.Close();
        }

        [Fact]
        public async Task TestAcquireLockLeaveDataConsistentData_RvnChanged()
        {
            // ARRANGE
            string data = "testAcquireLockLeaveData";

            // ACT
            LockItem item = await this.lockClient.AcquireLockAsync(new AcquireLockOptions("testKey1") { Data = GetMemoryStream(data) });

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);
            this.lockClient.ReleaseLock(new ReleaseLockOptions(item) { DeleteLock = false });
            Assert.Null(await this.lockClient.GetLockAsync("testKey1"));
            item = (await this.lockClient.GetLockFromDynamoDBAsync(new GetLockOptions("testKey1")));
            Assert.True(item.IsReleased());

            // Report a different RVN from what's in ddb
            item.UpdateRecordVersionNumber(Guid.NewGuid().ToString(), 0, 0);
            this.lockClientMock.Setup(x => x.GetLockFromDynamoDBAsync(It.IsAny<GetLockOptions>())).ReturnsAsync(item);

            await Assert.ThrowsAsync<LockNotGrantedException>(() => this.lockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_CONSISTENT_DATA_TRUE));
        }

        [Fact]
        public async Task TestAcquireLockLeaveDataConsistentDataWithSortKey()
        {
            // ARRANGE
            string data = "testAcquireLockLeaveData";

            // ACT
            LockItem item = await this.lockClientForRangeKeyTable.AcquireLockAsync(new AcquireLockOptions("testKey1") { Data = GetMemoryStream(data), SortKey = "1" });

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);
            Assert.Equal("1", item.SortKey);

            this.lockClientForRangeKeyTable.ReleaseLock(new ReleaseLockOptions(item) { DeleteLock = false });

            Assert.Null(await this.lockClientForRangeKeyTable.GetLockAsync("testKey1", "1"));
            Assert.True((await this.lockClientForRangeKeyTable.GetLockFromDynamoDBAsync(new GetLockOptions("testKey1", "1"))).IsReleased());

            item = await this.lockClientForRangeKeyTable.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_CONSISTENT_DATA_TRUE_SORT_1);

            Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));
            item.Close();
        }

        [Fact]
        public async Task TestAcquireLockLeaveDataConsistentDataWithSortKey_RvnChanged()
        {
            // ARRANGE
            string data = "testAcquireLockLeaveData";

            // ACT
            LockItem item = await this.lockClientForRangeKeyTable.AcquireLockAsync(new AcquireLockOptions("testKey1") { Data = GetMemoryStream(data), SortKey = "1" });

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);
            Assert.Equal("1", item.SortKey);

            this.lockClientForRangeKeyTable.ReleaseLock(new ReleaseLockOptions(item) { DeleteLock = false });
            Assert.Null(await this.lockClient.GetLockAsync("testKey1"));
            item = (await this.lockClientForRangeKeyTable.GetLockFromDynamoDBAsync(new GetLockOptions("testKey1") { SortKey = "1" }));
            Assert.True(item.IsReleased());

            // Report a different RVN from what's in ddb
            item.UpdateRecordVersionNumber(Guid.NewGuid().ToString(), 0, 0);
            this.lockClientForRangeKeyTableMock.Setup(x => x.GetLockFromDynamoDBAsync(It.IsAny<GetLockOptions>())).ReturnsAsync(item);

            await Assert.ThrowsAsync<LockNotGrantedException>(() => this.lockClientForRangeKeyTable.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_CONSISTENT_DATA_TRUE_SORT_1));
        }

        [Fact]
        public async Task TestAcquireLockLeaveDataWhenUpdateExistingLockTrue()
        {
            // ARRANGE
            string data = "testAcquireLockLeaveData";

            // ACT
            LockItem item = await this.lockClient.AcquireLockAsync(new AcquireLockOptions("testKey1") { Data = GetMemoryStream(data) });

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);
            this.lockClient.ReleaseLock(new ReleaseLockOptions(item) { DeleteLock = false });
            Assert.Null(await this.lockClient.GetLockAsync("testKey1"));
            LockItem item2 = await this.lockClient.GetLockFromDynamoDBAsync(new GetLockOptions("testKey1"));
            Assert.True(item2.IsReleased());

            item = await this.lockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_UPDATE_EXISTING_TRUE);

            Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));
            item.Close();
        }

        [Fact]
        public async Task TestAcquireLockLeaveDataWhenUpdateExistingLockTrue_RvnChanged()
        {
            // ARRANGE
            string data = "testAcquireLockLeaveData";

            // ACT
            LockItem item = await this.lockClient.AcquireLockAsync(new AcquireLockOptions("testKey1") { Data = GetMemoryStream(data) });

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);

            this.lockClient.ReleaseLock(new ReleaseLockOptions(item) { DeleteLock = false });
            Assert.Null(await this.lockClient.GetLockAsync("testKey1"));
            item = (await this.lockClient.GetLockFromDynamoDBAsync(new GetLockOptions("testKey1")));
            Assert.True(item.IsReleased());

            // Report a different RVN from what's in ddb
            item.UpdateRecordVersionNumber(Guid.NewGuid().ToString(), 0, 0);
            this.lockClientMock.Setup(x => x.GetLockFromDynamoDBAsync(It.IsAny<GetLockOptions>())).ReturnsAsync(item);

            try
            {
                item = await this.lockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_UPDATE_EXISTING_TRUE);

                Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));              
            }
            catch (LockNotGrantedException)
            {
                throw new Exception("Lock should not fail to acquire dur to incorrect RVN - consistent data not enabled.");
            }
            finally
            {
                item.Close();
            }
        }

        [Fact]
        public async Task TestAcquireLockLeaveDataWhenUpdateExistingLockTrueWithSortKey()
        {
            // ARRANGE
            string data = "testAcquireLockLeaveData";

            // ACT
            LockItem item = await this.lockClientForRangeKeyTable.AcquireLockAsync(new AcquireLockOptions("testKey1") { Data = GetMemoryStream(data), SortKey = "1" });

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);
            Assert.Equal("1", item.SortKey);

            this.lockClientForRangeKeyTable.ReleaseLock(new ReleaseLockOptions(item) { DeleteLock = false });

            Assert.Null(await this.lockClientForRangeKeyTable.GetLockAsync("testKey1", "1"));
            Assert.True((await this.lockClientForRangeKeyTable.GetLockFromDynamoDBAsync(new GetLockOptions("testKey1") { SortKey = "1" })).IsReleased());

            item = await this.lockClientForRangeKeyTable.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_UPDATE_EXISTING_TRUE_SORT_1);

            Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));
            item.Close();
        }

        [Fact]
        public async Task TestAcquireLockLeaveDataWhenUpdateExistingLockTrueWithSortKey_RvnChanged()
        {
            // ARRANGE
            string data = "testAcquireLockLeaveData";

            // ACT
            LockItem item = await this.lockClientForRangeKeyTable.AcquireLockAsync(new AcquireLockOptions("testKey1") { Data = GetMemoryStream(data), SortKey = "1" });

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);
            Assert.Equal("1", item.SortKey);

            this.lockClientForRangeKeyTable.ReleaseLock(new ReleaseLockOptions(item) { DeleteLock = false });
            Assert.Null(await this.lockClient.GetLockAsync("testKey1"));
            item = (await this.lockClientForRangeKeyTable.GetLockFromDynamoDBAsync(new GetLockOptions("testKey1") { SortKey = "1" }));
            Assert.True(item.IsReleased());

            // Report a different RVN from what's in ddb
            item.UpdateRecordVersionNumber(Guid.NewGuid().ToString(), 0, 0);
            this.lockClientForRangeKeyTableMock.Setup(x => x.GetLockFromDynamoDBAsync(It.IsAny<GetLockOptions>())).ReturnsAsync(item);

            try
            {
                item = await this.lockClientForRangeKeyTable.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_UPDATE_EXISTING_TRUE_SORT_1);

                Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));
                item.Close();
            }
            catch (LockNotGrantedException)
            {
                throw new Exception("Lock should not fail to acquire dur to incorrect RVN - consistent data not enabled.");
            }
        }

        [Fact]
        public async Task TestAcquireLockLeaveDataConsistentDataWhenUpdateExistingLockTrue()
        {
            // ARRANGE
            string data = "testAcquireLockLeaveData";

            // ACT
            LockItem item = await this.lockClient.AcquireLockAsync(new AcquireLockOptions("testKey1") { Data = GetMemoryStream(data) });

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);
            this.lockClient.ReleaseLock(new ReleaseLockOptions(item) { DeleteLock = false });
            Assert.Null(await this.lockClient.GetLockAsync("testKey1"));
            LockItem item2 = await this.lockClient.GetLockFromDynamoDBAsync(new GetLockOptions("testKey1"));
            Assert.True(item2.IsReleased());

            item = await this.lockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_UPDATE_EXISTING_TRUE_CONSISTENT_DATA_TRUE);

            Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));
            item.Close();
        }

        [Fact]
        public async Task TestAcquireLockLeaveDataConsistentDataWhenUpdateExistingLockTrue_RvnChanged()
        {
            // ARRANGE
            string data = "testAcquireLockLeaveData";

            // ACT
            LockItem item = await this.lockClient.AcquireLockAsync(new AcquireLockOptions("testKey1") { Data = GetMemoryStream(data) });

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);
            this.lockClient.ReleaseLock(new ReleaseLockOptions(item) { DeleteLock = false });
            Assert.Null(await this.lockClient.GetLockAsync("testKey1"));
            item = (await this.lockClient.GetLockFromDynamoDBAsync(new GetLockOptions("testKey1")));
            Assert.True(item.IsReleased());

            // Report a different RVN from what's in ddb
            item.UpdateRecordVersionNumber(Guid.NewGuid().ToString(), 0, 0);
            this.lockClientMock.Setup(x => x.GetLockFromDynamoDBAsync(It.IsAny<GetLockOptions>())).ReturnsAsync(item);

            await Assert.ThrowsAsync<LockNotGrantedException>(() => this.lockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_UPDATE_EXISTING_TRUE_CONSISTENT_DATA_TRUE));

            item.Close();
        }

        [Fact]
        public async Task TestAcquireLockLeaveDataConsistentDataWhenUpdateExistingLockTrueWithSortKey()
        {
            // ARRANGE
            string data = "testAcquireLockLeaveData";

            // ACT
            LockItem item = await this.lockClientForRangeKeyTable.AcquireLockAsync(new AcquireLockOptions("testKey1") { Data = GetMemoryStream(data), SortKey = "1" });

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);
            Assert.Equal("1", item.SortKey);

            this.lockClientForRangeKeyTable.ReleaseLock(new ReleaseLockOptions(item) { DeleteLock = false });

            Assert.Null(await this.lockClientForRangeKeyTable.GetLockAsync("testKey1", "1"));
            Assert.True((await this.lockClientForRangeKeyTable.GetLockFromDynamoDBAsync(new GetLockOptions("testKey1") { SortKey = "1" })).IsReleased());

            item = await this.lockClientForRangeKeyTable.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_UPDATE_EXISTING_TRUE_CONSISTENT_DATA_TRUE_SORT_1);

            Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));
            item.Close();
        }

        [Fact]
        public async Task TestAcquireLockLeaveDataConsistentDataWhenUpdateExistingLockTrueWithSortKey_rvnChanged()
        {
            // ARRANGE
            string data = "testAcquireLockLeaveData";

            // ACT
            LockItem item = await this.lockClientForRangeKeyTable.AcquireLockAsync(new AcquireLockOptions("testKey1") { Data = GetMemoryStream(data), SortKey = "1" });

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);
            Assert.Equal("1", item.SortKey);

            this.lockClientForRangeKeyTable.ReleaseLock(new ReleaseLockOptions(item) { DeleteLock = false });
            Assert.Null(await this.lockClient.GetLockAsync("testKey1"));
            item = (await this.lockClientForRangeKeyTable.GetLockFromDynamoDBAsync(new GetLockOptions("testKey1") { SortKey = "1" }));
            Assert.True(item.IsReleased());

            // Report a different RVN from what's in ddb
            item.UpdateRecordVersionNumber(Guid.NewGuid().ToString(), 0, 0);
            this.lockClientForRangeKeyTableMock.Setup(x => x.GetLockFromDynamoDBAsync(It.IsAny<GetLockOptions>())).ReturnsAsync(item);

            await Assert.ThrowsAsync<LockNotGrantedException>(() => this.lockClientForRangeKeyTable.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_UPDATE_EXISTING_TRUE_CONSISTENT_DATA_TRUE_SORT_1));
        }

        [Fact]
        public async Task TestAcquireLockMustExistWhenNotUpdateExistingLock_LockDoesNotExist()
        {
            // ARRANGE

            // ACT

            // ASSERT
            await Assert.ThrowsAsync<LockNotGrantedException>(() => this.lockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_FALSE));
        }

        [Fact]
        public async Task TestAcquireLockMustExistWhenNotUpdateExistingLockConsistentData_LockDoesNotExist()
        {
            // ARRANGE

            // ACT

            // ASSERT
            await Assert.ThrowsAsync<LockNotGrantedException>(() => this.lockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_FALSE_CONSISTENT_DATA_TRUE));
        }

        [Fact]
        public async Task TestAcquireLockMustExistWhenNotUpdateExistingLockWithSortKey_LockDoesNotExist()
        {
            // ARRANGE

            // ACT

            // ASSERT
            await Assert.ThrowsAsync<LockNotGrantedException>(() => this.lockClientForRangeKeyTable.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_FALSE_SORT_1));
        }

        [Fact]
        public async Task TestAcquireLockMustExistWhenNotUpdateExistingLockConsistentDataWithSortKey_LockDoesNotExist()
        {
            // ARRANGE

            // ACT

            // ASSERT
            await Assert.ThrowsAsync<LockNotGrantedException>(() => this.lockClientForRangeKeyTable.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_FALSE_CONSISTENT_DATA_TRUE_SORT_1));
        }

        [Fact]
        public async Task TestAcquireLockMustExistWhenUpdateExistingLock_LockDoesNotExist()
        {
            // ARRANGE

            // ACT

            // ASSERT
            await Assert.ThrowsAsync<LockNotGrantedException>(() => this.lockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_TRUE));
        }

        [Fact]
        public async Task TestAcquireLockMustExistWhenUpdateExistingLockConsistentData_LockDoesNotExist()
        {
            // ARRANGE

            // ACT

            // ASSERT
            await Assert.ThrowsAsync<LockNotGrantedException>(() => this.lockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_TRUE_CONSISTENT_DATA_TRUE));
        }

        [Fact]
        public async Task TestAcquireLockMustExistWhenUpdateExistingLockWithSortKey_LockDoesNotExist()
        {
            // ARRANGE

            // ACT

            // ASSERT
            await Assert.ThrowsAsync<LockNotGrantedException>(() => this.lockClientForRangeKeyTable.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_TRUE_SORT_1));
        }

        [Fact]
        public async Task TestAcquireLockMustExistWhenUpdateExistingLockConsistentDataWithSortKey_LockDoesNotExist()
        {
            // ARRANGE

            // ACT

            // ASSERT
            await Assert.ThrowsAsync<LockNotGrantedException>(() => this.lockClientForRangeKeyTable.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_TRUE_CONSISTENT_DATA_TRUE_SORT_1));
        }

        [Fact]
        public async Task TestAcquireLockMustExistWhenNotUpdateExistingLock_LockExists()
        {
            // ARRANGE
            string data = "testAcquireLockMustExist";

            // ACT
            LockItem item = await this.lockClient.AcquireLockAsync(new AcquireLockOptions("testKey1") { Data = GetMemoryStream(data) });

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);

            this.lockClient.ReleaseLock(new ReleaseLockOptions(item) { DeleteLock = false });
            Assert.Null(await this.lockClient.GetLockAsync("testKey1"));
            item = (await this.lockClient.GetLockFromDynamoDBAsync(new GetLockOptions("testKey1") { SortKey = "1" }));
            Assert.True(item.IsReleased());

            item = await this.lockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_FALSE);
            Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));
            item.Close();
        }

        [Fact]
        public async Task TestAcquireLockMustExistWhenNotUpdateExistingLockConsistentData_LockExists()
        {
            // ARRANGE
            string data = "testAcquireLockMustExist";

            // ACT
            LockItem item = await this.lockClient.AcquireLockAsync(new AcquireLockOptions("testKey1") { Data = GetMemoryStream(data) });

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);

            this.lockClient.ReleaseLock(new ReleaseLockOptions(item) { DeleteLock = false });
            Assert.Null(await this.lockClient.GetLockAsync("testKey1"));
            item = (await this.lockClient.GetLockFromDynamoDBAsync(new GetLockOptions("testKey1") { SortKey = "1" }));
            Assert.True(item.IsReleased());

            item = await this.lockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_FALSE_CONSISTENT_DATA_TRUE);
            Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));
            item.Close();
        }

        [Fact]
        public async Task TestAcquireLockMustExistWhenNotUpdateExistingLockWithSortKey_LockExists()
        {
            // ARRANGE
            string data = "testAcquireLockMustExist";

            // ACT
            LockItem item = await this.lockClientForRangeKeyTable.AcquireLockAsync(new AcquireLockOptions("testKey1") { Data = GetMemoryStream(data), SortKey = "1" });

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);
            Assert.Equal("1", item.SortKey);

            this.lockClientForRangeKeyTable.ReleaseLock(new ReleaseLockOptions(item) { DeleteLock = false });

            Assert.Null(await this.lockClientForRangeKeyTable.GetLockAsync("testKey1", "1"));
            Assert.True((await this.lockClientForRangeKeyTable.GetLockFromDynamoDBAsync(new GetLockOptions("testKey1") { SortKey = "1" })).IsReleased());

            item = await this.lockClientForRangeKeyTable.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_FALSE_SORT_1);

            Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));
            item.Close();
        }

        [Fact]
        public async Task TestAcquireLockMustExistWhenNotUpdateExistingLockConsistentDataWithSortKey_LockExists()
        {
            // ARRANGE
            string data = "testAcquireLockMustExist";

            // ACT
            LockItem item = await this.lockClientForRangeKeyTable.AcquireLockAsync(new AcquireLockOptions("testKey1") { Data = GetMemoryStream(data), SortKey = "1" });

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);
            Assert.Equal("1", item.SortKey);

            this.lockClientForRangeKeyTable.ReleaseLock(new ReleaseLockOptions(item) { DeleteLock = false });

            Assert.Null(await this.lockClientForRangeKeyTable.GetLockAsync("testKey1", "1"));
            Assert.True((await this.lockClientForRangeKeyTable.GetLockFromDynamoDBAsync(new GetLockOptions("testKey1") { SortKey = "1" })).IsReleased());

            item = await this.lockClientForRangeKeyTable.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_FALSE_CONSISTENT_DATA_TRUE_SORT_1);

            Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));
            item.Close();
        }

        [Fact]
        public async Task TestAcquireLockMustExistWhenUpdateExistingLock_LockExist()
        {
            // ARRANGE
            string data = "testAcquireLockMustExist";

            // ACT
            LockItem item = await this.lockClient.AcquireLockAsync(new AcquireLockOptions("testKey1") { Data = GetMemoryStream(data) });

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);

            this.lockClient.ReleaseLock(new ReleaseLockOptions(item) { DeleteLock = false });
            Assert.Null(await this.lockClient.GetLockAsync("testKey1"));
            item = (await this.lockClient.GetLockFromDynamoDBAsync(new GetLockOptions("testKey1") { SortKey = "1" }));
            Assert.True(item.IsReleased());

            item = await this.lockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_TRUE);
            Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));
            item.Close();
        }

        [Fact]
        public async Task TestAcquireLockMustExistWhenUpdateExistingLockConsistentData_LockExist()
        {
            // ARRANGE
            string data = "testAcquireLockMustExist";

            // ACT
            LockItem item = await this.lockClient.AcquireLockAsync(new AcquireLockOptions("testKey1") { Data = GetMemoryStream(data) });

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);

            this.lockClient.ReleaseLock(new ReleaseLockOptions(item) { DeleteLock = false });
            Assert.Null(await this.lockClient.GetLockAsync("testKey1"));
            item = (await this.lockClient.GetLockFromDynamoDBAsync(new GetLockOptions("testKey1") { SortKey = "1" }));
            Assert.True(item.IsReleased());

            item = await this.lockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_TRUE_CONSISTENT_DATA_TRUE);
            Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));
            item.Close();
        }

        [Fact]
        public async Task TestAcquireLockMustExistWhenUpdateExistingLockWithSortKey_LockExist()
        {
            // ARRANGE
            string data = "testAcquireLockMustExist";

            // ACT
            LockItem item = await this.lockClientForRangeKeyTable.AcquireLockAsync(new AcquireLockOptions("testKey1") { Data = GetMemoryStream(data), SortKey = "1" });

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);
            Assert.Equal("1", item.SortKey);

            this.lockClientForRangeKeyTable.ReleaseLock(new ReleaseLockOptions(item) { DeleteLock = false });

            Assert.Null(await this.lockClientForRangeKeyTable.GetLockAsync("testKey1", "1"));
            Assert.True((await this.lockClientForRangeKeyTable.GetLockFromDynamoDBAsync(new GetLockOptions("testKey1") { SortKey = "1" })).IsReleased());

            item = await this.lockClientForRangeKeyTable.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_TRUE_SORT_1);

            Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));
            item.Close();
        }

        [Fact]
        public async Task TestAcquireLockMustExistWhenUpdateExistingLockConsistentDataWithSortKey_LockExist()
        {
            // ARRANGE
            string data = "testAcquireLockMustExist";

            // ACT
            LockItem item = await this.lockClientForRangeKeyTable.AcquireLockAsync(new AcquireLockOptions("testKey1") { Data = GetMemoryStream(data), SortKey = "1" });

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);
            Assert.Equal("1", item.SortKey);

            this.lockClientForRangeKeyTable.ReleaseLock(new ReleaseLockOptions(item) { DeleteLock = false });

            Assert.Null(await this.lockClientForRangeKeyTable.GetLockAsync("testKey1", "1"));
            Assert.True((await this.lockClientForRangeKeyTable.GetLockFromDynamoDBAsync(new GetLockOptions("testKey1") { SortKey = "1" })).IsReleased());

            item = await this.lockClientForRangeKeyTable.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_TRUE_CONSISTENT_DATA_TRUE_SORT_1);

            Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));
            item.Close();
        }

        [Fact]
        public async Task TestAcquireLockBasicWithUpdateExistingLockTrue()
        {
            // ARRANGE

            // ACT
            LockItem item = await this.lockClient.AcquireLockAsync(new AcquireLockOptions("testKey1") { UpdateExistingLockRecord = true });

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);
        }

        [Fact]
        public async Task TestAcquireLockWhenLockIsReleasedAndUpdateExistingLockIsTruePreserveAttributesFromPreviousLock()
        {
            // ARRANGE 
            IAmazonDynamoDBLockClient lockClient1 = new AmazonDynamoDBLockClient(
                new AmazonDynamoDBLockClientOptions(this.idynamodb, RANGE_KEY_TABLE_NAME, INTEGRATION_TESTER_2)
                { LeaseDuration = 3, HeartbeatPeriod = 1, TimeUnit = TimeUnit.SECONDS, SortKeyName = "rangeKey", CreateHeartbeatBackgroundThread = false });

            IAmazonDynamoDBLockClient lockClient2 = new AmazonDynamoDBLockClient(
                new AmazonDynamoDBLockClientOptions(this.idynamodb, RANGE_KEY_TABLE_NAME, INTEGRATION_TESTER_2)
                { LeaseDuration = 3, HeartbeatPeriod = 1, TimeUnit = TimeUnit.SECONDS, SortKeyName = "rangeKey", CreateHeartbeatBackgroundThread = false });

            string additionalValue = "doNotDelete";
            string lockPartitionKey = "testKey1";
            string lockSortKey = "1";
            Dictionary<string, AttributeValue> additional = new Dictionary<string, AttributeValue>()
            {
                { additionalValue, new AttributeValue(additionalValue) }
            };

            // ACT

            // Acquire first lock
            LockItem lockItem1 = await lockClient1.TryAcquireLockAsync(new AcquireLockOptions(lockPartitionKey) { SortKey = lockSortKey, DeleteLockOnRelease = false, AdditionalAttributes = additional });

            // ASSERT
            Assert.NotNull(lockItem1);
            lockClient1.ReleaseLock(lockItem1);

            // Acquire the same lock released above
            LockItem lockItem2 = await lockClient2.TryAcquireLockAsync(new AcquireLockOptions(lockPartitionKey) { SortKey = lockSortKey, DeleteLockOnRelease = false, AdditionalAttributes = additional });

            Assert.NotNull(lockItem2);
            Assert.Equal(INTEGRATION_TESTER_2, lockItem2.OwnerName);

            // Get the complete record for lock to verify other fields are not replaced
            Dictionary<string, AttributeValue> key = new Dictionary<string, AttributeValue>()
            {
                { this.lockClient1Options.PartitionKeyName, new AttributeValue(lockPartitionKey) },
                { "rangeKey", new AttributeValue(lockSortKey) }
            };

            GetItemResponse result = await this.lockClient1Options.DynamoDBClient.GetItemAsync(new GetItemRequest() { TableName = RANGE_KEY_TABLE_NAME, Key = key });
            Dictionary<string, AttributeValue> currentLockRecord = result.Item;

            // any values left from old locks shouls not be removed
            Assert.NotNull(currentLockRecord[additionalValue]);
            string additionalValuesExpected = currentLockRecord[additionalValue].S;
            Assert.Equal(additionalValue, additionalValuesExpected);

            lockClient1.Close();
            lockClient2.Close();
        }

        [Fact]
        public async Task TestAcquireLockWithSortKeyWhenLockIsExpiredAndUpdateExistingLockIsTruePreserveAdditionalAttributesFromPreviousLock()
        {
            // ARRANGE 
            IAmazonDynamoDBLockClient lockClient1 = new AmazonDynamoDBLockClient(
                new AmazonDynamoDBLockClientOptions(this.idynamodb, RANGE_KEY_TABLE_NAME, INTEGRATION_TESTER_2)
                { LeaseDuration = 3, HeartbeatPeriod = 1, TimeUnit = TimeUnit.SECONDS, SortKeyName = "rangeKey", CreateHeartbeatBackgroundThread = false });

            IAmazonDynamoDBLockClient lockClient2 = new AmazonDynamoDBLockClient(
                new AmazonDynamoDBLockClientOptions(this.idynamodb, RANGE_KEY_TABLE_NAME, INTEGRATION_TESTER_2)
                { LeaseDuration = 3, HeartbeatPeriod = 1, TimeUnit = TimeUnit.SECONDS, SortKeyName = "rangeKey", CreateHeartbeatBackgroundThread = false });

            string additionalValue = "doNotDelete";
            string lockPartitionKey = "testKey1";
            string lockSortKey = "1";
            Dictionary<string, AttributeValue> additional = new Dictionary<string, AttributeValue>()
            {
                { additionalValue, new AttributeValue(additionalValue) }
            };

            // ACT

            // Acquire first lock
            LockItem lockItem1 = await lockClient1.TryAcquireLockAsync(new AcquireLockOptions(lockPartitionKey) { SortKey = lockSortKey, DeleteLockOnRelease = false, AdditionalAttributes = additional });

            // ASSERT
            Assert.NotNull(lockItem1);
            lockClient1.ReleaseLock(lockItem1);

            // Acquire the same lock released above
            LockItem lockItem2 = await lockClient2.TryAcquireLockAsync(new AcquireLockOptions(lockPartitionKey) { SortKey = lockSortKey, UpdateExistingLockRecord = true });

            Assert.NotNull(lockItem2);
            Assert.Equal(INTEGRATION_TESTER_2, lockItem2.OwnerName);

            // Get the complete record for lock to verify other fields are not replaced
            Dictionary<string, AttributeValue> key = new Dictionary<string, AttributeValue>()
            {
                { this.lockClient1Options.PartitionKeyName, new AttributeValue(lockPartitionKey) },
                { "rangeKey", new AttributeValue(lockSortKey) }
            };

            GetItemResponse result = await this.lockClient1Options.DynamoDBClient.GetItemAsync(new GetItemRequest() { TableName = RANGE_KEY_TABLE_NAME, Key = key });
            Dictionary<string, AttributeValue> currentLockRecord = result.Item;

            // any values left from old locks shouls not be removed
            Assert.NotNull(currentLockRecord[additionalValue]);
            string additionalValuesExpected = currentLockRecord[additionalValue].S;
            Assert.Equal(additionalValue, additionalValuesExpected);

            lockClient1.Close();
            lockClient2.Close();
        }

        [Fact]
        public async Task TestAcquireLockWhenLockIsExpiredAndUpdateExistingLockIsTruePreserveAdditionalAttributesFromPreviousLock()
        {
            // ARRANGE 
            IAmazonDynamoDBLockClient lockClient1 = new AmazonDynamoDBLockClient(
                new AmazonDynamoDBLockClientOptions(this.idynamodb, TABLE_NAME, INTEGRATION_TESTER_2)
                { LeaseDuration = 3, HeartbeatPeriod = 1, TimeUnit = TimeUnit.SECONDS, CreateHeartbeatBackgroundThread = false });

            IAmazonDynamoDBLockClient lockClient2 = new AmazonDynamoDBLockClient(
                new AmazonDynamoDBLockClientOptions(this.idynamodb, TABLE_NAME, INTEGRATION_TESTER_2)
                { LeaseDuration = 3, HeartbeatPeriod = 1, TimeUnit = TimeUnit.SECONDS, CreateHeartbeatBackgroundThread = false });

            string additionalValue = "doNotDelete";
            string lockPartitionKey = "testKey1";

            Dictionary<string, AttributeValue> additional = new Dictionary<string, AttributeValue>()
            {
                { additionalValue, new AttributeValue(additionalValue) }
            };

            // ACT

            // Acquire first lock
            LockItem lockItem1 = await lockClient1.TryAcquireLockAsync(new AcquireLockOptions(lockPartitionKey) { DeleteLockOnRelease = false, AdditionalAttributes = additional });

            // ASSERT
            Assert.NotNull(lockItem1);

            // Try stealing the lock once it is expired
            LockItem lockItem2 = await lockClient2.TryAcquireLockAsync(new AcquireLockOptions(lockPartitionKey) { UpdateExistingLockRecord = true });
            Assert.NotNull(lockItem2);
            Assert.Equal(INTEGRATION_TESTER_2, lockItem2.OwnerName);

            // Get the complete record for lock to verify other fields are not replaced
            Dictionary<string, AttributeValue> key = new Dictionary<string, AttributeValue>()
            {
                { this.lockClient1Options.PartitionKeyName, new AttributeValue(lockPartitionKey) }
            };

            GetItemResponse result = await this.lockClient1Options.DynamoDBClient.GetItemAsync(new GetItemRequest() { TableName = TABLE_NAME, Key = key });
            Dictionary<string, AttributeValue> currentLockRecord = result.Item;

            // any values left from old locks shouls not be removed
            Assert.NotNull(currentLockRecord[additionalValue]);
            string additionalValuesExpected = currentLockRecord[additionalValue].S;
            Assert.Equal(additionalValue, additionalValuesExpected);

            lockClient1.Close();
            lockClient2.Close();
        }

        [Fact]
        public async Task TestAcquireLockLeaveDataAfterClose()
        {
            // ARRANGE
            string data = "testAcquireLockLeaveData";

            // ACT
            LockItem item = await this.lockClient.AcquireLockAsync(new AcquireLockOptions("testKey1") { Data = GetMemoryStream(data), RefreshPeriod = 1, AdditionalTimeToWaitForLock = 5, TimeUnit = TimeUnit.SECONDS, DeleteLockOnRelease = false });

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);

            this.lockClient.Close();
            this.lockClient = new AmazonDynamoDBLockClient(this.lockClient1Options);

            item = await this.lockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE);
            Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));

            item.Close();
        }

        [Fact]
        public async Task TestAcquireLockLeaveOrReplaceDataFromReleasedLock()
        {
            // ARRANGE
            string data = "testAcquireLockLeaveOrReplaceDataFromReleased";

            // ACT
            LockItem item = await this.lockClient.AcquireLockAsync(new AcquireLockOptions("testKey1"));
            this.lockClient.ReleaseLock(new ReleaseLockOptions(item) { DeleteLock = false });
            item = await this.lockClient.AcquireLockAsync(new AcquireLockOptions("testKey1") { ReplaceData = false, Data = GetMemoryStream(data) });

            // ASSERT
            Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));
            item.Close();
        }

        [Fact]
        public async Task TestAcquireLockLeaveOrReplaceDataFromAcquiredLock()
        {
            // ARRANGE
            string data = "testAcquireLockLeaveOrReplaceData";

            // ACT
            LockItem item = await this.lockClient.AcquireLockAsync(new AcquireLockOptions("testKey1"));

            item = await this.lockClient.AcquireLockAsync(new AcquireLockOptions("testKey1") { ReplaceData = false, Data = GetMemoryStream(data) });

            // ASSERT
            Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));
            item.Close();
        }

        [Fact]
        public async Task TestSendHeatbeatWithRangeKey()
        {
            // ARRANGE 
            string data = $"testSendHeartbeatLeaveData{SECURE_RANDOM.NextDouble()}";

            IAmazonDynamoDBLockClient lockClient = new AmazonDynamoDBLockClient(
                new AmazonDynamoDBLockClientOptions(this.idynamodb, RANGE_KEY_TABLE_NAME, INTEGRATION_TESTER_2)
                { LeaseDuration = 30, HeartbeatPeriod = 2, TimeUnit = TimeUnit.SECONDS, CreateHeartbeatBackgroundThread = false, SortKeyName = "rangeKey" });

            // ACT
            LockItem item = await lockClient.AcquireLockAsync(new AcquireLockOptions("testKey1") { DeleteLockOnRelease = true, ReplaceData = true, SortKey = TABLE_NAME, Data = GetMemoryStream(data) });

            // ASSERT
            Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));

            Assert.Null(await lockClient.GetLockAsync("testKey1", "nothing"));
            item = (await lockClient.GetLockAsync("testKey1", TABLE_NAME));
            Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));
            lockClient.SendHeartbeat(item);
            item = (await lockClient.GetLockAsync("testKey1", TABLE_NAME));
            Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));

            item.Close();
            lockClient.Close();
        }

        [Fact]
        public async Task TestSendHeartbeatLeaveData()
        {
            // ARRANGE 
            string data = $"testSendHeartbeatLeaveData{SECURE_RANDOM.NextDouble()}";

            // ACT
            LockItem item = await this.lockClient.AcquireLockAsync(new AcquireLockOptions("testKey1") { DeleteLockOnRelease = true, ReplaceData = true, Data = GetMemoryStream(data) });

            // ASSERT
            Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));

            item = (await lockClient.GetLockAsync("testKey1"));
            Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));
            lockClient.SendHeartbeat(item);
            item = (await lockClient.GetLockAsync("testKey1"));
            Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));

            item.Close();
        }

        [Fact]
        public async Task TestSendHeartbeatChangeData()
        {
            // ARRANGE 
            string data1 = $"testSendHeartbeatLeaveData{SECURE_RANDOM.NextDouble()}";
            string data2 = $"testSendHeartbeatLeaveData{SECURE_RANDOM.NextDouble()}";

            // ACT
            LockItem item = await this.lockClient.AcquireLockAsync(new AcquireLockOptions("testKey1") { DeleteLockOnRelease = true, ReplaceData = true, Data = GetMemoryStream(data1) });

            // ASSERT
            Assert.Equal(data1, Encoding.UTF8.GetString(item.Data.ToArray()));

            item = (await lockClient.GetLockFromDynamoDBAsync(new GetLockOptions("testKey1")));
            Assert.Equal(data1, Encoding.UTF8.GetString(item.Data.ToArray()));
            lockClient.SendHeartbeat(new SendHeartbeatOptions(item) { Data = GetMemoryStream(data2) });
            item = (await lockClient.GetLockFromDynamoDBAsync(new GetLockOptions("testKey1")));
            Assert.Equal(data2, Encoding.UTF8.GetString(item.Data.ToArray()));

            item.Close();
        }

        [Fact]
        public async Task TestSendHeartbeatRemoveData()
        {
            // ARRANGE 
            string data = $"testSendHeartbeatLeaveData{SECURE_RANDOM.NextDouble()}";

            // ACT
            LockItem item = await this.lockClient.AcquireLockAsync(new AcquireLockOptions("testKey1") { DeleteLockOnRelease = true, ReplaceData = true, Data = GetMemoryStream(data) });

            // ASSERT
            Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));

            item = (await lockClient.GetLockFromDynamoDBAsync(GET_LOCK_OPTIONS_DELETE_ON_RELEASE));
            Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));
            lockClient.SendHeartbeat(new SendHeartbeatOptions(item) { DeleteData = true });
            Assert.Null((await lockClient.GetLockFromDynamoDBAsync(GET_LOCK_OPTIONS_DELETE_ON_RELEASE)).Data);

            item.Close();
        }

        [Fact]
        public async Task TestReleaseLockLeaveItem()
        {
            // ARRANGE 
            string data = "testReleaseLockLeaveItem";

            // ACT
            LockItem item = await this.lockClient.AcquireLockAsync(new AcquireLockOptions("testKey1") { DeleteLockOnRelease = false, ReplaceData = true, Data = GetMemoryStream(data) });

            // ASSERT
            Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));

            item.Close();

            Assert.Null(await this.lockClient.GetLockAsync("testKey1"));
            Assert.True((await this.lockClient.GetLockFromDynamoDBAsync(GET_LOCK_OPTIONS_DO_NOT_DELETE_ON_RELEASE)).IsReleased());
            item = (await this.lockClient.GetLockFromDynamoDBAsync(GET_LOCK_OPTIONS_DO_NOT_DELETE_ON_RELEASE));
            Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));

            item = await this.lockClient.AcquireLockAsync(new AcquireLockOptions("testKey1") { DeleteLockOnRelease = false, ReplaceData = false });

            Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));

            item.Close();
        }

        [Fact]
        public async Task TestReleaseLockLeaveItemAndChangeData()
        {
            // ARRANGE 
            string data = "testReleaseLockLeaveItem";

            // ACT
            LockItem item = await this.lockClient.AcquireLockAsync(new AcquireLockOptions("testKey1") { DeleteLockOnRelease = false, ReplaceData = true, Data = GetMemoryStream(data) });

            // ASSERT
            Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));

            this.lockClient.ReleaseLock(new ReleaseLockOptions(item) { DeleteLock = false, Data = GetMemoryStream("newData") });

            Assert.Null(await this.lockClient.GetLockAsync("testKey1"));
            Assert.True((await this.lockClient.GetLockFromDynamoDBAsync(GET_LOCK_OPTIONS_DO_NOT_DELETE_ON_RELEASE)).IsReleased());
            item = (await this.lockClient.GetLockFromDynamoDBAsync(GET_LOCK_OPTIONS_DO_NOT_DELETE_ON_RELEASE));
            Assert.Equal("newData", Encoding.UTF8.GetString(item.Data.ToArray()));

            item = await this.lockClient.AcquireLockAsync(new AcquireLockOptions("testKey1") { DeleteLockOnRelease = false, ReplaceData = false });

            Assert.Equal("newData", Encoding.UTF8.GetString(item.Data.ToArray()));

            item.Close();
        }

        [Fact]
        public async Task TestReleaseLockRemoveItem()
        {
            // ARRANGE 
            string data = "testReleaseLockRemoveItem";

            // ACT
            LockItem item = await this.lockClient.AcquireLockAsync(new AcquireLockOptions("testKey1") { DeleteLockOnRelease = true, ReplaceData = true, Data = GetMemoryStream(data) });

            // ASSERT
            Assert.Equal(data, Encoding.UTF8.GetString(item.Data.ToArray()));

            item.Close();
            item = await this.lockClient.AcquireLockAsync(new AcquireLockOptions("testKey1") { DeleteLockOnRelease = true, ReplaceData = false });
            Assert.Null(item.Data);

            item.Close();
        }

        [Fact]
        public async Task TestReleaseLockBestEffort()
        {
            // ARRANGE
            IAmazonDynamoDBLockClient client = new AmazonDynamoDBLockClient(this.lockClient1Options);
            LockItem item = await client.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1);

            this.dynamoDBMock.Setup(x => x.DeleteItemAsync(It.IsAny<DeleteItemRequest>(), default(CancellationToken))).ThrowsAsync(new AmazonClientException("Client exception releasing lock"));

            ReleaseLockOptions options = new ReleaseLockOptions(item) { BestEffort = true };

            // ACT

            // ASSERT
            Assert.True(client.ReleaseLock(options));

            client.Close();
        }

        [Fact]
        public async Task TestReleaseLockNotBestEffort()
        {
            // ARRANGE
            IAmazonDynamoDBLockClient client = new AmazonDynamoDBLockClient(new AmazonDynamoDBLockClientOptions(this.idynamodb, TABLE_NAME) { OwnerName = LOCALHOST });
            LockItem item = await client.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1);

            this.dynamoDBMock.Setup(x => x.DeleteItemAsync(It.IsAny<DeleteItemRequest>(), default(CancellationToken))).ThrowsAsync(new AmazonClientException("Client exception releasing lock"));

            // ACT

            // ASSERT
            try
            {
                Assert.Throws<AmazonClientException>(() => client.ReleaseLock(item));
            }
            finally
            {
                client.Close();
            }
        }

        [Fact]
        public async Task TestAcquireLockAfterTimeout()
        {
            // ARRANGE
            LockItem item = await this.lockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1);

            // ACT

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);
            Assert.NotNull(await this.lockClient.TryAcquireLockAsync(ACQUIRE_LOCK_OPTIONS_5_SECONDS));
        }

        [Fact]
        public async Task TestSucceedToAcquireLockAfterTimeout()
        {
            // ARRANGE
            LockItem item = await this.lockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1);

            // ACT

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);
            Assert.NotNull(await this.lockClient.TryAcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_2));
            Assert.NotNull(await this.lockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_5_SECONDS));
        }

        [Fact]
        public async Task TestEmptyData()
        {
            // ARRANGE
            LockItem item = await this.lockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1_NO_DATA);

            // ACT

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);
            Assert.NotNull(await this.lockClient.TryAcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_2_NO_DATA));
            Assert.Null((await this.lockClient.GetLockAsync("testKey1")).Data);
            Assert.Null((await this.lockClient.GetLockAsync("testKey2")).Data);
        }

        [Fact]
        public void TestSetupTooSmallHearbeatCheck()
        {
            // ASSERT
            Assert.Throws<ArgumentException>(() => new AmazonDynamoDBLockClient(new AmazonDynamoDBLockClientOptions(this.idynamodb, TABLE_NAME) { OwnerName = INTEGRATION_TESTER, LeaseDuration = 5, HeartbeatPeriod = 4, TimeUnit = TimeUnit.SECONDS, CreateHeartbeatBackgroundThread = true }));
        }

        [Fact]
        public async Task TestAcquiringSomeoneElsesLock()
        {
            // ARRANGE
            IAmazonDynamoDBLockClient lockClient1 = new AmazonDynamoDBLockClient(this.lockClient1Options);

            IAmazonDynamoDBLockClient lockClient2 = new AmazonDynamoDBLockClient(
                new AmazonDynamoDBLockClientOptions(this.idynamodb, TABLE_NAME, INTEGRATION_TESTER_2)
                { LeaseDuration = 3, HeartbeatPeriod = 1, TimeUnit = TimeUnit.SECONDS, CreateHeartbeatBackgroundThread = false });

            // ACT
            LockItem lockItem1 = await lockClient1.TryAcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1);

            // ASSERT
            Assert.NotNull(lockItem1);

            // Steal Lock
            LockItem lockItem2 = await lockClient2.TryAcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1);

            Assert.NotNull(lockItem2);
            Assert.Equal(INTEGRATION_TESTER_2, lockItem2.OwnerName);

            // Release a lock that is now expired, should not actuall succceed
            Assert.False(lockClient1.ReleaseLock(lockItem1));

            // Make sure the lock is still there and owned by unitTester2
            LockItem lockItem3 = await lockClient2.GetLockAsync("testKey1");
            Assert.Equal(INTEGRATION_TESTER_2, lockItem3.OwnerName);

            lockClient1.Close();
            lockClient2.Close();
        }

        [Fact]
        public async Task CanReleaseExpiredLock()
        {
            // ARRANGE
            LockItem item = await this.lockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1);

            // ACT

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);

            Thread.Sleep(4000);
            item.Close();

            Assert.Null(await this.lockClient.GetLockAsync("testKey1"));
        }

        [Fact]
        public async Task CannotHeartbeatExpiredLock()
        {
            // ARRANGE
            LockItem item = await this.lockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1);

            // ACT

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);

            // Sleep for 1 more second than lease duration
            Thread.Sleep((DEFAULT_LEASE_DURATION_SECONDS * 1000) + 1000);
            Assert.Throws<LockNotGrantedException>(() => item.SendHeartbeat());
        }

        [Fact]
        public async Task CannotReleaseLockYouDontOwn()
        {
            // ARRANGE
            LockItem item = await this.lockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1);
            IAmazonDynamoDBLockClient lockClient2 = new AmazonDynamoDBLockClient(
                new AmazonDynamoDBLockClientOptions(this.idynamodb, TABLE_NAME, INTEGRATION_TESTER_2)
                { LeaseDuration = 30, HeartbeatPeriod = 2, TimeUnit = TimeUnit.SECONDS, CreateHeartbeatBackgroundThread = false });

            // ACT

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);

            LockItem item2 = await lockClient2.GetLockAsync("testKey1");

            Assert.False(lockClient2.ReleaseLock(item2));
            lockClient2.Close();
        }

        [Fact]
        public async Task TestNullLock()
        {
            // ARRANGE

            // ACT

            // ASSERT
            Assert.Null(await this.lockClient.GetLockAsync("lock1"));
        }

        [Fact]
        public async Task TestReleaseAllLocks()
        {
            // ARRANGE

            // ACT
            await this.lockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1);
            await this.lockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_2);
            await this.lockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_3);

            // ASSERT
            Assert.NotNull(await this.lockClient.GetLockAsync("testKey1"));
            Assert.NotNull(await this.lockClient.GetLockAsync("testKey2"));
            Assert.NotNull(await this.lockClient.GetLockAsync("testKey3"));

            this.lockClient.Close();

            Assert.Null(await this.lockClient.GetLockAsync("testKey1"));
            Assert.Null(await this.lockClient.GetLockAsync("testKey2"));
            Assert.Null(await this.lockClient.GetLockAsync("testKey3"));
        }

        [Fact]
        public async Task TestEnsureLock()
        {
            // ARRANGE
            LockItem item = await this.lockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1);

            // ACT

            // ASSERT
            Assert.Equal(DEFAULT_LEASE_DURATION_SECONDS * 1000, (await this.lockClient.GetLockAsync("testKey1")).LeaseDuration);
            string versionNumber = (await this.lockClient.GetLockAsync("testKey1")).RecordVersionNumber;

            // We have the lock for at least more than 1 second, so this should do nothing
            item.Ensure(1, TimeUnit.SECONDS);
            Assert.Equal(DEFAULT_LEASE_DURATION_SECONDS * 1000, (await this.lockClient.GetLockAsync("testKey1")).LeaseDuration);
            Assert.Equal(versionNumber, (await this.lockClient.GetLockAsync("testKey1")).RecordVersionNumber);

            // Now let's extend it so we have the lock for +1 seconds
            item.Ensure(DEFAULT_LEASE_DURATION_SECONDS  + 1, TimeUnit.SECONDS);
            Assert.Equal((DEFAULT_LEASE_DURATION_SECONDS + 1) * 1000, (await this.lockClient.GetLockAsync("testKey1")).LeaseDuration);
            Assert.NotEqual(versionNumber, (await this.lockClient.GetLockAsync("testKey1")).RecordVersionNumber);

            Thread.Sleep(2400);

            // Now the lock is about to expire, so we can still extend it
            item.Ensure(DEFAULT_LEASE_DURATION_SECONDS - 1, TimeUnit.SECONDS);
            Assert.Equal((DEFAULT_LEASE_DURATION_SECONDS - 1) * 1000, (await this.lockClient.GetLockAsync("testKey1")).LeaseDuration);
            Assert.NotEqual(versionNumber, (await this.lockClient.GetLockAsync("testKey1")).RecordVersionNumber);

            item.Close();
        }
              
        [Fact]
        public async Task TestGetLock()
        {
            // ARRANGE

            // ACT
            await this.lockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1);

            // ASSERT
            Assert.NotNull(await this.lockClient.GetLockAsync("testKey1"));
            (await this.lockClient.GetLockAsync("testKey1")).Close();
            Assert.Null(await this.lockClient.GetLockAsync("testKey1"));
        }

        [Fact]
        public async Task TestRangeKey()
        {
            // ARRANGE
            IAmazonDynamoDBLockClient lockClient1 = new AmazonDynamoDBLockClient(
               new AmazonDynamoDBLockClientOptions(this.idynamodb, RANGE_KEY_TABLE_NAME, INTEGRATION_TESTER_2)
               { LeaseDuration = 10, HeartbeatPeriod = 1, TimeUnit = TimeUnit.SECONDS, SortKeyName = "rangeKey", CreateHeartbeatBackgroundThread = true });

            IAmazonDynamoDBLockClient lockClient2 = new AmazonDynamoDBLockClient(
                new AmazonDynamoDBLockClientOptions(this.idynamodb, RANGE_KEY_TABLE_NAME, INTEGRATION_TESTER_2)
                { LeaseDuration = 10, HeartbeatPeriod = 1, TimeUnit = TimeUnit.SECONDS, SortKeyName = "rangeKey", CreateHeartbeatBackgroundThread = true });


            // ACT
            LockItem item = await lockClient1.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1_SORT_1);

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);
            Assert.Equal("1", (await lockClient1.GetLockAsync("testKey1", "1")).SortKey);

            LockItem @lock = await lockClient2.TryAcquireLockAsync(new AcquireLockOptions("testKey1") { Data = TEST_DATA_STREAM, ReplaceData = false, DeleteLockOnRelease = false, RefreshPeriod = 0, AdditionalTimeToWaitForLock = 0, TimeUnit = TimeUnit.MILLISECONDS, SortKey = "1" });

            Assert.Null(@lock);
            item.Close();

            @lock = await lockClient2.TryAcquireLockAsync(new AcquireLockOptions("testKey1") { Data = TEST_DATA_STREAM, ReplaceData = false, DeleteLockOnRelease = false, RefreshPeriod = 0, AdditionalTimeToWaitForLock = 0, TimeUnit = TimeUnit.MILLISECONDS, SortKey = "1" });
            Assert.NotNull(@lock);

            lockClient1.Close();
            lockClient2.Close();
        }

        [Fact]
        public async Task TestRangeKeyAcquiredAfterTimeout()
        {
            // ARRANGE
            IAmazonDynamoDBLockClient lockClient1 = new AmazonDynamoDBLockClient(
               new AmazonDynamoDBLockClientOptions(this.idynamodb, RANGE_KEY_TABLE_NAME, INTEGRATION_TESTER_2)
               { LeaseDuration = 5, HeartbeatPeriod = 1, TimeUnit = TimeUnit.SECONDS, SortKeyName = "rangeKey", CreateHeartbeatBackgroundThread = false });

            IAmazonDynamoDBLockClient lockClient2 = new AmazonDynamoDBLockClient(
                new AmazonDynamoDBLockClientOptions(this.idynamodb, RANGE_KEY_TABLE_NAME, INTEGRATION_TESTER_3)
                { LeaseDuration = 5, HeartbeatPeriod = 1, TimeUnit = TimeUnit.SECONDS, SortKeyName = "rangeKey", CreateHeartbeatBackgroundThread = false });


            // ACT
            LockItem item = await lockClient1.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1_SORT_1);

            // ASSERT
            Assert.Equal("testKey1", item.PartitionKey);
            Assert.Equal("1", (await lockClient1.GetLockAsync("testKey1", "1")).SortKey);

            LockItem item2 = await lockClient2.TryAcquireLockAsync(new AcquireLockOptions("testKey1") { Data = TEST_DATA_STREAM, ReplaceData = false, DeleteLockOnRelease = false, RefreshPeriod = 3, AdditionalTimeToWaitForLock = 1, TimeUnit = TimeUnit.SECONDS, SortKey = "1" });

            Assert.NotNull(item2);

            item2 = await lockClient2.GetLockAsync("testKey1", "1");
            Assert.False(item2.IsExpired());

            lockClient1.Close();
            lockClient2.Close();
        }

        [Fact]
        public async Task TestAdditionalAttributes()
        {
            // ARRANGE
            Dictionary<string, AttributeValue> additionalAttributes = new Dictionary<string, AttributeValue>()
            {
                { TABLE_NAME, new AttributeValue("ok") }
            };

            // ACT
            LockItem item = await this.lockClient.AcquireLockAsync(new AcquireLockOptions("testKey1") { Data = TEST_DATA_STREAM, AdditionalAttributes = additionalAttributes });

            // ASSERT
            Assert.Equal("ok", item.AdditionalAttributes[TABLE_NAME].S);
            item = (await this.lockClient.GetLockAsync("testKey1"));
            Assert.Equal("ok", item.AdditionalAttributes[TABLE_NAME].S);

            IAmazonDynamoDBLockClient client = new AmazonDynamoDBLockClient(this.lockClient1Options);

            item = (await client.GetLockAsync("testKey1"));
            Assert.Equal("ok", item.AdditionalAttributes[TABLE_NAME].S);
            Assert.Equal(item.AdditionalAttributes.Count, additionalAttributes.Count);
            Assert.True(item.AdditionalAttributes.Select(x => $"{x.Key}{x.Value.S}").SequenceEqual(additionalAttributes.Select(x => $"{x.Key}{x.Value.S}")));

            client.Close();
        }

        [Fact]
        public async Task TestInvalidAttributesData()
        {
            // ASSERT
            await Assert.ThrowsAsync<ArgumentException>(() => this.TestInvalidAttribute("data"));
        }

        [Fact]
        public async Task TestInvalidAttributesKey()
        {
            // ASSERT
            await Assert.ThrowsAsync<ArgumentException>(() => this.TestInvalidAttribute("key"));
        }

        [Fact]
        public async Task TestInvalidAttributesLeaseDuration()
        {
            // ASSERT
            await Assert.ThrowsAsync<ArgumentException>(() => this.TestInvalidAttribute("leaseDuration"));
        }

        [Fact]
        public async Task TestInvalidAttributesRecordVersionNumber()
        {
            // ASSERT
            await Assert.ThrowsAsync<ArgumentException>(() => this.TestInvalidAttribute("recordVersionNumber"));
        }

        [Fact]
        public async Task TestInvalidAttributesOwnerName()
        {
            // ASSERT
            await Assert.ThrowsAsync<ArgumentException>(() => this.TestInvalidAttribute("ownerName"));
        }

        [Fact]
        public async Task TestLockItemToString()
        {
            // ARRANGE
            LockItem item = await this.lockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1);
            Regex regex = new Regex("LockItem\\{Partition Key=testKey1, Sort Key=, Owner Name=" + INTEGRATION_TESTER + ", Lookup Time=\\d+, Lease Duration=" + DEFAULT_LEASE_DURATION_SECONDS * 1000 + ", "
            + "Record Version Number=\\w+-\\w+-\\w+-\\w+-\\w+, Delete On Close=true, Is Released=false\\}");

            // ACT
            string str = item.ToString();
            System.Text.RegularExpressions.Match match = regex.Match(str);
            // ASSERT

            Assert.True(match.Success);
        }

        [Fact]
        public async Task TestLockItemHasSessionMonitor()
        {
            // ARRANGE
            LockItem item = await this.GetShortLeaseLockWithSessionMonitor(30);

            // ACT

            // ASSERT
            Assert.True(item.HasSessionMonitor());
            item.Close();
        }

        [Fact]
        public async Task TestLockItemDoesNotHaveSessionMonitor()
        {
            // ARRANGE
            LockItem item = await this.GetShortLeaseLock();

            // ACT

            // ASSERT
            Assert.False(item.HasSessionMonitor());
            item.Close();
        }

        [Fact]
        public async Task TestSafeTimeLessThanHeartbeat()
        {
            // ARRANGE
            long badDangerZoneTimeMillis = 10; // must be greater than heartbeat frequency, which is 10 millis

            // ACT

            // ASSERT
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => this.GetShortLeaseLockWithSessionMonitor(badDangerZoneTimeMillis));
        }

        [Fact]
        public async Task TestSafeTimeMoreThanLeaseDuration()
        {
            // ARRANGE
            long badDangerZoneTimeMillis = long.MaxValue; // must be less than lease duration

            // ACT

            // ASSERT
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => this.GetShortLeaseLockWithSessionMonitor(badDangerZoneTimeMillis));
        }

        [Fact]
        public void TestTimeUnitNotSetInAcquireLockOptionsWithSessionMonitor()
        {
            // ARRANGE
            AcquireLockOptions options = new AcquireLockOptions("testKey1");

            // ACT

            // ASSERT
            Assert.Throws<ArgumentException>(() => options.AddSessionMonitor(SHORT_LEASE_DUR / 2));
        }

        [Fact]
        public async Task TestFiringCallbackWithoutSessionMonitor()
        {
            // ARRANGE
            LockItem item = await this.GetShortLeaseLock();

            // ACT

            // ASSERT

            try
            {
                await Assert.ThrowsAsync<SessionMonitorNotSetException>(() => item.RunSessionMonitor());
            }
            finally
            {
                item.Close();
            }
        }

        [Fact]
        public async Task TestAboutToExpireWithoutSessionMonitor()
        {
            // ARRANGE
            LockItem item = await this.GetShortLeaseLock();

            // ACT

            // ASSERT

            try
            {
                Assert.Throws<SessionMonitorNotSetException>(() => item.AmIAboutToExpire());
            }
            finally
            {
                item.Close();
            }
        }

        [Fact]
        public async Task TestSessionMonitorCallbackFiredOnNonHeartbeatingLock()
        {
            // ARRANGE
            CountdownEvent cde = new CountdownEvent(1);

            // ACT
            LockItem item = await this.GetShortLeaseLockWithSessionMonitor(SHORT_LEASE_DUR / 2, NotifyObj(cde));

            // ASSERT

            // Since there is no heartbeating thread, the lock doesn't get renewed: the callback should fire.
            // If the callback is not run in a timely manner, waitOn() will throw an AssertionError
            WaitOn(cde, SHORT_LEASE_DUR + 500); // Goal is just SHORT_LEASE_DUR, but this test is a race condition, adding
            // 500 more ms typically helps it pass nearly all of the time
            Debug.WriteLine($"{LockClientUtils.MillisecondTime()} : Calling ASSERT");
            Assert.True(cde.IsSet);
        }

        
        [Fact]
        public async Task TestShortLeaseNoHeartbeatCallbackFired()
        {
            // ASSERT
            await this.TestDangerZoneCallback(200, 0, 15, 170);
        }
        
        
        [Fact]
        public async Task TestLongLeaseHeartbeatCallbackFired()
        {
            // ASSERT
            await this.TestDangerZoneCallback(6000, 5, 100, 3500);
        }
        
        
        [Fact]
        public async Task TestHeartbeatAfterSafetyTimeout()
        {
            // ASSERT
            await this.TestDangerZoneCallback(100, 0, 60, 80);
        }
        
        /*
        [Fact]
        public async Task TestCallbackNotCalledOnClosingClient()
        {
            // ARRANGE
            long heartbeatFreq = 100;
            Mock<AmazonDynamoDBLockClient> heartClient = new Mock<AmazonDynamoDBLockClient>(new AmazonDynamoDBLockClientOptions(this.idynamodb, TABLE_NAME, LOCALHOST)
            {
                LeaseDuration = 3 * heartbeatFreq,
                HeartbeatPeriod = heartbeatFreq,
                TimeUnit = TimeUnit.MILLISECONDS,
                CreateHeartbeatBackgroundThread = true
            })
            {
                CallBase = true
            };

            AtomicLong counter = new AtomicLong();
            Action intSetter = () =>
            {
                counter.Set(1);
            };
            AcquireLockOptions options = StdSessionMonitorOptions(2 * heartbeatFreq, intSetter);

            // ACT
            LockItem item = await heartClient.Object.AcquireLockAsync(options);
            heartClient.Object.Close();

            foreach (int i in LockClientUtils.Decompose64BitInt(item.LeaseDuration))
            {
                Thread.Sleep(i);
            }

            // ASSERT
            Assert.Equal(0, counter);
        }
        */
        [Fact]
        public async Task TestHeartbeatAllLocks()
        {
            // ARRANGE
            long heartbeatFreq = 100;
            Mock<AmazonDynamoDBLockClient> heartClient = new Mock<AmazonDynamoDBLockClient>(new AmazonDynamoDBLockClientOptions(this.dynamoDBMock.Object, TABLE_NAME, LOCALHOST)
            {
                LeaseDuration = 3 * heartbeatFreq,
                HeartbeatPeriod = heartbeatFreq,
                TimeUnit = TimeUnit.MILLISECONDS,
                CreateHeartbeatBackgroundThread = false
            })
            {
                CallBase = true
            };

            LockItem lockItem1 = await heartClient.Object.AcquireLockAsync(new AcquireLockOptions("lock1"));
            LockItem lockItem2 = await heartClient.Object.AcquireLockAsync(new AcquireLockOptions("lock2"));
            LockItem lockItem3 = await heartClient.Object.AcquireLockAsync(new AcquireLockOptions("lock3"));

            heartClient.Setup(x => x.SendHeartbeat(lockItem1)).Throws(new Exception("lock1"));
            heartClient.Setup(x => x.SendHeartbeat(lockItem2)).Verifiable();
            heartClient.Setup(x => x.SendHeartbeat(lockItem3)).Throws(new Exception("lock3"));

            CancellationTokenSource src = new CancellationTokenSource();
            Task t = Task.Run(() => heartClient.Object.Run(src.Token), src.Token);

            Thread.Sleep(LockClientUtils.Decompose64BitInt(heartbeatFreq * 6).First()); // Should be 5, but thread scheduing makes this a race condition

            src.Cancel(); // thread interrupt
            t.Wait(LockClientUtils.Decompose64BitInt(heartbeatFreq * 5).First()); // thread join

            // ASSERT
            heartClient.Verify(x => x.SendHeartbeat(lockItem1), Times.AtLeast(4));
            heartClient.Verify(x => x.SendHeartbeat(lockItem2), Times.AtLeast(4));
            heartClient.Verify(x => x.SendHeartbeat(lockItem3), Times.AtLeast(4));

            heartClient.Object.Close();
        }

        [Fact]
        public async Task TestAcquireLockOnClientException()
        {
            // ARRANGE
            string lockName = "lock1";
            long heartbeatFreq = 100L;
            long refreshPeriod = 200L;
            int expectedAttempts = 3;
            long additionalWaitTime = expectedAttempts * refreshPeriod;

            Mock<AmazonDynamoDBLockClient> testLockClient = new Mock<AmazonDynamoDBLockClient>(new AmazonDynamoDBLockClientOptions(this.idynamodb, TABLE_NAME, LOCALHOST)
            {
                LeaseDuration = expectedAttempts * heartbeatFreq,
                HeartbeatPeriod = heartbeatFreq,
                TimeUnit = TimeUnit.MILLISECONDS,
                CreateHeartbeatBackgroundThread = false
            })
            {
                CallBase = true
            };

            testLockClient.Setup(x => x.GetLockFromDynamoDBAsync(It.IsAny<GetLockOptions>())).ThrowsAsync(new AmazonClientException("Client exception acquiring lock"));

            // ACT

            // ASSERT

            await Assert.ThrowsAsync<LockNotGrantedException>(() => testLockClient.Object.AcquireLockAsync(new AcquireLockOptions(lockName) { RefreshPeriod = refreshPeriod, AdditionalTimeToWaitForLock = additionalWaitTime, TimeUnit = TimeUnit.MILLISECONDS }));
            testLockClient.Verify(x => x.GetLockFromDynamoDBAsync(It.IsAny<GetLockOptions>()), Times.AtLeast(expectedAttempts + 1));
            testLockClient.Object.Close();            
        }

        [Fact]
        public async Task TestSetRequestLevelMetricCollector()
        {
            // ARRANGE
            Mock<AmazonDynamoDBClient> dynamoDB = new Mock<AmazonDynamoDBClient>();

            List<GetItemRequest> getItemRequests = new List<GetItemRequest>();
            List<PutItemRequest> putItemRequests = new List<PutItemRequest>();

            dynamoDB.Setup(x => x.GetItemAsync(Moq.Capture.In<GetItemRequest>(getItemRequests), default(CancellationToken))).ReturnsAsync(new GetItemResponse() { Item = null });
            dynamoDB.Setup(x => x.PutItemAsync(Moq.Capture.In<PutItemRequest>(putItemRequests), default(CancellationToken))).ReturnsAsync(new PutItemResponse());

            AmazonDynamoDBLockClientOptions lockClientOptions = new AmazonDynamoDBLockClientOptions(dynamoDB.Object, TABLE_NAME)
            {
                OwnerName = "ownerName0",
                TimeUnit = TimeUnit.SECONDS,
                SortKeyName = "rangeKeyName"
            };

            AcquireLockOptions acquireLockOptions = new AcquireLockOptions("key0")
            {
                SortKey = "rangeKey0",
                DeleteLockOnRelease = false
            };

            AmazonDynamoDBLockClient testLockClient = new AmazonDynamoDBLockClient(lockClientOptions);

            try
            {
                // ACT
                await testLockClient.AcquireLockAsync(acquireLockOptions);
            }
            finally
            {
                // ASSERT
                dynamoDB.Verify(x => x.GetItemAsync(getItemRequests.First(), default(CancellationToken)));
                dynamoDB.Verify(x => x.PutItemAsync(putItemRequests.First(), default(CancellationToken)));
            }
        }

        #region Private Methods

        private async Task TestInvalidAttribute(string invalidAttribute)
        {
            Dictionary<string, AttributeValue> additionalAttributes = new Dictionary<string, AttributeValue>()
            {
                { invalidAttribute, new AttributeValue("ok") }
            };

            await this.lockClient.AcquireLockAsync(new AcquireLockOptions("testKey1") { Data = TEST_DATA_STREAM, AdditionalAttributes = additionalAttributes });
        }

        private async Task<LockItem> GetShortLeaseLockWithSessionMonitor(long safeTimeMillis)
        {
            return await GetShortLeaseLockWithSessionMonitor(safeTimeMillis, null);
        }

        private async Task<LockItem> GetShortLeaseLockWithSessionMonitor(long safeTimeMillis, Action callback)
        {
            return await this.shortLeaseLockClient.AcquireLockAsync(StdSessionMonitorOptions(safeTimeMillis, callback));
        }

        private async Task<LockItem> GetShortLeaseLock()
        {
            return await this.shortLeaseLockClient.AcquireLockAsync(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1);
        }

        private static AcquireLockOptions StdSessionMonitorOptions(long safeTimeMillis, Action callback)
        {
            AcquireLockOptions options = new AcquireLockOptions("testKey1")
            {
                TimeUnit = TimeUnit.MILLISECONDS
            };

            if (callback != null)
            {
                options.AddSessionMonitor(safeTimeMillis, callback);
            }
            else
            {
                options.AddSessionMonitor(safeTimeMillis);
            }

            return options;
        }

        private static bool WaitOn(CountdownEvent cde, long timeoutMillis)
        {
            lock (cde)
            {
                // Need the timeout since pulse could be called before we start waiting
                return cde.Wait(LockClientUtils.Decompose64BitInt(timeoutMillis).First());
            }
        }

        private static Action NotifyObj(CountdownEvent cde)
        {
            return () =>
            {
                lock (cde)
                {
                    cde.Signal();
                    Debug.WriteLine($"{LockClientUtils.MillisecondTime()} : Signaled CDE");
                }
            };
        }

        private async Task TestDangerZoneCallback(long leaseDurationMillis, int nHeartbeats, long heartbeatFrequencyMillis, long safeTimeWithoutHeartbeatMillis)
        {
            IAmazonDynamoDBLockClient dbClient = new AmazonDynamoDBLockClient(new AmazonDynamoDBLockClientOptions(this.idynamodb, TABLE_NAME, INTEGRATION_TESTER)
            {
                LeaseDuration = leaseDurationMillis,
                HeartbeatPeriod = heartbeatFrequencyMillis,
                TimeUnit = TimeUnit.MILLISECONDS,
                CreateHeartbeatBackgroundThread = false
            });

            CountdownEvent heartbeatCount = new CountdownEvent(nHeartbeats);
            CountdownEvent callbackSignal = new CountdownEvent(1);

            try
            {
                // Acquire the lock, set a session monitor to call
                // heartbeatCount.notify() when the lock's lease goes
                // into the danger zone
                AcquireLockOptions options = StdSessionMonitorOptions(safeTimeWithoutHeartbeatMillis, NotifyObj(callbackSignal));
                LockItem item = await dbClient.AcquireLockAsync(options);

                long estimatedLeaseLifetimeMillis = (nHeartbeats * heartbeatFrequencyMillis) + leaseDurationMillis;
                // Start heartbeating on the lock (runs in the background)
                CountdownEvent taskStartedSignal = new CountdownEvent(1);
                Task t = HeartBeatNTimes(item, heartbeatCount, nHeartbeats, heartbeatFrequencyMillis, taskStartedSignal);
                taskStartedSignal.Wait(); // wait to make sure the task is running

                // Wait for heartbeatCount.notify()
                int wait = LockClientUtils.Decompose64BitInt(estimatedLeaseLifetimeMillis).First();
                bool success = WaitOn(callbackSignal, wait);

                // Lock should be in danger zone, not expired
                Assert.False(item.IsExpired());
                Assert.True(item.AmIAboutToExpire());

                // We should have heartbeated the proper number of times, reducing the count to zero
                Assert.True(heartbeatCount.IsSet);
            }
            finally
            {
                dbClient.Close();
            }
        }

        private static Task HeartBeatNTimes(LockItem item, CountdownEvent heartbeatCount, int nHeartbeats, long heartbeatFreqMillis, CountdownEvent taskStartedSignal)
        {
            return Task.Run(() =>
            {
                taskStartedSignal.Signal(); // Make sure the task is actually running, signal the cde 
                for (heartbeatCount.Reset(nHeartbeats); heartbeatCount.CurrentCount > 0; heartbeatCount.Signal())
                {
                    long startTimeMillis = LockClientUtils.MillisecondTime();
                    item.SendHeartbeat();
                    long timeSpentWorkingMillis = LockClientUtils.MillisecondTime() - startTimeMillis;
                    long timeLeftToSleepMillis = Math.Max(0, heartbeatFreqMillis - timeSpentWorkingMillis);

                    foreach (int j in LockClientUtils.Decompose64BitInt(timeLeftToSleepMillis))
                    {
                        Thread.Sleep(j);
                    }
                }
            });
        }

        #endregion
    }
}
