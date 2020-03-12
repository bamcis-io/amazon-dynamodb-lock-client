using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2.Util;
using BAMCIS.Util.Concurrent;
using Moq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using Xunit;

namespace Amazon.DynamoDBv2.Tests
{
    public class LockItemTests
    {
        private static AmazonDynamoDBLockClient lockClient = new AmazonDynamoDBLockClient(new AmazonDynamoDBLockClientOptions(new AmazonDynamoDBClient(), "lockTable"));

        [Fact]
        public void HashCode_Returns()
        {
            // ARRANGE
            CreateLockItem().GetHashCode();

            // ACT

            // ASSERT
        }

        [Fact]
        public void Equals_RightNull_ReturnFalse()
        {
            // ARRANGE

            // ACT

            // ASSERT
            Assert.False(CreateLockItem().Equals(null));
        }

        [Fact]
        public void Equals_RightNotLockItem_ReturnFalse()
        {
            // ARRANGE

            // ACT

            // ASSERT

            Assert.False(CreateLockItem().Equals(""));
        }

        [Fact]
        public void Equals_DifferentPartitionKey_ReturnFalse()
        {
            // ARRANGE
            LockItem left = CreateLockItem();
            LockItem right = (LockItem)Activator.CreateInstance(typeof(LockItem), BindingFlags.NonPublic | BindingFlags.Instance, null, new object[] {
                lockClient,
                "squat",
                "sortKey",
                new MemoryStream(Encoding.UTF8.GetBytes("data")),
                false, //delete lock item on close
                "ownerName",
                1L, //lease duration
                1000, //last updated time in milliseconds
                "recordVersionNumber",
                false, //released
                new SessionMonitor(1000), //session monitor
                new Dictionary<string, AttributeValue>()
            }, null);

            // ACT

            // ASSERT
            Assert.False(left.Equals(right));
        }

        [Fact]
        public void Equals_DifferentOwner_ReturnFalse()
        {
            // ARRANGE
            LockItem left = CreateLockItem();
            LockItem right = (LockItem)Activator.CreateInstance(typeof(LockItem), BindingFlags.NonPublic | BindingFlags.Instance, null, new object[] {
                lockClient,
                "partitionKey",
                "sortKey",
                new MemoryStream(Encoding.UTF8.GetBytes("data")),
                false, //delete lock item on close
                "squat",
                1L, //lease duration
                1000, //last updated time in milliseconds
                "recordVersionNumber",
                false, //released
                new SessionMonitor(1000), //session monitor
                new Dictionary<string, AttributeValue>()
            }, null);

            // ACT

            // ASSERT
            Assert.False(left.Equals(right));
        }

        [Fact]
        public void Equals_Same_ReturnTrue()
        {
            // ARRANGE
            LockItem left = CreateLockItem();
            LockItem right = CreateLockItem();

            // ACT

            // ASSERT
            Assert.True(left.Equals(right));
        }

        [Fact]
        public void IsExpired_WhenIsReleasedTrue_returnTrue()
        {
            // ARRANGE
            LockItem item = CreateReleasedLockItem();

            // ACT

            // ASSERT
            Assert.True(item.IsExpired());
        }

        [Fact]
        public void IsReleased_WhenIsReleasedFalseInConstructor_ReturnsFalse()
        {
            // ARRANGE
            LockItem item = CreateLockItem();

            // ACT

            // ASSERT
            Assert.False(item.IsReleased());
        }

        [Fact]
        public void Ensure_WhenIsReleasedTrue_ThrowsLockNotGrantedException()
        {
            // ARRANGE

            // ACT

            // ASSERT
            Assert.Throws<LockNotGrantedException>(() => CreateReleasedLockItem().Ensure(2, TimeUnit.MILLISECONDS));
        }

        [Fact]
        public void MillisecondsUntilDangerZoneEntered_WhenIsReleasedTrue_ThrowsIllegalStateException()
        {
            // ARRANGE
            LockItem item = CreateReleasedLockItem();
            MethodInfo Info = typeof(LockItem).GetMethod("MillisecondsUntilDangerZoneEntered", BindingFlags.NonPublic | BindingFlags.Instance);

            // ACT

            // ASSERT
            Assert.Throws<InvalidOperationException>(() =>
            {
                try { Info.Invoke(item, null); } catch (TargetInvocationException e) { throw e.InnerException; }
            });
        }

        [Fact]
        public void AmIAboutToExpire_WhenMsUntilDangerZoneEnteredZero_ReturnsTrue()
        {
            // ARRANGE
            Mock<LockItem> item = CreateLockItemMock();
            item.Setup(x => x.MillisecondsUntilDangerZoneEntered()).Returns(0);
                
            // ACT

            // ASSERT
            Assert.True(item.Object.AmIAboutToExpire());
        }

        [Fact]
        public void AmIAboutToExpire_WhenMsUntilDangerZoneEnteredOne_ReturnsFalse()
        {
            // ARRANGE
            var item = CreateLockItemMock();
            item.Setup(x => x.MillisecondsUntilDangerZoneEntered()).Returns(1);

            // ACT

            // ASSERT
            Assert.False(item.Object.AmIAboutToExpire());
        }

        [Fact]
        public void HasCallback_SessionMonitorNotPresent_ThrowSessionMonitorNotSetException()
        {
            // ARRANGE
            LockItem item = (LockItem)Activator.CreateInstance(typeof(LockItem), BindingFlags.NonPublic | BindingFlags.Instance, null, new object[] {
                new AmazonDynamoDBLockClient(new AmazonDynamoDBLockClientOptions(new AmazonDynamoDBClient(), "lockTable")),
                "partitionKey",
                "sortKey",
                new MemoryStream(Encoding.UTF8.GetBytes("data")),
                false,
                "ownerName",
                1, // Lease Duration
                1000, // Last updated time in milliseconds
                "recordVersionNumber",
                false, // Released
                null, //session monitor
                new Dictionary<string, AttributeValue>()
            }, null);

            // ACT

            // ASSERT
            Assert.Throws<SessionMonitorNotSetException>(() => item.HasCallback());
        }

        [Fact]
        public void UpdateLookupTime_WhenLookupTimeIsUpdated_ThenGetLookupTimeReturnsTheUpdatedTime()
        {
            // ARRANGE
            LockItem item = CreateLockItem();

            // ACT
            Assert.Equal(1000, item.LookupTime);
            item.UpdateLookupTime(2000);

            // ASSERT
            Assert.Equal(2000, item.LookupTime);
        }

        private static LockItem CreateLockItem()
        {
            return (LockItem)Activator.CreateInstance(typeof(LockItem), BindingFlags.NonPublic | BindingFlags.Instance, null, new object[] {
                new AmazonDynamoDBLockClient(new AmazonDynamoDBLockClientOptions(new AmazonDynamoDBClient(), "lockTable")),
                "partitionKey",
                "sortKey",
                new MemoryStream(Encoding.UTF8.GetBytes("data")),
                false,
                "ownerName",
                1, // Lease Duration
                1000, // Last updated time in milliseconds
                Guid.NewGuid().ToString(),
                false, // Released
                new SessionMonitor(1000), //session monitor
                new Dictionary<string, AttributeValue>()
            }, null);
        }

        private static Mock<LockItem> CreateLockItemMock()
        {
            return new Mock<LockItem>(
                MockBehavior.Loose,
                new AmazonDynamoDBLockClient(new AmazonDynamoDBLockClientOptions(new AmazonDynamoDBClient(), "lockTable")),
                "partitionKey",
                "sortKey",
                new MemoryStream(Encoding.UTF8.GetBytes("data")),
                false,
                "ownerName",
                1, // Lease Duration
                LockClientUtils.MillisecondTime(), // Last updated time in milliseconds
                Guid.NewGuid().ToString(),
                false, // Released
                new SessionMonitor(1000), //session monitor
                new Dictionary<string, AttributeValue>()
            );              
        }

        private static LockItem CreateReleasedLockItem()
        {
            return (LockItem)Activator.CreateInstance(typeof(LockItem), BindingFlags.NonPublic | BindingFlags.Instance, null, new object[] {
                new AmazonDynamoDBLockClient(new AmazonDynamoDBLockClientOptions(new AmazonDynamoDBClient(), "lockTable")),
                "partitionKey",
                "sortKey",
                new MemoryStream(Encoding.UTF8.GetBytes("data")),
                false,
                "ownerName",
                1, // Lease Duration
                LockClientUtils.MillisecondTime(), // Last updated time in milliseconds
                Guid.NewGuid().ToString(),
                true, // Released
                new SessionMonitor(1000), //session monitor
                new Dictionary<string, AttributeValue>()
            }, null);
        }
    }
}
