using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2.Util;
using System;
using System.Collections.Generic;
using System.Reflection;
using Xunit;

namespace Amazon.DynamoDBv2.Tests
{
    public class ReleaseLockOptionsTests
    {
        [Fact]
        public void WithLockItem_SetsLockItem()
        {
            // ARRANGE
            LockItem lockItem = (LockItem)Activator.CreateInstance(typeof(LockItem), BindingFlags.NonPublic | BindingFlags.Instance, null, new object[] {
                new AmazonDynamoDBLockClient(new AmazonDynamoDBLockClientOptions(new AmazonDynamoDBClient(), "lockTable")),
                "asdf",
                "sortKey",
                null,
                false,
                "owner",
                10000,
                LockClientUtils.MillisecondTime(),
                Guid.NewGuid().ToString(),
                false,
                null,
                new Dictionary<string, AttributeValue>()
            }, null);

            ReleaseLockOptions options = new ReleaseLockOptions(lockItem);

            // ACT

            // ASSERT
            Assert.Equal(lockItem, options.LockItem);
        }
    }
}
