using Amazon.DynamoDBv2.Model;
using BAMCIS.Util.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Xunit;

namespace Amazon.DynamoDBv2.Tests
{
    public class AcquireLockOptionsTest
    {
        [Fact]
        public void Test_InitializeOptionsWithRequestMetrics()
        {
            // ARRANGE
            AcquireLockOptions options = new AcquireLockOptions("hashKey");

            // ACT

            // ASSERT
            Assert.NotNull(options);
        }

        [Fact]
        public void Equals_LeftOkRightNull_ReturnsFalse()
        {
            // ARRANGE
            AcquireLockOptions left = this.CreateLockOptions();

            // ACT

            // ASSERT
            Assert.False(left.Equals(null));
        }

        [Fact]
        public void Equals_LeftOkRightNotAcquireLockOptions_ReturnsFalse()
        {
            // ARRANGE
            AcquireLockOptions left = this.CreateLockOptions();

            // ACT

            // ASSERT
            Assert.False(left.Equals(""));
        }

        [Fact]
        public void Equals_AllSame_ReturnsTrue()
        {
            // ARRANGE
            AcquireLockOptions left = this.CreateLockOptions();
            AcquireLockOptions right = this.CreateLockOptions();
            
            // ACT

            // ASSERT
            Assert.Equal(left, right);
        }

        [Fact]
        public void Equals_PartitionKeyDifferent_ReturnsFalse()
        {
            // ARRANGE
            AcquireLockOptions left = this.CreateLockOptions();
            AcquireLockOptions right = new AcquireLockOptions("squat")
            {
                SortKey = "sortKey",
                Data = new MemoryStream(Encoding.UTF8.GetBytes("data")),
                ReplaceData = true,
                DeleteLockOnRelease = true,
                AcquireOnlyIfLockAlreadyExists = false,
                RefreshPeriod = 1,
                AdditionalTimeToWaitForLock = 1,
                TimeUnit = TimeUnit.MILLISECONDS,
                AdditionalAttributes = new Dictionary<string, AttributeValue>(),
                UpdateExistingLockRecord = false,
                ShouldSkipBlockingWait = true,
                AcquireReleasedLocksConsistently = true
            }; //.AddSessionMonitor(1); //never equal if session monitor is set

            // ACT

            // ASSERT
            Assert.False(left.Equals(right));
        }

        [Fact]
        public void Equals_SortKeyDifferent_ReturnsFalse()
        {
            // ARRANGE
            AcquireLockOptions left = this.CreateLockOptions();
            AcquireLockOptions right = new AcquireLockOptions("partitionKey")
            {
                SortKey = "squat",
                Data = new MemoryStream(Encoding.UTF8.GetBytes("data")),
                ReplaceData = true,
                DeleteLockOnRelease = true,
                AcquireOnlyIfLockAlreadyExists = false,
                RefreshPeriod = 1,
                AdditionalTimeToWaitForLock = 1,
                TimeUnit = TimeUnit.MILLISECONDS,
                AdditionalAttributes = new Dictionary<string, AttributeValue>(),
                UpdateExistingLockRecord = false,
                ShouldSkipBlockingWait = true,
                AcquireReleasedLocksConsistently = true
            }; //.AddSessionMonitor(1); //never equal if session monitor is set

            // ACT

            // ASSERT
            Assert.False(left.Equals(right));
        }

        [Fact]
        public void Equals_DataDifferent_ReturnsFalse()
        {
            // ARRANGE
            AcquireLockOptions left = this.CreateLockOptions();
            AcquireLockOptions right = new AcquireLockOptions("partitionKey")
            {
                SortKey = "sortKey",
                Data = new MemoryStream(Encoding.UTF8.GetBytes("squat")),
                ReplaceData = true,
                DeleteLockOnRelease = true,
                AcquireOnlyIfLockAlreadyExists = false,
                RefreshPeriod = 1,
                AdditionalTimeToWaitForLock = 1,
                TimeUnit = TimeUnit.MILLISECONDS,
                AdditionalAttributes = new Dictionary<string, AttributeValue>(),
                UpdateExistingLockRecord = false,
                ShouldSkipBlockingWait = true,
                AcquireReleasedLocksConsistently = true
            }; //.AddSessionMonitor(1); //never equal if session monitor is set

            // ACT

            // ASSERT
            Assert.False(left.Equals(right));
        }

        [Fact]
        public void Equals_ReplaceDataDifferent_ReturnsFalse()
        {
            // ARRANGE
            AcquireLockOptions left = this.CreateLockOptions();
            AcquireLockOptions right = new AcquireLockOptions("partitionKey")
            {
                SortKey = "sortKey",
                Data = new MemoryStream(Encoding.UTF8.GetBytes("data")),
                ReplaceData = false,
                DeleteLockOnRelease = true,
                AcquireOnlyIfLockAlreadyExists = false,
                RefreshPeriod = 1,
                AdditionalTimeToWaitForLock = 1,
                TimeUnit = TimeUnit.MILLISECONDS,
                AdditionalAttributes = new Dictionary<string, AttributeValue>(),
                UpdateExistingLockRecord = false,
                ShouldSkipBlockingWait = true,
                AcquireReleasedLocksConsistently = true
            }; //.AddSessionMonitor(1); //never equal if session monitor is set

            // ACT

            // ASSERT
            Assert.False(left.Equals(right));
        }

        [Fact]
        public void Equals_DeleteLockOnReleaseDifferent_ReturnsFalse()
        {
            // ARRANGE
            AcquireLockOptions left = this.CreateLockOptions();
            AcquireLockOptions right = new AcquireLockOptions("partitionKey")
            {
                SortKey = "sortKey",
                Data = new MemoryStream(Encoding.UTF8.GetBytes("data")),
                ReplaceData = true,
                DeleteLockOnRelease = false,
                AcquireOnlyIfLockAlreadyExists = false,
                RefreshPeriod = 1,
                AdditionalTimeToWaitForLock = 1,
                TimeUnit = TimeUnit.MILLISECONDS,
                AdditionalAttributes = new Dictionary<string, AttributeValue>(),
                UpdateExistingLockRecord = false,
                ShouldSkipBlockingWait = true,
                AcquireReleasedLocksConsistently = true
            }; //.AddSessionMonitor(1); //never equal if session monitor is set

            // ACT

            // ASSERT
            Assert.NotEqual(left, right);
        }

        [Fact]
        public void Equals_AcquireOnlyIfLockAlreadyExistsDifferent_ReturnsFalse()
        {
            // ARRANGE
            AcquireLockOptions left = this.CreateLockOptions();
            AcquireLockOptions right = new AcquireLockOptions("partitionKey")
            {
                SortKey = "sortKey",
                Data = new MemoryStream(Encoding.UTF8.GetBytes("data")),
                ReplaceData = true,
                DeleteLockOnRelease = true,
                AcquireOnlyIfLockAlreadyExists = true,
                RefreshPeriod = 1,
                AdditionalTimeToWaitForLock = 1,
                TimeUnit = TimeUnit.MILLISECONDS,
                AdditionalAttributes = new Dictionary<string, AttributeValue>(),
                UpdateExistingLockRecord = false,
                ShouldSkipBlockingWait = true,
                AcquireReleasedLocksConsistently = true
            }; //.AddSessionMonitor(1); //never equal if session monitor is set

            // ACT

            // ASSERT
            Assert.NotEqual(left, right);
        }

        [Fact]
        public void Equals_RefreshPeriodDifferent_ReturnsFalse()
        {
            // ARRANGE
            AcquireLockOptions left = this.CreateLockOptions();
            AcquireLockOptions right = new AcquireLockOptions("partitionKey")
            {
                SortKey = "sortKey",
                Data = new MemoryStream(Encoding.UTF8.GetBytes("data")),
                ReplaceData = true,
                DeleteLockOnRelease = true,
                AcquireOnlyIfLockAlreadyExists = false,
                RefreshPeriod = 2,
                AdditionalTimeToWaitForLock = 1,
                TimeUnit = TimeUnit.MILLISECONDS,
                AdditionalAttributes = new Dictionary<string, AttributeValue>(),
                UpdateExistingLockRecord = false,
                ShouldSkipBlockingWait = true,
                AcquireReleasedLocksConsistently = true
            }; //.AddSessionMonitor(1); //never equal if session monitor is set

            // ACT

            // ASSERT
            Assert.False(left.Equals(right));
        }

        [Fact]
        public void Equals_AdditionalTimeToWaitForLockDifferent_ReturnsFalse()
        {
            // ARRANGE
            AcquireLockOptions left = this.CreateLockOptions();
            AcquireLockOptions right = new AcquireLockOptions("partitionKey")
            {
                SortKey = "sortKey",
                Data = new MemoryStream(Encoding.UTF8.GetBytes("data")),
                ReplaceData = true,
                DeleteLockOnRelease = true,
                AcquireOnlyIfLockAlreadyExists = false,
                RefreshPeriod = 1,
                AdditionalTimeToWaitForLock = 2,
                TimeUnit = TimeUnit.MILLISECONDS,
                AdditionalAttributes = new Dictionary<string, AttributeValue>(),
                UpdateExistingLockRecord = false,
                ShouldSkipBlockingWait = true,
                AcquireReleasedLocksConsistently = true
            }; //.AddSessionMonitor(1); //never equal if session monitor is set

            // ACT

            // ASSERT
            Assert.False(left.Equals(right));
        }

        [Fact]
        public void Equals_TimeUnitDifferent_ReturnsFalse()
        {
            // ARRANGE
            AcquireLockOptions left = this.CreateLockOptions();
            AcquireLockOptions right = new AcquireLockOptions("partitionKey")
            {
                SortKey = "sortKey",
                Data = new MemoryStream(Encoding.UTF8.GetBytes("data")),
                ReplaceData = true,
                DeleteLockOnRelease = true,
                AcquireOnlyIfLockAlreadyExists = false,
                RefreshPeriod = 1,
                AdditionalTimeToWaitForLock = 1,
                TimeUnit = TimeUnit.SECONDS,
                AdditionalAttributes = new Dictionary<string, AttributeValue>(),
                UpdateExistingLockRecord = false,
                ShouldSkipBlockingWait = true,
                AcquireReleasedLocksConsistently = true
            }; //.AddSessionMonitor(1); //never equal if session monitor is set

            // ACT

            // ASSERT
            Assert.False(left.Equals(right));
        }

        [Fact]
        public void Equals_AdditionalAttributesDifferent_ReturnsFalse()
        {
            // ARRANGE
            AcquireLockOptions left = this.CreateLockOptions();
            Dictionary<string, AttributeValue> additionalAttributes = new Dictionary<string, AttributeValue>()
            {
                { "asdf", new AttributeValue() { NULL = true } }
            };

            AcquireLockOptions right = new AcquireLockOptions("partitionKey")
            {
                SortKey = "sortKey",
                Data = new MemoryStream(Encoding.UTF8.GetBytes("data")),
                ReplaceData = true,
                DeleteLockOnRelease = true,
                AcquireOnlyIfLockAlreadyExists = false,
                RefreshPeriod = 1,
                AdditionalTimeToWaitForLock = 1,
                TimeUnit = TimeUnit.MILLISECONDS,
                AdditionalAttributes = additionalAttributes,
                UpdateExistingLockRecord = false,
                ShouldSkipBlockingWait = true,
                AcquireReleasedLocksConsistently = true
            }; //.AddSessionMonitor(1); //never equal if session monitor is set

            // ACT

            // ASSERT
            Assert.NotEqual(left, right);
        }

        [Fact]
        public void Equals_UpdateExistingLockRecordDifferent_ReturnsFalse()
        {
            // ARRANGE
            AcquireLockOptions left = this.CreateLockOptions();
            AcquireLockOptions right = new AcquireLockOptions("partitionKey")
            {
                SortKey = "sortKey",
                Data = new MemoryStream(Encoding.UTF8.GetBytes("data")),
                ReplaceData = true,
                DeleteLockOnRelease = true,
                AcquireOnlyIfLockAlreadyExists = false,
                RefreshPeriod = 1,
                AdditionalTimeToWaitForLock = 1,
                TimeUnit = TimeUnit.MILLISECONDS,
                AdditionalAttributes = new Dictionary<string, AttributeValue>(),
                UpdateExistingLockRecord = true,
                ShouldSkipBlockingWait = true,
                AcquireReleasedLocksConsistently = true
            }; //.AddSessionMonitor(1); //never equal if session monitor is set

            // ACT

            // ASSERT
            Assert.NotEqual(left, right);
        }

        [Fact]
        public void Equals_ShouldSkipBlockingWaitDifferent_ReturnsFalse()
        {
            // ARRANGE
            AcquireLockOptions left = this.CreateLockOptions();
            AcquireLockOptions right = new AcquireLockOptions("partitionKey")
            {
                SortKey = "sortKey",
                Data = new MemoryStream(Encoding.UTF8.GetBytes("data")),
                ReplaceData = true,
                DeleteLockOnRelease = true,
                AcquireOnlyIfLockAlreadyExists = false,
                RefreshPeriod = 1,
                AdditionalTimeToWaitForLock = 1,
                TimeUnit = TimeUnit.MILLISECONDS,
                AdditionalAttributes = new Dictionary<string, AttributeValue>(),
                UpdateExistingLockRecord = false,
                ShouldSkipBlockingWait = false,
                AcquireReleasedLocksConsistently = true
            }; //.AddSessionMonitor(1); //never equal if session monitor is set

            // ACT

            // ASSERT
            Assert.NotEqual(left, right);
        }

        [Fact]
        public void Equals_ConsistentLockDataDifferent_ReturnsFalse()
        {
            // ARRANGE
            AcquireLockOptions left = this.CreateLockOptions();

            AcquireLockOptions right = new AcquireLockOptions("partitionKey")
            {
                SortKey = "sortKey",
                Data = new MemoryStream(Encoding.UTF8.GetBytes("data")),
                ReplaceData = true,
                DeleteLockOnRelease = true,
                AcquireOnlyIfLockAlreadyExists = false,
                RefreshPeriod = 1,
                AdditionalTimeToWaitForLock = 1,
                TimeUnit = TimeUnit.MILLISECONDS,
                AdditionalAttributes = new Dictionary<string, AttributeValue>(),
                UpdateExistingLockRecord = false,
                ShouldSkipBlockingWait = true,
                AcquireReleasedLocksConsistently = true
            }; //.AddSessionMonitor(1); //never equal if session monitor is set

            // ACT

            // ASSERT
            Assert.NotEqual(left, right);
        }

        [Fact]
        public void Equals_SessionMonitorSet_ReturnsFalse()
        {
            // ARRANGE
            AcquireLockOptions left = this.CreateLockOptions();
            AcquireLockOptions right = new AcquireLockOptions("partitionKey")
            {
                SortKey = "sortKey",
                Data = new MemoryStream(Encoding.UTF8.GetBytes("data")),
                ReplaceData = true,
                DeleteLockOnRelease = true,
                AcquireOnlyIfLockAlreadyExists = false,
                RefreshPeriod = 1,
                AdditionalTimeToWaitForLock = 1,
                TimeUnit = TimeUnit.MILLISECONDS,
                AdditionalAttributes = new Dictionary<string, AttributeValue>(),
                UpdateExistingLockRecord = false,
                ShouldSkipBlockingWait = true,
                AcquireReleasedLocksConsistently = false
            }.AddSessionMonitor(1); //never equal if session monitor is set

            // ACT

            //ASSERT
            Assert.NotEqual(left, right);
        }

        [Fact]
        public void HashCode_DoesntThrow()
        {
            this.CreateLockOptions().GetHashCode();
        }

        [Fact]
        public void ToString_DoesntThrow()
        {
            this.CreateLockOptions().ToString();
        }

        
        private AcquireLockOptions CreateLockOptions()
        {
            return new AcquireLockOptions("partitionKey")
            {
                SortKey = "sortKey",
                Data = new MemoryStream(Encoding.UTF8.GetBytes("data")),
                ReplaceData = true,
                DeleteLockOnRelease = true,
                AcquireOnlyIfLockAlreadyExists = false,
                RefreshPeriod = 1,
                AdditionalTimeToWaitForLock = 1,
                TimeUnit = TimeUnit.MILLISECONDS,
                AdditionalAttributes = new Dictionary<string, AttributeValue>(),
                UpdateExistingLockRecord = false,
                ShouldSkipBlockingWait = true,
                AcquireReleasedLocksConsistently = false
            };
        }
    }
}
