using Amazon.Runtime;
using BAMCIS.Util.Concurrent;
using Moq;
using System;
using System.IO;
using System.Text;

namespace Amazon.DynamoDBv2.Tests
{
    public abstract class InMemoryLockClientTester
    {
        #region Protected Fields

        protected Mock<AmazonDynamoDBClient> dynamoDBMock;
        protected IAmazonDynamoDB idynamodb;
        protected IAmazonDynamoDBLockClient lockClient;
        protected Mock<AmazonDynamoDBLockClient> lockClientMock;
        protected IAmazonDynamoDBLockClient lockClientWithHeartbeating;
        protected Mock<AmazonDynamoDBLockClient> lockClientWithHeartbeatingMock;
        protected IAmazonDynamoDBLockClient lockClientForRangeKeyTable;
        protected Mock<AmazonDynamoDBLockClient> lockClientForRangeKeyTableMock;
        protected IAmazonDynamoDBLockClient lockClientWithHeartbeatingForRangeKeyTable;
        protected Mock<AmazonDynamoDBLockClient> lockClientWithHeartbeatingForRangeKeyTableMock;
        protected IAmazonDynamoDBLockClient shortLeaseLockClient;
        protected Mock<AmazonDynamoDBLockClient> shortLeaseLockClientMock;
        protected IAmazonDynamoDBLockClient shortLeaseLockClientWithHeartbeating;
        protected Mock<AmazonDynamoDBLockClient> shortLeaseLockClientWithHeartbeatingMock;
        protected IAmazonDynamoDBLockClient shortLeaseLockClientForRangeKeyTable;
        protected Mock<AmazonDynamoDBLockClient> shortLeaseLockClientForRangeKeyTableMock;
        protected IAmazonDynamoDBLockClient lockClientNoTable;
        protected Mock<AmazonDynamoDBLockClient> lockClientNoTableMock;
        protected AmazonDynamoDBLockClientOptions lockClient1Options;
        private int DefaultLeaseDuration = 3;


        protected static readonly string LOCALHOST = "mars";
        protected static readonly string INTEGRATION_TESTER = "integrationTester";
        protected static readonly Random SECURE_RANDOM = new Random();
        protected static readonly string TABLE_NAME = "test";
        protected static readonly string RANGE_KEY_TABLE_NAME = "rangeTest";
        protected static readonly string TEST_DATA = "test_data";
        protected static readonly long SHORT_LEASE_DUR = 60; // 60 milliseconds
        protected static readonly MemoryStream TEST_DATA_STREAM = new MemoryStream(Encoding.UTF8.GetBytes(TEST_DATA));

        protected static readonly AcquireLockOptions ACQUIRE_LOCK_OPTIONS_TEST_KEY_1_NO_DATA = new AcquireLockOptions("testKey1");
        protected static readonly AcquireLockOptions ACQUIRE_LOCK_OPTIONS_TEST_KEY_2_NO_DATA = new AcquireLockOptions("testKey2");
        protected static readonly AcquireLockOptions ACQUIRE_LOCK_OPTIONS_TEST_KEY_1 = new AcquireLockOptions("testKey1") { Data = TEST_DATA_STREAM };
        protected static readonly AcquireLockOptions ACQUIRE_LOCK_OPTIONS_TEST_KEY_1_SORT_1 = new AcquireLockOptions("testKey1") { Data = TEST_DATA_STREAM, SortKey = "1" };
        protected static readonly AcquireLockOptions ACQUIRE_LOCK_OPTIONS_TEST_KEY_1_SORT_2 = new AcquireLockOptions("testKey1") { Data = TEST_DATA_STREAM, SortKey = "2" };

        protected static readonly AcquireLockOptions ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_FALSE = new AcquireLockOptions("testKey1")
        {
            Data = TEST_DATA_STREAM,
            AcquireOnlyIfLockAlreadyExists = true,
            ReplaceData = false,
            UpdateExistingLockRecord = false
        };
        protected static readonly AcquireLockOptions ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_FALSE_CONSISTENT_DATA_TRUE = new AcquireLockOptions("testKey1")
        {
            Data = TEST_DATA_STREAM,
            AcquireOnlyIfLockAlreadyExists = true,
            ReplaceData = false,
            UpdateExistingLockRecord = false,
            AcquireReleasedLocksConsistently = true
        };
        protected static readonly AcquireLockOptions ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_FALSE_SORT_1 = new AcquireLockOptions("testKey1")
        {
            Data = TEST_DATA_STREAM,
            SortKey = "1",
            AcquireOnlyIfLockAlreadyExists = true,
            ReplaceData = false,
            UpdateExistingLockRecord = false
        };
        protected static readonly AcquireLockOptions ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_FALSE_CONSISTENT_DATA_TRUE_SORT_1 = new AcquireLockOptions("testKey1")
        {
            Data = TEST_DATA_STREAM,
            SortKey = "1",
            AcquireOnlyIfLockAlreadyExists = true,
            ReplaceData = false,
            UpdateExistingLockRecord = false,
            AcquireReleasedLocksConsistently = true
        };
        protected static readonly AcquireLockOptions ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_TRUE = new AcquireLockOptions("testKey1")
        {
            Data = TEST_DATA_STREAM,
            AcquireOnlyIfLockAlreadyExists = true,
            ReplaceData = false,
            UpdateExistingLockRecord = true
        };
        protected static readonly AcquireLockOptions ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_TRUE_CONSISTENT_DATA_TRUE = new AcquireLockOptions("testKey1")
        {
            Data = TEST_DATA_STREAM,
            AcquireOnlyIfLockAlreadyExists = true,
            ReplaceData = false,
            UpdateExistingLockRecord = true,
            AcquireReleasedLocksConsistently = true
        };
        protected static readonly AcquireLockOptions ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_TRUE_SORT_1 = new AcquireLockOptions("testKey1")
        {
            Data = TEST_DATA_STREAM,
            SortKey = "1",
            AcquireOnlyIfLockAlreadyExists = true,
            ReplaceData = false,
            UpdateExistingLockRecord = true
        };
        protected static readonly AcquireLockOptions ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_TRUE_CONSISTENT_DATA_TRUE_SORT_1 = new AcquireLockOptions("testKey1")
        {
            Data = TEST_DATA_STREAM,
            SortKey = "1",
            AcquireOnlyIfLockAlreadyExists = true,
            ReplaceData = false,
            UpdateExistingLockRecord = true,
            AcquireReleasedLocksConsistently = true
        };
        protected static readonly AcquireLockOptions ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE = new AcquireLockOptions("testKey1")
        {
            Data = TEST_DATA_STREAM,
            ReplaceData = false
        };
        protected static readonly AcquireLockOptions ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_SORT_1 = new AcquireLockOptions("testKey1")
        {
            Data = TEST_DATA_STREAM,
            SortKey = "1",
            ReplaceData = false
        };
        protected static readonly AcquireLockOptions ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_CONSISTENT_DATA_TRUE = new AcquireLockOptions("testKey1")
        {
            Data = TEST_DATA_STREAM,
            ReplaceData = false,
            AcquireReleasedLocksConsistently = true
        };
        protected static readonly AcquireLockOptions ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_CONSISTENT_DATA_TRUE_SORT_1 = new AcquireLockOptions("testKey1")
        {
            Data = TEST_DATA_STREAM,
            SortKey = "1",
            AcquireOnlyIfLockAlreadyExists = true,
            ReplaceData = false,
            AcquireReleasedLocksConsistently = true
        };
        protected static readonly AcquireLockOptions ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_UPDATE_EXISTING_TRUE = new AcquireLockOptions("testKey1")
        {
            Data = TEST_DATA_STREAM,
            ReplaceData = false,
            UpdateExistingLockRecord = true
        };
        protected static readonly AcquireLockOptions ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_UPDATE_EXISTING_TRUE_SORT_1 = new AcquireLockOptions("testKey1")
        {
            Data = TEST_DATA_STREAM,
            SortKey = "1",
            UpdateExistingLockRecord = true,
            ReplaceData = false
        };
        protected static readonly AcquireLockOptions ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_UPDATE_EXISTING_TRUE_CONSISTENT_DATA_TRUE = new AcquireLockOptions("testKey1")
        {
            Data = TEST_DATA_STREAM,
            ReplaceData = false,
            UpdateExistingLockRecord = true,
            AcquireReleasedLocksConsistently = true
        };
        protected static readonly AcquireLockOptions ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_UPDATE_EXISTING_TRUE_CONSISTENT_DATA_TRUE_SORT_1 = new AcquireLockOptions("testKey1")
        {
            Data = TEST_DATA_STREAM,
            SortKey = "1",
            UpdateExistingLockRecord = true,
            ReplaceData = false,
            AcquireReleasedLocksConsistently = true
        };

        protected static readonly AcquireLockOptions ACQUIRE_LOCK_OPTIONS_TEST_KEY_2 = new AcquireLockOptions("testKey2") { Data = TEST_DATA_STREAM };
        protected static readonly AcquireLockOptions ACQUIRE_LOCK_OPTIONS_TEST_KEY_3 = new AcquireLockOptions("testKey3") { Data = TEST_DATA_STREAM };

        protected static readonly AcquireLockOptions ACQUIRE_LOCK_OPTIONS_WITH_NO_WAIT = new AcquireLockOptions("testKey1")
        {
            Data = TEST_DATA_STREAM,
            AdditionalTimeToWaitForLock = 0,
            RefreshPeriod = 0,
            TimeUnit = TimeUnit.SECONDS
        };
        protected static readonly AcquireLockOptions ACQUIRE_LOCK_OPTIONS_WITH_NO_WAIT_SORT_1 = new AcquireLockOptions("testKey1")
        {
            Data = TEST_DATA_STREAM,
            SortKey = "1",
            AdditionalTimeToWaitForLock = 0,
            RefreshPeriod = 0,
            TimeUnit = TimeUnit.SECONDS
        };
        protected static readonly AcquireLockOptions ACQUIRE_LOCK_OPTIONS_5_SECONDS = new AcquireLockOptions("testKey1")
        {
            Data = TEST_DATA_STREAM,
            AdditionalTimeToWaitForLock = 5,
            RefreshPeriod = 1,
            TimeUnit = TimeUnit.SECONDS
        };

        protected static readonly GetLockOptions GET_LOCK_OPTIONS_DELETE_ON_RELEASE = new GetLockOptions("testKey1")
        {
            DeleteLockOnRelease = true
        };
        protected static readonly GetLockOptions GET_LOCK_OPTIONS_DO_NOT_DELETE_ON_RELEASE = new GetLockOptions("testKey1")
        {
            DeleteLockOnRelease = false
        };

        #endregion

        #region Constructors

        public InMemoryLockClientTester()
        {
            this.Setup();
        }

        public InMemoryLockClientTester(int defaultLeastDuration)
        {
            this.DefaultLeaseDuration = defaultLeastDuration;
            this.Setup();
        }

        #endregion

        #region Protected Methods

        protected static Mock<AmazonDynamoDBClient> SafelySpyDDB(AWSCredentials credentials, AmazonDynamoDBConfig config)
        {
            return new Mock<AmazonDynamoDBClient>(credentials, config)
            {
                CallBase = true
            };
        }

        protected static Mock<AmazonDynamoDBLockClient> SafelySpyDDBLockClientMock(AmazonDynamoDBLockClientOptions options)
        {
            return new Mock<AmazonDynamoDBLockClient>(options)
            {
                CallBase = true
            };
        }

        protected static IAmazonDynamoDBLockClient SafelySpyDDBLockClient(AmazonDynamoDBLockClientOptions options)
        {
            return SafelySpyDDBLockClientMock(options).Object;
        }

        protected static MemoryStream GetMemoryStream(string data)
        {
            return new MemoryStream(Encoding.UTF8.GetBytes(data));
        }

        #endregion

        #region Private Methods

        private void Setup()
        {
            AWSCredentials credentials = new BasicAWSCredentials(TABLE_NAME, "d");
            string endpoint = Environment.GetEnvironmentVariable("dynamodb-local.endpoint");

            /*
            if (String.IsNullOrEmpty(endpoint))
            {
                throw new InvalidOperationException("The test was not launched with the dynamodb-local.endpoint environment variable set");
            }
            */

            endpoint = "http://localhost:4567";

            AmazonDynamoDBConfig config = new AmazonDynamoDBConfig()
            {
                ServiceURL = endpoint
            };

            this.dynamoDBMock = SafelySpyDDB(credentials, config);
            this.idynamodb = this.dynamoDBMock.Object;

            this.lockClient1Options = new AmazonDynamoDBLockClientOptions(this.dynamoDBMock.Object, TABLE_NAME, INTEGRATION_TESTER)
            {
                LeaseDuration = DefaultLeaseDuration,
                HeartbeatPeriod = 1,
                TimeUnit = TimeUnit.SECONDS,
                CreateHeartbeatBackgroundThread = false
            };

            this.lockClientMock = SafelySpyDDBLockClientMock(this.lockClient1Options);
            this.lockClient = this.lockClientMock.Object;
            this.lockClientWithHeartbeatingMock = SafelySpyDDBLockClientMock(new AmazonDynamoDBLockClientOptions(this.idynamodb, TABLE_NAME, INTEGRATION_TESTER) { LeaseDuration = DefaultLeaseDuration, HeartbeatPeriod = 1, TimeUnit = TimeUnit.SECONDS, CreateHeartbeatBackgroundThread = true });
            this.lockClientWithHeartbeating = this.lockClientWithHeartbeatingMock.Object;
            this.lockClientNoTableMock = SafelySpyDDBLockClientMock(new AmazonDynamoDBLockClientOptions(this.idynamodb, "doesNotExist", INTEGRATION_TESTER) { LeaseDuration = DefaultLeaseDuration, HeartbeatPeriod = 1, TimeUnit = TimeUnit.SECONDS, CreateHeartbeatBackgroundThread = false });
            this.lockClientNoTable = this.lockClientNoTableMock.Object;
            this.shortLeaseLockClientMock = SafelySpyDDBLockClientMock(new AmazonDynamoDBLockClientOptions(this.idynamodb, TABLE_NAME, INTEGRATION_TESTER) { LeaseDuration = SHORT_LEASE_DUR, HeartbeatPeriod = 10, TimeUnit = TimeUnit.MILLISECONDS, CreateHeartbeatBackgroundThread = false });
            this.shortLeaseLockClient = this.shortLeaseLockClientMock.Object;
            this.shortLeaseLockClientWithHeartbeatingMock = SafelySpyDDBLockClientMock(new AmazonDynamoDBLockClientOptions(this.idynamodb, TABLE_NAME, INTEGRATION_TESTER) { LeaseDuration = SHORT_LEASE_DUR, HeartbeatPeriod = 10, TimeUnit = TimeUnit.MILLISECONDS, CreateHeartbeatBackgroundThread = true });
            this.shortLeaseLockClientWithHeartbeating = this.shortLeaseLockClientWithHeartbeatingMock.Object;
            this.lockClientForRangeKeyTableMock = SafelySpyDDBLockClientMock(new AmazonDynamoDBLockClientOptions(this.idynamodb, RANGE_KEY_TABLE_NAME, INTEGRATION_TESTER) { LeaseDuration = DefaultLeaseDuration, HeartbeatPeriod = 1, TimeUnit = TimeUnit.SECONDS, CreateHeartbeatBackgroundThread = false, SortKeyName = "rangeKey" });
            this.lockClientForRangeKeyTable = this.lockClientForRangeKeyTableMock.Object;
            this.lockClientWithHeartbeatingForRangeKeyTableMock = SafelySpyDDBLockClientMock(new AmazonDynamoDBLockClientOptions(this.idynamodb, RANGE_KEY_TABLE_NAME, INTEGRATION_TESTER) { LeaseDuration = DefaultLeaseDuration, HeartbeatPeriod = 1, TimeUnit = TimeUnit.SECONDS, CreateHeartbeatBackgroundThread = true, SortKeyName = "rangeKey" });
            this.lockClientWithHeartbeatingForRangeKeyTable = this.lockClientWithHeartbeatingForRangeKeyTableMock.Object;
            this.shortLeaseLockClientForRangeKeyTableMock = SafelySpyDDBLockClientMock(new AmazonDynamoDBLockClientOptions(this.idynamodb, RANGE_KEY_TABLE_NAME, INTEGRATION_TESTER) { LeaseDuration = SHORT_LEASE_DUR, HeartbeatPeriod = 10, TimeUnit = TimeUnit.MILLISECONDS, CreateHeartbeatBackgroundThread = false, SortKeyName = "rangeKey" });
            this.shortLeaseLockClientForRangeKeyTable = this.shortLeaseLockClientForRangeKeyTableMock.Object;
        }

        #endregion
    }
}
