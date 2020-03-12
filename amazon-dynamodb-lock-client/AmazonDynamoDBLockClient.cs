using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2.Util;
using Amazon.Runtime;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Amazon.DynamoDBv2
{
    public class AmazonDynamoDBLockClient : IAmazonDynamoDBLockClient
    {
        #region Private Fields

        private static ISet<TableStatus> AvailableStatuses;

        protected IAmazonDynamoDB DynamoDB;
        protected string TableName;
        private string PartitionKeyName;
        private string SortKeyName;
        private long LeaseDurationInMilliseconds;
        private long HeartbeatPeriodInMilliseconds;
        private bool HoldLockOnServiceUnavailable;
        private string OwnerName;
        private volatile ConcurrentDictionary<string, LockItem> Locks;
        private volatile ConcurrentDictionary<string, BackgroundTask> SessionMonitors;
        private BackgroundTask BackgroundThread;
        private volatile bool ShuttingDown = false;
        private volatile object SyncObject = new object();

        /*
        * Used as a default buffer for how long extra to wait when querying DynamoDB for a lock in acquireLock (can be overriden by
        * specifying a timeout when calling acquireLock)
        */
        private static readonly long DEFAULT_BUFFER_MS = 1000;

        private volatile CancellationTokenSource CTS;

        #endregion

        #region Protected Fields

        protected static readonly string SK_PATH_EXPRESSION_VARIABLE = "#sk";
        protected static readonly string PK_PATH_EXPRESSION_VARIABLE = "#pk";
        protected static readonly string NEW_RVN_VALUE_EXPRESSION_VARIABLE = ":newRvn";
        protected static readonly string LEASE_DURATION_PATH_VALUE_EXPRESSION_VARIABLE = "#ld";
        protected static readonly string LEASE_DURATION_VALUE_EXPRESSION_VARIABLE = ":ld";
        protected static readonly string RVN_PATH_EXPRESSION_VARIABLE = "#rvn";
        protected static readonly string RVN_VALUE_EXPRESSION_VARIABLE = ":rvn";
        protected static readonly string OWNER_NAME_PATH_EXPRESSION_VARIABLE = "#on";
        protected static readonly string OWNER_NAME_VALUE_EXPRESSION_VARIABLE = ":on";
        protected static readonly string DATA_PATH_EXPRESSION_VARIABLE = "#d";
        protected static readonly string DATA_VALUE_EXPRESSION_VARIABLE = ":d";
        protected static readonly string IS_RELEASED_PATH_EXPRESSION_VARIABLE = "#ir";
        protected static readonly string IS_RELEASED_VALUE_EXPRESSION_VARIABLE = ":ir";

        //attribute_not_exists(#pk)
        protected static string ACQUIRE_LOCK_THAT_DOESNT_EXIST_PK_CONDITION = $"attribute_not_exists({PK_PATH_EXPRESSION_VARIABLE})";

        //attribute_not_exists(#pk) AND attribute_not_exists(#sk)
        protected static string ACQUIRE_LOCK_THAT_DOESNT_EXIST_PK_SK_CONDITION = $"attribute_not_exists({PK_PATH_EXPRESSION_VARIABLE}) AND attribute_not_exists({SK_PATH_EXPRESSION_VARIABLE})";

        //attribute_exists(#pk) AND #ir = :ir
        protected static string PK_EXISTS_AND_IS_RELEASED_CONDITION = $"attribute_exists({PK_PATH_EXPRESSION_VARIABLE}) AND {IS_RELEASED_PATH_EXPRESSION_VARIABLE} = {IS_RELEASED_VALUE_EXPRESSION_VARIABLE}";

        //attribute_exists(#pk) AND attribute_exists(#sk) AND #ir = :ir
        protected static string PK_EXISTS_AND_SK_EXISTS_AND_IS_RELEASED_CONDITION = $"attribute_exists({PK_PATH_EXPRESSION_VARIABLE}) AND attribute_exists({SK_PATH_EXPRESSION_VARIABLE}) AND {IS_RELEASED_PATH_EXPRESSION_VARIABLE} = {IS_RELEASED_VALUE_EXPRESSION_VARIABLE}";

        //attribute_exists(#pk) AND attribute_exists(#sk) AND #rvn = :rvn AND #ir = :ir
        protected static string PK_EXISTS_AND_SK_EXISTS_AND_RVN_IS_THE_SAME_AND_IS_RELEASED_CONDITION = $"attribute_exists({PK_PATH_EXPRESSION_VARIABLE}) AND attribute_exists({SK_PATH_EXPRESSION_VARIABLE}) AND {RVN_PATH_EXPRESSION_VARIABLE } = {RVN_VALUE_EXPRESSION_VARIABLE} AND {IS_RELEASED_PATH_EXPRESSION_VARIABLE} = {IS_RELEASED_VALUE_EXPRESSION_VARIABLE}";

        //attribute_exists(#pk) AND attribute_exists(#sk) AND #rvn = :rvn
        protected static string PK_EXISTS_AND_SK_EXISTS_AND_RVN_IS_THE_SAME_CONDITION = $"attribute_exists({PK_PATH_EXPRESSION_VARIABLE}) AND attribute_exists({SK_PATH_EXPRESSION_VARIABLE}) AND {RVN_PATH_EXPRESSION_VARIABLE} = {RVN_VALUE_EXPRESSION_VARIABLE}";

        //(attribute_exists(#pk) AND attribute_exists(#sk) AND #rvn = :rvn) AND (attribute_not_exists(#if) OR #if = :if) AND #on = :on
        protected static string PK_EXISTS_AND_SK_EXISTS_AND_OWNER_NAME_SAME_AND_RVN_SAME_CONDITION = $"{PK_EXISTS_AND_SK_EXISTS_AND_RVN_IS_THE_SAME_CONDITION} AND {OWNER_NAME_PATH_EXPRESSION_VARIABLE} = {OWNER_NAME_VALUE_EXPRESSION_VARIABLE}";

        //(attribute_exists(#pk) AND #rvn = :rvn AND #ir = :ir) AND (attribute_not_exists(#if) OR #if = :if)
        protected static string PK_EXISTS_AND_RVN_IS_THE_SAME_AND_IS_RELEASED_CONDITION = $"(attribute_exists({PK_PATH_EXPRESSION_VARIABLE}) AND {RVN_PATH_EXPRESSION_VARIABLE} = {RVN_VALUE_EXPRESSION_VARIABLE} AND {IS_RELEASED_PATH_EXPRESSION_VARIABLE} = {IS_RELEASED_VALUE_EXPRESSION_VARIABLE})";

        //attribute_exists(#pk) AND #rvn = :rvn AND (attribute_not_exists(#if) OR #if = :if)
        protected static string PK_EXISTS_AND_RVN_IS_THE_SAME_CONDITION = $"attribute_exists({PK_PATH_EXPRESSION_VARIABLE}) AND {RVN_PATH_EXPRESSION_VARIABLE} = {RVN_VALUE_EXPRESSION_VARIABLE}";

        //attribute_exists(#pk) AND #rvn = :rvn AND (attribute_not_exists(#if) OR #if = :if) AND #on = :on
        protected static string PK_EXISTS_AND_OWNER_NAME_SAME_AND_RVN_SAME_CONDITION = $"{PK_EXISTS_AND_RVN_IS_THE_SAME_CONDITION} AND {OWNER_NAME_PATH_EXPRESSION_VARIABLE} = {OWNER_NAME_VALUE_EXPRESSION_VARIABLE}";

        protected static string UPDATE_IS_RELEASED = $"SET {IS_RELEASED_PATH_EXPRESSION_VARIABLE} = {IS_RELEASED_VALUE_EXPRESSION_VARIABLE}";
        protected static string UPDATE_IS_RELEASED_AND_DATA = $"{UPDATE_IS_RELEASED}, {DATA_PATH_EXPRESSION_VARIABLE} = {DATA_VALUE_EXPRESSION_VARIABLE}";
        protected static string UPDATE_LEASE_DURATION_AND_RVN = $"SET {LEASE_DURATION_PATH_VALUE_EXPRESSION_VARIABLE} = {LEASE_DURATION_VALUE_EXPRESSION_VARIABLE}, {RVN_PATH_EXPRESSION_VARIABLE} = {NEW_RVN_VALUE_EXPRESSION_VARIABLE}";
        protected static string UPDATE_LEASE_DURATION_AND_RVN_AND_REMOVE_DATA = $"{UPDATE_LEASE_DURATION_AND_RVN} REMOVE {DATA_PATH_EXPRESSION_VARIABLE}";
        protected static string UPDATE_LEASE_DURATION_AND_RVN_AND_DATA = $"{UPDATE_LEASE_DURATION_AND_RVN}, {DATA_PATH_EXPRESSION_VARIABLE} = {DATA_VALUE_EXPRESSION_VARIABLE}";
        protected static string REMOVE_IS_RELEASED_UPDATE_EXPRESSION = $" REMOVE {IS_RELEASED_PATH_EXPRESSION_VARIABLE}";

        // These are the keys that are stored in the DynamoDB table to keep track of the locks
        protected static string DATA = "data";
        protected static string OWNER_NAME = "ownerName";
        protected static string LEASE_DURATION = "leaseDuration";
        protected static string RECORD_VERSION_NUMBER = "recordVersionNumber";
        protected static string IS_RELEASED = "isReleased";
        protected static string IS_RELEASED_VALUE = "1";
        protected static AttributeValue IS_RELEASED_ATTRIBUTE_VALUE = new AttributeValue() { S = IS_RELEASED_VALUE };
        protected static int LockClientId = 0;
        protected static bool IS_RELEASED_INDICATOR = true;

        #endregion

        #region Constructors

        static AmazonDynamoDBLockClient()
        {
            AvailableStatuses = new HashSet<TableStatus>() { TableStatus.ACTIVE, TableStatus.UPDATING };
        }

        /// <summary>
        /// Initializes an AmazonDynamoDBLockClient using the lock client options specified
        /// in the AmazonDynamoDBLockClientOptions object.
        /// </summary>
        /// <param name="amazonDynamoDBLockClientOptions">The options to use when initializing the client, i.e. the table name, sort key value, etc.</param>
        public AmazonDynamoDBLockClient(AmazonDynamoDBLockClientOptions amazonDynamoDBLockClientOptions)
        {
            LockClientUtils.RequireNonNull(amazonDynamoDBLockClientOptions, "DynamoDB Lock Client Options cannot be null.", "amazonDynamoDBLockClientOptions");
            LockClientUtils.RequireNonNull(amazonDynamoDBLockClientOptions.DynamoDBClient, "DynamoDB client object cannot be null.");
            LockClientUtils.RequireNonNullOrEmpty(amazonDynamoDBLockClientOptions.TableName, "Table name cannot be null or empty.");
            LockClientUtils.RequireNonNullOrEmpty(amazonDynamoDBLockClientOptions.OwnerName, "Owner name cannot be null or empty.");
            LockClientUtils.RequireNonNull(amazonDynamoDBLockClientOptions.TimeUnit, "Time unit cannot be null.");
            LockClientUtils.RequireNonNullOrEmpty(amazonDynamoDBLockClientOptions.PartitionKeyName, "Partition Key Name cannot be null or empty.");
            LockClientUtils.RequireNonNull(amazonDynamoDBLockClientOptions.SortKeyName, "Sort Key Name cannot be null (use Optional.Empty).");

            this.DynamoDB = amazonDynamoDBLockClientOptions.DynamoDBClient;
            this.TableName = amazonDynamoDBLockClientOptions.TableName;
            this.Locks = new ConcurrentDictionary<string, LockItem>();
            this.SessionMonitors = new ConcurrentDictionary<string, BackgroundTask>();
            this.OwnerName = amazonDynamoDBLockClientOptions.OwnerName;
            this.LeaseDurationInMilliseconds = amazonDynamoDBLockClientOptions.TimeUnit.ToMilliseconds(amazonDynamoDBLockClientOptions.LeaseDuration);
            this.HeartbeatPeriodInMilliseconds = amazonDynamoDBLockClientOptions.TimeUnit.ToMilliseconds(amazonDynamoDBLockClientOptions.HeartbeatPeriod);
            this.PartitionKeyName = amazonDynamoDBLockClientOptions.PartitionKeyName;
            this.SortKeyName = amazonDynamoDBLockClientOptions.SortKeyName;
            this.HoldLockOnServiceUnavailable = amazonDynamoDBLockClientOptions.HoldLockOnServiceUnavailable;

            this.CTS = amazonDynamoDBLockClientOptions.BackgroundTaskToken ?? new CancellationTokenSource();

            if (amazonDynamoDBLockClientOptions.CreateHeartbeatBackgroundThread)
            {
                if (this.LeaseDurationInMilliseconds < 2 * this.HeartbeatPeriodInMilliseconds)
                {
                    throw new ArgumentException("Heartbeat period must be no more than half the length of the Lease Duration, or locks might expire due to the heartbeat thread taking too long to update them (recommendation is to make it much greater, for example 4+ times greater.");
                }

                this.BackgroundThread = this.StartBackgroundTask();
            }
            else
            {
                this.BackgroundThread = null;
            }
        }

        ~AmazonDynamoDBLockClient()
        {
            this.Close();
        }

        #endregion

        #region Public Methods

        /// <summary>
        /// Checkes whether the lock table exists in DynamoDB
        /// </summary>
        /// <returns>True if the table exists, false otherwise</returns>
        public async Task<bool> LockTableExistsAsync()
        {
            try
            {
                DescribeTableResponse result = await this.DynamoDB.DescribeTableAsync(new DescribeTableRequest() { TableName = this.TableName });
                return AvailableStatuses.Contains(result.Table.TableStatus);
            }
            catch (ResourceNotFoundException)
            {
                return false;
            }
        }

        /// <summary>
        /// Asserts that the lock table exists in DynamoDB. You can use this method
        /// during application initialization to ensure that the lock client will be
        /// usable. Since this is a no-arg assertion as opposed to a check that returns
        /// a value, this method is also suitable as an init-method.
        /// 
        /// Throws a LockTableDoesNotExistException is the table does not exist.
        /// </summary>
        /// <returns></returns>
        public async Task AssertLockTableExistsAsync()
        {
            try
            {
                bool exists = await this.LockTableExistsAsync();

                if (!exists)
                {
                    throw new LockTableDoesNotExistException($"Lock table {this.TableName} does not exist.");
                }
            }
            catch (Exception e)
            {
                throw new LockTableDoesNotExistException($"Lock table {this.TableName} does not exist.", e);
            }
        }

        /// <summary>
        /// Creates a DynamoDB table with the right schema for it to be used by this locking library. The table should be set up in advance,
        /// because it takes a few minutes for DynamoDB to provision a new instance. Also, if the table already exists, this will throw an exception.
        /// 
        /// This method lets you specify a sort key to be used by the lock client. This sort key then needs to be specified in the
        /// AmazonDynamoDBLockClientOptions when the lock client object is created.
        /// </summary>
        /// <param name="createDynamoDBTableOptions">The options used to create the lock table</param>
        /// <returns></returns>
        public async Task<CreateTableResponse> CreateLockTableInDynamoDBAsync(CreateDynamoDBTableOptions createDynamoDBTableOptions)
        {
            return await AmazonDynamoDBLockClient.StaticCreateLockTableInDynamoDBAsync(createDynamoDBTableOptions);
        }

        /// <summary>
        /// Creates a DynamoDB table with the right schema for it to be used by this locking library. The table should be set up in advance,
        /// because it takes a few minutes for DynamoDB to provision a new instance. Also, if the table already exists, this will throw an exception.
        /// 
        /// This method lets you specify a sort key to be used by the lock client. This sort key then needs to be specified in the
        /// AmazonDynamoDBLockClientOptions when the lock client object is created.
        /// </summary>
        /// <param name="createDynamoDBTableOptions">The options used to create the lock table</param>
        /// <returns></returns>
        public static async Task<CreateTableResponse> StaticCreateLockTableInDynamoDBAsync(CreateDynamoDBTableOptions createDynamoDBTableOptions)
        {
            LockClientUtils.RequireNonNull(createDynamoDBTableOptions, "DynamoDB Create Table Options cannot be null.", "createDynamoDBTableOptions");
            LockClientUtils.RequireNonNull(createDynamoDBTableOptions.DynamoDBClient, "DynamoDB client object cannot be null.");
            LockClientUtils.RequireNonNullOrEmpty(createDynamoDBTableOptions.TableName, "Table name cannot be null or empty.");
            LockClientUtils.RequireNonNullOrEmpty(createDynamoDBTableOptions.PartitionKeyName, "Partition Key Name cannot be null or empty.");
            LockClientUtils.RequireNonNull(createDynamoDBTableOptions.SortKeyName, "Sort Key Name cannot be null.");

            if (createDynamoDBTableOptions.BillingMode == BillingMode.PROVISIONED)
            {
                LockClientUtils.RequireNonNull(createDynamoDBTableOptions.ProvisionedThroughput, "Provisioned throughput cannot be null when billing mode is PROVISIONED.");
                LockClientUtils.RequireNonNull(createDynamoDBTableOptions.ProvisionedThroughput, "Provisioned throughput cannot be null when billing mode is PROVISIONED.");
            }

            KeySchemaElement partitionKeyElement = new KeySchemaElement(createDynamoDBTableOptions.PartitionKeyName, KeyType.HASH);

            List<KeySchemaElement> keySchema = new List<KeySchemaElement>() { partitionKeyElement };

            List<AttributeDefinition> attributeDefinitions = new List<AttributeDefinition>() {
                new AttributeDefinition() {
                    AttributeName = createDynamoDBTableOptions.PartitionKeyName,
                    AttributeType = ScalarAttributeType.S
                }
            };

            if (!String.IsNullOrEmpty(createDynamoDBTableOptions.SortKeyName))
            {
                KeySchemaElement sortKeyElement = new KeySchemaElement(createDynamoDBTableOptions.SortKeyName, KeyType.RANGE);
                keySchema.Add(sortKeyElement);
                attributeDefinitions.Add(new AttributeDefinition()
                {
                    AttributeName = createDynamoDBTableOptions.SortKeyName,
                    AttributeType = ScalarAttributeType.S
                });
            }

            CreateTableRequest createTableRequest = new CreateTableRequest()
            {
                TableName = createDynamoDBTableOptions.TableName,
                KeySchema = keySchema,
                AttributeDefinitions = attributeDefinitions,
                BillingMode = createDynamoDBTableOptions.BillingMode
            };

            if (createTableRequest.BillingMode == BillingMode.PROVISIONED)
            {
                createTableRequest.ProvisionedThroughput = createDynamoDBTableOptions.ProvisionedThroughput;
            }

            CreateTableResponse response = await createDynamoDBTableOptions.DynamoDBClient.CreateTableAsync(createTableRequest);
            return response;
        }

        /// <summary>
        /// Finds out who owns the given lock, but does not acquire the lock. It returns the metadata currently associated with the
        /// given lock. If the client currently has the lock, it will return the lock, and operations such as ReleaseLock will work.
        /// However, if the client does not have the lock, then operations like ReleaseLock will not work (after calling GetLock, the
        /// caller should check LockItem.IsExpired() to figure out if it currently has the lock.)
        /// </summary>
        /// <param name="key">The partition key representing the lock</param>
        /// <param name="sortKey">The sort key if present</param>
        /// <returns></returns>
        public async Task<LockItem> GetLockAsync(string key, string sortKey = "")
        {
            if (sortKey == null)
            {
                sortKey = String.Empty;
            }

            if (this.Locks.TryGetValue($"{key}{sortKey}", out LockItem localLock))
            {
                return localLock;
            }

            LockItem lockItem = await this.GetLockFromDynamoDBAsync(
                new GetLockOptions(key, sortKey, false)
            );

            if (lockItem != null)
            {
                if (lockItem.IsReleased())
                {
                    // Return empty if a lock was released but still left in the table
                    return null;
                }
                else
                {

                    // Clear out the record version number so that the caller cannot accidentally 
                    // perform updates on this lock (since the caller has not acquired the lock)
                    lockItem.UpdateRecordVersionNumber("", 0, lockItem.LeaseDuration);
                }
            }

            return lockItem;
        }

        /// <summary>
        /// Retrieves the lock item from DynamoDB. Note that this will return a
        /// LockItem even if it was released -- do NOT use this method if your goal
        /// is to acquire a lock for doing work.
        /// </summary>
        /// <param name="options">The options such as the key, etc.</param>
        /// <returns>The LockItemm, or absent if it is not present. Not that the
        /// item can exist in the table even if it is released, as noted by IsReleased()</returns>
        /// Marked virtual for mocking purposes
        public virtual async Task<LockItem> GetLockFromDynamoDBAsync(GetLockOptions options)
        {
            LockClientUtils.RequireNonNull(options, "AcquireLockOptions cannot be null.", "options");
            LockClientUtils.RequireNonNullOrEmpty(options.PartitionKey, "Cannot lookup null or empty key.");

            GetItemResponse result = await this.ReadFromDynamoDBAsync(options.PartitionKey, options.SortKey);

            if (result == null)
            {
                throw new AmazonClientException("The DynamoDB client could not contact a DDB endpoint and returned a null response.");
            }

            Dictionary<string, AttributeValue> item = result.Item;

            if (item == null || !item.Any())
            {
                return null;
            }

            return this.CreateLockItem(options, item);
        }

        /// <summary>
        /// Retrieves all lock items from DynamoDB
        /// 
        /// Note that this may return a lock item even if it was released
        /// </summary>
        /// <param name="deleteOnRelease"></param>
        /// <returns></returns>
        public IEnumerable<LockItem> GetAllLocksFromDynamoDB(bool deleteOnRelease, bool consistentRead = false)
        {
            ScanRequest scanRequest = new ScanRequest()
            {
                TableName = this.TableName,
                ConsistentRead = consistentRead
            };

            LockItemPaginatedScanIterator iterator = new LockItemPaginatedScanIterator(this.DynamoDB, scanRequest, new LockItemFactory(deleteOnRelease, this));

            while (iterator.MoveNext())
            {
                yield return iterator.Current;
            }
        }

        /// <summary>
        /// Attempts to acquire a lock until it either acquires the lock, or a specified AdditionalTimeToWaitForLock is
        /// reached. This method will poll DynamoDB based on the RefreshPeriod. If it does not see the lock in DynamoDB, it
        /// will immediately return the lock to the caller. If it does see the lock, it will note the lease expiration on the lock. If
        /// the lock is deemed stale, (that is, there is no heartbeat on it for at least the length of its lease duration) then this
        /// will acquire and return it. Otherwise, if it waits for as long as AdditionalTimeToWaitForLockvwithout acquiring the
        /// lock, then it will throw a LockNotGrantedException.
        ///
        /// Note that this method will wait for at least as long as the LeaseDuration} in order to acquire a lock that already
        /// exists. If the lock is not acquired in that time, it will wait an additional amount of time specified in
        /// AdditionalTimeToWaitForLock before giving up.
        ///
        /// See the defaults set when constructing a new AcquireLockOptions object for any fields that you do not set explicitly.
        /// </summary>
        /// <param name="options">A combination of optional arguments that may be passed in for acquiring the lock</param>
        /// <returns>The lock</returns>
        public async Task<LockItem> AcquireLockAsync(AcquireLockOptions options)
        {
            LockClientUtils.RequireNonNull(options, "Cannot acquire lock when options is null.", "options");
            LockClientUtils.RequireNonNullOrEmpty(options.PartitionKey, "Cannot acquire lock when partition key is null.");

            string key = options.PartitionKey;
            string sortKey = options.SortKey;

            if (options.AdditionalAttributes.ContainsKey(this.PartitionKeyName) ||
                options.AdditionalAttributes.ContainsKey(OWNER_NAME) ||
                options.AdditionalAttributes.ContainsKey(LEASE_DURATION) ||
                options.AdditionalAttributes.ContainsKey(RECORD_VERSION_NUMBER) ||
                options.AdditionalAttributes.ContainsKey(DATA) ||
                !String.IsNullOrEmpty(this.SortKeyName) && options.AdditionalAttributes.ContainsKey(this.SortKeyName))
            {
                throw new ArgumentException($"Additional attribute cannot be one of the following types: {this.PartitionKeyName}, {OWNER_NAME}, {LEASE_DURATION}, {RECORD_VERSION_NUMBER}, {DATA}");
            }

            long millisecondsToWait = DEFAULT_BUFFER_MS;

            if (options.AdditionalTimeToWaitForLock > 0)
            {
                LockClientUtils.RequireNonNull(options.TimeUnit, "TimeUnit must not be null if AdditionalTimeToWaitForLock is greater than 0.");
                millisecondsToWait = options.TimeUnit.ToMilliseconds(options.AdditionalTimeToWaitForLock);
            }

            long refreshPeriodInMilliseconds = DEFAULT_BUFFER_MS;

            if (options.RefreshPeriod > 0)
            {
                LockClientUtils.RequireNonNull(options.TimeUnit, "TimeUnit must not be null if RefreshPeriod is non-null.");
                refreshPeriodInMilliseconds = options.TimeUnit.ToMilliseconds(options.RefreshPeriod);
            }

            bool deleteLockOnRelease = options.DeleteLockOnRelease;
            bool replaceData = options.ReplaceData;

            SessionMonitor sessionMonitor = options.SessionMonitor;

            if (sessionMonitor != null)
            {
                SessionMonitorArgsValidate(sessionMonitor.SafeTimeWithoutHeartbeatMillis, this.HeartbeatPeriodInMilliseconds, this.LeaseDurationInMilliseconds);
            }

            long currentTimeMillis = LockClientUtils.MillisecondTime();

            // This is the lock we are trying to acquire. If it already exists, then we can try
            // to steal it if it does not get updated after its LEASE_DURATION expires.
            LockItem lockTryingToBeAcquired = null;
            bool alreadySleptOnceForOneLeasePeriod = false;

            GetLockOptions getLockOptions = new GetLockOptions(key)
            {
                SortKey = sortKey,
                DeleteLockOnRelease = deleteLockOnRelease
            };

            while (true)
            {
                try
                {
                    try
                    {
                        LockClientUtils.Logger.Trace($"Call GetItem to see if the lock for {this.PartitionKeyName} = {key}, {this.SortKeyName} = {sortKey} exists in the table");
                        LockItem existingLock = await this.GetLockFromDynamoDBAsync(getLockOptions);

                        if (options.AcquireOnlyIfLockAlreadyExists && existingLock == null)
                        {
                            throw new LockNotGrantedException("Lock does not exist.");
                        }

                        if (options.ShouldSkipBlockingWait && existingLock != null && !existingLock.IsExpired())
                        {
                            // The lock is being held by some one and is still not expired. And the caller explicitly said not to
                            // perform a blocking wait. We will throw back a lock not granted exception, so that the caller
                            // can retry if needed.
                            throw new LockCurrentlyUnavailableException("The lock being requested is being held by another client.");
                        }

                        MemoryStream newLockData = null;

                        if (replaceData)
                        {
                            newLockData = options.Data;
                        }
                        else if (existingLock != null)
                        {
                            newLockData = existingLock.Data;
                        }

                        if (newLockData == null)
                        {
                            newLockData = options.Data; // If there is no existing data, we write the input data to the lock.
                        }

                        string recordVersionNumber = Guid.NewGuid().ToString();
                        Dictionary<string, AttributeValue> item = new Dictionary<string, AttributeValue>(options.AdditionalAttributes.ToDictionary(x => x.Key, y => y.Value))
                        {
                            { this.PartitionKeyName, new AttributeValue() { S = key } },
                            { OWNER_NAME, new AttributeValue() { S = this.OwnerName } },
                            { LEASE_DURATION, new AttributeValue() { S = this.LeaseDurationInMilliseconds.ToString() } },
                            { RECORD_VERSION_NUMBER, new AttributeValue() { S = recordVersionNumber } }
                        };

                        if (!String.IsNullOrEmpty(this.SortKeyName))
                        {
                            item.Add(SortKeyName, new AttributeValue() { S = sortKey });
                        }
                        
                        if (newLockData != null)
                        {
                            item.Add(DATA, new AttributeValue() { B = newLockData });
                        }

                        // If the existing lock does not exist or exists and is released
                        if (existingLock == null && !options.AcquireOnlyIfLockAlreadyExists)
                        {
                            return await UpsertAndMonitorNewLockAsync(options, key, sortKey, deleteLockOnRelease, sessionMonitor, newLockData, item, recordVersionNumber);
                        }
                        else if (existingLock != null && existingLock.IsReleased())
                        {
                            return await UpsertAndMonitorReleasedLockAsync(options, key, sortKey, deleteLockOnRelease, sessionMonitor, existingLock, newLockData, item, recordVersionNumber);
                        }

                        // We know that we didn't enter the if block above because it returns at the end and
                        // we also know that the ExistingLock.IsPresent() is true
                        if (lockTryingToBeAcquired == null)
                        {
                            // This branch of logic only happens once, in the first iteration of the while loop
                            // LockTryingToBeAcquired only ever gets set to non-null values after this point,
                            // so it is impossible to get in this.
                            // Someone else has the lock and they have the lock for LEASE_DURATION time. At this point,
                            // we need to wait at least LEASE_DURATION milliseconds before we can try to acquire the lock
                            lockTryingToBeAcquired = existingLock;

                            if (!alreadySleptOnceForOneLeasePeriod)
                            {
                                alreadySleptOnceForOneLeasePeriod = true;
                                millisecondsToWait += existingLock.LeaseDuration;
                            }
                        }
                        else
                        {
                            if (lockTryingToBeAcquired.RecordVersionNumber == existingLock.RecordVersionNumber)
                            {
                                // If the version numbers match, then we can acquire the lock, assuming it has already expired
                                if (lockTryingToBeAcquired.IsExpired())
                                {
                                    return await UpsertAndMonitorExpiredLockAsync(options, key, sortKey, deleteLockOnRelease, sessionMonitor, existingLock, newLockData, item, recordVersionNumber);
                                }
                            }
                            else
                            {
                                // If the version number changed since we last queried the lock, then we need to update
                                // LockTryingToBeAcquired as the lock has been refreshed sine we last checked
                                lockTryingToBeAcquired = existingLock;
                            }
                        }
                    }
                    catch (ConditionalCheckFailedException e)
                    {
                        // Someone else acquired the lock while we tried to do so, so we throw an exception
                        LockClientUtils.Logger.Debug("Somone else acquired the lock.", e);
                        throw new LockNotGrantedException("Could not acquire lock because someone else acquired it.", e);
                    }
                    catch (ProvisionedThroughputExceededException e)
                    {
                        // Request exceeded maximum allowed provisioned throughput for the table
                        // or one or more global secondary indexes.
                        LockClientUtils.Logger.Debug("Maximum provisioned throughput for the table exceeded.", e);
                        throw new LockNotGrantedException("Could not acquire lock because provisioned throughput for the table exceeded.", e);
                    }
                    catch (AmazonClientException e)
                    {
                        // This indicates that we were unable to successfully connect and make a service call to DDB. Often
                        // indicative of a network failure, such as a socket timeout. We retry if still within the time we
                        // can wait to acquire the lock
                        LockClientUtils.Logger.Warn("Could not acquire the lock because of a client side failure in talking to DDB.", e);
                    }
                }
                catch (LockNotGrantedException e)
                {
                    if ((LockClientUtils.MillisecondTime() - currentTimeMillis) > millisecondsToWait)
                    {
                        LockClientUtils.Logger.Debug($"This client waited more than MillisecondsToWait={millisecondsToWait} ms since the beginning of this acquire call.", e);
                        throw;
                    }
                }

                if ((LockClientUtils.MillisecondTime() - currentTimeMillis) > millisecondsToWait)
                {
                    throw new LockNotGrantedException($"Didn't acquire lock after sleeping for {(LockClientUtils.MillisecondTime() - currentTimeMillis).ToString()} milliseonds.");
                }

                LockClientUtils.Logger.Trace($"Sleeping for a refresh period of {refreshPeriodInMilliseconds} ms.");

                foreach (int time in LockClientUtils.Decompose64BitInt(refreshPeriodInMilliseconds))
                {
                    Thread.Sleep(time);
                }
            }
        }

        /// <summary>
        /// Attempts to acquire a lock. If successful, returns the lock. Otherwise,
        /// returns Optional.Empty. For more details on behavior, please see AcquireLock().
        /// </summary>
        /// <param name="options"></param>
        /// <returns></returns>
        public async Task<LockItem> TryAcquireLockAsync(AcquireLockOptions options)
        {
            try
            {
                return await this.AcquireLockAsync(options);
            }
            catch (LockNotGrantedException)
            {
                return null;
            }
        }

        /// <summary>
        /// Releases the given lock if the current user still has it, returning true if the lock was successfully released, and false
        /// if someone else already stole the lock. Deletes the lock item if it is released and DeleteLockItemOnClose is set.
        /// </summary>
        /// <param name="lockItem">The lock item to release</param>
        /// <returns>True if the lock is released, false otherwise</returns>
        public bool ReleaseLock(LockItem lockItem)
        {
            return this.ReleaseLock(new ReleaseLockOptions(lockItem) { DeleteLock = lockItem.DeleteLockItemOnclose });
        }

        /// <summary>
        /// Releases the given lock if the current user still has it, returning true if the lock was successfully released, and false
        /// if someone else already stole the lock. Deletes the lock item if it is released and DeleteLockItemOnClose is set.
        /// </summary>
        /// <param name="lockItem">The release lock options</param>
        /// <returns>True if the lock is released, false otherwise</returns>
        public bool ReleaseLock(ReleaseLockOptions options)
        {
            LockClientUtils.RequireNonNull(options, "ReleaseLockOptions cannot be null.", "options");
            LockClientUtils.RequireNonNull(options.LockItem, "Cannot release null LockItem");

            LockItem lockItem = options.LockItem;
            bool deleteLock = options.DeleteLock;
            bool bestEffort = options.BestEffort;
            MemoryStream data = options.Data;

            if (!lockItem.OwnerName.Equals(this.OwnerName))
            {
                return false;
            }

            lock (SyncObject)
            {
                string conditionalExpression;
                Dictionary<string, AttributeValue> expressionAttributeValues;
                Dictionary<string, string> expressionAttributeNames;

                try
                {
                    // Always remove the heartbeat for the lock. The
                    // caller's intention is to release the lock. Stopping the
                    // heartbeat alone will do that regardless of whether the Dynamo
                    // write succeeds or fails.
                    this.Locks.TryRemove(lockItem.GetUniqueIdentifier(), out LockItem Temp);

                    //set up expression stuff for DeleteItem or UpdateItem
                    //basically any changes require:
                    //1. I own the lock
                    //2. I know the current version number
                    //3. The lock already exists (UpdateItem API can cause a new item to be created if you do not condition the primary keys with attribute_exists)


                    expressionAttributeValues = new Dictionary<string, AttributeValue>()
                    {
                        { RVN_VALUE_EXPRESSION_VARIABLE, new AttributeValue() { S = lockItem.RecordVersionNumber } },
                        { OWNER_NAME_VALUE_EXPRESSION_VARIABLE, new AttributeValue() { S = lockItem.OwnerName } }
                    };

                    expressionAttributeNames = new Dictionary<string, string>()
                    {
                        { PK_PATH_EXPRESSION_VARIABLE, this.PartitionKeyName },
                        { OWNER_NAME_PATH_EXPRESSION_VARIABLE, OWNER_NAME},
                        { RVN_PATH_EXPRESSION_VARIABLE, RECORD_VERSION_NUMBER}
                    };

                    if (!String.IsNullOrEmpty(this.SortKeyName))
                    {
                        conditionalExpression = PK_EXISTS_AND_SK_EXISTS_AND_OWNER_NAME_SAME_AND_RVN_SAME_CONDITION;
                        expressionAttributeNames.Add(SK_PATH_EXPRESSION_VARIABLE, this.SortKeyName);
                    }
                    else
                    {
                        conditionalExpression = PK_EXISTS_AND_OWNER_NAME_SAME_AND_RVN_SAME_CONDITION;
                    }

                    Dictionary<string, AttributeValue> key = GetItemKeys(lockItem);

                    if (deleteLock)
                    {
                        DeleteItemRequest deleteItemRequest = new DeleteItemRequest()
                        {
                            TableName = this.TableName,
                            Key = key,
                            ConditionExpression = conditionalExpression,
                            ExpressionAttributeNames = expressionAttributeNames,
                            ExpressionAttributeValues = expressionAttributeValues
                        };

                        Task.Run(() => this.DynamoDB.DeleteItemAsync(deleteItemRequest)).Wait();
                    }
                    else
                    {
                        string updateExpression;
                        expressionAttributeNames.Add(IS_RELEASED_PATH_EXPRESSION_VARIABLE, IS_RELEASED);
                        expressionAttributeValues.Add(IS_RELEASED_VALUE_EXPRESSION_VARIABLE, IS_RELEASED_ATTRIBUTE_VALUE);

                        if (data != null)
                        {
                            updateExpression = UPDATE_IS_RELEASED_AND_DATA;
                            expressionAttributeNames.Add(DATA_PATH_EXPRESSION_VARIABLE, DATA);
                            expressionAttributeValues.Add(DATA_VALUE_EXPRESSION_VARIABLE, new AttributeValue() { B = data });
                        }
                        else
                        {
                            updateExpression = UPDATE_IS_RELEASED;
                        }

                        UpdateItemRequest updateItemRequest = new UpdateItemRequest()
                        {
                            TableName = this.TableName,
                            Key = key,
                            UpdateExpression = updateExpression,
                            ConditionExpression = conditionalExpression,
                            ExpressionAttributeNames = expressionAttributeNames,
                            ExpressionAttributeValues = expressionAttributeValues
                        };

                        Task.Run(() => this.DynamoDB.UpdateItemAsync(updateItemRequest)).Wait();
                    }
                }
                catch (AggregateException e) when (e.InnerException is ConditionalCheckFailedException)
                {
                    // This can happen if the owner name or record version number changed, which can happen
                    // if someone else acquires the lock after this client
                    LockClientUtils.Logger.Debug("Someone else acquired the lock before you asked to release it.", e.InnerException);
                    return false;
                }
                catch (AggregateException e) when (e.InnerException is AmazonClientException)
                {
                    if (bestEffort)
                    {
                        LockClientUtils.Logger.Warn("Ignore AmazonClientException and continue to clean up.", e.InnerException);
                    }
                    else
                    {
                        throw e.InnerException;
                    }
                }

                // Only remove the session monitor if no exception thrown above.
                // While moving the heartbeat removal before the DynamoDB call
                // should not cause existing clients problems, there
                // may be existing clients that depend on the monitor firing if they
                // get exceptions from this method.
                this.RemoveKillSessionMonitorAsync(lockItem.GetUniqueIdentifier()).Wait();
            }

            return true;
        }

        /// <summary>
        /// Sends a heartbeat to indicate that the given lock is still being worked on. If using
        /// 'CreateHeartbeatBackgroundThread = true' when setting up this object, then this method
        /// is unnecessary, because the background thread will be periodically calling it and sending
        /// heartbeats. However, if 'CreateHeartbeatBackgroundThread = false', then this method must
        /// be called to instruct DynamoDB that the lock should not be expired.
        /// 
        /// This lease duration of the lock will be set to the default specified in the constructor of this class.
        /// </summary>
        /// <param name="lockItem">The lock item row to send a heartbeat and extend lock expiry</param>
        /// Marked virtual for mocking
        public virtual void SendHeartbeat(LockItem lockItem)
        {
            this.SendHeartbeat(new SendHeartbeatOptions(lockItem));
        }

        /// <summary>
        /// Sends a heartbeat to indicate that the given lock is still being worked on. If using
        /// 'CreateHeartbeatBackgroundThread = true' when setting up this object, then this method
        /// is unnecessary, because the background thread will be periodically calling it and sending
        /// heartbeats. However, if 'CreateHeartbeatBackgroundThread = false', then this method must
        /// be called to instruct DynamoDB that the lock should not be expired.
        /// 
        /// This method will also set the lease duration of the lock to the give value.
        /// 
        /// This will also either update of delete the data from the lock, as specified in the options.
        /// </summary>
        /// <param name="options">A set of optional arguments for how to send the heartbeat.</param>
        /// Mocked
        public virtual void SendHeartbeat(SendHeartbeatOptions options)
        {
            LockClientUtils.RequireNonNull(options, "Options is required.", "options");
            LockClientUtils.RequireNonNull(options.LockItem, "Cannot send heartbeat for null lock.");

            bool deleteData = options.DeleteData;

            if (deleteData && options.Data != null)
            {
                throw new ArgumentException("Data must not be present if DeleteData is true.");
            }

            long leaseDurationToEnsureInMilliseconds = this.LeaseDurationInMilliseconds;
            if (options.LeaseDurationToEnsure > 0)
            {
                LockClientUtils.RequireNonNull(options.TimeUnit, "TimeUnit must not be null if LeaseDurationToEnsure is not null.");
                leaseDurationToEnsureInMilliseconds = options.TimeUnit.ToMilliseconds(options.LeaseDurationToEnsure);
            }

            LockItem lockItem = options.LockItem;
            bool expired = lockItem.IsExpired();

            if (expired || !lockItem.OwnerName.Equals(this.OwnerName) || lockItem.IsReleased())
            {
                this.Locks.TryRemove(lockItem.GetUniqueIdentifier(), out LockItem Temp);
                throw new LockNotGrantedException("Cannot send heartbeat because lock is not granted.");
            }

            lock (SyncObject)
            {
                // Set up condition for UpdateItem.Basically any changes require:
                //1. I own the lock
                //2. I know the current version number
                //3. The lock already exists (UpdateItem API can cause a new item to be created if you do not condition the primary keys with attribute_exists)
                Dictionary<string, AttributeValue> expressionAttributeValues = new Dictionary<string, AttributeValue>()
                {
                    { RVN_VALUE_EXPRESSION_VARIABLE, new AttributeValue() { S = lockItem.RecordVersionNumber } },
                    { OWNER_NAME_VALUE_EXPRESSION_VARIABLE, new AttributeValue() { S = lockItem.OwnerName } }
                };

                Dictionary<string, string> expressionAttributeNames = new Dictionary<string, string>()
                {
                    { PK_PATH_EXPRESSION_VARIABLE, this.PartitionKeyName },
                    { LEASE_DURATION_PATH_VALUE_EXPRESSION_VARIABLE, LEASE_DURATION },
                    { RVN_PATH_EXPRESSION_VARIABLE, RECORD_VERSION_NUMBER },
                    { OWNER_NAME_PATH_EXPRESSION_VARIABLE, OWNER_NAME }
                };

                string conditionalExpression;

                if (!String.IsNullOrEmpty(this.SortKeyName))
                {
                    conditionalExpression = PK_EXISTS_AND_SK_EXISTS_AND_OWNER_NAME_SAME_AND_RVN_SAME_CONDITION;
                    expressionAttributeNames.Add(SK_PATH_EXPRESSION_VARIABLE, this.SortKeyName);
                }
                else
                {
                    conditionalExpression = PK_EXISTS_AND_OWNER_NAME_SAME_AND_RVN_SAME_CONDITION;
                }

                string recordVersionNumber = Guid.NewGuid().ToString();

                expressionAttributeValues.Add(NEW_RVN_VALUE_EXPRESSION_VARIABLE, new AttributeValue() { S = recordVersionNumber });
                expressionAttributeValues.Add(LEASE_DURATION_VALUE_EXPRESSION_VARIABLE, new AttributeValue() { S = this.LeaseDurationInMilliseconds.ToString() });

                //Set up update expression for UpdateItem.
                string updateExpression;
                if (deleteData)
                {
                    expressionAttributeNames.Add(DATA_PATH_EXPRESSION_VARIABLE, DATA);
                    updateExpression = UPDATE_LEASE_DURATION_AND_RVN_AND_REMOVE_DATA;
                }
                else if (options.Data != null && options.Data != null)
                {
                    expressionAttributeNames.Add(DATA_PATH_EXPRESSION_VARIABLE, DATA);
                    expressionAttributeValues.Add(DATA_VALUE_EXPRESSION_VARIABLE, new AttributeValue() { B = options.Data });
                    updateExpression = UPDATE_LEASE_DURATION_AND_RVN_AND_DATA;
                }
                else
                {
                    updateExpression = UPDATE_LEASE_DURATION_AND_RVN;
                }

                UpdateItemRequest updateItemRequest = new UpdateItemRequest()
                {
                    TableName = this.TableName,
                    Key = this.GetItemKeys(lockItem),
                    ConditionExpression = conditionalExpression,
                    UpdateExpression = updateExpression,
                    ExpressionAttributeNames = expressionAttributeNames,
                    ExpressionAttributeValues = expressionAttributeValues
                };

                try
                {
                    long lastUpdateOfLock = LockClientUtils.MillisecondTime();
                    Task.Run(() => this.DynamoDB.UpdateItemAsync(updateItemRequest)).Wait();
                    lockItem.UpdateRecordVersionNumber(recordVersionNumber, lastUpdateOfLock, leaseDurationToEnsureInMilliseconds);
                }
                catch (AggregateException e) when (e.InnerException is ConditionalCheckFailedException)
                {
                    ConditionalCheckFailedException ex = e.InnerException as ConditionalCheckFailedException;
                    LockClientUtils.Logger.Debug("Someone else acquired the lock, so we will stop heartbeating it.", ex);
                    this.Locks.TryRemove(lockItem.GetUniqueIdentifier(), out LockItem Temp);
                    throw new LockNotGrantedException("Someone else acquired the lock, so we will stop heartbeating it.", ex);
                }
                catch (AggregateException e) when (e.InnerException is AmazonServiceException)
                {
                    AmazonServiceException ex = e.InnerException as AmazonServiceException;
                    if (this.HoldLockOnServiceUnavailable && ex.StatusCode == HttpStatusCode.ServiceUnavailable)
                    {
                        // When DynamoDB service is unavailable, other threads may get the same exception and no thread may have the lock.
                        // For systems which should always hold a lock on an item and it is okay for multiple threads to hold the lock,
                        // the lookUpTime of local state can be updated to make it believe that it still has the lock.
                        LockClientUtils.Logger.Info("DynamoDB Service Unavailable. Holding the lock.");
                        lockItem.UpdateLookupTime(LockClientUtils.MillisecondTime());
                    }
                    else
                    {
                        throw ex;
                    }
                }
            }
        }

        /// <summary>
        /// Loops forever, sending hearbeats for all the locks this thread needs to keep track of. You should run
        /// this off of the main thread with Task.Run(() => Run(ct));
        /// </summary>
        /// <param name="ct">A cancellation token for cancelling the task</param>
        /// <returns>A task that has been scheduled on the thread pool to run</returns>
        public void Run(CancellationToken ct = default(CancellationToken))
        {
            while (true)
            {
                try
                {
                    if (ct.IsCancellationRequested)
                    {
                        ct.ThrowIfCancellationRequested();
                    }

                    if (this.ShuttingDown)
                    {
                        throw new OperationCanceledException();
                    }

                    long timeWorkBegins = LockClientUtils.MillisecondTime();
                    Dictionary<string, LockItem> workingCopyOfLocks = new Dictionary<string, LockItem>(this.Locks);

                    foreach (KeyValuePair<string, LockItem> lockEntry in workingCopyOfLocks)
                    {
                        if (ct.IsCancellationRequested)
                        {
                            ct.ThrowIfCancellationRequested();
                        }

                        try
                        {
                            System.Diagnostics.Debug.WriteLine($"Sending heartbeat for {lockEntry.Key}");
                            this.SendHeartbeat(lockEntry.Value);
                        }
                        catch (LockNotGrantedException e)
                        {
                            LockClientUtils.Logger.Debug($"Heartbeat failed for {lockEntry.Key} : {lockEntry.Value}", e);
                        }
                        catch (Exception e)
                        {
                            LockClientUtils.Logger.Warn($"Exception sending heartbeat for {lockEntry.Key} : {lockEntry.Value}.", e);
                        }
                    }

                    long timeElapsed = LockClientUtils.MillisecondTime() - timeWorkBegins;

                    if (this.ShuttingDown)
                    {
                        throw new OperationCanceledException(); // sometimes libraries wrap interrupted and other exceptions
                    }

                    // If we want to hearbeat every 9 seconds, and it took 3 seconds to send the heartbeats, we only sleep 6 seconds

                    foreach (int time in LockClientUtils.Decompose64BitInt(Math.Max(this.HeartbeatPeriodInMilliseconds - timeElapsed, 0)))
                    {
                        if (ct.IsCancellationRequested)
                        {
                            ct.ThrowIfCancellationRequested();
                        }

                        Thread.Sleep(time);
                    }
                }
                catch (OperationCanceledException e)
                {
                    LockClientUtils.Logger.Info("Heartbeat thread recieved interrupt, exiting Run() (possibly exiting thread).", e);
                    return;
                }
                catch (Exception e)
                {
                    LockClientUtils.Logger.Warn("Exception sending heartbeat.", e);
                }
            }
        }

        /// <summary>
        /// Releases all of the locks by calling ReleaseAllLocks()
        /// </summary>
        public void Close()
        {
            // release the locks before interrupting the heartbeat thread to avoid partially updated/stale locks
            IEnumerable<Exception> results = this.ReleaseAllLocks();

            /* Need a way to report failure separately without throwing
            if (results.Any())
            {
                throw new AggregateException(results);
            }
            */

            if (this.BackgroundThread != null)
            {
                this.ShuttingDown = true;
                this.BackgroundThread.Cancel();

                try
                {
                    this.BackgroundThread.Wait();
                }
                catch (OperationCanceledException)
                {
                    LockClientUtils.Logger.Warn("Caught OperationCanceledException waiting for background thread to exit, interrupting current thread.");
                    // Thread.Interrupt not available in netstandard1.6
                }
            }
        }

        #endregion

        #region Private Methods

        /// <summary>
        /// Upserts the lock item and monitors the expired lock
        /// </summary>
        /// <param name="options"></param>
        /// <param name="key"></param>
        /// <param name="sortKey"></param>
        /// <param name="deleteLockOnRelease"></param>
        /// <param name="sessionMonitor"></param>
        /// <param name="existingLock"></param>
        /// <param name="newLockData"></param>
        /// <param name="item"></param>
        /// <param name="recordVersionNumber"></param>
        /// <returns></returns>
        private async Task<LockItem> UpsertAndMonitorExpiredLockAsync(
            AcquireLockOptions options,
            string key,
            string sortKey,
            bool deleteLockOnRelease,
            SessionMonitor sessionMonitor,
            LockItem existingLock,
            MemoryStream newLockData,
            Dictionary<string, AttributeValue> item,
            string recordVersionNumber)
        {
            string conditionalExpression;
            Dictionary<string, AttributeValue> expressionAttributeValues = new Dictionary<string, AttributeValue>()
            {
                {RVN_VALUE_EXPRESSION_VARIABLE, new AttributeValue() { S = existingLock.RecordVersionNumber } }
            };
            bool updateExistingLockRecord = options.UpdateExistingLockRecord;


            Dictionary<string, string> expressionAttributeNames = new Dictionary<string, string>()
            {
                { PK_PATH_EXPRESSION_VARIABLE, this.PartitionKeyName },
                { RVN_PATH_EXPRESSION_VARIABLE, RECORD_VERSION_NUMBER }
            };

            if (!String.IsNullOrEmpty(this.SortKeyName))
            {
                // We do not check the owner here because the lock is expired and it is OK to overwrite the owner
                conditionalExpression = PK_EXISTS_AND_SK_EXISTS_AND_RVN_IS_THE_SAME_CONDITION;
                expressionAttributeNames.Add(SK_PATH_EXPRESSION_VARIABLE, this.SortKeyName);
            }
            else
            {
                conditionalExpression = PK_EXISTS_AND_RVN_IS_THE_SAME_CONDITION;
            }

            if (updateExistingLockRecord)
            {
                item.Remove(this.PartitionKeyName);

                if (!String.IsNullOrEmpty(this.SortKeyName))
                {
                    item.Remove(SortKeyName);
                }

                string updateExpression = this.GetUpdateExpressionAndUpdateNameValueMaps(item, expressionAttributeNames, expressionAttributeValues);

                UpdateItemRequest updateItemRequest = new UpdateItemRequest()
                {
                    TableName = this.TableName,
                    Key = this.GetItemKeys(existingLock),
                    UpdateExpression = updateExpression,
                    ExpressionAttributeNames = expressionAttributeNames,
                    ExpressionAttributeValues = expressionAttributeValues,
                    ConditionExpression = conditionalExpression
                };

                LockClientUtils.Logger.Trace($"Acquiring an existing lock whose RevisionVersionNumber did not change for {this.PartitionKeyName} PartitionKeyName = {key}, {this.SortKeyName} = {sortKey}");

                return await this.UpdateItemAndStartSessionMonitorAsync(options, key, sortKey, deleteLockOnRelease, sessionMonitor, newLockData, recordVersionNumber, updateItemRequest);
            }
            else
            {
                PutItemRequest putItemRequest = new PutItemRequest()
                {
                    Item = item,
                    TableName = this.TableName,
                    ConditionExpression = conditionalExpression,
                    ExpressionAttributeNames = expressionAttributeNames,
                    ExpressionAttributeValues = expressionAttributeValues
                };

                LockClientUtils.Logger.Trace($"Acquiring an existing lock whose RevisionVersionNumber did not change for {this.PartitionKeyName} PartitionKeyName = {key}, {this.SortKeyName} = {sortKey}");

                return await this.PutLockItemAndStartSessionMonitorAsync(options, key, sortKey, deleteLockOnRelease, sessionMonitor, newLockData, recordVersionNumber, putItemRequest);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="options"></param>
        /// <param name="key"></param>
        /// <param name="sortKey"></param>
        /// <param name="deleteLockOnRelease"></param>
        /// <param name="sessionMonitor"></param>
        /// <param name="existingLock"></param>
        /// <param name="newLockData"></param>
        /// <param name="item"></param>
        /// <param name="recordVersionNumber"></param>
        /// <returns></returns>
        private async Task<LockItem> UpsertAndMonitorReleasedLockAsync(
            AcquireLockOptions options,
            string key,
            string sortKey,
            bool deleteLockOnRelease,
            SessionMonitor sessionMonitor,
            LockItem existingLock,
            MemoryStream newLockData,
            Dictionary<string, AttributeValue> item,
            string recordVersionNumber
        )
        {
            string conditionalExpression;
            bool updateExistingLockRecord = options.UpdateExistingLockRecord;
            bool consistentLockData = options.AcquireReleasedLocksConsistently;

            Dictionary<string, string> expressionAttributeNames = new Dictionary<string, string>();
            Dictionary<string, AttributeValue> expressionAttributeValues = new Dictionary<string, AttributeValue>();

            if (consistentLockData)
            {
                expressionAttributeValues.Add(RVN_VALUE_EXPRESSION_VARIABLE, new AttributeValue() { S = existingLock.RecordVersionNumber });
                expressionAttributeNames.Add(RVN_PATH_EXPRESSION_VARIABLE, RECORD_VERSION_NUMBER);
            }

            expressionAttributeNames.Add(PK_PATH_EXPRESSION_VARIABLE, this.PartitionKeyName);
            expressionAttributeNames.Add(IS_RELEASED_PATH_EXPRESSION_VARIABLE, IS_RELEASED);

            if (!String.IsNullOrEmpty(this.SortKeyName))
            {
                //We do not check the owner here because the lock is expired and it is OK to overwrite the owner
                if (consistentLockData)
                {
                    conditionalExpression = PK_EXISTS_AND_SK_EXISTS_AND_RVN_IS_THE_SAME_AND_IS_RELEASED_CONDITION;
                }
                else
                {
                    conditionalExpression = PK_EXISTS_AND_SK_EXISTS_AND_IS_RELEASED_CONDITION;
                }

                expressionAttributeNames.Add(SK_PATH_EXPRESSION_VARIABLE, this.SortKeyName);
            }
            else
            {
                if (consistentLockData)
                {
                    conditionalExpression = PK_EXISTS_AND_RVN_IS_THE_SAME_AND_IS_RELEASED_CONDITION;
                }
                else
                {
                    conditionalExpression = PK_EXISTS_AND_IS_RELEASED_CONDITION;
                }
            }

            expressionAttributeValues.Add(IS_RELEASED_VALUE_EXPRESSION_VARIABLE, IS_RELEASED_ATTRIBUTE_VALUE);

            if (updateExistingLockRecord)
            {
                item.Remove(this.PartitionKeyName);

                if (!String.IsNullOrEmpty(this.SortKeyName))
                {
                    item.Remove(this.SortKeyName);
                }

                string updateExpression = GetUpdateExpressionAndUpdateNameValueMaps(item, expressionAttributeNames, expressionAttributeValues)
                        + REMOVE_IS_RELEASED_UPDATE_EXPRESSION;

                UpdateItemRequest updateItemRequest = new UpdateItemRequest()
                {
                    TableName = this.TableName,
                    Key = GetItemKeys(existingLock),
                    UpdateExpression = updateExpression,
                    ExpressionAttributeNames = expressionAttributeNames,
                    ExpressionAttributeValues = expressionAttributeValues,
                    ConditionExpression = conditionalExpression
                };

                LockClientUtils.Logger.Trace($"Acquiring an existing released lock whose RevisionVersionNumber did not change for {this.PartitionKeyName} PartitionKeyName = {key}, {this.SortKeyName} = {sortKey}");

                return await this.UpdateItemAndStartSessionMonitorAsync(options, key, sortKey, deleteLockOnRelease, sessionMonitor, newLockData, recordVersionNumber, updateItemRequest);
            }
            else
            {
                PutItemRequest putItemRequest = new PutItemRequest(this.TableName, item)
                {
                    ConditionExpression = conditionalExpression,
                    ExpressionAttributeNames = expressionAttributeNames,
                    ExpressionAttributeValues = expressionAttributeValues
                };

                LockClientUtils.Logger.Trace($"Acquiring an existing released lock whose RevisionVersionNumber did not change for {this.PartitionKeyName} PartitionKeyName = {key}, {this.SortKeyName} = {sortKey}");

                return await this.PutLockItemAndStartSessionMonitorAsync(options, key, sortKey, deleteLockOnRelease, sessionMonitor, newLockData, recordVersionNumber, putItemRequest);
            }
        }

        private async Task<LockItem> UpdateItemAndStartSessionMonitorAsync(
            AcquireLockOptions options,
            string key,
            string sortKey,
            bool deleteLockOnRelease,
            SessionMonitor sessionMonitor,
            MemoryStream newLockData,
            string recordVersionNumber,
            UpdateItemRequest updateItemRequest
        )
        {
            long lastUpdatedTime = LockClientUtils.MillisecondTime();

            await this.DynamoDB.UpdateItemAsync(updateItemRequest);

            LockItem lockItem = new LockItem(
                this,
                key,
                sortKey,
                newLockData,
                deleteLockOnRelease,
                this.OwnerName,
                this.LeaseDurationInMilliseconds,
                lastUpdatedTime,
                recordVersionNumber,
                !IS_RELEASED_INDICATOR,
                sessionMonitor,
                options.AdditionalAttributes
            );

            this.Locks.AddOrUpdate(lockItem.GetUniqueIdentifier(), lockItem, (id, item) => item);
            this.TryAddSessionMonitor(lockItem.GetUniqueIdentifier(), lockItem);
            return lockItem;
        }

        private async Task<LockItem> UpsertAndMonitorNewLockAsync(
            AcquireLockOptions options,
            string key,
            string sortKey,
            bool deleteLockOnRelease,
            SessionMonitor sessionMonitor,
            MemoryStream newLockData,
            Dictionary<string, AttributeValue> item,
            string recordVersionNumber
        )
        {
            Dictionary<string, string> expressionAttributeNames = new Dictionary<string, string>()
            {
                {PK_PATH_EXPRESSION_VARIABLE, this.PartitionKeyName }
            };

            bool updateExistingLockRecord = options.UpdateExistingLockRecord;

            string conditionalExpression;

            if (!String.IsNullOrEmpty(this.SortKeyName))
            {
                conditionalExpression = ACQUIRE_LOCK_THAT_DOESNT_EXIST_PK_SK_CONDITION;
                expressionAttributeNames.Add(SK_PATH_EXPRESSION_VARIABLE, this.SortKeyName);
            }
            else
            {
                conditionalExpression = ACQUIRE_LOCK_THAT_DOESNT_EXIST_PK_CONDITION;
            }

            if (updateExistingLockRecord)
            {
                // Remove keys from item to create updateExpression
                item.Remove(this.PartitionKeyName);

                if (!String.IsNullOrEmpty(this.SortKeyName))
                {
                    item.Remove(this.SortKeyName);
                }

                Dictionary<string, AttributeValue> expressionAttributeValues = new Dictionary<string, AttributeValue>();
                string updateExpression = this.GetUpdateExpressionAndUpdateNameValueMaps(item, expressionAttributeNames, expressionAttributeValues);

                UpdateItemRequest updateItemRequest = new UpdateItemRequest()
                {
                    TableName = this.TableName,
                    Key = this.GetKeys(key, sortKey),
                    UpdateExpression = updateExpression,
                    ExpressionAttributeNames = expressionAttributeNames,
                    ExpressionAttributeValues = expressionAttributeValues,
                    ConditionExpression = conditionalExpression
                };

                LockClientUtils.Logger.Trace($"Acquiring a new lock on {this.PartitionKeyName} PartitionKeyName = {key}, {this.SortKeyName} = {sortKey}");

                return await this.UpdateItemAndStartSessionMonitorAsync(options, key, sortKey, deleteLockOnRelease, sessionMonitor, newLockData, recordVersionNumber, updateItemRequest);
            }
            else
            {
                PutItemRequest putItemRequest = new PutItemRequest()
                {
                    Item = item,
                    TableName = this.TableName,
                    ConditionExpression = conditionalExpression,
                    ExpressionAttributeNames = expressionAttributeNames
                };

                LockClientUtils.Logger.Trace($"Acquiring a new lock on {this.PartitionKeyName} PartitionKeyName = {key}, {this.SortKeyName} = {sortKey}");

                // No one has the lock, go ahead and acquire it.
                // The person storing the lock into DynamoDB should err on the side of thinking the lock will expire
                // sooner than it actually will, so they start counting towards its expiration before the Put succeeds
                return await this.PutLockItemAndStartSessionMonitorAsync(options, key, sortKey, deleteLockOnRelease, sessionMonitor, newLockData, recordVersionNumber, putItemRequest);
            }
        }

        private async Task<LockItem> PutLockItemAndStartSessionMonitorAsync(
            AcquireLockOptions options,
            string key, 
            string sortKey,
            bool deleteLockOnRelease,
            SessionMonitor sessionMonitor,
            MemoryStream newLockData,
            string recordVersionNumber,
            PutItemRequest putItemRequest
        )
        {
            long lastUpdatedTime = LockClientUtils.MillisecondTime();

            await this.DynamoDB.PutItemAsync(putItemRequest);

            LockItem lockItem = new LockItem(
                this,
                key,
                sortKey,
                newLockData,
                deleteLockOnRelease,
                this.OwnerName,
                this.LeaseDurationInMilliseconds,
                lastUpdatedTime,
                recordVersionNumber,
                false,
                sessionMonitor,
                options.AdditionalAttributes
            );
            this.Locks.AddOrUpdate(lockItem.GetUniqueIdentifier(), lockItem, (id, item) => item);
            this.TryAddSessionMonitor(lockItem.GetUniqueIdentifier(), lockItem);
            return lockItem;
        }

        /// <summary>
        /// Builds an UpdateExpression for all fields in item map and updates the corresponding expression
        /// attribute name and value maps.
        /// </summary>
        /// <param name="item">Item map of Name and AttributeValue to update or create</param>
        /// <param name="expressionAttributeNames"></param>
        /// <param name="expressionAttributeValues"></param>
        /// <returns></returns>
        private string GetUpdateExpressionAndUpdateNameValueMaps(
            Dictionary<string, AttributeValue> item,
            Dictionary<string, string> expressionAttributeNames,
            Dictionary<string, AttributeValue> expressionAttributeValues
        )
        {
            string additionalUpdateExpression = "SET ";
            StringBuilder updateExpressionBuilder = new StringBuilder(additionalUpdateExpression);

            int i = 0;

            string expressionSeparator = ",";

            foreach (KeyValuePair<string, AttributeValue> Entry in item)
            {
                expressionAttributeNames.Add($"#k{i}", Entry.Key);
                expressionAttributeValues.Add($":v{i}", Entry.Value);

                updateExpressionBuilder.Append("#k").Append(i).Append("=").Append(":v").Append(i++).Append(expressionSeparator);
            }

            // Remove the last comma
            updateExpressionBuilder.Length = updateExpressionBuilder.Length - 1;

            return updateExpressionBuilder.ToString();
        }

        /// <summary>
        /// Releases all the locks currently held by the owner specified when creating this lock client
        /// </summary>
        /// <returns>Any exceptions that were raised while releasing locks are returned so they can be handled by the caller</returns>
        private IEnumerable<Exception> ReleaseAllLocks()
        {
            List<Exception> exceptions = new List<Exception>();

            lock (SyncObject)
            {
                Dictionary<string, LockItem> locks = new Dictionary<string, LockItem>(this.Locks);

                foreach (KeyValuePair<string, LockItem> lockEntry in locks)
                {
                    try
                    {
                        this.ReleaseLock(lockEntry.Value); // TODO catch exceptions and report failure separately
                    }
                    catch (Exception e)
                    {
                        exceptions.Add(e);
                    }
                }
            }

            return exceptions;
        }

        /// <summary>
        ///  Helper method to read a key from DynamoDB
        /// </summary>
        /// <param name="key"></param>
        /// <param name="sortKey"></param>
        /// <returns></returns>
        private async Task<GetItemResponse> ReadFromDynamoDBAsync(string key, string sortKey)
        {
            Dictionary<string, AttributeValue> dynamoDBKey = new Dictionary<string, AttributeValue>()
            {
                { this.PartitionKeyName, new AttributeValue() { S = key } }
            };

            if (!String.IsNullOrEmpty(this.SortKeyName))
            {
                dynamoDBKey.Add(this.SortKeyName, new AttributeValue() { S = sortKey });
            }

            GetItemRequest getItemRequest = new GetItemRequest()
            {
                TableName = this.TableName,
                Key = dynamoDBKey,
                ConsistentRead = true
            };

            return await this.DynamoDB.GetItemAsync(getItemRequest);
        }

        private LockItem CreateLockItem(GetLockOptions options, Dictionary<string, AttributeValue> immutableItem)
        {
            Dictionary<string, AttributeValue> item = new Dictionary<string, AttributeValue>(immutableItem);

            MemoryStream data = null;

            if (item.ContainsKey(DATA))
            {
                AttributeValue value = item[DATA];
                item.Remove(DATA);
                data = value.B;
            }

            AttributeValue ownerName = item[OWNER_NAME];
            item.Remove(OWNER_NAME);
            AttributeValue leaseDuration = item[LEASE_DURATION];
            item.Remove(LEASE_DURATION);
            AttributeValue recordVersionNumber = item[RECORD_VERSION_NUMBER];
            item.Remove(RECORD_VERSION_NUMBER);

            bool isReleased = item.ContainsKey(IS_RELEASED);
            item.Remove(IS_RELEASED);
            item.Remove(this.PartitionKeyName);

            // The person retrieving the lock in DynamoDB should err on the side of
            // not expiring the lock, so they don't start counting until after the
            // call to DynamoDB succeeds
            long lookupTime = LockClientUtils.MillisecondTime();

            LockItem lockItem = new LockItem(
                this,
                options.PartitionKey,
                options.SortKey,
                data,
                options.DeleteLockOnRelease,
                ownerName.S,
                Int64.Parse(leaseDuration.S),
                lookupTime,
                recordVersionNumber.S,
                isReleased,
                null,
                item
            );

            return lockItem;
        }

        /// <summary>
        /// Validates the arguments to ensure that they are safe to register a SessionMonitor
        /// on the lock to be aquired.
        ///
        /// The method will throw an ArgumentOutOfRange exception when the safeTimeWithoutHeartbeat is
        /// less than the heartbeat frequeny or greater than the lease duration
        /// </summary>
        /// <param name="safeTimeWithoutHeartbeatMillis">The amount of time in milliseconds a lock can go
        /// without a heartbeat before it is declared to be in the "danger zone"</param>
        /// <param name="heartbeatPeriodMillis">The heartbeat period in milliseconds</param>
        /// <param name="leaseDurationMillis">The lease duration in milliseconds</param>
        private static void SessionMonitorArgsValidate(long safeTimeWithoutHeartbeatMillis, long heartbeatPeriodMillis, long leaseDurationMillis)
        {
            if (safeTimeWithoutHeartbeatMillis <= heartbeatPeriodMillis)
            {
                throw new ArgumentOutOfRangeException("SafeTimeWithoutHeartbeat must be greater than heartbeat frequency");
            }
            else if (safeTimeWithoutHeartbeatMillis >= leaseDurationMillis)
            {
                throw new ArgumentOutOfRangeException("SafeTimeWithoutHeartbeat must be less than the lock's lease duration");
            }
        }

        private BackgroundTask StartBackgroundTask()
        {
            // The thread name does nothing here
            // Pass ct to Task.Run(Action action, Cancellationotken ct) for 2 reasons
            // 1) If the token has cancellation requested prior to the Task starting to execute, 
            // the Task won't execute.  Rather than transitioning to Running, it'll immediately 
            // transition to Canceled.  This avoids the costs of running the task if it would just 
            // be canceled while running anyway.
            // 2) If the body of the task is also monitoring the cancellation token and throws an 
            // OperationCanceledException containing that token(which is what ThrowIfCancellationRequested 
            // does), then when the task sees that OCE, it checks whether the OCE's token matches the Task's 
            // token.  If it does, that exception is viewed as an acknowledgement of cooperative cancellation 
            // and the Task transitions to the Canceled state(rather than the Faulted state).

            BackgroundTask bt = new BackgroundTask(() => this.Run(this.CTS.Token), this.CTS);
            bt.Start();
            return bt;
        }

        private Dictionary<string, AttributeValue> GetItemKeys(LockItem lockItem)
        {
            return GetKeys(lockItem.PartitionKey, lockItem.SortKey);
        }

        private Dictionary<string, AttributeValue> GetKeys(string partitionKey, string sortKey)
        {
            Dictionary<string, AttributeValue> key = new Dictionary<string, AttributeValue>()
            { 
                {this.PartitionKeyName, new AttributeValue() { S = partitionKey } }
            };

            if (!String.IsNullOrEmpty(sortKey))
            {
                // TODO: This will throw an exception if a client without a sort key set
                // is used to acquire a lock with a sort key provided in the acquire lock
                // options since we refer to the SortKeyName and not the provided parameter
                key.Add(this.SortKeyName, new AttributeValue() { S = sortKey });
            }

            return key;
        }

        /// <summary>
        /// Adds a session monitor to the list of session monitors and
        /// starts the background task of the callback action associated with the
        /// lockitem. This is called by Put and Update lock item calls when
        /// a lock is being acquired.
        /// </summary>
        /// <param name="lockName"></param>
        /// <param name="lock"></param>
        /// <returns></returns>
        private bool TryAddSessionMonitor(string lockName, LockItem @lock)
        {
            if (@lock.HasSessionMonitor() && @lock.HasCallback())
            {
                BackgroundTask monitorThread = LockSessionMonitorChecker(lockName, @lock);
                monitorThread.Start();
                return this.SessionMonitors.TryAdd(lockName, monitorThread);
            }

            return false;
        }

        /// <summary>
        /// If the tracked session monitors have one present with the provided name, 
        /// it is removed from tracking and the active background thread is canceled.
        /// </summary>
        /// <param name="monitorName"></param>
        /// <returns>Returns true if the session monitor was removed from tracking or if it wasn't present, returns
        /// false if the monitor couldn't be removed from tracking</returns>
        private async Task<bool> RemoveKillSessionMonitorAsync(string monitorName)
        {
            if (this.SessionMonitors.ContainsKey(monitorName))
            {
                if (this.SessionMonitors.TryRemove(monitorName, out BackgroundTask monitor))
                {
                    monitor.Cancel();

                    try
                    {
                        await monitor.WaitAsync();
                    }
                    catch (OperationCanceledException)
                    {
                        LockClientUtils.Logger.Warn("Caught OperationCanceledException waiting for session monitor thread to exit, ignoring.");                     
                    }

                    return true;
                }
                else
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// This is called by TryAddSessionMonitor when a new lock is acquired
        /// </summary>
        /// <param name="monitorName"></param>
        /// <param name="lock"></param>
        /// <returns></returns>
        private BackgroundTask LockSessionMonitorChecker(string monitorName, LockItem @lock)
        {
            CancellationTokenSource cts = new CancellationTokenSource();

            Action action = () =>
            {
                while (true)
                {
                    try
                    {
                        cts.Token.ThrowIfCancellationRequested();

                        long millisUntilDangerZone = @lock.MillisecondsUntilDangerZoneEntered();

                        if (millisUntilDangerZone > 0)
                        {
                            foreach (int time in LockClientUtils.Decompose64BitInt(millisUntilDangerZone))
                            {
                                Thread.Sleep(time);
                                cts.Token.ThrowIfCancellationRequested();
                            }
                        }
                        else
                        {
                            Debug.WriteLine($"{LockClientUtils.MillisecondTime()} : Lock for {@lock.PartitionKey} entered the danger zone.");
                            LockClientUtils.Logger.Trace($"Lock for {@lock.PartitionKey} entered the danger zone.");
                            Task t = @lock.RunSessionMonitor(); // This will check for the existence of the session monitor and run the callback
                            SessionMonitors.TryRemove(monitorName, out BackgroundTask temp);
                            t.Wait(); // Ok to run this sync because the action will be run in a separate task
                            return;
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }
                }
            };

            return new BackgroundTask(action, cts);
        }

        #endregion

        #region Private Classes

        private class LockItemFactory : ILockItemFactory
        {
            #region Private Fields

            private bool DeleteOnRelease;
            private AmazonDynamoDBLockClient Client;

            #endregion

            public LockItemFactory(bool deleteOnRelease, AmazonDynamoDBLockClient client)
            {
                this.DeleteOnRelease = deleteOnRelease;
                this.Client = client;
            }

            public LockItem Create(Dictionary<string, AttributeValue> item)
            {
                string key = item[this.Client.PartitionKeyName].S;

                GetLockOptions options = new GetLockOptions(key)
                {
                    DeleteLockOnRelease = this.DeleteOnRelease,
                };

                if (item.ContainsKey(this.Client.SortKeyName))
                {
                    options.SortKey = item[this.Client.SortKeyName].S;
                }

                LockItem lockItem = this.Client.CreateLockItem(options, item);
                return lockItem;
            }
        }

        #endregion
    }
}
