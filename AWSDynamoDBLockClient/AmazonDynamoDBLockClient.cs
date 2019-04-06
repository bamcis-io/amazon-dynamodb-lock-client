using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using BAMCIS.AWSDynamoDBLockClient.Model;
using BAMCIS.AWSDynamoDBLockClient.Util;
using BAMCIS.Util;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BAMCIS.AWSDynamoDBLockClient
{
    public class AmazonDynamoDBLockClient
    {
        #region Private Fields

        private static ISet<TableStatus> AvailableStatuses;

        protected IAmazonDynamoDB DynamoDB;
        protected string TableName;
        private string PartitionKeyName;
        private Optional<string> SortKeyName;
        private long LeaseDurationInMilliseconds;
        private long HeartbeatPeriodInMilliseconds;
        private bool HoldLockOnServiceUnavailable;
        private string OwnerName;
        private ConcurrentDictionary<string, LockItem> Locks;
        private ConcurrentDictionary<string, BackgroundTask> SessionMonitors;
        private Optional<BackgroundTask> BackgroundThread;
        // TODO: Probably not needed
        //private Func<string, TaskFactory> NamedThreadCreator;
        private volatile bool ShuttingDown = false;
        private volatile object SyncObject = new object();

        /*
        * Used as a default buffer for how long extra to wait when querying DynamoDB for a lock in acquireLock (can be overriden by
        * specifying a timeout when calling acquireLock)
        */
        private static readonly long DEFAULT_BUFFER_MS = 1000;

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
            
            // TODO: Probably not needed
            //LockClientUtils.RequireNonNull(amazonDynamoDBLockClientOptions.NamedThreadCreator, "Named thread creator cannot be null.");

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

            // TODO: Probably not needed
            //this.NamedThreadCreator = amazonDynamoDBLockClientOptions.NamedThreadCreator;

            if (amazonDynamoDBLockClientOptions.CreateHeartbeatBackgroundThread)
            {
                CancellationTokenSource CTS = new CancellationTokenSource();
          
                if (this.LeaseDurationInMilliseconds < 2 * this.HeartbeatPeriodInMilliseconds)
                {
                    throw new ArgumentException("Heartbeat period must be no more than half the length of the Lease Duration, or locks might expire due to the heartbeat thread taking too long to update them (recommendation is to make it much greater, for example 4+ times greater.");
                }

                this.BackgroundThread = Optional<BackgroundTask>.Of(this.StartBackgroundTask());
            }
            else
            {
                this.BackgroundThread = Optional<BackgroundTask>.Empty;
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
                DescribeTableResponse Result = await this.DynamoDB.DescribeTableAsync(new DescribeTableRequest() { TableName = this.TableName });
                return AvailableStatuses.Contains(Result.Table.TableStatus);
            }
            catch (ResourceNotFoundException e)
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
                bool Exists = await this.LockTableExistsAsync();

                if (!Exists)
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
        public static async Task<CreateTableResponse> CreateLockTableInDynamoDBAsync(CreateDynamoDBTableOptions createDynamoDBTableOptions)
        {
            LockClientUtils.RequireNonNull(createDynamoDBTableOptions, "DynamoDB Create Table Options cannot be null.", "createDynamoDBTableOptions");
            LockClientUtils.RequireNonNull(createDynamoDBTableOptions.DynamoDBClient, "DynamoDB client object cannot be null.");
            LockClientUtils.RequireNonNullOrEmpty(createDynamoDBTableOptions.TableName, "Table name cannot be null or empty.");
            LockClientUtils.RequireNonNullOrEmpty(createDynamoDBTableOptions.PartitionKeyName, "Partition Key Name cannot be null or empty.");
            LockClientUtils.RequireNonNull(createDynamoDBTableOptions.SortKeyName, "Sort Key Name cannot be null.");

            if (createDynamoDBTableOptions.BillingMode == BillingMode.PROVISIONED)
            {
                LockClientUtils.RequireNonNull(createDynamoDBTableOptions.ProvisionedThroughput, "Provisioned throughput cannot be null when billing mode is PROVISIONED.");
                LockClientUtils.RequireNonNull(createDynamoDBTableOptions.ProvisionedThroughput.Value, "Provisioned throughput cannot be null when billing mode is PROVISIONED.");
            }

            KeySchemaElement PartitionKeyElement = new KeySchemaElement(createDynamoDBTableOptions.PartitionKeyName, KeyType.HASH);

            List<KeySchemaElement> KeySchema = new List<KeySchemaElement>() { PartitionKeyElement };

            List<AttributeDefinition> AttributeDefinitions = new List<AttributeDefinition>() {
                new AttributeDefinition() {
                    AttributeName = createDynamoDBTableOptions.PartitionKeyName,
                    AttributeType = ScalarAttributeType.S
                }
            };

            if (createDynamoDBTableOptions.SortKeyName.IsPresent())
            {
                KeySchemaElement SortKeyElement = new KeySchemaElement(createDynamoDBTableOptions.SortKeyName.Value, KeyType.RANGE);
                KeySchema.Add(SortKeyElement);
                AttributeDefinitions.Add(new AttributeDefinition()
                {
                    AttributeName = createDynamoDBTableOptions.SortKeyName.Value,
                    AttributeType = ScalarAttributeType.S
                });
            }

            CreateTableRequest CreateTableRequest = new CreateTableRequest()
            {
                TableName = createDynamoDBTableOptions.TableName,
                KeySchema = KeySchema,
                AttributeDefinitions = AttributeDefinitions,
                BillingMode = createDynamoDBTableOptions.BillingMode
            };

            if (CreateTableRequest.BillingMode == BillingMode.PROVISIONED)
            {
                CreateTableRequest.ProvisionedThroughput = createDynamoDBTableOptions.ProvisionedThroughput.Value;
            }

            CreateTableResponse Response = await createDynamoDBTableOptions.DynamoDBClient.CreateTableAsync(CreateTableRequest);
            return Response;
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
        public async Task<Optional<LockItem>> GetLockAsync(string key, Optional<string> sortKey)
        {
            if (sortKey == null)
            {
                sortKey = Optional<string>.Empty;
            }

            if (this.Locks.TryGetValue($"{key}{sortKey.OrElse("")}", out LockItem LocalLock))
            {
                return Optional<LockItem>.Of(LocalLock);
            }

            Optional<LockItem> LockItem = await this.GetLockFromDynamoDBAsync(
                (new GetLockOptions.GetLockOptionsBuilder(key)).WithSortKey(sortKey.OrElse(null)).WithDeleteLockOnRelease(false).Build()
            );

            if (LockItem.IsPresent())
            {
                if (LockItem.Value.IsReleased())
                {
                    // Return empty if a lock was released but still left in the table
                    return Optional<LockItem>.Empty;
                }
                else
                {

                    // Clear out the record version number so that the caller cannot accidentally 
                    // perform updates on this lock (since the caller has not acquired the lock)
                    LockItem.Value.UpdateRecordVersionNumber("", 0, LockItem.Value.LeaseDuration);
                }
            }

            return LockItem;
        }

        /// <summary>
        /// Retrieves the lock item from DynamoDB. Note that this will return a
        /// LockItem even if it was released -- do NOT use this method if your goal
        /// is to acquire a lock for doing work.
        /// </summary>
        /// <param name="options">The options such as the key, etc.</param>
        /// <returns>The LockItemm, or absent if it is not present. Not that the
        /// item can exist in the table even if it is released, as noted by IsReleased()</returns>
        public async Task<Optional<LockItem>> GetLockFromDynamoDBAsync(GetLockOptions options)
        {
            LockClientUtils.RequireNonNull(options, "AcquireLockOptions cannot be null.", "options");
            LockClientUtils.RequireNonNullOrEmpty(options.PartitionKey, "Cannot lookup null or empty key.");

            GetItemResponse Result = await this.ReadFromDynamoDBAsync(options.PartitionKey, options.SortKey);
            Dictionary<string, AttributeValue> Item = Result.Item;

            if (Item == null || !Item.Any())
            {
                return Optional<LockItem>.Empty;
            }

            return Optional<LockItem>.Of(this.CreateLockItem(options, Item));
        }

        /// <summary>
        /// Retrieves all lock items from DynamoDB
        /// 
        /// Note that this may return a lock item even if it was released
        /// </summary>
        /// <param name="deleteOnRelease"></param>
        /// <returns></returns>
        public IEnumerable<LockItem> GetAllLocksFromDynamoDB(bool deleteOnRelease)
        {
            ScanRequest ScanRequest = new ScanRequest()
            {
                TableName = this.TableName
            };

            LockItemPaginatedScanIterator Iterator = new LockItemPaginatedScanIterator(this.DynamoDB, ScanRequest, x =>
            {
                string Key = x[this.PartitionKeyName].S;
                GetLockOptions.GetLockOptionsBuilder Options = GetLockOptions.Builder(Key).WithDeleteLockOnRelease(deleteOnRelease);

                Options = this.SortKeyName.Map(y => x[y]).Map(y => y.S).Map(y => Options.WithSortKey(y)).OrElse(Options);

                LockItem LockItem = this.CreateLockItem(Options.Build(), x);
                return LockItem;
            });

            while (Iterator.MoveNext())
            {
                yield return Iterator.Current;
            }
        }

        /// <summary>
        /// Attempts to acquire a lock until it either acquires the lock, or a specified AdditionalTimeToWaitForLock is
        /// reached. This method will poll DynamoDB based on the RefreshPeriod. If it does not see the lock in DynamoDB, it
        /// will immediately return the lock to the caller. If it does see the lock, it will note the lease expiration on the lock. If
        /// the lock is deemed stale, (that is, there is no heartbeat on it for at least the length of its lease duration) then this
        /// will acquire and return it.Otherwise, if it waits for as long as AdditionalTimeToWaitForLockvwithout acquiring the
        /// lock, then it will throw a LockNotGrantedException.
        ///
        /// Note that this method will wait for at least as long as the LeaseDuration} in order to acquire a lock that already
        /// exists.If the lock is not acquired in that time, it will wait an additional amount of time specified in
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

            string Key = options.PartitionKey;
            Optional<string> SortKey = options.SortKey;

            if (options.AdditionalAttributes.ContainsKey(this.PartitionKeyName) ||
                options.AdditionalAttributes.ContainsKey(OWNER_NAME) ||
                options.AdditionalAttributes.ContainsKey(LEASE_DURATION) ||
                options.AdditionalAttributes.ContainsKey(RECORD_VERSION_NUMBER) ||
                options.AdditionalAttributes.ContainsKey(DATA) ||
                this.SortKeyName.IsPresent() && options.AdditionalAttributes.ContainsKey(this.SortKeyName.Value))
            {
                throw new ArgumentException($"Additional attribute cannot be one of the following types: {this.PartitionKeyName}, {OWNER_NAME}, {LEASE_DURATION}, {RECORD_VERSION_NUMBER}, {DATA}");
            }

            long MillisecondsToWait = DEFAULT_BUFFER_MS;

            if (options.AdditionalTimeToWaitForLock > 0)
            {
                LockClientUtils.RequireNonNull(options.TimeUnit, "TimeUnit must not be null if AdditionalTimeToWaitForLock is greater than 0.");
                MillisecondsToWait = options.TimeUnit.ToMilliseconds(options.AdditionalTimeToWaitForLock);
            }

            long RefreshPeriodInMilliseconds = DEFAULT_BUFFER_MS;

            if (options.RefreshPeriod > 0)
            {
                LockClientUtils.RequireNonNull(options.TimeUnit, "TimeUnit must not be null if RefreshPeriod is non-null.");
                RefreshPeriodInMilliseconds = options.TimeUnit.ToMilliseconds(options.RefreshPeriod);
            }

            bool DeleteLockOnRelease = options.DeleteLockOnRelease;
            bool ReplaceData = options.ReplaceData;

            Optional<SessionMonitor> SessionMonitor = options.SessionMonitor;

            if (SessionMonitor.IsPresent())
            {
                SessionMonitorArgsValidate(SessionMonitor.Value.SafeTimeWithoutHeartbeatMillis, this.HeartbeatPeriodInMilliseconds, this.LeaseDurationInMilliseconds);
            }

            long CurrentTimeMillis = LockClientUtils.MillisecondTime();

            // This is the lock we are trying to acquire. If it already exists, then we can try
            // to steal it if it does not get updated after its LEASE_DURATION expires.
            LockItem LockTryingToBeAcquired = null;
            bool AlreadySleptOnceForOneLeasePeriod = false;

            GetLockOptions GetLockOptions = new GetLockOptions.GetLockOptionsBuilder(Key)
                .WithSortKey(SortKey.OrElse(null))
                .WithDeleteLockOnRelease(DeleteLockOnRelease)
                .Build();

            while (true)
            {
                try
                {
                    try
                    {
                        Optional<LockItem> ExistingLock = await this.GetLockFromDynamoDBAsync(GetLockOptions);

                        if (options.AcquireOnlyIfLockAlreadyExists && !ExistingLock.IsPresent())
                        {
                            throw new LockNotGrantedException("Lock does not exist.");
                        }

                        if (options.ShouldSkipBlockingWait && ExistingLock.IsPresent() && !ExistingLock.Value.IsExpired())
                        {
                            // The lock is being held by some one and is still not expired. And the caller explicitly said not to
                            // perform a blocking wait. We will throw back a lock not granted exception, so that the caller
                            // can retry if needed.
                            throw new LockCurrentlyUnavailableException("The lock being requested is being held by another client.");
                        }

                        Optional<MemoryStream> NewLockData = Optional<MemoryStream>.Empty;

                        if (ReplaceData)
                        {
                            NewLockData = options.Data;
                        }
                        else if (ExistingLock.IsPresent())
                        {
                            NewLockData = ExistingLock.Value.Data;
                        }

                        if (!NewLockData.IsPresent())
                        {
                            NewLockData = options.Data; // If there is no existing data, we write the input data to the lock.
                        }

                        Dictionary<string, AttributeValue> Item = new Dictionary<string, AttributeValue>(options.AdditionalAttributes.ToDictionary(x => x.Key, y => y.Value));
                        Item.Add(this.PartitionKeyName, new AttributeValue() { S = Key });
                        Item.Add(OWNER_NAME, new AttributeValue() { S = this.OwnerName });
                        Item.Add(LEASE_DURATION, new AttributeValue() { S = this.LeaseDurationInMilliseconds.ToString() });
                        string RecordVersionNumber = Guid.NewGuid().ToString();
                        Item.Add(RECORD_VERSION_NUMBER, new AttributeValue() { S = RecordVersionNumber });
                        SortKeyName.IfPresent(SortKeyName => Item.Add(SortKeyName, new AttributeValue() { S = SortKey.Value }));
                        NewLockData.IfPresent(ByteBuffer => Item.Add(DATA, new AttributeValue() { B = ByteBuffer }));

                        // If the existing lock does not exist or exists and is released
                        if (!ExistingLock.IsPresent() && !options.AcquireOnlyIfLockAlreadyExists)
                        {
                            return await UpsertAndMonitorNewLockAsync(options, Key, SortKey, DeleteLockOnRelease, SessionMonitor, NewLockData, Item, RecordVersionNumber);
                        }
                        else if (ExistingLock.IsPresent() && ExistingLock.Value.IsReleased())
                        {
                            return await UpsertAndMonitorReleasedLockAsync(options, Key, SortKey, DeleteLockOnRelease, SessionMonitor, ExistingLock, NewLockData, Item, RecordVersionNumber);
                        }

                        // We know that we didn't enter the if block above because it returns at the end and
                        // we also know that the ExistingLock.IsPresent() is true
                        if (LockTryingToBeAcquired == null)
                        {
                            // This branch of logic only happens once, in the first iteration of the while loop
                            // LockTryingToBeAcquired only ever gets set to non-null values after this point,
                            // so it is impossible to get in this.
                            // Someone else has the lok and they have the lock for LEASE_DURATION time. At this point,
                            // we need to wait at least LEASE_DURATION milliseconds before we can try to acquire the lock
                            LockTryingToBeAcquired = ExistingLock.Value;

                            if (!AlreadySleptOnceForOneLeasePeriod)
                            {
                                AlreadySleptOnceForOneLeasePeriod = true;
                                MillisecondsToWait += ExistingLock.Value.LeaseDuration;
                            }
                        }
                        else
                        {
                            if (LockTryingToBeAcquired.RecordVersionNumber == ExistingLock.Value.RecordVersionNumber)
                            {
                                // If the version numbers match, then we can acquire the lok, assuming it has already expired
                                if (LockTryingToBeAcquired.IsExpired())
                                {
                                    return await UpsertAndMonitorExpiredLockAsync(options, Key, SortKey, DeleteLockOnRelease, SessionMonitor, ExistingLock, NewLockData, Item, RecordVersionNumber);
                                }
                            }
                            else
                            {
                                // If the version number changed since we last queried the lok, then we need to update
                                // LockTryingToBeAcquired as the lock has been refreshed sine we last checked
                                LockTryingToBeAcquired = ExistingLock.Value;
                            }
                        }
                    }
                    catch (ConditionalCheckFailedException e)
                    {
                        // Someone else acquired the lok while we tried to do so, so we throw an exception
                        throw new LockNotGrantedException("Could not acquire lock because someone else acquired it.", e);
                    }
                    catch (ProvisionedThroughputExceededException e)
                    {
                        // Request exceeded maximum allowed provisioned throughput for the table
                        // or one or more global secondary indexes.
                        throw new LockNotGrantedException("Could not acquire lock because provisioned throughput for the table exceeded.", e);
                    }
                    catch (AmazonClientException e)
                    {
                        // This indicates that we were unable to successfully connect and make a service call to DDB. Often
                        // indicative of a network failure, such as a socket timeout. We retry if still within the time we
                        // can wait to acquire the lock
                    }
                }
                catch (LockNotGrantedException e)
                {
                    if (LockClientUtils.MillisecondTime() - CurrentTimeMillis > MillisecondsToWait)
                    {
                        throw e;
                    }
                }

                if (LockClientUtils.MillisecondTime() - CurrentTimeMillis > MillisecondsToWait)
                {
                    throw new LockNotGrantedException($"Didn't acquire lock after sleeping for {(LockClientUtils.MillisecondTime() - CurrentTimeMillis).ToString()} milliseonds.");
                }

                foreach (int Time in LockClientUtils.Decompose64BitInt(RefreshPeriodInMilliseconds))
                {
                    Thread.Sleep(Time);
                }
            }
        }

        /// <summary>
        /// Attempts to acquire a lock. If successful, returns the lock. Otherwise,
        /// returns Optional.Empty. For more details on behavior, please see AcquireLock().
        /// </summary>
        /// <param name="options"></param>
        /// <returns></returns>
        public async Task<Optional<LockItem>> TryAcquireLockAsync(AcquireLockOptions options)
        {
            try
            {
                return Optional<LockItem>.Of(await this.AcquireLockAsync(options));
            }
            catch (LockNotGrantedException)
            {
                return Optional<LockItem>.Empty;
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
            return this.ReleaseLock(ReleaseLockOptions.Builder(lockItem).WithDeleteLock(lockItem.DeleteLockItemOnclose).Build());
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

            LockItem LockItem = options.LockItem;
            bool DeleteLock = options.DeleteLock;
            bool BestEffort = options.BestEffort;
            Optional<MemoryStream> Data = options.Data;

            if (!LockItem.OwnerName.Equals(this.OwnerName))
            {
                return false;
            }

            lock (SyncObject)
            {
                try
                {
                    // Always remove the heartbeat for the lock. The
                    // caller's intention is to release the lock. Stopping the
                    // heartbeat alone will do that regardless of whether the Dynamo
                    // write succeeds or fails.
                    this.Locks.TryRemove(LockItem.GetUniqueIdentifier(), out LockItem Temp);

                    //set up expression stuff for DeleteItem or UpdateItem
                    //basically any changes require:
                    //1. I own the lock
                    //2. I know the current version number
                    //3. The lock already exists (UpdateItem API can cause a new item to be created if you do not condition the primary keys with attribute_exists)
                    string ConditionalExpression;
                    Dictionary<string, AttributeValue> ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                    {
                        { RVN_VALUE_EXPRESSION_VARIABLE, new AttributeValue() { S = LockItem.RecordVersionNumber } },
                        { OWNER_NAME_VALUE_EXPRESSION_VARIABLE, new AttributeValue() { S = LockItem.OwnerName } }
                    };

                    Dictionary<string, string> ExpressionAttributeNames = new Dictionary<string, string>()
                    {
                        { PK_PATH_EXPRESSION_VARIABLE, this.PartitionKeyName },
                        { OWNER_NAME_PATH_EXPRESSION_VARIABLE, OWNER_NAME},
                        { RVN_PATH_EXPRESSION_VARIABLE, RECORD_VERSION_NUMBER}
                    };

                    if (this.SortKeyName.IsPresent())
                    {
                        ConditionalExpression = PK_EXISTS_AND_SK_EXISTS_AND_OWNER_NAME_SAME_AND_RVN_SAME_CONDITION;
                        ExpressionAttributeNames.Add(SK_PATH_EXPRESSION_VARIABLE, this.SortKeyName.Value);
                    }
                    else
                    {
                        ConditionalExpression = PK_EXISTS_AND_OWNER_NAME_SAME_AND_RVN_SAME_CONDITION;
                    }

                    Dictionary<string, AttributeValue> Key = GetItemKeys(LockItem);

                    if (DeleteLock)
                    {
                        DeleteItemRequest DeleteItemRequest = new DeleteItemRequest()
                        {
                            TableName = this.TableName,
                            Key = Key,
                            ConditionExpression = ConditionalExpression,
                            ExpressionAttributeNames = ExpressionAttributeNames,
                            ExpressionAttributeValues = ExpressionAttributeValues
                        };

                        this.DynamoDB.DeleteItemAsync(DeleteItemRequest);
                    }
                    else
                    {
                        string UpdateExpression;
                        ExpressionAttributeNames.Add(IS_RELEASED_PATH_EXPRESSION_VARIABLE, IS_RELEASED);
                        ExpressionAttributeValues.Add(IS_RELEASED_VALUE_EXPRESSION_VARIABLE, IS_RELEASED_ATTRIBUTE_VALUE);

                        if (Data.IsPresent())
                        {
                            UpdateExpression = UPDATE_IS_RELEASED_AND_DATA;
                            ExpressionAttributeNames.Add(DATA_PATH_EXPRESSION_VARIABLE, DATA);
                            ExpressionAttributeValues.Add(DATA_VALUE_EXPRESSION_VARIABLE, new AttributeValue() { B = Data.Value });
                        }
                        else
                        {
                            UpdateExpression = UPDATE_IS_RELEASED;
                        }

                        UpdateItemRequest UpdateItemRequest = new UpdateItemRequest()
                        {
                            TableName = this.TableName,
                            Key = Key,
                            UpdateExpression = UpdateExpression,
                            ConditionExpression = ConditionalExpression,
                            ExpressionAttributeNames = ExpressionAttributeNames,
                            ExpressionAttributeValues = ExpressionAttributeValues
                        };

                        this.DynamoDB.UpdateItemAsync(UpdateItemRequest);
                    }
                }
                catch (ConditionalCheckFailedException e)
                {
                    //logger.debug("Someone else acquired the lock before you asked to release it", conditionalCheckFailedException);
                    return false;
                }
                catch (AmazonClientException e)
                {
                    if (BestEffort)
                    {
                        //logger.warn("Ignore SdkClientException and continue to clean up", sdkClientException);
                    }
                    else
                    {
                        throw e;
                    }
                }

                // Only remove the session monitor if no exception thrown above.
                // While moving the heartbeat removal before the DynamoDB call
                // should not cause existing clients problems, there
                // may be existing clients that depend on the monitor firing if they
                // get exceptions from this method.
                this.RemoveKillSessionMonitor(LockItem.GetUniqueIdentifier());

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
        public void SendHeartbeat(LockItem lockItem)
        {
            this.SendHeartbeat(SendHeartbeatOptions.Builder(lockItem).Build());
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
        public void SendHeartbeat(SendHeartbeatOptions options)
        {
            LockClientUtils.RequireNonNull(options, "Options is required.", "options");
            LockClientUtils.RequireNonNull(options.LockItem, "Cannot send heartbeat for null lock.");

            bool DeleteData = options.DeleteData;

            if (DeleteData && options.Data.IsPresent())
            {
                throw new ArgumentException("Data must not be present if DeleteData is true.");
            }

            long LeaseDurationToEnsureInMilliseconds = this.LeaseDurationInMilliseconds;
            if (options.LeaseDurationToEnsure > 0)
            {
                LockClientUtils.RequireNonNull(options.TimeUnit, "TimeUnit must not be null if LeaseDurationToEnsure is not null.");
                LeaseDurationToEnsureInMilliseconds = options.TimeUnit.ToMilliseconds(options.LeaseDurationToEnsure);
            }

            LockItem LockItem = options.LockItem;
            if (LockItem.IsExpired() || !LockItem.OwnerName.Equals(this.OwnerName) || LockItem.IsReleased())
            {
                this.Locks.TryRemove(LockItem.GetUniqueIdentifier(), out LockItem Temp);
                throw new LockNotGrantedException("Cannot send heartbeat because lock is not granted.");
            }

            lock (SyncObject)
            {
                // Set up condition for UpdateItem.Basically any changes require:
                //1. I own the lock
                //2. I know the current version number
                //3. The lock already exists (UpdateItem API can cause a new item to be created if you do not condition the primary keys with attribute_exists)
                Dictionary<string, AttributeValue> ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                {
                    { RVN_VALUE_EXPRESSION_VARIABLE, new AttributeValue() { S = LockItem.RecordVersionNumber } },
                    { OWNER_NAME_VALUE_EXPRESSION_VARIABLE, new AttributeValue() { S = LockItem.OwnerName } }
                };

                Dictionary<string, string> ExpressionAttributeNames = new Dictionary<string, string>()
                {
                    { PK_PATH_EXPRESSION_VARIABLE, this.PartitionKeyName },
                    { LEASE_DURATION_PATH_VALUE_EXPRESSION_VARIABLE, LEASE_DURATION },
                    { RVN_PATH_EXPRESSION_VARIABLE, RECORD_VERSION_NUMBER },
                    { OWNER_NAME_PATH_EXPRESSION_VARIABLE, OWNER_NAME }
                };

                string ConditionalExpression;

                if (this.SortKeyName.IsPresent())
                {
                    ConditionalExpression = PK_EXISTS_AND_SK_EXISTS_AND_OWNER_NAME_SAME_AND_RVN_SAME_CONDITION;
                    ExpressionAttributeNames.Add(SK_PATH_EXPRESSION_VARIABLE, this.SortKeyName.Value);
                }
                else
                {
                    ConditionalExpression = PK_EXISTS_AND_OWNER_NAME_SAME_AND_RVN_SAME_CONDITION;
                }

                string RecordVersionNumber = Guid.NewGuid().ToString();

                ExpressionAttributeValues.Add(NEW_RVN_VALUE_EXPRESSION_VARIABLE, new AttributeValue() { S = RecordVersionNumber });
                ExpressionAttributeValues.Add(LEASE_DURATION_VALUE_EXPRESSION_VARIABLE, new AttributeValue() { S = this.LeaseDurationInMilliseconds.ToString() });

                //Set up update expression for UpdateItem.
                string UpdateExpression;
                if (DeleteData)
                {
                    ExpressionAttributeNames.Add(DATA_PATH_EXPRESSION_VARIABLE, DATA);
                    UpdateExpression = UPDATE_LEASE_DURATION_AND_RVN_AND_REMOVE_DATA;
                }
                else if (options.Data.IsPresent())
                {
                    ExpressionAttributeNames.Add(DATA_PATH_EXPRESSION_VARIABLE, DATA);
                    ExpressionAttributeValues.Add(DATA_VALUE_EXPRESSION_VARIABLE, new AttributeValue() { B = options.Data.Value });
                    UpdateExpression = UPDATE_LEASE_DURATION_AND_RVN_AND_DATA;
                }
                else
                {
                    UpdateExpression = UPDATE_LEASE_DURATION_AND_RVN;
                }

                UpdateItemRequest UpdateItemRequest = new UpdateItemRequest()
                {
                    TableName = this.TableName,
                    Key = this.GetItemKeys(LockItem),
                    ConditionExpression = ConditionalExpression,
                    UpdateExpression = UpdateExpression,
                    ExpressionAttributeNames = ExpressionAttributeNames,
                    ExpressionAttributeValues = ExpressionAttributeValues
                };

                try
                {
                    long LastUpdateOfLock = LockClientUtils.MillisecondTime();
                    this.DynamoDB.UpdateItemAsync(UpdateItemRequest);
                    LockItem.UpdateRecordVersionNumber(RecordVersionNumber, LastUpdateOfLock, LeaseDurationToEnsureInMilliseconds);
                }
                catch (ConditionalCheckFailedException e)
                {
                    //logger.debug("Someone else acquired the lock, so we will stop heartbeating it", conditionalCheckFailedException);
                    this.Locks.TryRemove(LockItem.GetUniqueIdentifier(), out LockItem Temp);
                    throw new LockNotGrantedException("Someone else acquired the lock, so we will stop heartbeating it.", e);
                }
                catch (AmazonServiceException e)
                {
                    if (this.HoldLockOnServiceUnavailable && e.StatusCode == HttpStatusCode.ServiceUnavailable)
                    {
                        // When DynamoDB service is unavailable, other threads may get the same exception and no thread may have the lock.
                        // For systems which should always hold a lock on an item and it is okay for multiple threads to hold the lock,
                        // the lookUpTime of local state can be updated to make it believe that it still has the lock.
                        //logger.info("DynamoDB Service Unavailable. Holding the lock.");
                        LockItem.UpdateLookupTime(LockClientUtils.MillisecondTime());
                    }
                    else
                    {
                        throw e;
                    }
                }
            }
        }

        /// <summary>
        /// Loops forever, sending hearbeats for all the locks this thread needs to keep track of.
        /// </summary>
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

                    long TimeWorkBegins = LockClientUtils.MillisecondTime();
                    Dictionary<string, LockItem> WorkingCopyOfLocks = new Dictionary<string, LockItem>(this.Locks);

                    foreach (KeyValuePair<string, LockItem> LockEntry in WorkingCopyOfLocks)
                    {
                        try
                        {
                            this.SendHeartbeat(LockEntry.Value);
                        }
                        catch (LockNotGrantedException e)
                        {
                            //logger.debug("Heartbeat failed for " + lockEntry, x);
                        }
                        catch (Exception e)
                        {
                            //logger.warn("Exception sending heartbeat for " + lockEntry, x);
                        }
                    }

                    long TimeElapsed = LockClientUtils.MillisecondTime() - TimeWorkBegins;

                    if (this.ShuttingDown)
                    {
                        throw new OperationCanceledException(); // sometimes libraries wrap interrupted and other exceptions
                    }

                    /* If we want to hearbeat every 9 seconds, and it took 3 seconds to send the heartbeats, we only sleep 6 seconds */

                    foreach (int Time in LockClientUtils.Decompose64BitInt(Math.Max(this.HeartbeatPeriodInMilliseconds - TimeElapsed, 0)))
                    {
                        Thread.Sleep(Time);
                    }
                }
                catch (OperationCanceledException e)
                {
                    //logger.info("Heartbeat thread recieved interrupt, exiting run() (possibly exiting thread)", e);
                    return;
                }
                catch (Exception e)
                {
                    //logger.warn("Exception sending heartbeat", x);
                }
            }
        }

        /// <summary>
        /// Releases all of the locks by calling ReleaseAllLocks()
        /// </summary>
        public void Close()
        {
            // release the locks before interrupting the heartbeat thread to avoid partially updated/stale locks
            IEnumerable<Exception> Results = this.ReleaseAllLocks();

            if (Results.Any())
            {
                throw new AggregateException(Results);
            }

            if (this.BackgroundThread.IsPresent())
            {
                this.ShuttingDown = true;
                this.BackgroundThread.Value.Cancel();

                try
                {
                    this.BackgroundThread.Value.Task.Wait();
                }
                catch (OperationCanceledException e)
                {
                    //logger.warn("Caught InterruptedException waiting for background thread to exit, interrupting current thread");
                    //Thread.CurrentThread.Interrupt();
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
            Optional<string> sortKey,
            bool deleteLockOnRelease,
            Optional<SessionMonitor> sessionMonitor,
            Optional<LockItem> existingLock,
            Optional<MemoryStream> newLockData,
            Dictionary<string, AttributeValue> item,
            string recordVersionNumber)
        {
            string ConditionalExpression;
            Dictionary<string, AttributeValue> ExpressionAttributeValues = new Dictionary<string, AttributeValue>();
            bool UpdateExistingLockRecord = options.UpdateExistingLockRecord;

            ExpressionAttributeValues.Add(RVN_VALUE_EXPRESSION_VARIABLE, new AttributeValue() { S = existingLock.Value.RecordVersionNumber });

            Dictionary<string, string> ExpressionAttributeNames = new Dictionary<string, string>();
            ExpressionAttributeNames.Add(PK_PATH_EXPRESSION_VARIABLE, this.PartitionKeyName);
            ExpressionAttributeNames.Add(RVN_PATH_EXPRESSION_VARIABLE, RECORD_VERSION_NUMBER);

            if (this.SortKeyName.IsPresent())
            {
                // We do not check the owner here because the lock is expired and it is OK to overwrite the owner
                ConditionalExpression = PK_EXISTS_AND_SK_EXISTS_AND_RVN_IS_THE_SAME_CONDITION;
                ExpressionAttributeNames.Add(SK_PATH_EXPRESSION_VARIABLE, this.SortKeyName.Value);
            }
            else
            {
                ConditionalExpression = PK_EXISTS_AND_RVN_IS_THE_SAME_CONDITION;
            }

            if (UpdateExistingLockRecord)
            {
                item.Remove(this.PartitionKeyName);

                if (this.SortKeyName.IsPresent())
                {
                    item.Remove(SortKeyName.Value);
                }

                string UpdateExpression = GetUpdateExpressionAndUpdateNameValueMaps(item, ExpressionAttributeNames, ExpressionAttributeValues);

                UpdateItemRequest UpdateItemRequest = new UpdateItemRequest()
                {
                    TableName = this.TableName,
                    Key = this.GetItemKeys(existingLock.Value),
                    UpdateExpression = UpdateExpression,
                    ExpressionAttributeNames = ExpressionAttributeNames,
                    ExpressionAttributeValues = ExpressionAttributeValues,
                    ConditionExpression = ConditionalExpression
                };

                return await UpdateItemAndStartSessionMonitorAsync(options, key, sortKey, deleteLockOnRelease, sessionMonitor, newLockData, recordVersionNumber, UpdateItemRequest);
            }
            else
            {
                PutItemRequest PutItemRequest = new PutItemRequest()
                {
                    Item = item,
                    TableName = this.TableName,
                    ConditionExpression = ConditionalExpression,
                    ExpressionAttributeNames = ExpressionAttributeNames,
                    ExpressionAttributeValues = ExpressionAttributeValues
                };

                return await PutLockItemAndStartSessionMonitorAsync(options, key, sortKey, deleteLockOnRelease, sessionMonitor, newLockData, recordVersionNumber, PutItemRequest);
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
            Optional<string> sortKey,
            bool deleteLockOnRelease,
            Optional<SessionMonitor> sessionMonitor,
            Optional<LockItem> existingLock,
            Optional<MemoryStream> newLockData,
            Dictionary<String, AttributeValue> item,
            string recordVersionNumber
        )
        {

            string ConditionalExpression;
            bool UpdateExistingLockRecord = options.UpdateExistingLockRecord;
            bool ConsistentLockData = options.AcquireReleasedLocksConsistently;

            Dictionary<string, string> ExpressionAttributeNames = new Dictionary<string, string>();
            Dictionary<string, AttributeValue> ExpressionAttributeValues = new Dictionary<string, AttributeValue>();

            if (ConsistentLockData)
            {
                ExpressionAttributeValues.Add(RVN_VALUE_EXPRESSION_VARIABLE, new AttributeValue() { S = existingLock.Value.RecordVersionNumber });
                ExpressionAttributeNames.Add(RVN_PATH_EXPRESSION_VARIABLE, RECORD_VERSION_NUMBER);
            }

            ExpressionAttributeNames.Add(PK_PATH_EXPRESSION_VARIABLE, this.PartitionKeyName);
            ExpressionAttributeNames.Add(IS_RELEASED_PATH_EXPRESSION_VARIABLE, IS_RELEASED);

            if (this.SortKeyName.IsPresent())
            {
                //We do not check the owner here because the lock is expired and it is OK to overwrite the owner
                if (ConsistentLockData)
                {
                    ConditionalExpression = PK_EXISTS_AND_SK_EXISTS_AND_RVN_IS_THE_SAME_AND_IS_RELEASED_CONDITION;
                }
                else
                {
                    ConditionalExpression = PK_EXISTS_AND_SK_EXISTS_AND_IS_RELEASED_CONDITION;
                }

                ExpressionAttributeNames.Add(SK_PATH_EXPRESSION_VARIABLE, this.SortKeyName.Value);
            }
            else
            {
                if (ConsistentLockData)
                {
                    ConditionalExpression = PK_EXISTS_AND_RVN_IS_THE_SAME_AND_IS_RELEASED_CONDITION;
                }
                else
                {
                    ConditionalExpression = PK_EXISTS_AND_IS_RELEASED_CONDITION;
                }
            }

            ExpressionAttributeValues.Add(IS_RELEASED_VALUE_EXPRESSION_VARIABLE, IS_RELEASED_ATTRIBUTE_VALUE);

            if (UpdateExistingLockRecord)
            {
                item.Remove(this.PartitionKeyName);

                if (this.SortKeyName.IsPresent())
                {
                    item.Remove(this.SortKeyName.Value);
                }

                string UpdateExpression = GetUpdateExpressionAndUpdateNameValueMaps(item, ExpressionAttributeNames, ExpressionAttributeValues)
                        + REMOVE_IS_RELEASED_UPDATE_EXPRESSION;

                UpdateItemRequest UpdateItemRequest = new UpdateItemRequest()
                {
                    TableName = this.TableName,
                    Key = GetItemKeys(existingLock.Value),
                    UpdateExpression = UpdateExpression,
                    ExpressionAttributeNames = ExpressionAttributeNames,
                    ExpressionAttributeValues = ExpressionAttributeValues,
                    ConditionExpression = ConditionalExpression
                };

                return await UpdateItemAndStartSessionMonitorAsync(options, key, sortKey, deleteLockOnRelease, sessionMonitor, newLockData, recordVersionNumber, UpdateItemRequest);
            }
            else
            {
                PutItemRequest PutItemRequest = new PutItemRequest()
                {
                    TableName = this.TableName,
                    ConditionExpression = ConditionalExpression,
                    ExpressionAttributeNames = ExpressionAttributeNames,
                    ExpressionAttributeValues = ExpressionAttributeValues
                };

                return await PutLockItemAndStartSessionMonitorAsync(options, key, sortKey, deleteLockOnRelease, sessionMonitor, newLockData, recordVersionNumber, PutItemRequest);
            }

        }

        private async Task<LockItem> UpdateItemAndStartSessionMonitorAsync(
            AcquireLockOptions options,
            string key,
            Optional<string> sortKey,
            bool deleteLockOnRelease,
            Optional<SessionMonitor> sessionMonitor,
            Optional<MemoryStream> newLockData,
            string recordVersionNumber,
            UpdateItemRequest updateItemRequest
        )
        {
            long LastUpdatedTime = LockClientUtils.MillisecondTime();

            await this.DynamoDB.UpdateItemAsync(updateItemRequest);

            LockItem LockItem = new LockItem(
                this,
                key,
                sortKey,
                newLockData,
                deleteLockOnRelease,
                this.OwnerName,
                this.LeaseDurationInMilliseconds,
                LastUpdatedTime,
                recordVersionNumber,
                !IS_RELEASED_INDICATOR,
                sessionMonitor,
                options.AdditionalAttributes
            );

            this.Locks.AddOrUpdate(LockItem.GetUniqueIdentifier(), LockItem, (id, item) => item);
            this.TryAddSessionMonitor(LockItem.GetUniqueIdentifier(), LockItem);
            return LockItem;
        }

        private async Task<LockItem> UpsertAndMonitorNewLockAsync(
            AcquireLockOptions options,
            string key,
            Optional<string> sortKey,
            bool deleteLockOnRelease,
            Optional<SessionMonitor> sessionMonitor,
            Optional<MemoryStream> newLockData,
            Dictionary<string, AttributeValue> item,
            string recordVersionNumber
        )
        {
            Dictionary<string, string> ExpressionAttributeNames = new Dictionary<string, string>()
            {
                {PK_PATH_EXPRESSION_VARIABLE, this.PartitionKeyName }
            };

            bool UpdateExistingLockRecord = options.UpdateExistingLockRecord;

            string ConditionalExpression;

            if (this.SortKeyName.IsPresent())
            {
                ConditionalExpression = ACQUIRE_LOCK_THAT_DOESNT_EXIST_PK_SK_CONDITION;
                ExpressionAttributeNames.Add(SK_PATH_EXPRESSION_VARIABLE, this.SortKeyName.Value);
            }
            else
            {
                ConditionalExpression = ACQUIRE_LOCK_THAT_DOESNT_EXIST_PK_CONDITION;
            }

            if (UpdateExistingLockRecord)
            {
                // Remove keys from item to create updateExpression
                item.Remove(this.PartitionKeyName);

                if (this.SortKeyName.IsPresent())
                {
                    item.Remove(this.SortKeyName.Value);
                }

                Dictionary<string, AttributeValue> ExpressionAttributeValues = new Dictionary<string, AttributeValue>();
                string UpdateExpression = this.GetUpdateExpressionAndUpdateNameValueMaps(item, ExpressionAttributeNames, ExpressionAttributeValues);

                UpdateItemRequest UpdateItemRequest = new UpdateItemRequest()
                {
                    TableName = this.TableName,
                    Key = this.GetKeys(key, sortKey),
                    UpdateExpression = UpdateExpression,
                    ExpressionAttributeNames = ExpressionAttributeNames,
                    ExpressionAttributeValues = ExpressionAttributeValues,
                    ConditionExpression = ConditionalExpression
                };

                return await this.UpdateItemAndStartSessionMonitorAsync(options, key, sortKey, deleteLockOnRelease, sessionMonitor, newLockData, recordVersionNumber, UpdateItemRequest);
            }
            else
            {
                PutItemRequest PutItemRequest = new PutItemRequest()
                {
                    Item = item,
                    TableName = this.TableName,
                    ConditionExpression = ConditionalExpression,
                    ExpressionAttributeNames = ExpressionAttributeNames
                };

                // No one has the lock, go ahead and acquire it.
                // The person storing the lock into DynamoDB should err on the side of thinking the lock will expire
                // sooner than it actually will, so they start counting towards its expiration before the Put succeeds
                return await this.PutLockItemAndStartSessionMonitorAsync(options, key, sortKey, deleteLockOnRelease, sessionMonitor, newLockData, recordVersionNumber, PutItemRequest);
            }
        }

        private async Task<LockItem> PutLockItemAndStartSessionMonitorAsync(
            AcquireLockOptions options,
            string key, Optional<string> sortKey,
            bool deleteLockOnRelease,
            Optional<SessionMonitor> sessionMonitor,
            Optional<MemoryStream> newLockData,
            string recordVersionNumber,
            PutItemRequest putItemRequest
        )
        {
            long LastUpdatedTime = LockClientUtils.MillisecondTime();

            await this.DynamoDB.PutItemAsync(putItemRequest);

            LockItem LockItem = new LockItem(
                this,
                key,
                sortKey,
                newLockData,
                deleteLockOnRelease,
                this.OwnerName,
                this.LeaseDurationInMilliseconds,
                LastUpdatedTime,
                recordVersionNumber,
                false,
                sessionMonitor,
                options.AdditionalAttributes
            );
            this.Locks.AddOrUpdate(LockItem.GetUniqueIdentifier(), LockItem, (id, item) => item);
            this.TryAddSessionMonitor(LockItem.GetUniqueIdentifier(), LockItem);
            return LockItem;
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
            Dictionary<string, string> ExpressionAttributeNames,
            Dictionary<string, AttributeValue> ExpressionAttributeValues
        )
        {
            string AdditionalUpdateExpression = "SET ";
            StringBuilder UpdateExpressionBuilder = new StringBuilder(AdditionalUpdateExpression);

            int i = 0;

            string ExpressionSeparator = ",";

            foreach (KeyValuePair<string, AttributeValue> Entry in item)
            {

                ExpressionAttributeNames.Add($"#k{i}", Entry.Key);
                ExpressionAttributeValues.Add($":v{i}", Entry.Value);

                UpdateExpressionBuilder.Append("#k").Append(i).Append("=").Append(":v").Append(i++).Append(ExpressionSeparator);
            }

            // Remove the last comma
            UpdateExpressionBuilder.Length = UpdateExpressionBuilder.Length - 1;

            return UpdateExpressionBuilder.ToString();
        }

        /// <summary>
        /// Releases all the locks currently held by the owner specified when creating this lock client
        /// </summary>
        /// <returns>Any exceptions that were raised while releasing locks are returned so they can be handled by the caller</returns>
        private IEnumerable<Exception> ReleaseAllLocks()
        {
            List<Exception> Exceptions = new List<Exception>();

            lock (SyncObject)
            {
                Dictionary<string, LockItem> Locks = new Dictionary<string, LockItem>(this.Locks);

                foreach (KeyValuePair<string, LockItem> LockEntry in Locks)
                {
                    try
                    {
                        this.ReleaseLock(LockEntry.Value); // TODO catch exceptions and report failure separately
                    }
                    catch (Exception e)
                    {
                        Exceptions.Add(e);
                    }
                }
            }

            return Exceptions;
        }

        /// <summary>
        ///  Helper method to read a key from DynamoDB
        /// </summary>
        /// <param name="key"></param>
        /// <param name="sortKey"></param>
        /// <returns></returns>
        private async Task<GetItemResponse> ReadFromDynamoDBAsync(string key, Optional<string> sortKey)
        {
            Dictionary<string, AttributeValue> DynamoDBKey = new Dictionary<string, AttributeValue>()
            {
                { this.PartitionKeyName, new AttributeValue() { S = key } }
            };

            if (this.SortKeyName.IsPresent())
            {
                DynamoDBKey.Add(this.SortKeyName.Value, new AttributeValue() { S = sortKey.Value });
            }

            GetItemRequest GetItemRequest = new GetItemRequest()
            {
                TableName = this.TableName,
                Key = DynamoDBKey,
                ConsistentRead = true
            };

            return await this.DynamoDB.GetItemAsync(GetItemRequest);
        }

        private LockItem CreateLockItem(GetLockOptions options, Dictionary<string, AttributeValue> immutableItem)
        {
            Dictionary<string, AttributeValue> Item = new Dictionary<string, AttributeValue>(immutableItem);

            Optional<MemoryStream> Data = Optional<AttributeValue>.OfNullable(Item[DATA]).Map(x =>
            {
                Item.Remove(DATA);
                return x.B;
            });

            AttributeValue OwnerName = Item[OWNER_NAME];
            Item.Remove(OWNER_NAME);
            AttributeValue LeaseDuration = Item[LEASE_DURATION];
            Item.Remove(LEASE_DURATION);
            AttributeValue RecordVersionNumber = Item[RECORD_VERSION_NUMBER];
            Item.Remove(RECORD_VERSION_NUMBER);

            bool IsReleased = Item.ContainsKey(IS_RELEASED);
            Item.Remove(IS_RELEASED);
            Item.Remove(this.PartitionKeyName);

            // The person retrieving the lock in DynamoDB should err on the side of
            // not expiring the lock, so they don't start counting until after the
            // call to DynamoDB succeeds
            long LookupTime = LockClientUtils.MillisecondTime();

            LockItem LockItem = new LockItem(
                this,
                options.PartitionKey,
                options.SortKey,
                Data,
                options.DeleteLockOnRelease,
                OwnerName.S,
                Int64.Parse(LeaseDuration.S),
                LookupTime,
                RecordVersionNumber.S,
                IsReleased,
                Optional<SessionMonitor>.Empty,
                Item
            );

            return LockItem;
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


            CancellationTokenSource CTS = new CancellationTokenSource();
            BackgroundTask Bt = new BackgroundTask(() => this.Run(CTS.Token), CTS);
            Bt.Start();
            return Bt;
            // Doing the above instead of Task.Run(() => this.Run(ct), ct) and passing the CT in as method parameter

            //return this.NamedThreadCreator.Invoke($"dynamodb-lock-client-{Interlocked.Increment(ref LockClientId).ToString()}").(() => this.Run(ct), ct);
        }

        private Dictionary<string, AttributeValue> GetItemKeys(LockItem lockItem)
        {
            return GetKeys(lockItem.PartitionKey, lockItem.SortKey);
        }

        private Dictionary<string, AttributeValue> GetKeys(string partitionKey, Optional<string> sortKey)
        {
            Dictionary<string, AttributeValue> Key = new Dictionary<string, AttributeValue>();

            Key.Add(this.PartitionKeyName, new AttributeValue() { S = partitionKey });

            if (sortKey.IsPresent())
            {
                Key.Add(this.SortKeyName.Value, new AttributeValue() { S = sortKey.Value });
            }

            return Key;
        }

        private bool TryAddSessionMonitor(string lockName, LockItem @lock)
        {
            if (@lock.HasSessionMonitor() && @lock.HasCallback())
            {
                BackgroundTask MonitorThread = LockSessionMonitorChecker(lockName, @lock);
                MonitorThread.Start();
                return this.SessionMonitors.TryAdd(lockName, MonitorThread);
            }

            return false;
        }

        private bool RemoveKillSessionMonitor(string monitorName)
        {
            if (this.SessionMonitors.ContainsKey(monitorName))
            {
                if (this.SessionMonitors.TryRemove(monitorName, out BackgroundTask Monitor))
                {
                    Monitor.Cancel();

                    try
                    {
                        Monitor.Task.Wait();
                        return true;
                    }
                    catch (OperationCanceledException e)
                    {
                        //logger.warn("Caught InterruptedException waiting for session monitor thread to exit, ignoring");
                    }
                }
            }

            return false;
        }

        private BackgroundTask LockSessionMonitorChecker(string monitorName, LockItem @lock)
        {
            CancellationTokenSource CTS = new CancellationTokenSource();
            CancellationToken CT = CTS.Token;

            Action action = () =>
            {
                while (true)
                {
                    if (CT.IsCancellationRequested)
                    {
                        CT.ThrowIfCancellationRequested();
                    }

                    try
                    {
                        long MillisUntilDangerZone = @lock.MillisecondsUntilDangerZoneEntered();

                        if (MillisUntilDangerZone > 0)
                        {
                            foreach (int Time in LockClientUtils.Decompose64BitInt(MillisUntilDangerZone))
                            {
                                Thread.Sleep(Time);
                            }
                        }
                        else
                        {
                            @lock.RunSessionMonitor();
                            SessionMonitors.TryRemove(monitorName, out BackgroundTask Temp);
                            return;
                        }
                    }
                    catch (OperationCanceledException e)
                    {
                        return;
                    }
                }
            };

            return new BackgroundTask(action, CTS);
        }

        #endregion
    }
}
