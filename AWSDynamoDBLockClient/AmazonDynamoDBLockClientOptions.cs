using Amazon.DynamoDBv2;
using BAMCIS.Util;
using BAMCIS.Util.Concurrent;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace BAMCIS.AWSDynamoDBLockClient
{
    /// <summary>
    /// An options class for setting up a lock client with various overrides
    /// to the defaults. In order to use this, it must be constructred using 
    /// the builder.
    /// </summary>
    public class AmazonDynamoDBLockClientOptions
    {
        #region Defaults

        internal static readonly string DEFAULT_PARTITION_KEY_NAME = "key";
        internal static readonly long DEFAULT_LEASE_DURATION = 20;
        internal static readonly long DEFAULT_HEARTBEAT_PERIOD = 5;
        internal static readonly TimeUnit DEFAULT_TIME_UNIT = TimeUnit.SECONDS;
        internal static readonly bool DEFAULT_CREATE_HEARTBEAT_BACKGROUND_THREAD = true;
        internal static readonly bool DEFAULT_HOLD_LOCK_ON_SERVICE_UNAVAILABLE = false;

        #endregion

        #region Public Properties

        /// <summary>
        /// The DynamoDB client that will be the basis of the lock client
        /// </summary>
        public IAmazonDynamoDB DynamoDBClient { get; }

        /// <summary>
        /// The table being operated on with locks
        /// </summary>
        public string TableName { get; }

        /// <summary>
        /// The partition key name in the table
        /// </summary>
        public string PartitionKeyName { get; }

        /// <summary>
        /// The optional sort key name being used in the table. If not specified,
        /// we assume that the table does not have a sort key defined.
        /// </summary>
        public Optional<string> SortKeyName { get; }

        /// <summary>
        /// The person that is acquiring the lock, for example, box.amazon.com.
        /// </summary>
        public string OwnerName { get; }

        /// <summary>
        /// The length of time that the lease for the lock will be granted for. If this is set
        /// to, for example, 30 seconds, then the lock will expire if the heartbeat is not sent
        /// for at least 30 seconds (which could happen if the box of the heartbeat thread dies).
        /// </summary>
        public long LeaseDuration { get; }

        /// <summary>
        /// How often the client updates DynamoDB to note that the instance is still running 
        /// (recommendation is to make this at least 3 times smaller than the lease duration -- for
        /// example HeartbeatPeriod = 1 seconds, LeaseDuration = 10 seconds could be a reasonable
        /// configuration, make sure to include a buffer for network latency).
        /// </summary>
        public long HeartbeatPeriod { get; }

        /// <summary>
        /// The unit of time used for all times in this object including the Heartbeat Period and
        /// the Lease Duration.
        /// </summary>
        public TimeUnit TimeUnit { get; }

        /// <summary>
        /// Whether or not a thread is automatically created to send heartbeats.
        /// </summary>
        public bool CreateHeartbeatBackgroundThread { get; }

        /// <summary>
        /// This parameter should be set to true only in applications which do not have string locking requirements. When
        /// this is set to true, on DynamoDB service unavailable errors, it is possible that two different clients can hold
        /// the lock.
        /// 
        /// When the heartbeat fails for the lease duration period, the lock expires. If this parameter is set to true, and if
        /// a heartbeat receives an AmazoneServiceException with a status code of HttpStatus.SC_SERVICE_UNAVAILABLE(503), the 
        /// lock client will assume the heartbeat was a success and update the local state accordingly and will keep holding
        /// this lock.
        /// </summary>
        public bool HoldLockOnServiceUnavailable { get; }

        //TODO: Probably not needed
        /// <summary>
        /// This creates names threads for the heartbeat mechanism.
        /// </summary>
        //public Func<string, TaskFactory> NamedThreadCreator { get; }

        #endregion

        #region Constructors

        /// <summary>
        /// Private constructor for the builder class
        /// </summary>
        /// <param name="dynamoDBClient"></param>
        /// <param name="tableName"></param>
        /// <param name="partitionKeyName"></param>
        /// <param name="sortKeyName"></param>
        /// <param name="ownerName"></param>
        /// <param name="leaseDuration"></param>
        /// <param name="heartbeatPeriod"></param>
        /// <param name="timeUnit"></param>
        /// <param name="createHeartbeatBackgroundThread"></param>
        /// <param name="namedThreadCreator"></param>
        /// <param name="holdLockOnServiceUnavailable"></param>
        private AmazonDynamoDBLockClientOptions(
            IAmazonDynamoDB dynamoDBClient, 
            string tableName, 
            string partitionKeyName,
            Optional<string> sortKeyName, 
            string ownerName, 
            long leaseDuration, 
            long heartbeatPeriod, 
            TimeUnit timeUnit,
            bool createHeartbeatBackgroundThread, 
            //Func<string, TaskFactory> namedThreadCreator, 
            bool holdLockOnServiceUnavailable
        )
        {
            this.DynamoDBClient = dynamoDBClient;
            this.TableName = tableName;
            this.PartitionKeyName = partitionKeyName;
            this.SortKeyName = sortKeyName;
            this.OwnerName = ownerName;
            this.LeaseDuration = leaseDuration;
            this.HeartbeatPeriod = heartbeatPeriod;
            this.TimeUnit = timeUnit;
            this.CreateHeartbeatBackgroundThread = createHeartbeatBackgroundThread;
            // TODO: Probably not needed
            //this.NamedThreadCreator = namedThreadCreator;
            this.HoldLockOnServiceUnavailable = holdLockOnServiceUnavailable;
        }

        #endregion

        #region Public Methods

        /// <summary>
        /// Creates an AmazonDynamoDBLockClientOptions builder object, which can be used to
        /// create an AmazonDynamoDBLockClient. The only required parameters are the client and
        /// the table name.
        /// </summary>
        /// <param name="dynamoDBClient"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public static AmazonDynamoDBLockClientOptionsBuilder Builder(IAmazonDynamoDB dynamoDBClient, string tableName)
        {
            return new AmazonDynamoDBLockClientOptionsBuilder(dynamoDBClient, tableName);
        }

        public override int GetHashCode()
        {
            return Utilities.Hash(
                this.CreateHeartbeatBackgroundThread,
                this.DynamoDBClient,
                this.HeartbeatPeriod,
                this.HoldLockOnServiceUnavailable,
                this.LeaseDuration,
                //this.NamedThreadCreator,
                this.OwnerName,
                this.PartitionKeyName,
                this.SortKeyName,
                this.TableName,
                this.TimeUnit);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            if (obj == null || this.GetType() != obj.GetType())
            {
                return false;
            }

            AmazonDynamoDBLockClientOptions Other = (AmazonDynamoDBLockClientOptions)obj;

            return this.CreateHeartbeatBackgroundThread == Other.CreateHeartbeatBackgroundThread &&
                this.DynamoDBClient == Other.DynamoDBClient &&
                this.HeartbeatPeriod == Other.HeartbeatPeriod &&
                this.HoldLockOnServiceUnavailable == Other.HoldLockOnServiceUnavailable &&
                this.LeaseDuration == Other.LeaseDuration &&
                //this.NamedThreadCreator == Other.NamedThreadCreator &&
                this.OwnerName == Other.OwnerName &&
                this.PartitionKeyName == Other.PartitionKeyName &&
                this.SortKeyName == Other.SortKeyName &&
                this.TableName == Other.TableName &&
                this.TimeUnit == Other.TimeUnit;             
        }

        public static bool operator ==(AmazonDynamoDBLockClientOptions left, AmazonDynamoDBLockClientOptions right)
        {
            if (ReferenceEquals(left, right))
            {
                return true;
            }

            if (right is null || left is null)
            {
                return false;
            }

            return left.Equals(right);
        }

        public static bool operator !=(AmazonDynamoDBLockClientOptions left, AmazonDynamoDBLockClientOptions right)
        {
            return !(left == right);
        }


        #endregion

        #region Inner Class

        /// <summary>
        /// A builder for setting up an AmazonDynamoDBLockClientOptions object. By default, it is setup to have a partition
        /// key name of "key", a lease duration of 20 seconds, and a default heartbeat period of 5 seconds. These defaults
        /// can be overriden.
        /// </summary>
        public class AmazonDynamoDBLockClientOptionsBuilder
        {
            #region Private Fields

            private IAmazonDynamoDB DynamoDBClient;
            private string TableName;
            private string PartitionKeyName;
            private Optional<string> SortKeyName;
            private string OwnerName;
            private long LeaseDuration;
            private long HeartbeatPeriod;
            private TimeUnit TimeUnit;
            private bool CreateHeartbeatBackgroundThread;

            /// <summary>
            /// This parameter should be set to true only in applications which do not have string locking requirements. When
            /// this is set to true, on DynamoDB service unavailable errors, it is possible that two different clients can hold
            /// the lock.
            /// 
            /// When the heartbeat fails for the lease duration period, the lock expires. If this parameter is set to true, and if
            /// a heartbeat receives an AmazoneServiceException with a status code of HttpStatus.SC_SERVICE_UNAVAILABLE(503), the 
            /// lock client will assume the heartbeat was a success and update the local state accordingly and will keep holding
            /// this lock.
            /// </summary>
            private bool HoldLockOnServiceUnavailable;
            //private Func<string, TaskFactory> _NamedThreadCreator;

            #endregion

            /// <summary>
            /// Creates a builder
            /// </summary>
            /// <param name="dynamoDBClient"></param>
            /// <param name="tableName"></param>
            internal AmazonDynamoDBLockClientOptionsBuilder(
                IAmazonDynamoDB dynamoDBClient,
                string tableName
            ) : this(dynamoDBClient, tableName, GenerateOwnerNameFromLocalhost()/*, NamedThreadCreator()*/)
            {
            }

            /// <summary>
            /// Creates a builder
            /// </summary>
            /// <param name="dynamoDBClient"></param>
            /// <param name="tableName"></param>
            /// <param name="ownerName"></param>
            /*internal AmazonDynamoDBLockClientOptionsBuilder(
                AmazonDynamoDBClient dynamoDBClient, 
                string tableName, 
                string ownerName
            ) : this(dynamoDBClient, tableName, ownerName, NamedThreadCreator())
            { }*/

            /// <summary>
            /// Creates a builder
            /// </summary>
            /// <param name="dynamoDBClient"></param>
            /// <param name="tableName"></param>
            /// <param name="ownerName"></param>
            /// <param name="namedThreadCreator"></param>
            internal AmazonDynamoDBLockClientOptionsBuilder(
                IAmazonDynamoDB dynamoDBClient, 
                string tableName, 
                string ownerName//, 
                //Func<string, TaskFactory> namedThreadCreator
            )
            {
                this.DynamoDBClient = dynamoDBClient;
                this.TableName = tableName;
                this.PartitionKeyName = DEFAULT_PARTITION_KEY_NAME;
                this.LeaseDuration = DEFAULT_LEASE_DURATION;
                this.HeartbeatPeriod = DEFAULT_HEARTBEAT_PERIOD;
                this.TimeUnit = DEFAULT_TIME_UNIT;
                this.CreateHeartbeatBackgroundThread = DEFAULT_CREATE_HEARTBEAT_BACKGROUND_THREAD;
                this.SortKeyName = Optional<string>.Empty;
                this.OwnerName = !String.IsNullOrEmpty(ownerName) ? ownerName : GenerateOwnerNameFromLocalhost();
                //this._NamedThreadCreator = namedThreadCreator ?? NamedThreadCreator();
                this.HoldLockOnServiceUnavailable = DEFAULT_HOLD_LOCK_ON_SERVICE_UNAVAILABLE;
            }

            #region Public Methods

            /// <summary>
            /// Sets the partition key name. If not specified, the default partition key name of "key" is used.
            /// </summary>
            /// <param name="partitionKeyName"></param>
            /// <returns></returns>
            public AmazonDynamoDBLockClientOptionsBuilder WithPartitionKey(string partitionKeyName)
            {
                this.PartitionKeyName = partitionKeyName;
                return this;
            }

            /// <summary>
            /// The sort key name. If not specified, we assume that the table does not have a sort key defined.
            /// </summary>
            /// <param name="sortKeyName"></param>
            /// <returns></returns>
            public AmazonDynamoDBLockClientOptionsBuilder WithSortKeyName(string sortKeyName)
            {
                this.SortKeyName = Optional<string>.OfNullable(sortKeyName);
                return this;
            }

            /// <summary>
            /// The person that is acquiring the lock (for example, box.amazon.com). If not specified, this is built from the local host name with a guid.
            /// </summary>
            /// <param name="ownerName"></param>
            /// <returns></returns>
            public AmazonDynamoDBLockClientOptionsBuilder WithOwnerName(string ownerName)
            {
                this.OwnerName = ownerName;
                return this;
            }

            /// <summary>
            /// Sets the length of time that the lease for the lock will be granted for. If this is set to, for example,
            /// 30 seconds, then the lock will expire if the heartbeat is not sent for at least 30 seconds (which would happen
            /// if the box or the heartbeat thread dies as examples).
            /// </summary>
            /// <param name="leaseDuration"></param>
            /// <returns></returns>
            public AmazonDynamoDBLockClientOptionsBuilder WithLeaseDuration(long leaseDuration)
            {
                this.LeaseDuration = leaseDuration;
                return this;
            }

            /// <summary>
            /// Sets how often to update DynamoDB to ntoe that the instance is still running (recommendation is to make this
            /// at least 3 times smaller than the lease duration -- for example HeartbeatPeriod = 1 second, LeaseDuration = 10 seconds
            /// could be a reasonable configuration, make sure to include a buffer for network latency).
            /// </summary>
            /// <param name="heartbeatPeriod"></param>
            /// <returns></returns>
            public AmazonDynamoDBLockClientOptionsBuilder WithHeartbeatPeriod(long heartbeatPeriod)
            {
                this.HeartbeatPeriod = heartbeatPeriod;
                return this;
            }

            /// <summary>
            /// Sets the time unit to use for all times in this object, including HeartbeatPeriod and LeaseDuration.
            /// </summary>
            /// <param name="timeUnit"></param>
            /// <returns></returns>
            public AmazonDynamoDBLockClientOptionsBuilder WithTimeUnit(TimeUnit timeUnit)
            {
                this.TimeUnit = timeUnit;
                return this;
            }

            /// <summary>
            /// Sets whether or not to create a thread to automatically heartbeat. If false, you must call SendHeartbeat() manuualy.
            /// </summary>
            /// <param name="createHeartbeatBackgroundThread"></param>
            /// <returns></returns>
            public AmazonDynamoDBLockClientOptionsBuilder WithCreateHeartbeatBackgroundThread(bool createHeartbeatBackgroundThread)
            {
                this.CreateHeartbeatBackgroundThread = createHeartbeatBackgroundThread;
                return this;
            }

            /// <summary>
            /// This parameter specifies whether to treat a service unavailable exception as a successful heartbeat response.
            /// </summary>
            /// <param name="holdLockOnServiceUnavailable"></param>
            /// <returns></returns>
            public AmazonDynamoDBLockClientOptionsBuilder WithHoldLockOnServiceUnavailable(bool holdLockOnServiceUnavailable)
            {
                this.HoldLockOnServiceUnavailable = holdLockOnServiceUnavailable;
                return this;
            }

            /// <summary>
            /// Builds a new DynamoDB Lock Client Options object.
            /// </summary>
            /// <returns></returns>
            public AmazonDynamoDBLockClientOptions Build()
            {
                if (String.IsNullOrEmpty(this.TableName))
                {
                    throw new ArgumentException("Table Name must not be null or empty.");
                }

                if (String.IsNullOrEmpty(this.OwnerName))
                {
                    throw new ArgumentException("Owner Name must not be null or empty.");
                }

                return new AmazonDynamoDBLockClientOptions(
                    this.DynamoDBClient, 
                    this.TableName, 
                    this.PartitionKeyName,
                    this.SortKeyName, 
                    this.OwnerName, 
                    this.LeaseDuration, 
                    this.HeartbeatPeriod, 
                    this.TimeUnit,
                    this.CreateHeartbeatBackgroundThread, 
                    //this._NamedThreadCreator, 
                    this.HoldLockOnServiceUnavailable
                );
            }

            public override string ToString()
            {
                return $"AmazonDynamoDBLockClientOptionsBuilder(DynamoDBClient={this.DynamoDBClient}, TableName={this.TableName}, PartitionKeyName={this.PartitionKeyName}, SortKeyName={this.SortKeyName}, OwnerName={this.OwnerName}, LeaseDuration={this.LeaseDuration}, HeartbeatPeriod={this.HeartbeatPeriod}, TimeUnit={this.TimeUnit}, CreateHeartbeatBackgroundThread={this.CreateHeartbeatBackgroundThread}, HoldLockOnServiceUnavailable={this.HoldLockOnServiceUnavailable})";
            }

            #endregion

            #region Private Methods

            private static string GenerateOwnerNameFromLocalhost()
            {
                return $"{Environment.MachineName}{Guid.NewGuid().ToString()}";
            }

            // TODO: This needs to be updated since it's not creating
            // named threads, whih also don't work the same way in C# as Java
           /* private static Func<string, TaskFactory> NamedThreadCreator()
            {
                return (string threadName) =>
                {
                    return new TaskFactory();
                };
            }*/

            #endregion
        }

        #endregion
    }
}
