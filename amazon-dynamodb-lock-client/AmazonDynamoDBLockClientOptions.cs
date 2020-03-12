using Amazon.DynamoDBv2.Util;
using BAMCIS.Util.Concurrent;
using System;
using System.Threading;

namespace Amazon.DynamoDBv2
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
        public IAmazonDynamoDB DynamoDBClient { get; set; }

        /// <summary>
        /// The table being operated on with locks
        /// </summary>
        public string TableName { get; set; }

        /// <summary>
        /// The partition key name in the table
        /// </summary>
        public string PartitionKeyName { get; set; }

        /// <summary>
        /// The optional sort key name being used in the table. If not specified,
        /// we assume that the table does not have a sort key defined.
        /// </summary>
        public string SortKeyName { get; set; }

        /// <summary>
        /// The person that is acquiring the lock, for example, box.amazon.com.
        /// </summary>
        public string OwnerName { get; set; }

        /// <summary>
        /// The length of time that the lease for the lock will be granted for. If this is set
        /// to, for example, 30 seconds, then the lock will expire if the heartbeat is not sent
        /// for at least 30 seconds (which could happen if the box of the heartbeat thread dies).
        /// </summary>
        public long LeaseDuration { get; set; }

        /// <summary>
        /// How often the client updates DynamoDB to note that the instance is still running 
        /// (recommendation is to make this at least 3 times smaller than the lease duration -- for
        /// example HeartbeatPeriod = 1 seconds, LeaseDuration = 10 seconds could be a reasonable
        /// configuration, make sure to include a buffer for network latency).
        /// </summary>
        public long HeartbeatPeriod { get; set; }

        /// <summary>
        /// The unit of time used for all times in this object including the Heartbeat Period and
        /// the Lease Duration.
        /// </summary>
        public TimeUnit TimeUnit { get; set; }

        /// <summary>
        /// Whether or not a thread is automatically created to send heartbeats.
        /// </summary>
        public bool CreateHeartbeatBackgroundThread { get; set; }

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
        public bool HoldLockOnServiceUnavailable { get; set; }

        /// <summary>
        /// The cancellation token source to use for cancelling background threads
        /// </summary>
        public CancellationTokenSource BackgroundTaskToken { get; set; }

        #endregion

        #region Constructors

        /// <summary>
        /// Creates a new lock client options with the only 2 required parameters. The owner name will be
        /// generated from the local host name
        /// </summary>
        /// <param name="dynamoDBClient"></param>
        /// <param name="tableName"></param>
        public AmazonDynamoDBLockClientOptions(IAmazonDynamoDB dynamoDBClient, string tableName) : this(dynamoDBClient, tableName, StaticGenerateOwnerNameFromLocalhost())
        { }

        /// <summary>
        /// Creates a new lock client options with the 2 required parameters and optionally a specified owner
        /// name.
        /// </summary>
        /// <param name="dynamoDBClient"></param>
        /// <param name="tableName"></param>
        /// <param name="ownerName"></param>
        public AmazonDynamoDBLockClientOptions(IAmazonDynamoDB dynamoDBClient, string tableName, string ownerName)
        {
            LockClientUtils.RequireNonNull(dynamoDBClient, "The DynamoDB client cannot be null.", "dynamoDBClient");
            LockClientUtils.RequireNonNullOrEmpty(tableName, "The table name cannot be null or empty.", "tableName");

            this.DynamoDBClient = dynamoDBClient;
            this.TableName = tableName;

            this.PartitionKeyName = DEFAULT_PARTITION_KEY_NAME;
            this.LeaseDuration = DEFAULT_LEASE_DURATION;
            this.HeartbeatPeriod = DEFAULT_HEARTBEAT_PERIOD;
            this.TimeUnit = DEFAULT_TIME_UNIT;
            this.CreateHeartbeatBackgroundThread = DEFAULT_CREATE_HEARTBEAT_BACKGROUND_THREAD;
            this.SortKeyName = String.Empty;
            this.OwnerName = !String.IsNullOrEmpty(ownerName) ? ownerName : GenerateOwnerNameFromLocalhost();
            this.HoldLockOnServiceUnavailable = DEFAULT_HOLD_LOCK_ON_SERVICE_UNAVAILABLE;
            this.BackgroundTaskToken = new CancellationTokenSource();
        }

        #endregion

        #region Public Methods

        public override int GetHashCode()
        {
            return LockClientUtils.Hash(
                this.CreateHeartbeatBackgroundThread,
                this.DynamoDBClient,
                this.HeartbeatPeriod,
                this.HoldLockOnServiceUnavailable,
                this.LeaseDuration,
                this.OwnerName,
                this.PartitionKeyName,
                this.SortKeyName,
                this.TableName,
                this.TimeUnit,
                this.BackgroundTaskToken);
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

            AmazonDynamoDBLockClientOptions other = (AmazonDynamoDBLockClientOptions)obj;

            return this.CreateHeartbeatBackgroundThread == other.CreateHeartbeatBackgroundThread &&
                this.DynamoDBClient == other.DynamoDBClient &&
                this.HeartbeatPeriod == other.HeartbeatPeriod &&
                this.HoldLockOnServiceUnavailable == other.HoldLockOnServiceUnavailable &&
                this.LeaseDuration == other.LeaseDuration &&
                //this.NamedThreadCreator == Other.NamedThreadCreator &&
                this.OwnerName == other.OwnerName &&
                this.PartitionKeyName == other.PartitionKeyName &&
                this.SortKeyName == other.SortKeyName &&
                this.TableName == other.TableName &&
                this.TimeUnit == other.TimeUnit;             
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

        #region Private Methods

        private string GenerateOwnerNameFromLocalhost()
        {
            return StaticGenerateOwnerNameFromLocalhost();
        }

        private static string StaticGenerateOwnerNameFromLocalhost()
        {
            return $"{Environment.MachineName}{Guid.NewGuid().ToString()}";
        }

        #endregion

        #endregion
    }
}
