using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2.Util;
using BAMCIS.Util.Concurrent;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Amazon.DynamoDBv2
{
    public class AcquireLockOptions
    {
        #region Public Properties

        /// <summary>
        /// The partition key to lock on
        /// </summary>
        public string PartitionKey { get; set; }

        /// <summary>
        /// The sort key to try and acquire the lock on (specify if and only if the table has sort keys).
        /// </summary>
        public string SortKey { get; set; }

        /// <summary>
        /// Sets data to be stored alongside the lock
        /// </summary>
        public MemoryStream Data { get; set; }

        /// <summary>
        /// Sets whether or not to replace any existing lock data with the data parameter
        /// </summary>
        public bool ReplaceData { get; set; }

        /// <summary>
        /// Whether or not the lock should be deleted when Close() is called on the resulting LockItem
        /// </summary>
        public bool DeleteLockOnRelease { get; set; }

        /// <summary>
        /// If this is true, the lock client will only update the current lock record if present, otherwise create a new one.
        /// </summary>
        public bool UpdateExistingLockRecord { get; set; }

        /// <summary>
        /// If this is true, the lock client will ensure that released locks are acquired consistently in order to preserve lock data in DynamoDB.
        /// 
        /// This option is currently disabled by default.  Will be enabled by default in the next major release.
        /// </summary>
        public bool AcquireReleasedLocksConsistently { get; set; }

        /// <summary>
        /// Sets whether or not to allow acquiring locks if the lock does not exist already
        /// </summary>
        public bool AcquireOnlyIfLockAlreadyExists { get; set; }

        /// <summary>
        /// How long to wait before trying to get the lock again (if set to seconds, for example, it would
        /// attempt to do so every 10 seconds). If set, TimeUnit must also be set.
        /// </summary>
        public long RefreshPeriod { get; set; }

        /// <summary>
        /// How long to wait in addition to the lease duration (if set to 10 minutes, this will try to acquire
        /// a lock for at least 10 minutes before giving up and throwing the exception). If set, TimeUnit must also be set.
        /// </summary>
        public long AdditionalTimeToWaitForLock { get; set; }

        /// <summary>
        /// The TimeUnit for all time parameters in this object including RefreshPeriod and AdditionalTimeToWaitForLock.
        /// </summary>
        public TimeUnit TimeUnit { get; set; }

        /// <summary>
        /// Stores some additional attributes with each lock. This can be used to add any arbitrary parameters to each lock row.
        /// </summary>
        public Dictionary<string, AttributeValue> AdditionalAttributes { get; set; }

        /// <summary>
        /// Registers a session monitor.
        /// 
        /// The purpose of this SessionMonitor is to provide two abilities: provide
        /// the ability to determine if the lock is about to expire, and run a
        /// user-provided callback when the lock is about to expire. The advantage
        /// this provides is notification that your lock is about to expire before it
        /// is actually expired, and in case of leader election will help in
        /// preventing that there are no two leaders present simultaneously.
        ///
        /// If due to any reason heartbeating is unsuccessful for a configurable
        /// period of time, your lock enters into a phase known as "danger zone." It
        /// is during this "danger zone" that the callback will be run. It is also
        /// during this period that a call to lockItem.AmIAboutToExpire() will return
        /// true and your application can take appropriate measures.
        ///
        /// Bear in mind that the callback on a SessionMonitor may be null. In this
        /// case, no callback will be run upon the lock entering the "danger zone";
        /// yet, one can still make use of the AmIAboutToExpire() method.
        /// Furthermore, non-null callbacks can only ever be executed once in a
        /// lock's lifetime. Independent of whether or not a callback is run, the
        /// client will attempt to heartbeat the lock until the lock is released or
        /// obtained by someone else.
        /// 
        /// Consider an example which uses this mechanism for leader election. One
        /// way to make use of this SessionMonitor is to register a callback that
        /// kills the process in case the leader's lock enters the danger zone:
        ///
        /// try {
        ///     AcquireLockOptions options = AcquireLockOptions.builder("myLock")
        ///          .withSessionMonitor(5, () => { Environment.Exit(1); })
        ///          .withTimeUnit(TimeUnit.MINUTES)
        ///          .build();
        ///     LockItem leaderLock = lockClient.acquireLock(options);
        ///     goRunLeaderProcedures(); // safely run code knowing that after at least 5 minutes without heartbeating, the JVM will crash
        /// } catch (...) {
        ///     ... //handle errors
        /// }
        /// </summary>
        public SessionMonitor SessionMonitor { get; private set; }

        /// <summary>
        /// Setting this flag to true will prevent the thread from being blocked (put to sleep) for the lease duration and
        /// instead will return the call with the lock not granted exception back to the caller. It is up to the caller to
        /// optionally back-off and retry and to acquire the lock.
        /// </summary>
        public bool ShouldSkipBlockingWait { get; set; }

        #endregion

        #region Constructors

        /// <summary>
        /// Creates a new AcquireLockOptions using the only required parameter,
        /// which is a specific partition key.
        /// </summary>
        /// <param name="partitionKey"></param>
        public AcquireLockOptions(string partitionKey)
        {
            this.PartitionKey = partitionKey;

            this.AcquireOnlyIfLockAlreadyExists = false;
            this.AcquireReleasedLocksConsistently = false;
            this.AdditionalAttributes = new Dictionary<string, AttributeValue>();
            this.AdditionalTimeToWaitForLock = 0;
            this.Data = null;
            this.DeleteLockOnRelease = true;
            this.RefreshPeriod = 0;
            this.ReplaceData = true;
            this.SessionMonitor = null;
            this.ShouldSkipBlockingWait = false;
            this.SortKey = String.Empty;
            this.TimeUnit = null;
            this.UpdateExistingLockRecord = false;
        }

        #endregion

        #region Public Methods

        /// <summary>
        /// Adds a session monitor to the AcquireLockOptions object.
        /// 
        /// If the TimeUnit has not been set for the object, you must provide it as a parameter
        /// or the method will throw an ArgumentException.
        /// </summary>
        /// <param name="safeTimeWithoutHeartbeat">The amount of time the lock can go without heartbeating
        /// before the lock is declared ot be in the "danger zone".</param>
        /// <param name="sessionMonitorCallback">The callback to run when the lock's lease enters the danger zone.</param>
        /// <param name="timeUnit">If the TimeUnit has not been specified for the object, provide it
        /// here and it will be added to the AcquireLockOptions object.</param>
        /// <returns>This AcquireLockOptions object</returns>
        public AcquireLockOptions AddSessionMonitor(long safeTimeWithoutHeartbeat, Action sessionMonitorCallback, TimeUnit timeUnit = null)
        {
            if (timeUnit != null)
            {
                this.TimeUnit = timeUnit;
            }

            if (this.TimeUnit == null)
            {
                throw new ArgumentException("You must specify a TimeUnit if it is not already set in order to add a session monitor.");
            }

            this.SessionMonitor = new SessionMonitor(this.TimeUnit.ToMilliseconds(safeTimeWithoutHeartbeat), sessionMonitorCallback);

            return this;
        }

        public AcquireLockOptions AddSessionMonitor(long safeTimeWithoutHeartbeat, TimeUnit timeUnit = null)
        {
            if (timeUnit != null)
            {
                this.TimeUnit = timeUnit;
            }

            if (this.TimeUnit == null)
            {
                throw new ArgumentException("You must specify a TimeUnit if it is not already set in order to add a session monitor.");
            }

            this.SessionMonitor = new SessionMonitor(this.TimeUnit.ToMilliseconds(safeTimeWithoutHeartbeat));

            return this;
        }

        public override int GetHashCode()
        {
            return LockClientUtils.Hash(
                this.AcquireOnlyIfLockAlreadyExists,
                this.AcquireReleasedLocksConsistently,
                this.AdditionalAttributes,
                this.AdditionalTimeToWaitForLock,
                this.Data,
                this.DeleteLockOnRelease,
                this.PartitionKey,
                this.RefreshPeriod,
                this.ReplaceData,
                this.SessionMonitor,
                this.ShouldSkipBlockingWait,
                this.SortKey,
                this.TimeUnit,
                this.UpdateExistingLockRecord);
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

            AcquireLockOptions other = (AcquireLockOptions)obj;

            return this.PartitionKey == other.PartitionKey &&
                this.RefreshPeriod == other.RefreshPeriod &&
                this.ReplaceData == other.ReplaceData &&
                this.SessionMonitor == other.SessionMonitor &&
                this.ShouldSkipBlockingWait == other.ShouldSkipBlockingWait &&
                this.SortKey == other.SortKey &&
                this.TimeUnit == other.TimeUnit &&
                this.UpdateExistingLockRecord == other.UpdateExistingLockRecord &&
                this.AcquireOnlyIfLockAlreadyExists == other.AcquireOnlyIfLockAlreadyExists &&
                this.AcquireReleasedLocksConsistently == other.AcquireReleasedLocksConsistently &&
                this.AdditionalAttributes.SequenceEqual(other.AdditionalAttributes) &&
                this.AdditionalTimeToWaitForLock == other.AdditionalTimeToWaitForLock &&
                // Perhaps not an optimal comparison for MemoryStream, but items must be under 400 KB, so these will always be small
                (this.Data != null && other.Data != null ? LockClientUtils.StreamsEqual(this.Data, other.Data) : this.Data is null == other.Data is null) &&
                this.DeleteLockOnRelease == other.DeleteLockOnRelease;
        }

        public static bool operator ==(AcquireLockOptions left, AcquireLockOptions right)
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

        public static bool operator !=(AcquireLockOptions left, AcquireLockOptions right)
        {
            return !(left == right);
        }

        #endregion
    }
}
