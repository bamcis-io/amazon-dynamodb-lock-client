using Amazon.DynamoDBv2.Model;
using BAMCIS.AWSDynamoDBLockClient.Util;
using BAMCIS.Util;
using BAMCIS.Util.Concurrent;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace BAMCIS.AWSDynamoDBLockClient
{
    public class AcquireLockOptions
    {
        #region Public Properties

        /// <summary>
        /// The partition key to lock on
        /// </summary>
        public string PartitionKey { get; }

        /// <summary>
        /// The sort key to try and acquire the lock on (specify if and only if the table has sort keys).
        /// </summary>
        public Optional<string> SortKey { get; }

        /// <summary>
        /// Sets data to be stored alongside the lock
        /// </summary>
        public Optional<MemoryStream> Data { get; }

        /// <summary>
        /// Sets whether or not to replace any existing lock data with the data parameter
        /// </summary>
        public bool ReplaceData { get; }

        /// <summary>
        /// Whether or not the lock should be deleted when Close() is called on the resulting LockItem
        /// </summary>
        public bool DeleteLockOnRelease { get; }

        /// <summary>
        /// If this is true, the lock client will only update the current lock record if present, otherwise create a new one.
        /// </summary>
        public bool UpdateExistingLockRecord { get; }

        /// <summary>
        /// If this is true, the lock client will ensure that released locks are acquired consistently in order to preserve lock data in DynamoDB.
        /// 
        /// This option is currently disabled by default.  Will be enabled by default in the next major release.
        /// </summary>
        public bool AcquireReleasedLocksConsistently { get; }

        /// <summary>
        /// Sets whether or not to allow acquiring locks if the lock does not exist already
        /// </summary>
        public bool AcquireOnlyIfLockAlreadyExists { get; }

        /// <summary>
        /// How long to wait before trying to get the lock again (if set to seconds, for example, it would
        /// attempt to do so every 10 seconds). If set, TimeUnit must also be set.
        /// </summary>
        public long RefreshPeriod { get; }

        /// <summary>
        /// How long to wait in addition to the lease duration (if set to 10 minutes, this will try to acquire
        /// a lock for at least 10 minutes before giving up and throwing the exception). If set, TimeUnit must also be set.
        /// </summary>
        public long AdditionalTimeToWaitForLock { get; }

        /// <summary>
        /// The TimeUnit for all time parameters in this object including RefreshPeriod and AdditionalTimeToWaitForLock.
        /// </summary>
        public TimeUnit TimeUnit { get; }

        /// <summary>
        /// Stores some additional attributes with each lock. This can be used to add any arbitrary parameters to each lock row.
        /// </summary>
        public Dictionary<string, AttributeValue> AdditionalAttributes { get; }

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
        public Optional<SessionMonitor> SessionMonitor { get; }

        /// <summary>
        /// Setting this flag to true will prevent the thread from being blocked (put to sleep) for the lease duration and
        /// instead will return the call with the lock not granted exception back to the caller. It is up to the caller to
        /// optionally back-off and retry and to acquire the lock.
        /// </summary>
        public bool ShouldSkipBlockingWait { get; }

        #endregion

        #region Constructors

        /// <summary>
        /// Private constructor for builder object
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="sortKey"></param>
        /// <param name="data"></param>
        /// <param name="replaceData"></param>
        /// <param name="deleteLockOnRelease"></param>
        /// <param name="acquireOnlyIfLockAlreadyExists"></param>
        /// <param name="refreshPeriod"></param>
        /// <param name="additionalTimeToWaitForLock"></param>
        /// <param name="timeUnit"></param>
        /// <param name="additionalAttributes"></param>
        /// <param name="sessionMonitor"></param>
        /// <param name="updateExistingLockRecord"></param>
        /// <param name="shouldSkipBlockingWait"></param>
        /// <param name="acquireReleasedLocksConsistently"></param>
        private AcquireLockOptions(
            string partitionKey,
            Optional<string> sortKey,
            Optional<MemoryStream> data,
            bool replaceData,
            bool deleteLockOnRelease,
            bool acquireOnlyIfLockAlreadyExists,
            long refreshPeriod,
            long additionalTimeToWaitForLock,
            TimeUnit timeUnit,
            Dictionary<string, AttributeValue> additionalAttributes,
            Optional<SessionMonitor> sessionMonitor,
            bool updateExistingLockRecord,
            bool shouldSkipBlockingWait,
            bool acquireReleasedLocksConsistently
            )
        {
            this.PartitionKey = partitionKey;
            this.SortKey = sortKey;
            this.Data = data;
            this.ReplaceData = replaceData;
            this.DeleteLockOnRelease = deleteLockOnRelease;
            this.AcquireOnlyIfLockAlreadyExists = acquireOnlyIfLockAlreadyExists;
            this.RefreshPeriod = refreshPeriod;
            this.AdditionalTimeToWaitForLock = additionalTimeToWaitForLock;
            this.TimeUnit = timeUnit;
            this.AdditionalAttributes = additionalAttributes;
            this.SessionMonitor = sessionMonitor;
            this.UpdateExistingLockRecord = updateExistingLockRecord;
            this.ShouldSkipBlockingWait = shouldSkipBlockingWait;
            this.AcquireReleasedLocksConsistently = acquireReleasedLocksConsistently;
        }

        #endregion

        #region Public Methods

        /// <summary>
        /// Creates a new version of AcquireLockOptionsBuilder using the only required parameter,
        /// which is a specific partition key.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <returns></returns>
        public static AcquireLockOptionsBuilder Builder(string partitionKey)
        {
            return new AcquireLockOptionsBuilder(partitionKey);
        }

        public override int GetHashCode()
        {
            return LockClientUtils.Hash(
                this.AcquireOnlyIfLockAlreadyExists,
                this.AcquireReleasedLocksConsistently,
                this.AdditionalAttributes,
                this.AdditionalTimeToWaitForLock,
                this.Data.Value,
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

            AcquireLockOptions Other = (AcquireLockOptions)obj;

            return this.PartitionKey == Other.PartitionKey &&
                this.RefreshPeriod == Other.RefreshPeriod &&
                this.ReplaceData == Other.ReplaceData &&
                this.SessionMonitor == Other.SessionMonitor &&
                this.ShouldSkipBlockingWait == Other.ShouldSkipBlockingWait &&
                this.SortKey == Other.SortKey &&
                this.TimeUnit == Other.TimeUnit &&
                this.UpdateExistingLockRecord == Other.UpdateExistingLockRecord &&
                this.AcquireOnlyIfLockAlreadyExists == Other.AcquireOnlyIfLockAlreadyExists &&
                this.AcquireReleasedLocksConsistently == Other.AcquireReleasedLocksConsistently &&
                this.AdditionalAttributes.SequenceEqual(Other.AdditionalAttributes) &&
                this.AdditionalTimeToWaitForLock == Other.AdditionalTimeToWaitForLock &&
                // Perhaps not an optimal comparison for MemoryStream, but items must be under 400 KB, so these will always be small
                (this.Data.IsPresent() && Other.Data.IsPresent() ? Utilities.StreamsEqual(this.Data.Value, Other.Data.Value) : this.Data.IsPresent() == Other.Data.IsPresent()) &&
                this.DeleteLockOnRelease == Other.DeleteLockOnRelease;
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

        #region Inner Class

        /// <summary>
        /// A builder for setting up an AcquireLockOptions object. This allows clients to configure their settings when calling
        /// AcquireLock(). The only required parameter is the partition key for the lock.
        /// </summary>
        public class AcquireLockOptionsBuilder
        {
            #region Private Fields

            private string PartitionKey;

            private Optional<string> SortKey;

            private Optional<MemoryStream> Data;

            private bool ReplaceData;

            private bool DeleteLockOnRelease;

            private bool UpdateExistingLockRecord;

            private bool AcquireReleasedLocksConsistently;

            private bool AcquireOnlyIfLockAlreadyExists;

            private long RefreshPeriod;

            private long AdditionalTimeToWaitForLock;

            private TimeUnit TimeUnit;

            private Dictionary<string, AttributeValue> AdditionalAttributes;

            private bool ShouldSkipBlockingWait;

            private long SafeTimeWithoutHeartbeat;

            private Optional<Action> SessionMonitorCallback;

            private bool IsSessionMonitorSet = false;

            #endregion

            #region Constructors

            /// <summary>
            /// Internal constructor called by the parent class
            /// </summary>
            /// <param name="partitionKey"></param>
            internal AcquireLockOptionsBuilder(string partitionKey)
            {
                this.PartitionKey = partitionKey;
                this.AdditionalAttributes = new Dictionary<string, AttributeValue>();
                this.SortKey = Optional<string>.Empty;
                this.Data = Optional<MemoryStream>.Empty;
                this.ReplaceData = true;
                this.DeleteLockOnRelease = true;
                this.AcquireOnlyIfLockAlreadyExists = false;
                this.UpdateExistingLockRecord = false;
                this.ShouldSkipBlockingWait = false;
                this.AcquireReleasedLocksConsistently = false;
            }

            #endregion

            #region Public Methods

            /// <summary>
            ///  The sort key to try and acquire the lock on (specify if and only if the table has sort keys).
            /// </summary>
            /// <param name="sortKey"></param>
            /// <returns></returns>
            public AcquireLockOptionsBuilder WithSortKey(string sortKey)
            {
                this.SortKey = Optional<string>.OfNullable(sortKey);
                return this;
            }

            /// <summary>
            /// Sets data to be stored alongside the lock
            /// </summary>
            /// <param name="data"></param>
            /// <returns></returns>
            public AcquireLockOptionsBuilder WithData(MemoryStream data)
            {
                this.Data = Optional<MemoryStream>.OfNullable(data);
                return this;
            }

            /// <summary>
            /// Sets whether or not to replace any existing lock data with the data parameter.
            /// </summary>
            /// <param name="replaceData"></param>
            /// <returns></returns>
            public AcquireLockOptionsBuilder WithReplaceData(bool replaceData)
            {
                this.ReplaceData = replaceData;
                return this;
            }

            /// <summary>
            /// Sets whether or not the lock should be deleted when Close() is called on the resulting LockItem
            /// </summary>
            /// <param name="deleteLockOnRelease"></param>
            /// <returns></returns>
            public AcquireLockOptionsBuilder WithDeleteLockOnRelease(bool deleteLockOnRelease)
            {
                this.DeleteLockOnRelease = deleteLockOnRelease;
                return this;
            }

            /// <summary>
            /// Sets whether or not to allow acquiring locks if the lock does not exist already
            /// </summary>
            /// <param name="acquireOnlyIfLockAlreadyExists"></param>
            /// <returns></returns>
            public AcquireLockOptionsBuilder WithAcquireOnlyIfLockAlreadyExists(bool acquireOnlyIfLockAlreadyExists)
            {
                this.AcquireOnlyIfLockAlreadyExists = acquireOnlyIfLockAlreadyExists;
                return this;
            }

            /// <summary>
            /// How long to wait before trying to get the lock again (if set to seconds, for example, it would
            /// attempt to do so every 10 seconds). If set, TimeUnit must also be set.
            /// </summary>
            /// <param name="refreshPeriod"></param>
            /// <returns></returns>
            public AcquireLockOptionsBuilder WithRefreshPeriod(long refreshPeriod)
            {
                this.RefreshPeriod = refreshPeriod;
                return this;
            }

            /// <summary>
            /// How long to wait in addition to the lease duration (if set to 10 minutes, this will try to acquire
            /// a lock for at least 10 minutes before giving up and throwing the exception). If set, TimeUnit must also be set.
            /// </summary>
            /// <param name="additionalTimeToWaitForLock"></param>
            /// <returns></returns>
            public AcquireLockOptionsBuilder WithAdditionalTimeToWaitForLock(long additionalTimeToWaitForLock)
            {
                this.AdditionalTimeToWaitForLock = additionalTimeToWaitForLock;
                return this;
            }

            /// <summary>
            /// The TimeUnit for all time parameters in this object including RefreshPeriod and AdditionalTimeToWaitForLock.
            /// </summary>
            /// <param name="timeUnit"></param>
            /// <returns></returns>
            public AcquireLockOptionsBuilder WithTimeUnit (TimeUnit timeUnit)
            {
                this.TimeUnit = timeUnit;
                return this;
            }

            /// <summary>
            /// Stores some additional attributes with each lock. This can be used to add any arbitrary parameters to each lock row.
            /// </summary>
            /// <param name="additionalAttributes"></param>
            /// <returns></returns>
            public AcquireLockOptionsBuilder WithAdditionalAttributes(Dictionary<string, AttributeValue> additionalAttributes)
            {
                this.AdditionalAttributes = new Dictionary<string, AttributeValue>(additionalAttributes);
                return this;
            }

            /// <summary>
            /// With this being true lock client will only update the current lock record if present otherwise create a new one.
            /// </summary>
            /// <param name="updateExistingLockRecord"></param>
            /// <returns></returns>
            public AcquireLockOptionsBuilder WithUpdateExistingLockRecord(bool updateExistingLockRecord)
            {
                this.UpdateExistingLockRecord = updateExistingLockRecord;
                return this;
            }

            /// <summary>
            /// With this being true, the lock client will not block the running thread and wait for lock, rather will fast fail the request,
            /// so that the caller can either choose to back-off and process the same request or start processing a new request.
            /// </summary>
            /// <param name="shouldSkipBlockingWait"></param>
            /// <returns></returns>
            public AcquireLockOptionsBuilder WithShouldSkipBlockingWait(bool shouldSkipBlockingWait)
            {
                this.ShouldSkipBlockingWait = shouldSkipBlockingWait;
                return this;
            }

            /// <summary>
            /// With this being true, the lock client will ensure that released locks are acquired consistently in order to preserve existing
            /// lock data in DynamoDB.
            /// 
            /// This option is currently disabled by default.  Will be enabled by default in the next major release.
            /// </summary>
            /// <param name="acquireReleasedLocksConsistently"></param>
            /// <returns></returns>
            public AcquireLockOptionsBuilder WithAcquireReleasedLocksConsistently(bool acquireReleasedLocksConsistently)
            {
                this.AcquireReleasedLocksConsistently = acquireReleasedLocksConsistently;
                return this;
            }

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
            /// <param name="safeTimeWithoutHeartbeat">The amount of time the lock</param>
            /// <param name="sessionMonitorCallback"></param>
            /// <returns></returns>
            public AcquireLockOptionsBuilder WithSessionMonitor(long safeTimeWithoutHeartbeat, Optional<Action> sessionMonitorCallback)
            {
                this.SafeTimeWithoutHeartbeat = safeTimeWithoutHeartbeat;
                this.SessionMonitorCallback = sessionMonitorCallback;
                this.IsSessionMonitorSet = true;

                return this;
            }

            /// <summary>
            /// Creates a new AcquireLockOptions object
            /// </summary>
            /// <returns></returns>
            public AcquireLockOptions Build()
            {
                Optional<SessionMonitor> SessionMonitor = Optional<SessionMonitor>.Empty;

                if (this.IsSessionMonitorSet)
                {
                    if (this.TimeUnit == null)
                    {
                        throw new ArgumentException("The TimeUnit must not be null if SessionMonitor is non-null");
                       
                    }

                    SessionMonitor = Optional<SessionMonitor>.Of(new SessionMonitor(this.TimeUnit.ToMilliseconds(this.SafeTimeWithoutHeartbeat), this.SessionMonitorCallback));
                }

                return new AcquireLockOptions(
                    this.PartitionKey,
                    this.SortKey,
                    this.Data,
                    this.ReplaceData,
                    this.DeleteLockOnRelease,
                    this.AcquireOnlyIfLockAlreadyExists,
                    this.RefreshPeriod,
                    this.AdditionalTimeToWaitForLock,
                    this.TimeUnit,
                    this.AdditionalAttributes,
                    SessionMonitor,
                    this.UpdateExistingLockRecord,
                    this.ShouldSkipBlockingWait,
                    this.AcquireReleasedLocksConsistently
                );
            }

            public override string ToString()
            {
                return $"AcquireLockOptions.AcquireLockOptionsBuilder(Key={this.PartitionKey}, SortKey={this.SortKey}, Data={this.Data}, ReplaceData={this.ReplaceData}, " +
                    $"DeleteLockOnRelease={this.DeleteLockOnRelease}, RefreshPeriod={this.RefreshPeriod}, AdditionalTimeToWaitForLock={this.AdditionalTimeToWaitForLock}, " +
                    $"TimeUnit={this.TimeUnit}, AdditionalAttributes={this.AdditionalAttributes}, SafeTimeWithoutHeartbeat={this.SafeTimeWithoutHeartbeat}, " +
                    $"SessionMonitorCallback={this.SessionMonitorCallback}, AcquireReleasedLocksConsistently={this.AcquireReleasedLocksConsistently})";
            }

            #endregion
        }

        #endregion
    }
}
