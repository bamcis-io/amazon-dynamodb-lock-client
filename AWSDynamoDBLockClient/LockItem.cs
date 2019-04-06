using Amazon.DynamoDBv2.Model;
using BAMCIS.AWSDynamoDBLockClient.Model;
using BAMCIS.AWSDynamoDBLockClient.Util;
using BAMCIS.Util;
using BAMCIS.Util.Concurrent;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;

namespace BAMCIS.AWSDynamoDBLockClient
{
    public class LockItem : IDisposable
    {
        #region Private Fields

        /// <summary>
        /// The current record version number of the lock -- this is globally unique and changes each time the lock is updated
        /// </summary>
        private StringBuilder _RecordVersionNumber;

        /// <summary>
        /// Whether or not the lock was marked as released when loaded from DynamoDB. Does not consider expiration time.
        /// </summary>
        private bool _IsReleased;

        private Optional<SessionMonitor> SessionMonitor;

        private long _LeaseDuration;
        private long _LookupTime;

        #endregion

        #region Public Properties

        /// <summary>
        /// The key associated with this lock, which is unique for every lock (unless the lock has a sort key, in which case key + sortKey is unique.
        /// </summary>
        public string PartitionKey { get; }

        /// <summary>
        /// The sort key associated with the lock, if there is one.
        /// </summary>
        public Optional<string> SortKey { get; }

        /// <summary>
        /// The data associated with the lock, which is optional.
        /// </summary>
        public Optional<MemoryStream> Data { get; }

        /// <summary>
        /// The name of the owner that owns this lock.
        /// </summary>
        public string OwnerName { get; }

        /// <summary>
        /// A boolean indicating whether the lock should be deleted from DynamoDB after release.
        /// </summary>
        public bool DeleteLockItemOnclose { get; }

        /// <summary>
        /// The last time this lock was updated. Note that this will use LockClientUtils.millisecondTime() so it does not represent an actual absolute time.
        /// 
        /// TODO: Should be AtomicLong
        /// </summary>
        public long LookupTime
        {
            get
            {
                return Volatile.Read(ref this._LookupTime);
            }
            private set
            {
                Volatile.Write(ref this._LookupTime, value);
            }
        }

        /// <summary>
        /// The current record version number of the lock in DynamoDB. This is what tells the lock client when the lock is stale.
        /// </summary>
        public string RecordVersionNumber {
            get
            {
                return this._RecordVersionNumber.ToString();
            }
        }
        
        /// <summary>
        /// The amount of time that the client has this lock for, which can be kept up to date by calling SendHeartbeat().
        /// 
        /// TODO: Should be AtomicLong
        /// </summary>
        public long LeaseDuration
        {
            get
            {
                return Volatile.Read(ref this._LeaseDuration);
            }
            private set
            {
                Volatile.Write(ref this._LeaseDuration, value);
            }
        }

        /// <summary>
        /// The additional attributes that can optionally be stored alongside the lock.
        /// </summary>
        public Dictionary<string, AttributeValue> AdditionalAttributes { get; }

        /// <summary>
        /// The AmazonDynamoDBLockClient object associated with this lock
        /// </summary>
        public AmazonDynamoDBLockClient Client { get; }

        #endregion

        #region Constructors

        /// <summary>
        /// Creates a lock item representing a given key. This constructor should only be called by the lock client -- the caller should use the version of
        /// LockItem returned by AcquireLock(). In order to enforce this, it is internal.
        /// </summary>
        /// <param name="client"></param>
        /// <param name="partitionKey"></param>
        /// <param name="sortKey"></param>
        /// <param name="data"></param>
        /// <param name="deleteLockItemOnClose"></param>
        /// <param name="ownerName"></param>
        /// <param name="leaseDuration"></param>
        /// <param name="lastUpdatedTimeInMilliseconds"></param>
        /// <param name="recordVersionNumber"></param>
        /// <param name="isReleased"></param>
        /// <param name="sessionMonitor"></param>
        /// <param name="additionalAttributes"></param>
        internal LockItem(
            AmazonDynamoDBLockClient client,
            string partitionKey,
            Optional<string> sortKey,
            Optional<MemoryStream> data,
            bool deleteLockItemOnClose,
            string ownerName,
            long leaseDuration,
            long lastUpdatedTimeInMilliseconds,
            string recordVersionNumber,
            bool isReleased,
            Optional<SessionMonitor> sessionMonitor,
            Dictionary<string, AttributeValue> additionalAttributes
        )
        {
            LockClientUtils.RequireNonNullOrEmpty(partitionKey, "Cannot create a lock with a null or empty partition key.", "partitionKey");
            LockClientUtils.RequireNonNullOrEmpty(ownerName, "Cannot create a lock with a null or empty owner.", "ownerName");

            if (sortKey == null)
            {
                sortKey = Optional<string>.Empty;
            }
       
            if (data == null)
            {
                data = Optional<MemoryStream>.Empty;
            }

            this.Client = client;
            this.PartitionKey = partitionKey;
            this.SortKey = sortKey;
            this.Data = data;
            this.OwnerName = ownerName;
            this.DeleteLockItemOnclose = deleteLockItemOnClose;

            this.LeaseDuration = leaseDuration;
            this.LookupTime = lastUpdatedTimeInMilliseconds;
            this._RecordVersionNumber = new StringBuilder(recordVersionNumber);
            this._IsReleased = isReleased;
            this.SessionMonitor = sessionMonitor;
            this.AdditionalAttributes = additionalAttributes;
        }

        #endregion

        #region Public Methods

        /// <summary>
        /// Returns whether or not the lock is expired, based on the lease duration and when the last heartbeat was sent.
        /// </summary>
        /// <returns></returns>
        public bool IsExpired()
        {
            if (this.IsReleased())
            {
                return true;
            }

            return LockClientUtils.MillisecondTime() - this.LookupTime > this.LeaseDuration;
        }

        /// <summary>
        /// Returns whether or not the lock was marked as released when loaded from DynamoDB. Does not consider expiration time.
        /// </summary>
        /// <returns>True if the lock was marked as released when loaded from DynamoDB</returns>
        public bool IsReleased()
        {
            return this._IsReleased;
        }

        /// <summary>
        /// Sends a heartbeat to indicate that the given lock is still being worked on. If using CreateHeartbeatBackgroundThread = true
        /// when setting up this object, then this method is unnecessary, because the background thread will be periodically calling it 
        /// and sending heartbeats. However, if CreateHeartbeatBackgroundThread = false, then this method must be called to instruct DynamoDB 
        /// that the lock should not be expired.
        /// 
        /// This is equivalent to calling LockClient.SendHeartbeat(lockItem).
        /// </summary>
        public void SendHeartbeat()
        {
            this.Client?.SendHeartbeat(this);
        }

        /// <summary>
        /// Ensures that this owner has the lock for a specified period of time. If the lock will expire in less than the amount of
        /// time passed in, then this method will do nothing. Otherwise, it will set the LeaseDuration 
        /// to that value and send a heartbeat, such that the lock will expire no sooner than after LeaseDuration
        /// elapses.
        /// 
        /// This method is not required if using heartbeats, because the client could simply call IsExpired 
        /// before every operation to ensure that it still has the lock. However, it is possible for the client 
        /// to instead call this method before executing every operation if they all require different lengths 
        /// of time, and the client wants to ensure it always has enough time.
        ///
        /// This method will throw a LockNotGrantedException if it does not currently hold the lock.
        /// </summary>
        /// <param name="leaseDurationToEnsure"></param>
        /// <param name="timeUnit"></param>
        public void Ensure(long leaseDurationToEnsure, TimeUnit timeUnit)
        {
            if (timeUnit == null)
            {
                throw new ArgumentNullException("timeUnit");
            }

            if (this.IsReleased())
            {
                throw new LockNotGrantedException("Lock is released.");
            }

            // Say the client wants to ensure 10 seconds
            long LeaseDurationToEnsureInMilliseconds = timeUnit.ToMilliseconds(leaseDurationToEnsure);

            // Take the total lease duration that was previously set, say 10000 ms
            // Take current system time and subtract the lookup time, will result in
            // in some relatively small positive number, then take the requested lease
            // duration and subtract that, which gives the amount of time left in the
            // lease, if that is less than the time we want to ensure, send a heartbeat
            // to update the lock item with the new lease period we want to ensure
            if (this.LeaseDuration - (LockClientUtils.MillisecondTime() - this.LookupTime) <= LeaseDurationToEnsureInMilliseconds)
            {
                this.Client?.SendHeartbeat(SendHeartbeatOptions.Builder(this).WithLeaseDurationToEnsure(leaseDurationToEnsure).WithTimeUnit(timeUnit).Build());
            }
        }

        /// <summary>
        /// Updates the last updated time of the lock
        /// </summary>
        /// <param name="lastUpdateOfLock"></param>
        public void UpdateLookupTime(long lastUpdateOfLock)
        {
            this.LookupTime = lastUpdateOfLock;
        }

        /// <summary>
        /// Returns whether or not the lock is entering the "danger zone" time period.
        /// </summary>
        /// <returns>
        /// True is the lock has been released or the lock's lease has entered the "danger zone". False
        /// if the lock has not been released and the lock has not yet entered the "danger zone".
        /// </returns>
        public bool AmIAboutToExpire()
        {
            return this.MillisecondsUntilDangerZoneEntered() <= 0;
        }
       
        /// <summary>
        /// Releases the lock for others to use.
        /// </summary>
        public void Close()
        {
            this.Client?.ReleaseLock(this);
        }

        /// <summary>
        /// Releases the lock for others to use.
        /// </summary>
        public void Dispose()
        {
            this.Close();
        }

        public override string ToString()
        {
            return $"LockItem(Partition Key={this.PartitionKey}, Sort Key={this.SortKey}, Owner Name={this.OwnerName}, Lookup Time={this.LookupTime}, Lease Duration={this.LeaseDuration}, Record Version Number={this.RecordVersionNumber}, Delete On Close={this.DeleteLockItemOnclose}, Is Released={this.IsReleased()})";
        }

        public override int GetHashCode()
        {
            return Utilities.Hash(
                this.PartitionKey,
                this.OwnerName
            );
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

            LockItem Other = (LockItem)obj;

            return this.PartitionKey == Other.PartitionKey &&
                this.OwnerName == Other.OwnerName;
        }

        public static bool operator ==(LockItem left, LockItem right)
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

        public static bool operator !=(LockItem left, LockItem right)
        {
            return !(left == right);
        }

        #endregion

        #region Internal Methods

        /// <summary>
        /// Updates the record version number of the lock. This method is internal -- it should only be called by the lock client.
        /// </summary>
        /// <param name="recordVersionNumber"></param>
        /// <param name="lastUpdateOfLock"></param>
        /// <param name="leaseDurationToEnsureInMilliseconds"></param>
        internal void UpdateRecordVersionNumber(string recordVersionNumber, long lastUpdateOfLock, long leaseDurationToEnsureInMilliseconds)
        {
            this._RecordVersionNumber.Clear();
            this._RecordVersionNumber.Length = 0;
            this._RecordVersionNumber.Append(recordVersionNumber);

            this.LookupTime = lastUpdateOfLock;
            this.LeaseDuration = leaseDurationToEnsureInMilliseconds;
        }

        /// <summary>
        /// Returns whether or not the lock has a SessionMonitor instance.
        /// </summary>
        /// <returns></returns>
        internal bool HasSessionMonitor()
        {
            return this.SessionMonitor.IsPresent();
        }

        /// <summary>
        /// Returns whether or not the lock's SessionMonitor has a callback
        /// </summary>
        /// <returns></returns>
        internal bool HasCallback()
        {
            if (!this.SessionMonitor.IsPresent())
            {
                throw new SessionMonitorNotSetException("SessionMonitor is not set.");
            }

            return this.SessionMonitor.Value.HasCallback();
        }

        /// <summary>
        /// Calls RunCallback() on the SessionMonitor object after checking the SessionMonitor's existence.
        /// </summary>
        internal void RunSessionMonitor()
        {
            if (!this.SessionMonitor.IsPresent())
            {
                throw new SessionMonitorNotSetException("SessionMonitor is not set.");
            }

            this.SessionMonitor.Value.RunCallBack();
        }

        /// <summary>
        /// Returns the unique identifier for the lock so it can be stored in a HashMap under that key
        /// </summary>
        /// <returns></returns>
        internal string GetUniqueIdentifier()
        {
            return $"{this.PartitionKey}{this.SortKey.OrElse("")}";
        }

        /// <summary>
        /// Returns the amount of time left before the lock enters the "danger zone"
        /// </summary>
        /// <returns>
        /// Number of milliseconds before the lock enters the "danger zone". If no heartbeats are sent for it, may be negative if it has already passed.
        /// </returns>
        internal long MillisecondsUntilDangerZoneEntered()
        {
            if (!this.SessionMonitor.IsPresent())
            {
                throw new SessionMonitorNotSetException("SessionMonitor is not set.");
            }

            if (this.IsReleased())
            {
                throw new InvalidOperationException("Lock is already released.");
            }

            return this.SessionMonitor.Value.MillisecondsUntilLeaseEntersDangerZone(this.LookupTime);
        }

        #endregion
    }
}
