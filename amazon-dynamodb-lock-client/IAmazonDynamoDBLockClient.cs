using Amazon.DynamoDBv2.Model;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Amazon.DynamoDBv2
{
    /// <summary>
    /// Provides a simple interface for using the AmazonDynamoDBLockClient. The lock client uses
    /// DynamoDB's consistent read/write feature for managing distributed locks.
    /// </summary>
    public interface IAmazonDynamoDBLockClient
    {
        /// <summary>
        /// Checkes whether the lock table exists in DynamoDB
        /// </summary>
        /// <returns>True if the table exists, false otherwise</returns>
        Task<bool> LockTableExistsAsync();

        /// <summary>
        /// Asserts that the lock table exists in DynamoDB. You can use this method
        /// during application initialization to ensure that the lock client will be
        /// usable. Since this is a no-arg assertion as opposed to a check that returns
        /// a value, this method is also suitable as an init-method.
        /// 
        /// Throws a LockTableDoesNotExistException is the table does not exist.
        /// </summary>
        /// <returns></returns>
        Task AssertLockTableExistsAsync();

        /// <summary>
        /// Creates a DynamoDB table with the right schema for it to be used by this locking library. The table should be set up in advance,
        /// because it takes a few minutes for DynamoDB to provision a new instance. Also, if the table already exists, this will throw an exception.
        /// 
        /// This method lets you specify a sort key to be used by the lock client. This sort key then needs to be specified in the
        /// AmazonDynamoDBLockClientOptions when the lock client object is created.
        /// </summary>
        /// <param name="createDynamoDBTableOptions">The options used to create the lock table</param>
        /// <returns></returns>
        Task<CreateTableResponse> CreateLockTableInDynamoDBAsync(CreateDynamoDBTableOptions createDynamoDBTableOptions);

        /// <summary>
        /// Finds out who owns the given lock, but does not acquire the lock. It returns the metadata currently associated with the
        /// given lock. If the client currently has the lock, it will return the lock, and operations such as ReleaseLock will work.
        /// However, if the client does not have the lock, then operations like ReleaseLock will not work (after calling GetLock, the
        /// caller should check LockItem.IsExpired() to figure out if it currently has the lock.)
        /// </summary>
        /// <param name="key">The partition key representing the lock</param>
        /// <param name="sortKey">The sort key if present</param>
        /// <returns></returns>
        Task<LockItem> GetLockAsync(string key, string sortKey = "");

        /// <summary>
        /// Retrieves the lock item from DynamoDB. Note that this will return a
        /// LockItem even if it was released -- do NOT use this method if your goal
        /// is to acquire a lock for doing work.
        /// </summary>
        /// <param name="options">The options such as the key, etc.</param>
        /// <returns>The LockItemm, or absent if it is not present. Not that the
        /// item can exist in the table even if it is released, as noted by IsReleased()</returns>
        Task<LockItem> GetLockFromDynamoDBAsync(GetLockOptions options);

        /// <summary>
        /// Retrieves all lock items from DynamoDB
        /// 
        /// Note that this may return a lock item even if it was released
        /// </summary>
        /// <param name="deleteOnRelease"></param>
        /// <returns></returns>
        IEnumerable<LockItem> GetAllLocksFromDynamoDB(bool deleteOnRelease, bool consistentRead = false);

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
        Task<LockItem> AcquireLockAsync(AcquireLockOptions options);

        /// <summary>
        /// Attempts to acquire a lock. If successful, returns the lock. Otherwise,
        /// returns Optional.Empty. For more details on behavior, please see AcquireLock().
        /// </summary>
        /// <param name="options"></param>
        /// <returns></returns>
        Task<LockItem> TryAcquireLockAsync(AcquireLockOptions options);

        /// <summary>
        /// Releases the given lock if the current user still has it, returning true if the lock was successfully released, and false
        /// if someone else already stole the lock. Deletes the lock item if it is released and DeleteLockItemOnClose is set.
        /// </summary>
        /// <param name="lockItem">The lock item to release</param>
        /// <returns>True if the lock is released, false otherwise</returns>
        bool ReleaseLock(LockItem lockItem);

        /// <summary>
        /// Releases the given lock if the current user still has it, returning true if the lock was successfully released, and false
        /// if someone else already stole the lock. Deletes the lock item if it is released and DeleteLockItemOnClose is set.
        /// </summary>
        /// <param name="lockItem">The release lock options</param>
        /// <returns>True if the lock is released, false otherwise</returns>
        bool ReleaseLock(ReleaseLockOptions options);

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
        void SendHeartbeat(LockItem lockItem);

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
        void SendHeartbeat(SendHeartbeatOptions options);

        /// <summary>
        /// Loops forever, sending hearbeats for all the locks this thread needs to keep track of.
        /// </summary>
        void Run(CancellationToken ct = default(CancellationToken));

        /// <summary>
        /// Releases all of the locks by calling ReleaseAllLocks()
        /// </summary>
        void Close();
    }
}
