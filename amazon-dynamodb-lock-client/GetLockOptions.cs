using System;

namespace Amazon.DynamoDBv2
{
    /// <summary>
    /// Provides options for getting a lock when calling the GetLock() method. In order
    /// to use this, it must be created using the builder.
    /// </summary>
    public class GetLockOptions
    {
        #region Public Properties

        /// <summary>
        /// The partition key
        /// </summary>
        public string PartitionKey { get; set; }

        /// <summary>
        /// An optional sort key
        /// </summary>
        public string SortKey { get; set; }

        /// <summary>
        /// Specifies is the lock is deleted when released
        /// </summary>
        public bool DeleteLockOnRelease { get; set; }

        #endregion

        #region Constructors

        /// <summary>
        /// Default constructor
        /// </summary>
        public GetLockOptions(string key)
        {
            this.PartitionKey = key;
        }

        /// <summary>
        /// Constructor with optional properties
        /// </summary>
        /// <param name="key"></param>
        /// <param name="sortKey"></param>
        /// <param name="deleteLockOnRelease"></param>
        public GetLockOptions(string key, bool deleteLockOnRelease) : this(key)
        {
            this.SortKey = String.Empty;
            this.DeleteLockOnRelease = deleteLockOnRelease;
        }

        /// <summary>
        /// Constructor with hash key and sort key
        /// </summary>
        /// <param name="key"></param>
        /// <param name="sortKey"></param>
        public GetLockOptions(string key, string sortKey) : this(key, sortKey, false) { }

        /// <summary>
        /// Constructor with all properties.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="sortKey"></param>
        /// <param name="deleteLockOnRelease"></param>
        public GetLockOptions(string key, string sortKey, bool deleteLockOnRelease) : this(key, deleteLockOnRelease)
        {
            this.SortKey = sortKey;
        }

        #endregion
    }
}
