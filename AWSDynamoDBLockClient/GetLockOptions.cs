using BAMCIS.Util;
using System;

namespace BAMCIS.AWSDynamoDBLockClient
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
        public string PartitionKey { get; }

        /// <summary>
        /// An optional sort key
        /// </summary>
        public Optional<string> SortKey { get; }

        /// <summary>
        /// Specifies is the lock is deleted when released
        /// </summary>
        public bool DeleteLockOnRelease { get; }

        #endregion

        #region Constructors

        /// <summary>
        /// Default constructor called from builder class
        /// </summary>
        /// <param name="key"></param>
        /// <param name="sortKey"></param>
        /// <param name="deleteLockOnRelease"></param>
        private GetLockOptions(string key, Optional<string> sortKey, bool deleteLockOnRelease)
        {
            this.PartitionKey = key;
            this.SortKey = sortKey;
            this.DeleteLockOnRelease = deleteLockOnRelease;
        }

        #endregion

        #region Public Methods

        /// <summary>
        /// Returns a new builder object
        /// </summary>
        /// <param name="partitionKey">The partition key for the lock options builder</param>
        /// <returns></returns>
        public static GetLockOptionsBuilder Builder(string partitionKey)
        {
            return new GetLockOptionsBuilder(partitionKey);
        }

        #endregion

        #region Inner Class

        /// <summary>
        /// A builder interface for creating the GetLockOptions object
        /// </summary>
        public class GetLockOptionsBuilder
        {
            #region Private Fields

            private string PartitionKey;
            private Optional<string> SortKey;
            private bool DeleteLockOnRelease;

            #endregion

            #region Constructors

            internal GetLockOptionsBuilder(string partitionKey)
            {
                this.SortKey = Optional<string>.Empty;
                this.DeleteLockOnRelease = false;
                this.PartitionKey = partitionKey ?? throw new ArgumentNullException("partitionKey");
            }

            #endregion

            #region Public Methods

            public GetLockOptionsBuilder WithSortKey(string sortKey)
            {
                this.SortKey = Optional<string>.OfNullable(sortKey);
                return this;
            }

            public GetLockOptionsBuilder WithDeleteLockOnRelease(bool deleteLockOnRelease)
            {
                this.DeleteLockOnRelease = deleteLockOnRelease;
                return this;
            }

            public GetLockOptions Build()
            {
                return new GetLockOptions(this.PartitionKey, this.SortKey, this.DeleteLockOnRelease);
            }

            public override string ToString()
            {
                return $"GetLockOptions.GetLockOptionsBuilder(partitionKey={this.PartitionKey}, sortKey={this.SortKey}, deleteLockOnRelease={this.DeleteLockOnRelease})";
            }

            #endregion

        }

        #endregion
    }
}
