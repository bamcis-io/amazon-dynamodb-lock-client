using BAMCIS.Util;
using System.IO;

namespace BAMCIS.AWSDynamoDBLockClient
{
    /// <summary>
    /// Provides options for releasing a lock whne calling the ReleaseLock() method. This
    /// class contains the options that may be configured during the act of releasing a
    /// lock.
    /// </summary>
    public class ReleaseLockOptions
    {
        #region Public Properties

        public LockItem LockItem { get; }

        public bool DeleteLock { get; }

        public bool BestEffort { get; }

        public Optional<MemoryStream> Data { get; }

        #endregion

        #region Constructors

        public ReleaseLockOptions(LockItem lockItem)
        {
            this.LockItem = lockItem;
        }

        /// <summary>
        /// Creates a new ReleaseLockOptions object
        /// </summary>
        /// <param name="lockItem"></param>
        /// <param name="deleteLock"></param>
        /// <param name="bestEffort"></param>
        /// <param name="data"></param>
        private ReleaseLockOptions(
            LockItem lockItem,
            bool deleteLock,
            bool bestEffort,
            Optional<MemoryStream> data
        )
        {
            this.LockItem = lockItem;
            this.DeleteLock = deleteLock;
            this.BestEffort = bestEffort;
            this.Data = data;
        }

        /// <summary>
        /// Finalizer (destructor) to dispose the memory stream
        /// </summary>
        ~ReleaseLockOptions()
        {
            if (this.Data != null && this.Data.IsPresent())
            {
                this.Data.Value.Dispose();
            }
        }

        #endregion

        #region Public Methods

        /// <summary>
        /// Creates a builder for the ReleaseLockOptions object. The only required parameter is the lock item. The
        /// rest are defaulted, such as deleting the lock on release is set to true and best effort is set to false. 
        /// The builder can be used to customize these parameters.
        /// </summary>
        /// <param name="lockItem"></param>
        /// <returns></returns>
        public static ReleaseLockOptionsBuilder Builder(LockItem lockItem)
        {
            return new ReleaseLockOptionsBuilder(lockItem);
        }

        #endregion

        #region Builder Class

        /// <summary>
        /// A builder for ReleaseLockOptions
        /// </summary>
        public class ReleaseLockOptionsBuilder
        {
            #region Private Fields

            private LockItem LockItem;
            private bool DeleteLock;
            private bool BestEffort;
            private Optional<MemoryStream> Data;

            #endregion

            #region Constructors

            /// <summary>
            /// Creates a new builder
            /// </summary>
            /// <param name="lockItem"></param>
            internal ReleaseLockOptionsBuilder(LockItem lockItem)
            {
                this.LockItem = lockItem;
                this.DeleteLock = true;
                this.BestEffort = false;
                this.Data = null;
            }

            #endregion

            #region Public Methods

            public ReleaseLockOptionsBuilder WithDeleteLock(bool deleteLock)
            {
                this.DeleteLock = deleteLock;
                return this;
            }

            public ReleaseLockOptionsBuilder WithBestEffort(bool bestEffort)
            {
                this.BestEffort = bestEffort;
                return this;
            }

            public ReleaseLockOptionsBuilder WithData(MemoryStream data)
            {
                this.Data = Optional<MemoryStream>.OfNullable(data);
                return this;
            }

            /// <summary>
            /// Creates the ReleaseLockOptions object with the specified configuration
            /// </summary>
            /// <returns></returns>
            public ReleaseLockOptions Build()
            {
                return new ReleaseLockOptions(
                    this.LockItem,
                    this.DeleteLock,
                    this.BestEffort,
                    this.Data
                );
            }

            #endregion
        }

        #endregion

    }
}
