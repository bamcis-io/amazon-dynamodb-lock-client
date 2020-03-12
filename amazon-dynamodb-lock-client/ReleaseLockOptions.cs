using System.IO;

namespace Amazon.DynamoDBv2
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

        public bool DeleteLock { get; set; }

        public bool BestEffort { get; set; }

        public MemoryStream Data { get; set; }

        #endregion

        #region Constructors

        /// <summary>
        /// Creates a lock options object
        /// </summary>
        /// <param name="lockItem"></param>
        public ReleaseLockOptions(LockItem lockItem)
        {
            this.LockItem = lockItem;
            this.DeleteLock = true;
            this.BestEffort = false;
            this.Data = null;
        }

        /// <summary>
        /// Finalizer (destructor) to dispose the memory stream
        /// </summary>
        ~ReleaseLockOptions()
        {
            if (this.Data != null)
            {
                this.Data.Dispose();
            }
        }

        #endregion
    }
}
