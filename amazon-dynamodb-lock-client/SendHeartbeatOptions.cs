using BAMCIS.Util.Concurrent;
using System.IO;

namespace Amazon.DynamoDBv2
{
    /// <summary>
    /// A simple class for sending lock heartbeats or updating the lock data with
    /// various combinations of overrides of the default behavior. This class avoids having
    /// to make every combination of override to the SendHeartBeat() method, or to have the
    /// user specify every argument.
    /// </summary>
    public class SendHeartbeatOptions
    {
        #region Public Properties

        /// <summary>
        /// The lock item
        /// </summary>
        public LockItem LockItem { get; }

        public MemoryStream Data { get; set; }

        public bool DeleteData { get; set; }

        public long LeaseDurationToEnsure { get; set; }

        public TimeUnit TimeUnit { get; set; }

        #endregion

        #region Constructors

        /// <summary>
        /// Creates a new options object
        /// </summary>
        /// <param name="lockItem"></param>
        public SendHeartbeatOptions(LockItem lockItem)
        {
            this.LockItem = lockItem;
            this.Data = null;
            this.TimeUnit = TimeUnit.MILLISECONDS;
            this.DeleteData = false;
            this.LeaseDurationToEnsure = 0;
        }

        /// <summary>
        /// Finalizer (destructor) that ensures the memory stream is disposed
        /// </summary>
        ~SendHeartbeatOptions()
        {
            if (this.Data != null)
            {
                this.Data.Dispose();
            }
        }

        #endregion
    }
}
