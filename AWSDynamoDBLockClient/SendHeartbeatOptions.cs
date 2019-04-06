using BAMCIS.Util;
using BAMCIS.Util.Concurrent;
using System.IO;

namespace BAMCIS.AWSDynamoDBLockClient
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

        public Optional<MemoryStream> Data { get; }

        public bool DeleteData { get; }

        public long LeaseDurationToEnsure { get; }

        public TimeUnit TimeUnit { get; }

        #endregion

        #region Constructors

        /// <summary>
        /// Creates a new options object
        /// </summary>
        /// <param name="lockItem"></param>
        /// <param name="data"></param>
        /// <param name="deleteData"></param>
        /// <param name="leaseDurationToEnsure"></param>
        /// <param name="timeUnit"></param>
        private SendHeartbeatOptions(
            LockItem lockItem, 
            Optional<MemoryStream> data, 
            bool deleteData, 
            long leaseDurationToEnsure, 
            TimeUnit timeUnit
        )
        {
            this.LockItem = lockItem;
            this.Data = data;
            this.DeleteData = deleteData;
            this.LeaseDurationToEnsure = leaseDurationToEnsure;
            this.TimeUnit = timeUnit;
        }

        /// <summary>
        /// Finalizer (destructor) that ensures the memory stream is disposed
        /// </summary>
        ~SendHeartbeatOptions()
        {
            if (this.Data != null && this.Data.IsPresent())
            {
                this.Data.Value.Dispose();
            }
        }

        #endregion

        #region Public Methods

        /// <summary>
        /// Creates a new SendHeartbeatOptionsBuilder, which can be used for setting
        /// options for the SendHeartBeat() method in the lock client. The only required
        /// parameter is the lock item.
        /// </summary>
        /// <param name="lockItem">The lock to send a heartbeat for</param>
        /// <returns></returns>
        public static SendHeartbeatOptionsBuilder Builder(LockItem lockItem)
        {
            return new SendHeartbeatOptionsBuilder(lockItem);
        }

        #endregion

        #region Builder Class

        /// <summary>
        /// A builder for the SendHeartBeatOptions object
        /// </summary>
        public class SendHeartbeatOptionsBuilder
        {
            #region Private Fields

            private LockItem LockItem;
            private Optional<MemoryStream> Data;
            private bool DeleteData;
            private long LeaseDurationToEnsure;
            private TimeUnit TimeUnit;

            #endregion

            #region Constructors

            /// <summary>
            /// Creates the builder with a default time unit of milliseconds
            /// </summary>
            /// <param name="lockItem"></param>
            internal SendHeartbeatOptionsBuilder(LockItem lockItem)
            {
                this.LockItem = lockItem;
                this.Data = null;
                this.TimeUnit = TimeUnit.MILLISECONDS;
            }

            #endregion

            #region Public Methods

            public SendHeartbeatOptionsBuilder WithData(MemoryStream data)
            {
                this.Data = Optional<MemoryStream>.OfNullable(data);
                return this;
            }

            public SendHeartbeatOptionsBuilder WithDeleteData(bool deleteData)
            {
                this.DeleteData = deleteData;
                return this;
            }

            public SendHeartbeatOptionsBuilder WithLeaseDurationToEnsure(long leaseDurationToEnsure)
            {
                this.LeaseDurationToEnsure = leaseDurationToEnsure;
                return this;
            }

            public SendHeartbeatOptionsBuilder WithTimeUnit(TimeUnit timeUnit)
            {
                this.TimeUnit = timeUnit;
                return this;
            }

            /// <summary>
            /// Creates the SendHeartbeatOptions object with the provided configuration
            /// </summary>
            /// <returns></returns>
            public SendHeartbeatOptions Build()
            {
                return new SendHeartbeatOptions(
                    this.LockItem,
                    this.Data,
                    this.DeleteData,
                    this.LeaseDurationToEnsure,
                    this.TimeUnit
                );
            }

            #endregion
        }

        #endregion
    }
}
