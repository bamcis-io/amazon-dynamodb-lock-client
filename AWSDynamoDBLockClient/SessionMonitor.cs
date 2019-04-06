using BAMCIS.Util;
using System;
using System.Threading.Tasks;

namespace BAMCIS.AWSDynamoDBLockClient
{
    /// <summary>
    /// Handles the information regarding a lock's lease entering a "danger zone", or alarming period of time before
    /// the lock's lease expires.
    /// </summary>
    public sealed class SessionMonitor
    {
        #region Private Fields

        /// <summary>
        /// The action run run when the lock's lease enters the danger zone
        /// </summary>
        private Optional<Action> Callback { get; }

        #endregion

        #region Public Properties

        /// <summary>
        /// The amount of time in milliseconds the lock can go without heartbeating
        /// before the lock is declared to be in the "danger zone"
        /// </summary>
        public long SafeTimeWithoutHeartbeatMillis { get; }

        #endregion

        #region Constructors

        /// <summary>
        /// Creates a session monitor object
        /// </summary>
        /// <param name="safeTimeWithoutHeartbeatMillis"></param>
        /// <param name="callback"></param>
        public SessionMonitor(long safeTimeWithoutHeartbeatMillis, Optional<Action> callback)
        {
            this.SafeTimeWithoutHeartbeatMillis = safeTimeWithoutHeartbeatMillis;
            this.Callback = callback;
        }

        #endregion

        #region Public Methods

        /// <summary>
        /// Given the last time the lease was renewed, determines the number of milliseconds
        /// until the lease will enter the period in which the callback should be fired
        /// </summary>
        /// <param name="lastAbsoluteTimeUpdatedMillis"></param>
        /// <returns></returns>
        public long MillisecondsUntilLeaseEntersDangerZone(long lastAbsoluteTimeUpdatedMillis)
        {
            return lastAbsoluteTimeUpdatedMillis + this.SafeTimeWithoutHeartbeatMillis - (DateTime.Now.Ticks / 10000);
        }

        /// <summary>
        /// Creates a background thread on the thread pool to run the callback
        /// </summary>
        public void RunCallBack()
        {
            if (this.Callback != null && this.Callback.IsPresent())
            {
                // Run the action in a background thread
                Task Job = Task.Run(this.Callback.Value);
            }
        }

        /// <summary>
        /// Returns whether or not the callback is non-null
        /// </summary>
        /// <returns></returns>
        public bool HasCallback()
        {
            return this.Callback != null && this.Callback.IsPresent();
        }

        /// <summary>
        /// Given the last time the lease was renewed, determines whether or not the lease has 
        /// entered the period in which the callback should be fired.
        /// </summary>
        /// <param name="lastAbsoluteTimeUpdatedMillis"></param>
        /// <returns>
        /// If the current time is greater than or equal to the projected time at which 
        /// the lease is said to go into the "danger zone", false otherwise 
        /// </returns>
        public bool IsLeaseEnteringDangerZone(long lastAbsoluteTimeUpdatedMillis)
        {
            return this.MillisecondsUntilLeaseEntersDangerZone(lastAbsoluteTimeUpdatedMillis) <= 0;
        }

        #endregion
    }
}
