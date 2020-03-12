using Amazon.DynamoDBv2.Util;
using System;
using System.Threading.Tasks;

namespace Amazon.DynamoDBv2
{
    /// <summary>
    /// Handles the information regarding a lock's lease entering a "danger zone", or alarming period of time before
    /// the lock's lease expires.
    /// </summary>
    public class SessionMonitor
    {
        #region Private Fields

        /// <summary>
        /// The action run run when the lock's lease enters the danger zone
        /// </summary>
        private Action Callback { get; }

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
        /// Creates a session monitor object with a callback
        /// </summary>
        /// <param name="safeTimeWithoutHeartbeatMillis"></param>
        /// <param name="callback"></param>
        public SessionMonitor(long safeTimeWithoutHeartbeatMillis, Action callback)
        {
            this.SafeTimeWithoutHeartbeatMillis = safeTimeWithoutHeartbeatMillis;
            this.Callback = callback ?? throw new ArgumentNullException("callback");
        }

        /// <summary>
        /// Creates a sesson monitor object
        /// </summary>
        /// <param name="safeTimeWithoutHeartbeatMillis"></param>
        public SessionMonitor(long safeTimeWithoutHeartbeatMillis)
        {
            this.SafeTimeWithoutHeartbeatMillis = safeTimeWithoutHeartbeatMillis;
            this.Callback = null;
        }

        #endregion

        #region Public Methods

        /// <summary>
        /// Given the last time the lease was renewed, determines the number of milliseconds
        /// until the lease will enter the period in which the callback should be fired
        /// </summary>
        /// <param name="lastAbsoluteTimeUpdatedMillis"></param>
        /// <returns></returns>
        /// Marked virtual to be able to mock this method
        public virtual long MillisecondsUntilLeaseEntersDangerZone(long lastAbsoluteTimeUpdatedMillis)
        {
            long now = LockClientUtils.MillisecondTime();
            long time = (lastAbsoluteTimeUpdatedMillis + this.SafeTimeWithoutHeartbeatMillis) - now;
            return time;
        }

        /// <summary>
        /// Creates a background thread on the thread pool to run the callback
        /// </summary>
        /// <returns>The running task if a callback was present or null if there was no callback set</returns>
        public Task RunCallBack()
        {
            if (this.Callback != null && this.Callback != null)
            {
                // Run the action in a background thread
                return Task.Run(this.Callback);
            }

            return null;
        }

        /// <summary>
        /// Returns whether or not the callback is non-null
        /// </summary>
        /// <returns></returns>
        public bool HasCallback()
        {
            return this.Callback != null;
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
