using System;

namespace Amazon.DynamoDBv2.Model
{
    /// <summary>
    /// Thrown to indicate that SessionMonitor is not set
    /// </summary>
    public class SessionMonitorNotSetException : DynamoDBLockClientException
    {
        #region Constructors

        /// <summary>
        /// Basic constructor
        /// </summary>
        public SessionMonitorNotSetException() : base()
        { }

        /// <summary>
        /// Constructor with message
        /// </summary>
        /// <param name="message"></param>
        public SessionMonitorNotSetException(string message) : base(message)
        { }

        /// <summary>
        /// Constructor with message and inner exception.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="innerException"></param>
        public SessionMonitorNotSetException(string message, Exception innerException) : base(message, innerException)
        { }

        #endregion
    }
}
