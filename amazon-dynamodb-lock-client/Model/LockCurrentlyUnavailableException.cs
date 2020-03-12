using System;

namespace Amazon.DynamoDBv2.Model
{
    /// <summary>
    /// This is a retry-able exception that indicates that the lock being requested has already been held by another worker
    /// and had not yet been released and the lease duration has not expired since the lock was last updated by the
    /// current owner.
    /// 
    /// The caller can retry acquiring the lock with or without a backoff.
    /// </summary>
    public class LockCurrentlyUnavailableException : DynamoDBLockClientException
    {
        #region Constructors

        /// <summary>
        /// Basic constructor
        /// </summary>
        public LockCurrentlyUnavailableException() : base()
        { }

        /// <summary>
        /// Constructor with message
        /// </summary>
        /// <param name="message"></param>
        public LockCurrentlyUnavailableException(string message) : base(message)
        { }

        /// <summary>
        /// Constructor with exception, the exception message is used as the message
        /// for this exception
        /// </summary>
        /// <param name="e"></param>
        public LockCurrentlyUnavailableException(Exception e) : base(e.Message, e)
        { }

        /// <summary>
        /// Constructor with message and inner exception
        /// </summary>
        /// <param name="message"></param>
        /// <param name="innerException"></param>
        public LockCurrentlyUnavailableException(string message, Exception innerException) : base(message, innerException)
        { }

        #endregion
    }
}
