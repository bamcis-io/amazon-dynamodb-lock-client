using System;

namespace BAMCIS.AWSDynamoDBLockClient.Model
{
    /// <summary>
    /// Thrown when a lock cannot be granted
    /// </summary>
    public class LockNotGrantedException : DynamoDBLockClientException
    {
        #region Constructors

        /// <summary>
        /// Basic constructor
        /// </summary>
        public LockNotGrantedException() : base()
        { }

        /// <summary>
        /// Constructor with message
        /// </summary>
        /// <param name="message"></param>
        public LockNotGrantedException(string message) : base (message)
        { }

        /// <summary>
        /// Constructor with message and inner exception
        /// </summary>
        /// <param name="message"></param>
        /// <param name="innerException"></param>
        public LockNotGrantedException(string message, Exception innerException) : base(message, innerException)
        { }

        #endregion
    }
}
