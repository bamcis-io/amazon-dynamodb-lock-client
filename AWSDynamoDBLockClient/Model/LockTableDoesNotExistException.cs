using System;

namespace BAMCIS.AWSDynamoDBLockClient.Model
{
    /// <summary>
    /// Thrown if the lock table does not exist
    /// </summary>
    public class LockTableDoesNotExistException : DynamoDBLockClientException
    {
        #region Constructors

        /// <summary>
        /// Basic constructor
        /// </summary>
        public LockTableDoesNotExistException() : base()
        { }

        /// <summary>
        /// Constructor with message
        /// </summary>
        /// <param name="message"></param>
        public LockTableDoesNotExistException(string message) : base(message)
        { }

        /// <summary>
        /// Constructor with message and inner exception
        /// </summary>
        /// <param name="message"></param>
        /// <param name="innerException"></param>
        public LockTableDoesNotExistException(string message, Exception innerException) : base(message, innerException)
        { }

        #endregion
    }
}
