using System;

namespace Amazon.DynamoDBv2.Model
{
    /// <summary>
    /// Base abstract class for all exceptions in this client
    /// </summary>
    public abstract class DynamoDBLockClientException : Exception
    {
        #region Constructors

        protected DynamoDBLockClientException() : base() { }

        protected DynamoDBLockClientException(string message) : base(message) { }

        protected DynamoDBLockClientException(string message, Exception innerException) : base(message, innerException) { }

        #endregion
    }
}
