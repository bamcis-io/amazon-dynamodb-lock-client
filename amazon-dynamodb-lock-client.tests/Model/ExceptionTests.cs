using Amazon.DynamoDBv2.Model;
using System;
using Xunit;

namespace Amazon.DynamoDBv2.Tests.Model
{
    public class ExceptionTests
    {
        [Fact]
        public void ConstructorWithMessageAndCause_SessionMonitorNotSetException()
        {
            // ARRANGE
            ArgumentException AE = new ArgumentException();
            
            // ACT
            SessionMonitorNotSetException E = new SessionMonitorNotSetException("message", AE);

            // ASSERT
            Assert.Equal(AE, E.InnerException);
            Assert.Equal("message", E.Message);
        }

        [Fact]
        public void ConstructorNoArgs_LockNotGrantedException()
        {
            // ARRANGE
            LockNotGrantedException E = new LockNotGrantedException();

            // ACT

            // ASSERT
            Assert.Null(E.InnerException);
        }

        [Fact]
        public void Constructor_LockCurrentlyUnavailableException()
        {
            // ARRANGE
            LockCurrentlyUnavailableException E = new LockCurrentlyUnavailableException();
            
            // ACT

            // ASSERT
            Assert.Null(E.InnerException);
        }

        [Fact]
        public void ConstructorWithMessageAndCause_LockCurrentlyUnavailableException()
        {
            // ARRANGE
            ArgumentException AE = new ArgumentException();

            // ACT
            LockCurrentlyUnavailableException E = new LockCurrentlyUnavailableException("message", AE);

            // ASSERT
            Assert.Equal(AE, E.InnerException);
            Assert.Equal("message", E.Message);
        }

        [Fact]
        public void ConstructorWithCause_LockCurrentlyUnavailableException()
        {
            // ARRANGE
            ArgumentException AE = new ArgumentException();

            // ACT
            LockCurrentlyUnavailableException E = new LockCurrentlyUnavailableException(AE);

            // ASSERT
            Assert.Equal(AE, E.InnerException);
        }

        [Fact]
        public void ConstructorWithMessage_LockCurrentlyUnavailableException()
        {
            // ARRANGE
            ArgumentException AE = new ArgumentException();

            // ACT
            LockCurrentlyUnavailableException E = new LockCurrentlyUnavailableException("message");

            // ASSERT
            Assert.Equal("message", E.Message);
        }

        [Fact]
        public void constructorWithMessageAndCauseAndSuppressionAndStackTrace_LockCurrentlyUnavailableException()
        {
            // ARRANGE
            ArgumentException AE = new ArgumentException();

            // ACT
            LockCurrentlyUnavailableException E = new LockCurrentlyUnavailableException("message", AE);

            // ASSERT
            Assert.Equal(AE, E.InnerException);
            Assert.Equal("message", E.Message);
        }
    }
}
