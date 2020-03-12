using Moq;
using System;
using Xunit;

namespace Amazon.DynamoDBv2.Tests
{
    public class SessionMonitorTests
    {
        [Fact]
        public void IsLeaseEnteringDangerZone_WhenThereAreZeroOrLessMillisUntilEnterDangerZone_ReturnTrue()
        {
            // ARRANGE
            var sut = new Mock<SessionMonitor>(1000);
            sut.Setup(x => x.MillisecondsUntilLeaseEntersDangerZone(25)).Returns(0);

            // ACT

            // ASSERT
            Assert.True(sut.Object.IsLeaseEnteringDangerZone(25));
        }

        [Fact]
        public void IsLeaseEnteringDangerZone_WhenThereAreMoreThanZeroMillisUntilEnterDangerZone_ReturnFalse()
        {
            // ARRANGE
            var sut = new Mock<SessionMonitor>(1000);
            sut.Setup(x => x.MillisecondsUntilLeaseEntersDangerZone(25)).Returns(1);

            // ACT

            // ASSERT
            Assert.False(sut.Object.IsLeaseEnteringDangerZone(25));
        }

        [Fact]
        public void RunCallback_WhenNotPresent_DoesNothing()
        {
            // ARRANGE
            SessionMonitor sut = new SessionMonitor(1000);

            // ACT
            sut.RunCallBack();

            // ASSERT
        }

        [Fact]
        public void HasCallback_WhenCallbackNull_ReturnFalse()
        {
            // ARRANGE
            SessionMonitor sut = new SessionMonitor(1000);

            // ACT

            // ASSERT
            Assert.False(sut.HasCallback());
        }

        [Fact]
        public void HasCallback_WhenCallbackNotNull_ReturnTrue()
        {
            // ARRANGE
            SessionMonitor sut = new SessionMonitor(1000, new Action(() =>  Console.WriteLine("test") ));

            // ACT

            // ASSERT
            Assert.True(sut.HasCallback());
        }
    }
}
