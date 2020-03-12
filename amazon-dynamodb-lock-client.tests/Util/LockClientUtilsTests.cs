using Amazon.DynamoDBv2.Util;
using System;
using System.IO;
using System.Linq;
using Xunit;

namespace Amazon.DynamoDBv2.Tests.Util
{
    public class LockClientUtilsTests
    {
        [Fact]
        public void RequireNonNull()
        {
            // ARRANGE
            object Test = null;

            // ACT


            // ASSERT
            Assert.Throws<ArgumentNullException>(() => LockClientUtils.RequireNonNull(Test, "null", "test"));
        }

        [Fact]
        public void RequireNonNull2()
        {
            // ARRANGE
            object Test = null;

            // ACT


            // ASSERT
            Assert.Throws<ArgumentException>(() => LockClientUtils.RequireNonNull(Test, "null"));
        }

        [Fact]
        public void RequireNonNullOrEmpty()
        {
            // ARRANGE
            string Test = String.Empty;

            // ACT


            // ASSERT
            Assert.Throws<ArgumentNullException>(() => LockClientUtils.RequireNonNullOrEmpty(Test, "null", "test"));
        }

        [Fact]
        public void RequireNonNullOrEmpty2()
        {
            // ARRANGE
            string Test = String.Empty;

            // ACT


            // ASSERT
            Assert.Throws<ArgumentException>(() => LockClientUtils.RequireNonNullOrEmpty(Test, "null"));
        }

        [Fact]
        public void DecomposeInt64Positive()
        {
            // ARRANGE
            long Value = (long)Int32.MaxValue + 1;

            // ACT
            int[] Split = LockClientUtils.Decompose64BitInt(Value).ToArray();

            // ASSERT
            Assert.Equal(2, Split.Length);
            Assert.Equal(Int32.MaxValue, Split[0]);
            Assert.Equal(1, Split[1]);
        }

        [Fact]
        public void DecomposeInt64Negative()
        {
            // ARRANGE
            long Value = (long)Int32.MinValue - 5;

            // ACT
            int[] Split = LockClientUtils.Decompose64BitInt(Value).ToArray();

            // ASSERT
            Assert.Equal(2, Split.Length);
            Assert.Equal(Int32.MinValue, Split[0]);
            Assert.Equal(-5, Split[1]);
        }

        [Fact]
        public void MillisecondTime()
        {
            // ARRANGE
            long Millis = LockClientUtils.MillisecondTime();

            // ACT

            // ASSERT
            Assert.True(Millis > 0);
        }

        [Fact]
        public void HashTest()
        {
            // ARRANGE
            int Hash = LockClientUtils.Hash("test", 1, TimeSpan.MinValue);

            // ACT

            // ASSERT
            
        }

        [Fact]
        public void TestEmptyStreamEquals()
        {
            // ARRANGE
            using (MemoryStream ms1 = new MemoryStream())
            {
                using (MemoryStream ms2 = new MemoryStream())
                {
                    // ACT & ASSERT
                    Assert.True(LockClientUtils.StreamsEqual(ms1, ms2));
                }
            }
        }

        [Fact]
        public void TestStreamEquals()
        {
            // ARRANGE
            byte[] test = { 0x00, 0x01, 0x02, 0x03 };

            using (MemoryStream ms1 = new MemoryStream())
            {
                ms1.Write(test, 0, test.Length);
                
                using (MemoryStream ms2 = new MemoryStream())
                {
                    ms2.Write(test, 0, test.Length);
                    // ACT & ASSERT
                    Assert.True(LockClientUtils.StreamsEqual(ms1, ms2));
                }
            }
        }

        [Fact]
        public void TestStreamNotEquals()
        {
            // ARRANGE
            byte[] test = { 0x00, 0x01, 0x02, 0x03 };
            byte[] test2 = { 0x00, 0x01, 0x02, 0x04 };

            using (MemoryStream ms1 = new MemoryStream())
            {
                ms1.Write(test, 0, test.Length);
                
                using (MemoryStream ms2 = new MemoryStream())
                {
                    ms2.Write(test2, 0, test2.Length);
                    // ACT & ASSERT
                    Assert.False(LockClientUtils.StreamsEqual(ms1, ms2));
                }
            }
        }
    }
}
