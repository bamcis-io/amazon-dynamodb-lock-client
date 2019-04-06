using Amazon.DynamoDBv2.Model;
using BAMCIS.AWSDynamoDBLockClient;
using BAMCIS.AWSDynamoDBLockClient.Model;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Xunit;

namespace AWSDynamoDBLockClient.Tests
{
    public class ClientTests
    {
        [Fact]
        public void Test1()
        {
            int[] Test = new int[] { 1, 2 };

            IEnumerator E = Test.GetEnumerator();

            E.MoveNext();

            int i = (int)E.Current;

            E.MoveNext();

            i = (int)E.Current;

            E.MoveNext();

            i = (int)E.Current;

            i = 0;
        }

        [Fact]
        public void TestEmptyStreamEquals()
        {
            // ARRANGE
            using (MemoryStream MS1 = new MemoryStream())
            {
                using (MemoryStream MS2 = new MemoryStream())
                {
                    // ACT & ASSERT
                    Assert.True(Utilities.StreamsEqual(MS1, MS2));
                }
            }
        }

        [Fact]
        public void TestStreamEquals()
        {
            // ARRANGE
            byte[] Test = { 0x00, 0x01, 0x02, 0x03 };

            
            using (MemoryStream MS1 = new MemoryStream())
            {
                MS1.Write(Test, 0, Test.Length);
                using (MemoryStream MS2 = new MemoryStream())
                {
                    MS2.Write(Test, 0, Test.Length);
                    // ACT & ASSERT
                    Assert.True(Utilities.StreamsEqual(MS1, MS2));
                }
            }
        }

        [Fact]
        public void TestStreamNotEquals()
        {
            // ARRANGE
            byte[] Test = { 0x00, 0x01, 0x02, 0x03 };

            byte[] Test2 = { 0x00, 0x01, 0x02, 0x04 };


            using (MemoryStream MS1 = new MemoryStream())
            {
                MS1.Write(Test, 0, Test.Length);
                using (MemoryStream MS2 = new MemoryStream())
                {
                    MS2.Write(Test2, 0, Test2.Length);
                    // ACT & ASSERT
                    Assert.False(Utilities.StreamsEqual(MS1, MS2));
                }
            }
        }
    }
}
