using Xunit;

namespace Amazon.DynamoDBv2.Tests
{
    public class GetLockOptionsTest
    {
        [Fact]
        public void Test_ExpectedInstanceProduced_WhenChainingMethods()
        {
            // ARRANGE
            GetLockOptions options = new GetLockOptions("key0")
            {
                SortKey = "rangeKey0",
                DeleteLockOnRelease = true
            };

            // ACT

            // ASSERT
            Assert.Equal("key0", options.PartitionKey);
            Assert.True(options.DeleteLockOnRelease);
            Assert.Equal("rangeKey0", options.SortKey);
        }
    }
}
