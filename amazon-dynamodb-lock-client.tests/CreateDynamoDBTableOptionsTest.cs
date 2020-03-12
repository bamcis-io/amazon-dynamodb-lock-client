using Amazon.DynamoDBv2.Model;
using Moq;
using Xunit;

namespace Amazon.DynamoDBv2.Tests
{
    public class CreateDynamoDBTableOptionsTest
    {
        AmazonDynamoDBClient dynamodb = new Mock<AmazonDynamoDBClient>().Object;

        [Fact]
        public void Builder_WhenDynamoDBClient_IsSame()
        {
            // ARRANGE
            CreateDynamoDBTableOptions options = new CreateDynamoDBTableOptions(dynamodb, "table") { ProvisionedThroughput = new ProvisionedThroughput(1, 1) };

            // ACT

            // ASSERT
            Assert.True(dynamodb.Equals(options.DynamoDBClient));
        }

        [Fact]
        public void Builder_WhenProvisionedThroughputReset_IsSame()
        {
            // ARRANGE
            ProvisionedThroughput pt = new ProvisionedThroughput(1, 1);
            CreateDynamoDBTableOptions options = new CreateDynamoDBTableOptions(dynamodb, "table") { ProvisionedThroughput = pt };

            // ACT

            // ASSERT
            Assert.True(pt.Equals(options.ProvisionedThroughput));
        }

        [Fact]
        public void builder_whenTableNameReset_isSame()
        {
            // ARRANGE
            string tableName = "table";
            CreateDynamoDBTableOptions options = new CreateDynamoDBTableOptions(dynamodb, tableName) { ProvisionedThroughput = new ProvisionedThroughput(1, 1) };

            // ACT

            // ASSERT
            Assert.Equal(tableName, options.TableName);
        }


        [Fact]
        public void Builder_WhenPartitionKeyNameReset_IsSame()
        {
            // ARRANGE
            CreateDynamoDBTableOptions options = new CreateDynamoDBTableOptions(dynamodb, "table") { ProvisionedThroughput = new ProvisionedThroughput(1, 1), PartitionKeyName = null };

            // ACT

            // ASSERT
            Assert.Null(options.PartitionKeyName);
            string partitionKey = "key";
            options.PartitionKeyName = partitionKey;
            Assert.Equal(partitionKey, options.PartitionKeyName);
        }
    }
}
