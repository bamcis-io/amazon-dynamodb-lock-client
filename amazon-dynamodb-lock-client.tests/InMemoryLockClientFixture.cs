using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Amazon.DynamoDBv2.Tests
{
    public class InMemoryLockClientFixture : IDisposable
    {
        #region Protected Fields

        protected IAmazonDynamoDB idynamodb;
        protected static readonly string TABLE_NAME = "test";
        protected static readonly string RANGE_KEY_TABLE_NAME = "rangeTest";

        #endregion

        #region Constructors

        public InMemoryLockClientFixture()
        {
           Task.Run(() => this.Setup()).Wait();
        }

        #endregion

        #region Public Methods
       
        public void Dispose()
        {
            Task.Run(() => this.DeleteTables()).Wait();
        }

        #endregion

        #region Private Methods

        private async Task Setup()
        {
            AWSCredentials credentials = new BasicAWSCredentials(TABLE_NAME, "d");
            string endpoint = String.IsNullOrEmpty(Environment.GetEnvironmentVariable("dynamodb-local.endpoint")) ? "http://localhost:4567" : Environment.GetEnvironmentVariable("dynamodb-local.endpoint");

            AmazonDynamoDBConfig config = new AmazonDynamoDBConfig()
            {
                ServiceURL = endpoint
            };

            this.idynamodb = new AmazonDynamoDBClient(credentials, config);

            await DeleteTables();
            await CreateTables();          
        }

        private async Task CreateTables()
        {
            try
            {

                CreateTableResponse response = await AmazonDynamoDBLockClient.StaticCreateLockTableInDynamoDBAsync(new CreateDynamoDBTableOptions(this.idynamodb, TABLE_NAME) { ProvisionedThroughput = new ProvisionedThroughput(10, 10) });
                TableDescription description = response.TableDescription;

                while (description.TableStatus != TableStatus.ACTIVE)
                {
                    Thread.Sleep(500);
                    description = (await this.idynamodb.DescribeTableAsync(TABLE_NAME)).Table;
                }
            }
            catch (ResourceInUseException)
            {
                // Tables already exists
            }

            try
            {
                CreateTableResponse response = await AmazonDynamoDBLockClient.StaticCreateLockTableInDynamoDBAsync(new CreateDynamoDBTableOptions(this.idynamodb, RANGE_KEY_TABLE_NAME) { ProvisionedThroughput = new ProvisionedThroughput(10, 10), SortKeyName = "rangeKey" });

                TableDescription description = response.TableDescription;

                while (description.TableStatus != TableStatus.ACTIVE)
                {
                    Thread.Sleep(500);
                    description = (await this.idynamodb.DescribeTableAsync(RANGE_KEY_TABLE_NAME)).Table;
                }
            }
            catch (ResourceInUseException)
            {
                // Table already exists
            }
        }

        private async Task DeleteTables()
        {
           

            try
            {
                await this.idynamodb.DeleteTableAsync(TABLE_NAME);

                while (true)
                {
                    Thread.Sleep(500);
                    await this.idynamodb.DescribeTableAsync(TABLE_NAME);
                }
            }
            catch (ResourceNotFoundException)
            {
                // Do nothing, this is what we want
            }

            try
            {
                await this.idynamodb.DeleteTableAsync(RANGE_KEY_TABLE_NAME);

                while (true)
                {
                    Thread.Sleep(500);
                    await this.idynamodb.DescribeTableAsync(RANGE_KEY_TABLE_NAME);
                }
            }
            catch (ResourceNotFoundException)
            {
                // Do nothing, this is what we want
            }
        }

        private static string ByteBufferToString(MemoryStream buffer, Encoding charset)
        {
            return charset.GetString(buffer.ToArray());
        }

        #endregion
    }
}
