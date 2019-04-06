using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using BAMCIS.Util;

namespace BAMCIS.AWSDynamoDBLockClient
{
    /// <summary>
    /// An options class for the CreateDynamoDBTable method in the lock client. This
    /// allows the user to create a DynamoDB table that is lock client-compatible and 
    /// specify optional parameters such as the desired throughput and whether or not
    /// to use a sort key.
    /// </summary>
    public class CreateDynamoDBTableOptions
    {
        #region Public Properties

        /// <summary>
        /// The client used to create the table
        /// </summary>
        public AmazonDynamoDBClient DynamoDBClient { get; }

        /// <summary>
        /// The provisioned throughput for the table. If you set BillingMode as PROVISIONED, you must specify this property. If you
        /// set BillingMode as PAY_PER_REQUEST, you cannot specify this property.
        /// </summary>
        public Optional<ProvisionedThroughput> ProvisionedThroughput { get; }

        /// <summary>
        /// The billing mode for the table. 
        /// Controls how you are charged for read and write throughput and how you manage
        /// capacity. This setting can be changed later.
        /// PROVISIONED - Sets the billing mode to PROVISIONED. We recommend using PROVISIONED
        /// for predictable workloads.
        /// PAY_PER_REQUEST - Sets the billing mode to PAY_PER_REQUEST. We recommend using
        /// PAY_PER_REQUEST for unpredictable workloads.
        /// </summary>
        public BillingMode BillingMode { get; }

        /// <summary>
        /// The name of the table to create
        /// </summary>
        public string TableName { get; }

        /// <summary>
        /// The name of the partition key for the table
        /// </summary>
        public string PartitionKeyName { get; }

        /// <summary>
        /// The optional sort key to use with the table
        /// </summary>
        public Optional<string> SortKeyName { get; }

        #endregion

        #region Constructors

        /// <summary>
        /// Private constructor for builder object
        /// </summary>
        /// <param name="dynamoDBClient"></param>
        /// <param name="provisionedThroughput"></param>
        /// <param name="billingMode"></param>
        /// <param name="tableName"></param>
        /// <param name="partitionKeyName"></param>
        /// <param name="sortKeyName"></param>
        private CreateDynamoDBTableOptions(
            AmazonDynamoDBClient dynamoDBClient, 
            Optional<ProvisionedThroughput> provisionedThroughput, 
            BillingMode billingMode,
            string tableName, 
            string partitionKeyName, 
            Optional<string> sortKeyName
        )
        {
            this.DynamoDBClient = dynamoDBClient;
            this.ProvisionedThroughput = provisionedThroughput;
            this.BillingMode = billingMode;
            this.TableName = tableName;
            this.PartitionKeyName = partitionKeyName;
            this.SortKeyName = sortKeyName;
        }

        #endregion

        #region Public Methods

        /// <summary>
        /// Creates a builder for a CreateDynamoDBTableOptions object. The three required
        /// parameters are the DynamoDB client, the provisioned throughput, and the table name.
        /// </summary>
        /// <param name="dynamoDBClient"></param>
        /// <param name="provisionedThroughput"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public static CreateDynamoDBTableOptionsBuilder Builder(AmazonDynamoDBClient dynamoDBClient, ProvisionedThroughput provisionedThroughput, string tableName)
        {
            return new CreateDynamoDBTableOptionsBuilder(dynamoDBClient, provisionedThroughput, tableName);
        }

        /// <summary>
        /// creates a builder for a CreateDynamoDBTableOptions object. The two required parameters
        /// are the DynamoDB client and the table name. This builder uses the pay per request billing mode.
        /// </summary>
        /// <param name="dynamoDBClient"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public static CreateDynamoDBTableOptionsBuilder Builder(AmazonDynamoDBClient dynamoDBClient, string tableName)
        {
            return new CreateDynamoDBTableOptionsBuilder(dynamoDBClient, tableName);
        }

        #endregion

        #region Inner Class

        /// <summary>
        /// A builder class used to create the CreateDynamoDBTableOptions class. By default it is set up to
        /// have a partition key name of "key". It will also use the default billing mode if provisioned
        /// throughput is specified, otherwise it will use the pay per request billing mode.
        /// </summary>
        public class CreateDynamoDBTableOptionsBuilder
        {
            #region Private Fields

            private AmazonDynamoDBClient DynamoDBClient;
            private Optional<ProvisionedThroughput> ProvisionedThroughput;
            private BillingMode BillingMode;
            private string TableName;
            private string PartitionKeyName;
            private Optional<string> SortKeyName;

            #endregion

            #region Constructors

            /// <summary>
            /// Internal constructor to be called by parent class
            /// </summary>
            /// <param name="dynamoDBClient"></param>
            /// <param name="provisionedThroughput"></param>
            /// <param name="billingMode"></param>
            /// <param name="tableName"></param>
            internal CreateDynamoDBTableOptionsBuilder(AmazonDynamoDBClient dynamoDBClient, ProvisionedThroughput provisionedThroughput, string tableName)
            {
                this.DynamoDBClient = dynamoDBClient;
                this.ProvisionedThroughput = Optional<ProvisionedThroughput>.OfNullable(provisionedThroughput);
                this.BillingMode = BillingMode.PROVISIONED;
                this.TableName = tableName;
                this.PartitionKeyName = AmazonDynamoDBLockClientOptions.DEFAULT_PARTITION_KEY_NAME;
                this.SortKeyName = Optional<string>.Empty;
            }

            /// <summary>
            /// Internal constructor to be called by parent class
            /// </summary>
            /// <param name="dynamoDBClient"></param>
            /// <param name="provisionedThroughput"></param>
            /// <param name="billingMode"></param>
            /// <param name="tableName"></param>
            internal CreateDynamoDBTableOptionsBuilder(AmazonDynamoDBClient dynamoDBClient, string tableName)
            {
                this.DynamoDBClient = dynamoDBClient;
                this.ProvisionedThroughput = Optional<ProvisionedThroughput>.Empty;
                this.BillingMode = BillingMode.PAY_PER_REQUEST;
                this.TableName = tableName;
                this.PartitionKeyName = AmazonDynamoDBLockClientOptions.DEFAULT_PARTITION_KEY_NAME;
                this.SortKeyName = Optional<string>.Empty;
            }

            #endregion

            #region Public Methods

            /// <summary>
            /// Sets the partition key for the create table request
            /// </summary>
            /// <param name="partitionKeyName"></param>
            /// <returns></returns>
            public CreateDynamoDBTableOptionsBuilder WithPartitionKey(string partitionKeyName)
            {
                this.PartitionKeyName = partitionKeyName;
                return this;
            }

            /// <summary>
            /// Sets the sort key name for the create table request
            /// </summary>
            /// <param name="sortKeyName"></param>
            /// <returns></returns>
            public CreateDynamoDBTableOptionsBuilder WithSortKeyName(string sortKeyName)
            {
                this.SortKeyName = Optional<string>.OfNullable(sortKeyName);
                return this;
            }

            /// <summary>
            /// Creates a new CreateDynamoDBTableOptions object
            /// </summary>
            /// <returns></returns>
            public CreateDynamoDBTableOptions Build()
            {
                return new CreateDynamoDBTableOptions(
                    this.DynamoDBClient, 
                    this.ProvisionedThroughput, 
                    this.BillingMode, 
                    this.TableName, 
                    this.PartitionKeyName, 
                    this.SortKeyName
                );
            }

            public override string ToString()
            {
                return $"CreateDynamoDBTableOptions.CreateDynamoDBTableOptionsBuilder(DynamoDBClient={this.DynamoDBClient}, ProvisionedThroughput={this.ProvisionedThroughput}, BillingMode={this.BillingMode}, TableName={this.TableName}, PartitionKeyName={this.PartitionKeyName}, SortKeyName={this.SortKeyName})";
            }

            #endregion
        }

        #endregion
    }
}
