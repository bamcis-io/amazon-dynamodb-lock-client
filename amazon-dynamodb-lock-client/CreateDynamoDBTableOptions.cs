using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2.Util;
using System;

namespace Amazon.DynamoDBv2
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
        public IAmazonDynamoDB DynamoDBClient { get; }

        /// <summary>
        /// The provisioned throughput for the table. If you set BillingMode as PROVISIONED, you must specify this property. If you
        /// set BillingMode as PAY_PER_REQUEST, you cannot specify this property.
        /// </summary>
        public ProvisionedThroughput ProvisionedThroughput { get; set; }

        /// <summary>
        /// The billing mode for the table. 
        /// Controls how you are charged for read and write throughput and how you manage
        /// capacity. This setting can be changed later.
        /// PROVISIONED - Sets the billing mode to PROVISIONED. We recommend using PROVISIONED
        /// for predictable workloads.
        /// PAY_PER_REQUEST - Sets the billing mode to PAY_PER_REQUEST. We recommend using
        /// PAY_PER_REQUEST for unpredictable workloads.
        /// </summary>
        public BillingMode BillingMode { get; set; }

        /// <summary>
        /// The name of the table to create
        /// </summary>
        public string TableName { get; }

        /// <summary>
        /// The name of the partition key for the table
        /// </summary>
        public string PartitionKeyName { get; set; }

        /// <summary>
        /// The optional sort key to use with the table
        /// </summary>
        public string SortKeyName { get; set; }

        #endregion

        #region Constructors

        /// <summary>
        /// Constructor that defaults to using On-Demand capacity billing mode
        /// </summary>
        /// <param name="dynamoDBClient"></param>
        /// <param name="tableName"></param>
        /// <param name="partitionKeyName"></param>
        public CreateDynamoDBTableOptions(IAmazonDynamoDB dynamoDBClient, string tableName)
        {
            this.DynamoDBClient = dynamoDBClient ?? throw new ArgumentNullException("dynamoDBClient");

            LockClientUtils.RequireNonNullOrEmpty(tableName, "Table name cannot be null or empty", "tableName");

            this.TableName = tableName;
            this.PartitionKeyName = AmazonDynamoDBLockClientOptions.DEFAULT_PARTITION_KEY_NAME;
            this.SortKeyName = String.Empty;
            this.ProvisionedThroughput = null;
            this.BillingMode = BillingMode.PAY_PER_REQUEST;
        }

        /// <summary>
        /// Constructor that uses provisioned capacity billing
        /// </summary>
        /// <param name="dynamoDBClient"></param>
        /// <param name="tableName"></param>
        /// <param name="provisionedThroughput"></param>
        public CreateDynamoDBTableOptions(IAmazonDynamoDB dynamoDBClient, string tableName, ProvisionedThroughput provisionedThroughput) : this(dynamoDBClient, tableName)
        {
            this.BillingMode = BillingMode.PROVISIONED;
            this.ProvisionedThroughput = provisionedThroughput;
        }

        #endregion
    }
}
