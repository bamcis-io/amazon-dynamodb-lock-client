using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Amazon.DynamoDBv2
{
    public sealed class LockItemPaginatedScanIterator : IEnumerator<LockItem>
    {
        #region Private Fields

        private IAmazonDynamoDB DynamoDB;
        private volatile ScanRequest ScanRequest;
        private ILockItemFactory LockItemFactory;
        private List<LockItem> CurrentPageResults = new List<LockItem>();
        private int CurrentPageResultsIndex = 0;

        /// <summary>
        /// Initally null to indicate that no pages have been loaded yet. Afterwards,
        /// it's last evaluated key is used to tell if there are more pages to load if it is not null.
        /// </summary>
        private volatile ScanResponse ScanResponse = null;

        #endregion

        #region Public Properties

        /// <summary>
        /// The current item in enumeration, will return null 
        /// </summary>
        public LockItem Current { get; private set; } 

        object IEnumerator.Current => Current;

        #endregion

        #region Constructors

        internal LockItemPaginatedScanIterator(
            IAmazonDynamoDB dynamoDB,
            ScanRequest scanRequest,
            ILockItemFactory lockItemFactory
        )
        {
            this.DynamoDB = dynamoDB ?? throw new ArgumentNullException("dynamoDB");
            this.ScanRequest = scanRequest ?? throw new ArgumentNullException("scanRequest");
            this.LockItemFactory = lockItemFactory ?? throw new ArgumentNullException("lockItemFactory");
            this.Current = null;
        }

        #endregion

        #region Public Methods

        public void Dispose()
        {
            this.DynamoDB.Dispose();
        }

        public bool HasNext()
        {
            // while the index is either equal to the count, meaning we've moved all the way through the list
            // or if both are 0, meaning a page hasn't been loaded, and there's another page to load, go ahead
            // and load it
            while (this.CurrentPageResultsIndex == this.CurrentPageResults.Count && this.HasAnotherPageToLoad())
            {
                Task tsk = Task.Run(async () => await this.LoadNextPageIntoResults());
                tsk.Wait();
            }

            // The will return false if the the current page results has a count of 0, otherwise if there
            // were any results in the scan response, then this will return true since LoadNextPageIntoResults
            // resets the index to 0
            return this.CurrentPageResultsIndex < this.CurrentPageResults.Count;
        }

        /// <summary>
        /// Moves to the next item in the current page scan results lists
        /// </summary>
        /// <returns></returns>
        public bool MoveNext()
        {
            if (!this.HasNext())
            {
                this.Current = null;
                return false;
            }
            else
            {
                this.Current = this.CurrentPageResults.ElementAt(this.CurrentPageResultsIndex);
                this.CurrentPageResultsIndex++;
                return true;
            }
        }

        public void Reset()
        {
            this.CurrentPageResultsIndex = 0;
            this.CurrentPageResults = new List<LockItem>();
            this.ScanRequest.ExclusiveStartKey = new Dictionary<string, AttributeValue>();
        }

        #endregion

        #region Private Methods

        /// <summary>
        /// Performs a scan request and then convers the items in the response to lock items. This list
        /// of lock items are stored as a new list in the CurrentPageResults and the index into that 
        /// list is reset to 0. The internal scan request object is also updated to the last evaluated
        /// key in the scan response.
        /// </summary>
        /// <returns></returns>
        private async Task LoadNextPageIntoResults()
        {
            this.ScanResponse = await this.DynamoDB.ScanAsync(this.ScanRequest);

            if (this.ScanResponse == null)
            {
                throw new AmazonClientException("The DynamoDB client could not contact a DDB endpoint and returned a null response.");
            }

            this.CurrentPageResults = this.ScanResponse.Items.Select(x => this.LockItemFactory.Create(x)).ToList();
            this.CurrentPageResultsIndex = 0;
            this.ScanRequest = new ScanRequest()
            {
                TableName = this.ScanRequest.TableName,
                ExclusiveStartKey = this.ScanResponse.LastEvaluatedKey
            };

        }

        /// <summary>
        /// Checks to see if there is another page to load from the DDB lock table. If a page hasn't
        /// been loaded yet, based on the scap response object being null, then the function returns true. Otherwise
        /// the scan response is evaluated to see if the last evaluated key is null.
        /// </summary>
        /// <returns></returns>
        private bool HasAnotherPageToLoad()
        {
            if (!this.HasLoadedFirstPage())
            {
                return true;
            }

            // Make sure the last evaluated key dictionary is not empty, if it's not empty, there
            // is another page to load
            return this.ScanResponse.LastEvaluatedKey != null && this.ScanResponse.LastEvaluatedKey.Any();
        }

        /// <summary>
        /// Checks to see if the first page of results has been loaded from the DDB lock table
        /// </summary>
        /// <returns></returns>
        private bool HasLoadedFirstPage()
        {
            return this.ScanResponse != null;
        }

        #endregion
    }
}
