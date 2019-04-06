using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace BAMCIS.AWSDynamoDBLockClient
{
    public sealed class LockItemPaginatedScanIterator : IEnumerator<LockItem>
    {
        #region Private Fields

        private IAmazonDynamoDB DynamoDB;
        private volatile ScanRequest ScanRequest;
        //private ILockItemFactory LockItemFactory;
        private Func<Dictionary<string, AttributeValue>, LockItem> LockItemFactory;
        private LockItem Curr = null;

        private List<LockItem> CurrentPageResults = new List<LockItem>();
        private int CurrentPageResultsIndex = -1;

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
        public LockItem Current
        {
            get
            {
                if (this.CurrentPageResultsIndex < this.CurrentPageResults.Count)
                {
                    return Curr;
                }
                else
                {
                    return null;
                }
            }
        }

        object IEnumerator.Current => Current;

        #endregion

        #region Constructors

        internal LockItemPaginatedScanIterator(
            IAmazonDynamoDB dynamoDB,
            ScanRequest scanRequest,
            //ILockItemFactory lockItemFactory
            Func<Dictionary<string, AttributeValue>, LockItem> lockItemFactory
        )
        {
            this.DynamoDB = dynamoDB ?? throw new ArgumentNullException("dynamoDB");
            this.ScanRequest = scanRequest ?? throw new ArgumentNullException("scanRequest");
            this.LockItemFactory = lockItemFactory ?? throw new ArgumentNullException("lockItemFactory");
            this.Curr = null;
        }

        #endregion

        #region Public Methods

        public void Dispose()
        {
            this.DynamoDB.Dispose();
        }

        public bool MoveNext()
        {
            while (this.CurrentPageResultsIndex == this.CurrentPageResults.Count && this.HasAnotherPageToLoad())
            {
                this.LoadNextPageIntoResults().RunSynchronously();
            }

            this.CurrentPageResultsIndex++;

            if (this.CurrentPageResultsIndex >= this.CurrentPageResults.Count)
            {
                return false;
            }
            else
            {
                this.Curr = this.CurrentPageResults.ElementAt(this.CurrentPageResultsIndex);
                return true;
            }
        }

        public void Reset()
        {
            this.CurrentPageResultsIndex = -1;
            this.CurrentPageResults = new List<LockItem>();
            this.ScanRequest.ExclusiveStartKey = new Dictionary<string, AttributeValue>();
        }

        #endregion

        #region Private Methods

        private async Task LoadNextPageIntoResults()
        {
            this.ScanResponse = await this.DynamoDB.ScanAsync(this.ScanRequest);
            //this.CurrentPageResults = this.ScanResponse.Items.Select(x => this.LockItemFactory.Create(x)).ToList();
            this.CurrentPageResults = this.ScanResponse.Items.Select(x => this.LockItemFactory.Invoke(x)).ToList();
            this.CurrentPageResultsIndex = 0;
            this.ScanRequest = new ScanRequest()
            {
                TableName = this.ScanRequest.TableName,
                ExclusiveStartKey = this.ScanResponse.LastEvaluatedKey
            };
        }

        private bool HasAnotherPageToLoad()
        {
            if (!this.HasLoadedFirstPage())
            {
                return true;
            }

            return this.ScanResponse.LastEvaluatedKey != null && this.ScanResponse.LastEvaluatedKey.Any();
        }

        private bool HasLoadedFirstPage()
        {
            return this.ScanResponse != null;
        }

        #endregion
    }
}
