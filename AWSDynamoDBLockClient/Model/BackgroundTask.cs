using System;
using System.Threading;
using System.Threading.Tasks;

namespace BAMCIS.AWSDynamoDBLockClient.Model
{
    public class BackgroundTask
    {
        #region Public Properties

        /// <summary>
        /// The task being tracked
        /// </summary>
        public Task Task { get; }

        /// <summary>
        ///  The cancellation token source whose token was provided to the task
        /// </summary>
        public CancellationTokenSource CancellationTokenSource { get; }

        #endregion

        #region Constructors

        public BackgroundTask(Action action, CancellationTokenSource cts)
        {
            this.Task = new Task(action, cts.Token);
            this.CancellationTokenSource = cts;
        }

        #endregion

        #region Public Methods

        public void Start()
        {
            this.Task.Start();
        }

        public void Cancel()
        {
            this.CancellationTokenSource.Cancel();
        }

        #endregion
    }
}
