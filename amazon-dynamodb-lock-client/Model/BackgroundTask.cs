using System;
using System.Threading;
using System.Threading.Tasks;

namespace Amazon.DynamoDBv2.Model
{
    /// <summary>
    /// Represents a task and its cancellation token source that can easily
    /// be started, cancelled, and waited on
    /// </summary>
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

        private BackgroundTask(CancellationTokenSource cts)
        {
            this.CancellationTokenSource = cts ?? throw new ArgumentNullException("cts");
        }

        /// <summary>
        /// Creates a new background task from the provided action
        /// </summary>
        /// <param name="action">A synchronous action</param>
        public BackgroundTask(Action action) : this(new CancellationTokenSource())
        {
            this.Task = new Task(action, this.CancellationTokenSource.Token);            
        }

        /// <summary>
        /// Creates a new background task from the provided action
        /// </summary>
        /// <param name="action">A synchronous action</param>
        /// <param name="cts">The cancellation token source for the task</param>
        public BackgroundTask(Action action, CancellationTokenSource cts) : this(cts)
        {
            this.Task = new Task(action, this.CancellationTokenSource.Token);
        }

        #endregion

        #region Public Methods

        /// <summary>
        /// Starts the task
        /// </summary>
        public void Start()
        {
            this.Task.Start();
        }

        /// <summary>
        /// Sends a cancellation notification to the task
        /// </summary>
        public void Cancel()
        {
            this.CancellationTokenSource.Cancel();
        }

        /// <summary>
        /// Asynchronously waits for the task to finish
        /// </summary>
        /// <returns></returns>
        public async Task WaitAsync()
        {
            await this.Task;
        }

        /// <summary>
        /// Performs a blocking wait on the task
        /// </summary>
        public void Wait()
        {
            this.Task?.Wait();
        }

        /// <summary>
        /// Performs a blocking wait on the task 
        /// </summary>
        /// <param name="millisecondsTimeout">The timeout for the wait</param>
        public void Wait(int millisecondsTimeout)
        {
            this.Task?.Wait(millisecondsTimeout);
        }

        #endregion
    }
}
