using Amazon.DynamoDBv2.Util;
using BAMCIS.Util.Concurrent;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Amazon.DynamoDBv2.Tests
{
    /// <summary>
    /// Used to verify consistent lock data functionality in multi-threaded lock environment
    /// </summary>
    [TestCaseOrderer("Amazon.DynamoDBv2.Tests.AlphabeticalOrderer", "dynamodb-lock-client.tests")]
    [InMemoryTester]
    [Collection("InMemoryTests")]
    public class ConsistentLockDataStressTest : InMemoryLockClientTester, IClassFixture<InMemoryLockClientFixture>
    {
        private int numOfThreads = 10; // Goal is 50
        private int numRepetitions = 10;
        private int maxWorkDoneMillis = 100;

        public ConsistentLockDataStressTest()
        {
        }

        [Fact]
        public async Task ConsistentDataUsingUpdateLockRecord()
        {
            // ARRANGE
            AcquireLockOptions options = new AcquireLockOptions("consistentDataUsingUpdateLockRecordKey")
            {
                AdditionalTimeToWaitForLock = long.MaxValue / 2,
                AcquireReleasedLocksConsistently = true,
                ReplaceData = false,
                UpdateExistingLockRecord = true,
                DeleteLockOnRelease = false,
                RefreshPeriod = 10,
                TimeUnit = TimeUnit.MILLISECONDS
            };

            // ACT
            LockItem lockItem = await this.RunTest(options, this.numOfThreads, this.numRepetitions, this.maxWorkDoneMillis);

            // ASSERT
            Assert.NotNull(lockItem);
            Assert.Equal(numOfThreads * numRepetitions, GetLockData(lockItem));
            lockItem.Close();
        }

        [Fact]
        public async Task NotConsistentLockDataUsingUpdateLockRecord()
        {
            // ARRANGE
            AcquireLockOptions options = new AcquireLockOptions("notConsistentLockDataUsingUpdateLockRecordKey")
            {
                AdditionalTimeToWaitForLock = long.MaxValue / 2,
                AcquireReleasedLocksConsistently = false,
                ReplaceData = false,
                UpdateExistingLockRecord = true,
                DeleteLockOnRelease = false,
                RefreshPeriod = 10,
                TimeUnit = TimeUnit.MILLISECONDS
            };

            // ACT
            LockItem lockItem = await this.RunTest(options, this.numOfThreads, this.numRepetitions, this.maxWorkDoneMillis);

            // ASSERT
            Assert.NotNull(lockItem);
            Assert.NotEqual(numOfThreads * numRepetitions, GetLockData(lockItem));
            lockItem.Close();
        }

        [Fact]
        public async Task ConsistentDataUsingPutLockRecord()
        {
            // ARRANGE
            AcquireLockOptions options = new AcquireLockOptions("consistentDataUsingPutLockRecordKey")
                {
                    AdditionalTimeToWaitForLock = long.MaxValue / 2,
                    AcquireReleasedLocksConsistently = true,
                    ReplaceData = false,
                    UpdateExistingLockRecord = false,
                    DeleteLockOnRelease = false,
                    RefreshPeriod = 10,
                    TimeUnit = TimeUnit.MILLISECONDS
                };

            // ACT
            LockItem lockItem = await this.RunTest(options, this.numOfThreads, this.numRepetitions, this.maxWorkDoneMillis);

            // ASSERT
            Assert.NotNull(lockItem);
            Assert.Equal(numOfThreads * numRepetitions, GetLockData(lockItem));
            lockItem.Close();
        }

        [Fact]
        public async Task NotConsistentLockDataUsingPutLockRecord()
        {
            // ARRANGE
            AcquireLockOptions options = new AcquireLockOptions("notConsistentLockDataUsingPutLockRecordKey")
            {
                AdditionalTimeToWaitForLock = long.MaxValue / 2,
                AcquireReleasedLocksConsistently = false,
                ReplaceData = false,
                UpdateExistingLockRecord = false,
                DeleteLockOnRelease = false,
                RefreshPeriod = 10,
                TimeUnit = TimeUnit.MILLISECONDS
            };

            // ACT
            LockItem lockItem = await this.RunTest(options, this.numOfThreads, this.numRepetitions, this.maxWorkDoneMillis);

            // ASSERT
            Assert.NotNull(lockItem);
            Assert.NotEqual(numOfThreads * numRepetitions, GetLockData(lockItem));
            lockItem.Close();
        }

        #region Private Functions

        private static int GetLockData(LockItem lockItem)
        {
            string str = Encoding.UTF8.GetString(lockItem.Data.ToArray());
            int value = Int32.Parse(str);
            return value;
        }

        private static void AssertConcurrent(IEnumerable<Func<Task>> runnables, int maxTimeoutSeconds, int numOfRepetitions)
        {
            Assert.True(numOfRepetitions > 0);
            int numThreads = runnables.Count();

            ConcurrentBag<Exception> exceptions = new ConcurrentBag<Exception>();

            CountdownEvent allExecutorThreadsReady = new CountdownEvent(numThreads);
            CountdownEvent afterInitBlocker = new CountdownEvent(1);
            CountdownEvent allDone = new CountdownEvent(numThreads);

            List<Task> threads = runnables.Select(x =>
            {
                return Task.Run(() =>
                {
                    allExecutorThreadsReady.Signal();
                    try
                    {
                        afterInitBlocker.Wait(); // Will return when count reaches 0
                        Repeat(x, numOfRepetitions);
                    }
                    catch (Exception e)
                    {
                        exceptions.Add(e);
                    }
                    finally
                    {
                        allDone.Signal();
                    }
                });
            }).ToList();

            // Make sure each task is actually executing
            bool result = allExecutorThreadsReady.Wait(numThreads * 1000);

            Assert.True(result);

            // Start all test runners
            afterInitBlocker.Signal();

            result = allDone.Wait(maxTimeoutSeconds * 1000); // convert to milliseconds

            // Also make sure they've all finished

            foreach (Task t in threads)
            {
                t.Wait();
            }

            //Assert.True(result); // This result can fail

            if (exceptions.Any())
            {
                foreach (Exception e in exceptions)
                {
                    LockClientUtils.Logger.Error(e.Message, e);
                }
            }

            Assert.Empty(exceptions);
        }

        private static void Repeat(Func<Task> action, int numOfRepetitions)
        {
            List<Task> tasks = new List<Task>();

            for (int i = 0; i < numOfRepetitions; i++)
            {
                tasks.Add(Task.Run(action));
            }

            Task.WaitAll(tasks.ToArray());
        }

        private async Task<LockItem> RunTest(AcquireLockOptions options, int numOfThreads, int numRepetitions, int maxWorkDoneMillis)
        {
            LockItem initialLock = await this.lockClientWithHeartbeating.AcquireLockAsync(options);

            bool success = this.lockClientWithHeartbeating.ReleaseLock(new ReleaseLockOptions(initialLock) { DeleteLock = false, Data = GetMemoryStream("0"), BestEffort = false });
            Assert.True(success);

            Func<Task> runnable = async () =>
            {
                LockItem lockItem = null;
                int count = 0;

                try
                {
                    lockItem = await this.lockClientWithHeartbeating.TryAcquireLockAsync(options);

                    Assert.NotNull(lockItem);
                    int lockData = GetLockData(lockItem);
                    count = lockData + 1;
                    Thread.Sleep(SECURE_RANDOM.Next(1, maxWorkDoneMillis));
                }
                finally
                {
                    if (lockItem != null)
                    {
                        this.lockClientWithHeartbeating.ReleaseLock(new ReleaseLockOptions(lockItem) { DeleteLock = false, Data = GetMemoryStream(count.ToString()), BestEffort = false });
                    }
                }
            };

            AssertConcurrent(Enumerable.Repeat(runnable, numOfThreads), 600, numRepetitions);

            LockItem lockItem2 = await this.lockClientWithHeartbeating.TryAcquireLockAsync(new AcquireLockOptions(options.PartitionKey)
            {
                AdditionalTimeToWaitForLock = options.AdditionalTimeToWaitForLock,
                AcquireReleasedLocksConsistently = options.AcquireReleasedLocksConsistently,
                ReplaceData = options.ReplaceData,
                UpdateExistingLockRecord = options.UpdateExistingLockRecord,
                DeleteLockOnRelease = true,
                RefreshPeriod = options.RefreshPeriod,
                TimeUnit = options.TimeUnit
            });

            return lockItem2;
        }

        #endregion
    }
}
