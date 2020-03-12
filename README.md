# Amazon DynamoDB Lock Client
The Amazon DynamoDB Lock Client is a general purpose distributed locking library built for DynamoDB. The DynamoDB Lock Client supports both fine-grained and coarse-grained locking as the lock keys can be any arbitrary string, up to a certain length. DynamoDB Lock Client is an open-source project that will be supported by the community. Please create issues in the GitHub repository with questions.

This is a port of the Java code found [here](https://github.com/awslabs/dynamodb-lock-client)

## Table of Contents
- [Use Cases](#use-cases)
- [Getting Started](#getting-started)
- [Revision History](#revision-history)

## Use Cases
A common use case for this lock client is: let's say you have a distributed system that needs to periodically do work on a given campaign (or a given customer, or any other object) and you want to make sure that two boxes don't work on the same campaign/customer at the same time. An easy way to fix this is to write a system that takes a lock on a customer, but fine-grained locking is a tough problem. This library attempts to simplify this locking problem on top of DynamoDB.

Another use case is leader election. If you only want one host to be the leader, then this lock client is a great way to pick one. When the leader fails, it will fail over to another host within a customizable leaseDuration that you set.

## Getting Started
To use the Amazon DynamoDB Lock Client, declare a dependency on the latest version of this artifact in your csproj file or add it through NuGet.

    <ItemGroup>
        <PackageReference Include="dynamodb-lock-client" Version="1.0.0" />
	</ItemGroup

Then, you need to set up a DynamoDB table that has a hash key on a key with the name key. For your convenience, there is a static method in the AmazonDynamoDBLockClient class called `createLockTableInDynamoDB` that you can use to set up your table, but it is also possible to set up the table in the AWS Console. The table should be created in advance, since it takes a couple minutes for DynamoDB to provision your table for you. The AmazonDynamoDBLockClient has comments that fully explain how the library works. Here is some example code to get you started:

    using Amazon.Runtime;
    using BAMCIS.Util;
    using BAMCIS.Util.Concurrent;
    using System.Diagnostics;
    using System.Threading.Tasks;
    
    namespace Amazon.DynamoDBv2.Tests
    {
        /// <summary>
        /// Usage example listed in README.md. Start DynamoDB Local first on port 4567.
        /// </summary>
        public class LockClientExample
        {
            public async Task UsageExample()
            {
                // Inject client configuration to the builder like the endpoint and signing region
                IAmazonDynamoDB dynamodb = new AmazonDynamoDBClient(new BasicAWSCredentials("a", "a"), new AmazonDynamoDBConfig()
                {
                    ServiceURL = "http://localhost:4567"
                });
    
                // Whether or not to create a heartbeating background thread
                bool createHeartbeatBackgroundThread = true;
      
                // Build the lock client
                IAmazonDynamoDBLockClient client = new AmazonDynamoDBLockClient(new AmazonDynamoDBLockClientOptions(dynamodb, "lockTable") { TimeUnit = TimeUnit.SECONDS, LeaseDuration = 10, HeartbeatPeriod = 3, CreateHeartbeatBackgroundThread = createHeartbeatBackgroundThread });
    
                try
                {
                    // Create the table
                    await client.CreateLockTableInDynamoDBAsync(new CreateDynamoDBTableOptions(dynamodb, "lockTable"));
    
                    // Try to acquire a lock on the partition key "Moe"
                    Optional<LockItem> lockItem = await client.TryAcquireLockAsync(new AcquireLockOptions("Moe"));
    
                    if (lockItem.IsPresent())
                    {
                        Console.WriteLine("Acquired lock! If I die, my lock will expire in 10 seconds.");
                        Console.WriteLine("Otherwise, I will hold it until I stop heartbeating.");
                        client.ReleaseLock(lockItem.Value);
                    }
                    else
                    {
                        Console.WriteLine("Failed to acquire lock!");
                    }  
                }
                finally
                {
                    await dynamodb.DeleteTableAsync("lockTable");
                    client.Close();
                }
            }
        }
    }

## Selected Features
### Send Automatic Heartbeats
When you call the constructor AmazonDynamoDBLockClient, you can specify `CeateHeartbeatBackgroundThread = true` like in the above example, and it will spawn a background thread that continually updates the record version number on your locks to prevent them from expiring (it does this by calling the SendHeartbeat() method in the lock client.) This will ensure that as long as your process is running, your locks will not expire until you call ReleaseLock() or lockItem.Close()

### Acquire Lock with Timeout
You can acquire a lock via two different methods: acquireLock or tryAcquireLock. The difference between the two methods is that tryAcquireLock will return Optional.absent() if the lock was not acquired, whereas acquireLock will throw a LockNotGrantedException. Both methods provide optional parameters where you can specify an additional timeout for acquiring the lock. Then they will try to acquire the lock for that amount of time before giving up. They do this by continually polling DynamoDB according to an interval you set up. Remember that acquireLock/tryAcquireLock will always poll DynamoDB for at least the leaseDuration period before giving up, because this is the only way it will be able to expire stale locks.

This example will poll DynamoDB every second for 5 additional seconds (beyond the lease duration period), trying to acquire a lock:

    LockItem lock = await lockClient.AcquireLockAsync("Moe", "Test Data", 1, 5, TimeUnit.SECONDS);

### Acquire Lock without Blocking the User Thread
Example Use Case: Suppose you have many messages that need to be processed for multiple lockable entities by a limited set of processor-consumers. Further suppose that the processing time for each message is significant (for example, 15 minutes). You also need to prevent multiple processing for the same resource.

    public async Task AcquireLockBlockingAsync()
	{
		AcquireLockOptions lockOptions = new AcquireLockOptions("partitionKey")
		{
			ShouldSkipBlockingWait = false
		};

		LockItem lock = await lockClient.AcquireLockAsync(lockOptions);
	}

The above implementation of the locking client, would try to acquire lock, waiting for at least the lease duration (15 minutes in our case). If the lock is already being held by other worker. This essentially blocks the threads from being used to process other messages in the queue.

So we introduced an optional behavior which offers a Non-Blocking acquire lock implementation. While trying to acquire lock, the client can now optionally set `ShouldSkipBlockingWait = true` to prevent the user thread from being blocked until the lease duration, if the lock has already been held by another worker and has not been released yet. The caller can chose to immediately retry the lock acquisition or to back off and retry the lock acquisition, if lock is currently unavailable.

     public async Task AcquireLockNonBlockingAsync()
	{
		AcquireLockOptions lockOptions = new AcquireLockOptions("partitionKey")
		{
			ShouldSkipBlockingWait = true
		};

		LockItem lock = await lockClient.AcquireLockAsync(lockOptions);
	}

If the lock does not exist or if the lock has been acquired by the other machine and is stale (has passed the lease duration), this would successfully acquire the lock.

If the lock has already been held by another worker and has not been released yet and the lease duration has not expired since the lock was last updated by the current owner, this will throw a LockCurrentlyUnavailableException exception. The caller can chose to immediately retry the lock acquisition or to delay the processing for that lock item by NACKing the message.

### Read the Data in a Lock without Acquiring it

You can read the data in the lock without acquiring it, and find out who owns the lock. Here's how:

    LockItem lock = await lockClient.GetLockAsync("Moe");

## How We Handle Clock Skew

The lock client never stores absolute times in DynamoDB -- only the relative "lease duration" time is stored in DynamoDB. The way locks are expired is that a call to acquireLock reads in the current lock, checks the RecordVersionNumber of the lock (which is a GUID) and starts a timer. If the lock still has the same GUID after the lease duration time has passed, the client will determine that the lock is stale and expire it. What this means is that, even if two different machines disagree about what time it is, they will still avoid clobbering each other's locks.

## Testing the DynamoDB Locking Client
To run all integration tests for the DynamoDB Lock client, issue the following dotnet command:

    dotnet test

There is one unit test that does not pass, `TestCallbackNotCalledOnClosingClient` in the `BasicLockClientTests` class. Based on the original code, I'm not sure how it passed, since it is effectively testing a race condition (which actually a number of the tests do). The test is commented out.

For the classes decorated with the `[InMemoryTester]` attribute, these tests rely on running a local version of DynamoDB, which you can download from [here](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html). Adjust the `endpoint` string in `InMemoryLockClientFixture` to match the port you are running DynamoDB locally. For example, you might start the local version with:

    "C:\Program Files\Amazon Corretto\jdk11.0.2_9\bin\java.exe" -Djava.library.path=D:\Dynamodb-Local\DynamodbLocal_lib -jar D:\Dynamodb-Local\DynamoDBLocal.jar -sharedDb -port 4567 -inMemory

Make sure the `endpoint` variable is set to:

    endpoint = "http://localhost:4567";

I've turned down the concurrent threads in the `ConsistentLockDataStressTest` class from 50 to 10 via the `numOfThreads` variable, my test platform didn't handle trying to manage that many tasks well.

## Revision History

### 1.0.0
Initial release of the library.