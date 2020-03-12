using Amazon.DynamoDBv2.Model;
using Moq;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Amazon.DynamoDBv2.Tests
{
    public class LockItemPaginatedScanIteratorTest
    {
        [Fact]
        public void Remove_ThrowsUnsupportedOperationException()
        {
            // ARRANGE
            var factory = new Mock<ILockItemFactory>();
            LockItemPaginatedScanIterator sut = new LockItemPaginatedScanIterator(new AmazonDynamoDBClient(), new ScanRequest(), factory.Object);

            // ACT

            // ASSERT
            // LockItemPaginatedScanIterator has no Remove() method inherited from IEnumerator like Iterator does in Java
        }

        [Fact]
        public void Next_WhenDoesNotHaveNext_ThrowsNoSuchElementException()
        {
            // ARRANGE
            ScanRequest request = new ScanRequest();
            var factory = new Mock<ILockItemFactory>();
            var client = new Mock<IAmazonDynamoDB>();

            List<Dictionary<string, AttributeValue>> list1 = new List<Dictionary<string, AttributeValue>>()
            {
                new Dictionary<string, AttributeValue>()
            };

            client.Setup(x => x.ScanAsync(It.IsAny<ScanRequest>(), default(CancellationToken))).Returns(
                Task.FromResult(                  
                    new ScanResponse()
                    {
                        Items = list1
                    }           
                )
            );

            LockItemPaginatedScanIterator sut = new LockItemPaginatedScanIterator(client.Object, new ScanRequest(), factory.Object);

            // ACT
            Assert.True(sut.MoveNext());

            // ASSERT
            Assert.False(sut.MoveNext());
        }
    }
}
