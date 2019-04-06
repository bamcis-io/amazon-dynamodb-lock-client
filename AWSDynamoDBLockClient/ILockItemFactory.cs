using Amazon.DynamoDBv2.Model;
using System.Collections.Generic;

namespace BAMCIS.AWSDynamoDBLockClient
{
    public interface ILockItemFactory
    {
        LockItem Create(Dictionary<string, AttributeValue> item);
    }
}
