using Amazon.DynamoDBv2.Model;
using System.Collections.Generic;

namespace Amazon.DynamoDBv2
{
    public interface ILockItemFactory
    {
        LockItem Create(Dictionary<string, AttributeValue> item);
    }
}
