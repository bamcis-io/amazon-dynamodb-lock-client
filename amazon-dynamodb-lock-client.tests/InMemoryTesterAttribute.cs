using System.Reflection;
using Xunit.Sdk;

namespace Amazon.DynamoDBv2.Tests
{
    public class InMemoryTesterAttribute : BeforeAfterTestAttribute
    {
        private InMemoryLockClientFixture fixture;

        public override void Before(MethodInfo methodUnderTest)
        {
            fixture = new InMemoryLockClientFixture();
        }

        public override void After(MethodInfo methodUnderTest)
        {
            fixture.Dispose();
        }
    }
}
