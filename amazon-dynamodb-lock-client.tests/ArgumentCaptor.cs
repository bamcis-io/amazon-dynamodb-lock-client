using Moq;
using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.DynamoDBv2.Tests
{
    public class ArgumentCaptor<T>
    {
        public T Capture()
        {
            return It.Is<T>(x => SaveValue(x));
        }

        private bool SaveValue(T t)
        {
            this.Value = t;
            return true;
        }

        public T Value { get; private set; }
    }
}
