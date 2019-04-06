using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace BAMCIS.AWSDynamoDBLockClient.Util
{
    public static class LockClientUtils
    {
        /// <summary>
        /// Tests an object to see if it is null, if it is, an exception is thrown.
        /// 
        /// If the parameterName is provided, and ArgumentNullException is thrown, otherwise an
        /// ArgumentException is thrown.
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="message"></param>
        /// <param name="parameterName"></param>
        public static void RequireNonNull(object obj, string message, string parameterName = "")
        {
            if (obj == null)
            {
                if (String.IsNullOrEmpty(parameterName))
                {
                    throw new ArgumentException(message);
                }
                else
                {
                    throw new ArgumentNullException(parameterName, message);
                }
            }
        }

        public static void RequireNonNullOrEmpty(string obj, string message, string parameterName = "")
        {
            if (String.IsNullOrEmpty(obj))
            {
                if (String.IsNullOrEmpty(parameterName))
                {
                    throw new ArgumentException(message);
                }
                else
                {
                    throw new ArgumentNullException(parameterName, message);
                }
            }
        }

        /// <summary>
        /// Takes a 64 bit integer and breaks it down into multiple Int32 values
        /// that sum to the 64 bit value
        /// </summary>
        /// <param name="value">The value to decompose</param>
        /// <returns></returns> 
        public static IEnumerable<int> Decompose64BitInt(long value)
        {
            if (value >= 0)
            {
                while (value > Int32.MaxValue)
                {
                    yield return Int32.MaxValue;
                    value += -Int32.MaxValue;
                }

                yield return (int)value;
            }
            else
            {
                while (value < Int32.MinValue)
                {
                    yield return Int32.MinValue;
                    value += (-(long)Int32.MinValue);
                }

                yield return (int)value;
            }
        }

        /// <summary>
        /// Gets the system time in milliseconds from the stopwatch timestamp
        /// </summary>
        /// <returns></returns>
        public static long MillisecondTime()
        {
            return Stopwatch.GetTimestamp() / TimeSpan.TicksPerMillisecond;
        }

        /// <summary>
        /// Computes a hash for a set of objects
        /// </summary>
        /// <param name="args">The arguments to hash</param>
        /// <returns>The hash code of the objects</returns>
        public static int Hash(params object[] args)
        {
            unchecked // Overflow is fine, just wrap
            {
                int Hash = 17;

                foreach (object Item in args)
                {
                    if (Item != null)
                    {
                        Hash = (Hash * 23) + Item.GetHashCode();
                    }
                }

                return Hash;
            }
        }
    }
}
