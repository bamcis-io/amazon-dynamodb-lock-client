using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace Amazon.DynamoDBv2.Util
{
    public static class LockClientUtils
    {
        /// <summary>
        /// Tests an object to see if it is null, if it is, an exception is thrown.
        /// 
        /// If the parameterName is provided, an ArgumentNullException is thrown, otherwise an
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

        /// <summary>
        /// Tests a string to see if it is null or empty, if it is, an exception is thrown
        /// 
        /// If the parameterName is provided, an ArgumentNullException is thrown, otherwise an
        /// ArgumentException is thrown.
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="message"></param>
        /// <param name="parameterName"></param>
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
                int hash = 17;

                foreach (object Item in args)
                {
                    if (Item != null)
                    {
                        hash = (hash * 23) + Item.GetHashCode();
                    }
                }

                return hash;
            }
        }

        /// <summary>
        /// Compares 2 or more streams for equality of their byte content
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        public static bool StreamsEqual(Stream first, Stream second, params Stream[] args)
        {
            // If something is null, then they must all be null to be equal
            if (first == null || second == null)
            {
                return first == null && second == null && args.All(x => x == null);
            }

            List<Stream> Streams = new List<Stream>(args);
            Streams.Insert(0, second);

            if (!Streams.All(x =>
            {
                x.Position = 0;
                return x.Length == first.Length;
            }))
            {
                return false;
            }

            first.Position = 0;

            for (int i = 0; i < first.Length; i++)
            {
                int Byte = first.ReadByte();

                foreach (Stream Str in Streams)
                {
                    if (Str.ReadByte() != Byte)
                    {
                        return false;
                    }
                }
            }

            return true;
        }

        public static class Logger
        {
            public static void Debug(string message)
            {
                LogToStdOut(message);
            }

            public static void Debug(string message, Exception e)
            {
                LogToStdOut(message, e);
            }

            public static void Trace(string message)
            {
                LogToStdOut(message);
            }

            public static void Info(string message)
            {
                LogToStdOut(message);
            }

            public static void Info(string message, Exception e)
            {
                LogToStdOut(message, e);
            }

            public static void Warn(string message)
            {
                LogToStdOut(message);
            }

            public static void Warn(string message, Exception e)
            {
                LogToStdOut(message, e);
            }

            public static void Error(string message)
            {
                LogToStdErr(message);
            }

            public static void Error(Exception e)
            {
                LogToStdErr(e);
            }

            public static void Error(string message, Exception e)
            {
                LogToStdErr(message, e);
            }

            public static void Fatal(string message)
            {
                LogToStdErr(message);
            }

            public static void Fatal(string message, Exception e)
            {
                LogToStdErr(message, e);
            }

            public static void Fatal(Exception e)
            {
                LogToStdErr(e);
            }

            /// <summary>
            /// Logs a message to StdOut
            /// </summary>
            /// <param name="message"></param>
            private static void LogToStdOut(string message)
            {
                Console.WriteLine($"[{DateTime.UtcNow.ToString("yyyy-MM-ddThh:mm:ss.fffZ")}] {message}");
            }

            /// <summary>
            /// Logs a message to StdOut
            /// </summary>
            /// <param name="message"></param>
            private static void LogToStdOut(Exception e)
            {
                LogToStdOut($"{e.GetType().FullName} : {e.Message}");
                LogToStdOut(e.StackTrace);
            }

            /// <summary>
            /// Logs a message and exception details to StdOut
            /// </summary>
            /// <param name="message"></param>
            /// <param name="e"></param>
            private static void LogToStdOut(string message, Exception e)
            {
                LogToStdOut(message);
                LogToStdOut(e);
            }

            /// <summary>
            /// Logs a message to StdErr
            /// </summary>
            /// <param name="message"></param>
            private static void LogToStdErr(string message)
            {
                Console.Error.WriteLine($"[{DateTime.UtcNow.ToString("yyyy-MM-ddThh:mm:ss.fffZ")}] {message}");
            }

            /// <summary>
            /// Logs an exception details to StdErr
            /// </summary>
            /// <param name="e"></param>
            private static void LogToStdErr(Exception e)
            {
                LogToStdErr($"{e.GetType().FullName} : {e.Message}");
                LogToStdErr(e.StackTrace);
            }

            /// <summary>
            /// Logs a message and exception details to StdErr
            /// </summary>
            /// <param name="message"></param>
            /// <param name="e"></param>
            private static void LogToStdErr(string message, Exception e)
            {
                LogToStdErr(message);
                LogToStdErr(e);
            }
        }
    }
}
