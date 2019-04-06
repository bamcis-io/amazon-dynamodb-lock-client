using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace BAMCIS.AWSDynamoDBLockClient
{
    public static class Utilities
    {
        // <summary>
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
    }
}
