using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sop.Utility
{
    /// <summary>
    /// Endianness neutral > 8 bit int writer/reader.
    /// NOTE: currently only works for 16 bit (ushort) read/write.
    /// </summary>
    public class BitHelper
    {
        public static byte[] Write(ushort value)
        {
            var r = BitConverter.GetBytes(value);
            ushortToLittleEndian(r);
            return r;
        }
        public static ushort Read(byte[] value, int index = 0)
        {
            ushortToLittleEndian(value);
            return BitConverter.ToUInt16(value, index);
        }
        private static void ushortToLittleEndian(byte[] data)
        {
            if (BitConverter.IsLittleEndian)
                return;
            // remap to LittleEndian...
            byte b = data[1];
            data[1] = data[0];
            data[0] = b;
        }
    }
}
