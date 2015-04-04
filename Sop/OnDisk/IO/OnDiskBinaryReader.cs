// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.IO;
using System.Xml.Serialization;

namespace Sop.OnDisk.IO
{
    /// <summary>
    /// OnDiskBinaryReader is a BinaryReader for reading basic data types
    /// from DataBlock(s)
    /// </summary>
    internal class OnDiskBinaryReader : BinaryReader
    {
        /// <summary>
        /// Constructor expecting Encoding to use while reading data
        /// </summary>
        /// <param name="encoding"></param>
        public OnDiskBinaryReader(System.Text.Encoding encoding)
            : base(new MemoryStream(), encoding)
        {
            _encoding = encoding;
        }

        // Summary:
        //     Exposes access to the underlying stream of the System.IO.BinaryReader.
        //
        // Returns:
        //     The underlying stream associated with the BinaryReader.
        public override Stream BaseStream
        {
            get
            {
                if (_reader != null)
                    return _reader.BaseStream;
                return null;
            }
        }

        // Summary:
        //     Closes the current reader and the underlying stream.
        public override void Close()
        {
            if (_reader != null)
                _reader.Close();
        }

        //
        // Summary:
        //     Returns the next available character and does not advance the byte or character
        //     position.
        //
        // Returns:
        //     The next available character, or -1 if no more characters are available or
        //     the stream does not support seeking.
        //
        // Exceptions:
        //   System.IO.IOException:
        //     An I/O error occurs.
        public override int PeekChar()
        {
            return _reader.PeekChar();
        }

        //
        // Summary:
        //     Reads characters from the underlying stream and advances the current position
        //     of the stream in accordance with the Encoding used and the specific character
        //     being read from the stream.
        //
        // Returns:
        //     The next character from the input stream, or -1 if no characters are currently
        //     available.
        //
        // Exceptions:
        //   System.ObjectDisposedException:
        //     The stream is closed.
        //
        //   System.IO.IOException:
        //     An I/O error occurs.
        public override int Read()
        {
            return _reader.Read();
        }

        //
        // Summary:
        //     Reads count bytes from the stream with index as the starting point in the
        //     byte array.
        //
        // Parameters:
        //   count:
        //     The number of characters to read.
        //
        //   buffer:
        //     The buffer to read data into.
        //
        //   index:
        //     The starting point in the buffer at which to begin reading into the buffer.
        //
        // Returns:
        //     The number of characters read into buffer. This might be less than the number
        //     of bytes requested if that many bytes are not available, or it might be zero
        //     if the end of the stream is reached.
        //
        // Exceptions:
        //   System.ArgumentNullException:
        //     buffer is null.
        //
        //   System.IO.IOException:
        //     An I/O error occurs.
        //
        //   System.ObjectDisposedException:
        //     The stream is closed.
        //
        //   System.ArgumentOutOfRangeException:
        //     index or count is negative.
        //
        //   System.ArgumentException:
        //     The buffer length minus index is less than count.
        public override int Read(byte[] buffer, int index, int count)
        {
            return _reader.Read(buffer, index, count);
        }

        //
        // Summary:
        //     Reads count characters from the stream with index as the starting point in
        //     the character array.
        //
        // Parameters:
        //   count:
        //     The number of characters to read.
        //
        //   buffer:
        //     The buffer to read data into.
        //
        //   index:
        //     The starting point in the buffer at which to begin reading into the buffer.
        //
        // Returns:
        //     The total number of characters read into the buffer. This might be less than
        //     the number of characters requested if that many characters are not currently
        //     available, or it might be zero if the end of the stream is reached.
        //
        // Exceptions:
        //   System.ArgumentNullException:
        //     buffer is null.
        //
        //   System.IO.IOException:
        //     An I/O error occurs.
        //
        //   System.ObjectDisposedException:
        //     The stream is closed.
        //
        //   System.ArgumentOutOfRangeException:
        //     index or count is negative.
        //
        //   System.ArgumentException:
        //     The buffer length minus index is less than count.
        public override int Read(char[] buffer, int index, int count)
        {
            return _reader.Read(buffer, index, count);
        }

        //
        // Summary:
        //     Reads a Boolean value from the current stream and advances the current position
        //     of the stream by one byte.
        //
        // Returns:
        //     true if the byte is nonzero; otherwise, false.
        //
        // Exceptions:
        //   System.ObjectDisposedException:
        //     The stream is closed.
        //
        //   System.IO.IOException:
        //     An I/O error occurs.
        //
        //   System.IO.EndOfStreamException:
        //     The end of the stream is reached.
        public override bool ReadBoolean()
        {
            return _reader.ReadBoolean();
        }

        //
        // Summary:
        //     Reads the next byte from the current stream and advances the current position
        //     of the stream by one byte.
        //
        // Returns:
        //     The next byte read from the current stream.
        //
        // Exceptions:
        //   System.ObjectDisposedException:
        //     The stream is closed.
        //
        //   System.IO.IOException:
        //     An I/O error occurs.
        //
        //   System.IO.EndOfStreamException:
        //     The end of the stream is reached.
        public override byte ReadByte()
        {
            return _reader.ReadByte();
        }

        //
        // Summary:
        //     Reads count bytes from the current stream into a byte array and advances
        //     the current position by count bytes.
        //
        // Parameters:
        //   count:
        //     The number of bytes to read.
        //
        // Returns:
        //     A byte array containing data read from the underlying stream. This might
        //     be less than the number of bytes requested if the end of the stream is reached.
        //
        // Exceptions:
        //   System.ObjectDisposedException:
        //     The stream is closed.
        //
        //   System.IO.IOException:
        //     An I/O error occurs.
        //
        //   System.ArgumentOutOfRangeException:
        //     count is negative.
        public override byte[] ReadBytes(int count)
        {
            return _reader.ReadBytes(count);
        }

        //
        // Summary:
        //     Reads the next character from the current stream and advances the current
        //     position of the stream in accordance with the Encoding used and the specific
        //     character being read from the stream.
        //
        // Returns:
        //     A character read from the current stream.
        //
        // Exceptions:
        //   System.ObjectDisposedException:
        //     The stream is closed.
        //
        //   System.IO.IOException:
        //     An I/O error occurs.
        //
        //   System.IO.EndOfStreamException:
        //     The end of the stream is reached.
        //
        //   System.ArgumentException:
        //     A surrogate character was read.
        public override char ReadChar()
        {
            return _reader.ReadChar();
        }

        //
        // Summary:
        //     Reads count characters from the current stream, returns the data in a character
        //     array, and advances the current position in accordance with the Encoding
        //     used and the specific character being read from the stream.
        //
        // Parameters:
        //   count:
        //     The number of characters to read.
        //
        // Returns:
        //     A character array containing data read from the underlying stream. This might
        //     be less than the number of characters requested if the end of the stream
        //     is reached.
        //
        // Exceptions:
        //   System.ObjectDisposedException:
        //     The stream is closed.
        //
        //   System.IO.IOException:
        //     An I/O error occurs.
        //
        //   System.IO.EndOfStreamException:
        //     The end of the stream is reached.
        //
        //   System.ArgumentOutOfRangeException:
        //     count is negative.
        public override char[] ReadChars(int count)
        {
            return _reader.ReadChars(count);
        }

        //
        // Summary:
        //     Reads a decimal value from the current stream and advances the current position
        //     of the stream by sixteen bytes.
        //
        // Returns:
        //     A decimal value read from the current stream.
        //
        // Exceptions:
        //   System.ObjectDisposedException:
        //     The stream is closed.
        //
        //   System.IO.IOException:
        //     An I/O error occurs.
        //
        //   System.IO.EndOfStreamException:
        //     The end of the stream is reached.
        public override decimal ReadDecimal()
        {
            return _reader.ReadDecimal();
        }

        //
        // Summary:
        //     Reads an 8-byte floating point value from the current stream and advances
        //     the current position of the stream by eight bytes.
        //
        // Returns:
        //     An 8-byte floating point value read from the current stream.
        //
        // Exceptions:
        //   System.ObjectDisposedException:
        //     The stream is closed.
        //
        //   System.IO.IOException:
        //     An I/O error occurs.
        //
        //   System.IO.EndOfStreamException:
        //     The end of the stream is reached.
        public override double ReadDouble()
        {
            return _reader.ReadDouble();
        }

        //
        // Summary:
        //     Reads a 2-byte signed integer from the current stream and advances the current
        //     position of the stream by two bytes.
        //
        // Returns:
        //     A 2-byte signed integer read from the current stream.
        //
        // Exceptions:
        //   System.ObjectDisposedException:
        //     The stream is closed.
        //
        //   System.IO.IOException:
        //     An I/O error occurs.
        //
        //   System.IO.EndOfStreamException:
        //     The end of the stream is reached.
        public override short ReadInt16()
        {
            return _reader.ReadInt16();
        }

        //
        // Summary:
        //     Reads a 4-byte signed integer from the current stream and advances the current
        //     position of the stream by four bytes.
        //
        // Returns:
        //     A 4-byte signed integer read from the current stream.
        //
        // Exceptions:
        //   System.ObjectDisposedException:
        //     The stream is closed.
        //
        //   System.IO.IOException:
        //     An I/O error occurs.
        //
        //   System.IO.EndOfStreamException:
        //     The end of the stream is reached.
        public override int ReadInt32()
        {
            return _reader.ReadInt32();
        }

        //
        // Summary:
        //     Reads an 8-byte signed integer from the current stream and advances the current
        //     position of the stream by eight bytes.
        //
        // Returns:
        //     An 8-byte signed integer read from the current stream.
        //
        // Exceptions:
        //   System.ObjectDisposedException:
        //     The stream is closed.
        //
        //   System.IO.IOException:
        //     An I/O error occurs.
        //
        //   System.IO.EndOfStreamException:
        //     The end of the stream is reached.
        public override long ReadInt64()
        {
            return _reader.ReadInt64();
        }

        //
        // Summary:
        //     Reads a signed byte from this stream and advances the current position of
        //     the stream by one byte.
        //
        // Returns:
        //     A signed byte read from the current stream.
        //
        // Exceptions:
        //   System.ObjectDisposedException:
        //     The stream is closed.
        //
        //   System.IO.IOException:
        //     An I/O error occurs.
        //
        //   System.IO.EndOfStreamException:
        //     The end of the stream is reached.
        //[CLSCompliant(false)]
        public override sbyte ReadSByte()
        {
            return _reader.ReadSByte();
        }

        //
        // Summary:
        //     Reads a 4-byte floating point value from the current stream and advances
        //     the current position of the stream by four bytes.
        //
        // Returns:
        //     A 4-byte floating point value read from the current stream.
        //
        // Exceptions:
        //   System.ObjectDisposedException:
        //     The stream is closed.
        //
        //   System.IO.IOException:
        //     An I/O error occurs.
        //
        //   System.IO.EndOfStreamException:
        //     The end of the stream is reached.
        public override float ReadSingle()
        {
            return _reader.ReadSingle();
        }

        //
        // Summary:
        //     Reads a string from the current stream. The string is prefixed with the length,
        //     encoded as an integer seven bits at a time.
        //
        // Returns:
        //     The string being read.
        //
        // Exceptions:
        //   System.ObjectDisposedException:
        //     The stream is closed.
        //
        //   System.IO.IOException:
        //     An I/O error occurs.
        //
        //   System.IO.EndOfStreamException:
        //     The end of the stream is reached.
        public override string ReadString()
        {
            return _reader.ReadString();
        }

        //
        // Summary:
        //     Reads a 2-byte unsigned integer from the current stream using little endian
        //     encoding and advances the position of the stream by two bytes.
        //
        // Returns:
        //     A 2-byte unsigned integer read from this stream.
        //
        // Exceptions:
        //   System.ObjectDisposedException:
        //     The stream is closed.
        //
        //   System.IO.IOException:
        //     An I/O error occurs.
        //
        //   System.IO.EndOfStreamException:
        //     The end of the stream is reached.
        //[CLSCompliant(false)]
        public override ushort ReadUInt16()
        {
            return _reader.ReadUInt16();
        }

        //
        // Summary:
        //     Reads a 4-byte unsigned integer from the current stream and advances the
        //     position of the stream by four bytes.
        //
        // Returns:
        //     A 4-byte unsigned integer read from this stream.
        //
        // Exceptions:
        //   System.ObjectDisposedException:
        //     The stream is closed.
        //
        //   System.IO.IOException:
        //     An I/O error occurs.
        //
        //   System.IO.EndOfStreamException:
        //     The end of the stream is reached.
        //[CLSCompliant(false)]
        public override uint ReadUInt32()
        {
            return _reader.ReadUInt32();
        }

        //
        // Summary:
        //     Reads an 8-byte unsigned integer from the current stream and advances the
        //     position of the stream by eight bytes.
        //
        // Returns:
        //     An 8-byte unsigned integer read from this stream.
        //
        // Exceptions:
        //   System.ObjectDisposedException:
        //     The stream is closed.
        //
        //   System.IO.IOException:
        //     An I/O error occurs.
        //
        //   System.IO.EndOfStreamException:
        //     The end of the stream is reached.
        //[CLSCompliant(false)]
        public override ulong ReadUInt64()
        {
            return _reader.ReadUInt64();
        }

        /// <summary>
        /// Set a DataBlock will write the Block's data byte array into this object's
        /// Memory Stream.
        /// </summary>
        public Sop.DataBlock DataBlock
        {
            set
            {
                if (value != null)
                {
                    if (_reader != null)
                        _reader.Close();
                    _dataBlock = value;
                    byte[] b = value.GetData();
                    _reader = new BinaryReader(new MemoryStream(b), _encoding);
                    BaseStream.Seek(0, SeekOrigin.Begin);
                }
            }
            get { return _dataBlock; }
        }
        private Sop.DataBlock _dataBlock = null;

        public long Seek(long offset, SeekOrigin seekOrigin)
        {
            return BaseStream.Seek(offset, seekOrigin);
        }

        /// <summary>
        /// Read InternalPersistent Value returns the Data (byte array) of a previously saved InternalPersistent.
        /// </summary>
        /// <returns></returns>
        public byte[] ReadPersistent(Sop.DataBlock dataBlock)
        {
            DataBlock = dataBlock;
            int size = ReadInt32();
            return ReadBytes(size);
        }

        /// <summary>
        /// Useful if wanting to DeSerialize back into IInternalPersistent type of object.
        /// </summary>
        /// <returns></returns>
        public byte[] ReadBytes(Sop.DataBlock dataBlock)
        {
            DataBlock = dataBlock;
            #region for removal
            //long l = BaseStream.Position;
            //var m = (MemoryStream) _reader.BaseStream;
            //m.Seek(0, SeekOrigin.End);
            //if (l < m.Position)
            //{
            //    long c = m.Position - l;
            //    m.Seek(l, SeekOrigin.Begin);
            //    return ReadBytes((int) c);
            //}
            //return null;
            #endregion
            long n = _reader.BaseStream.Length - BaseStream.Position;
            return n > 0 ? ReadBytes((int)n) : null;
        }

        /// <summary>
        /// Caller code need to provide the Value Type. Plan is Collection Manager will persist Type name
        /// of the Value it supports, then during reading, will use the persisted Type name and get the 
        /// Type from Types Collection.
        /// </summary>
        /// <param name="dataBlock"> </param>
        /// <returns></returns>
        public object ReadObject(Sop.DataBlock dataBlock)
        {
            this.DataBlock = dataBlock;
            return _serializer.Deserialize(this.BaseStream);
        }

        /// <summary>
        /// Serialize an Object to Xml. NOTE: object should be Xml Serializable
        /// </summary>
        /// <param name="serializer"></param>
        public object ReadFromXml(XmlSerializer serializer)
        {
            if (serializer == null)
                throw new ArgumentNullException("serializer");
            int dataSize;
            byte[] b = ReadRawData(out dataSize);
            using (var ms = new MemoryStream(b, 0, dataSize))
            {
                return serializer.Deserialize(ms);
            }
        }

        /// <summary>
        /// Read raw data from stream. This is useful to provide code
        /// capability to get raw data and do its own deserialization.
        /// </summary>
        /// <returns></returns>
        public byte[] ReadRawData(out int dataSize)
        {
            Array.Clear(_buffer, 0, _buffer.Length);
            return ReadRawData(_buffer, out dataSize);
        }
        /// <summary>
        /// Read raw data from stream. This is useful to provide code
        /// capability to get raw data and do its own deserialization.
        /// </summary>
        /// <returns></returns>
        public byte[] ReadRawData(byte[] target, out int dataSize)
        {
            if (target == null)
                throw new ArgumentNullException("target");
            dataSize = _reader.ReadInt32();
            byte[] b = null;
            b = dataSize <= target.Length ? target : new byte[dataSize];
            BaseStream.Read(b, 0, dataSize);
            return b;
        }


        private readonly byte[] _buffer = new byte[1024];

        private readonly System.Runtime.Serialization.Formatters.Binary.BinaryFormatter _serializer =
            new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();

        private System.IO.BinaryReader _reader;
        private readonly System.Text.Encoding _encoding;
    }
}