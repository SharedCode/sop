// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;
using System.IO;
using System.Xml.Serialization;
using Sop.Persistence;

namespace Sop.OnDisk.IO
{
    /// <summary>
    /// OnDiskBineryWriter overrides BinaryWriter to write basic data types
    /// to target DataBlock(s)
    /// </summary>
    internal class OnDiskBinaryWriter : BinaryWriter
    {
        /// <summary>
        /// Default Constructor
        /// </summary>
        public OnDiskBinaryWriter()
        {
            _buffer = new MemoryStream();
            this.OutStream = _buffer;
        }

        /// <summary>
        /// Constructor expecting Encoding for use while Writing
        /// </summary>
        /// <param name="encoding"></param>
        public OnDiskBinaryWriter(System.Text.Encoding encoding)
            : base(new MemoryStream(), encoding)
        {
            _buffer = (MemoryStream)this.OutStream;
        }

        /// <summary>
        /// Write a string Value
        /// </summary>
        /// <param name="value"></param>
        public override void Write(string value)
        {
            if (!_baseMode)
            {
                int si = (int)this.BaseStream.Position;
                _baseMode = true;
                try
                {
                    base.Write(value);
                }
                finally
                {
                    _baseMode = false;
                }
                this.write(si, (int)this.BaseStream.Position);
            }
            else
                base.Write(value);
        }

        /// <summary>
        /// Write a float Value
        /// </summary>
        /// <param name="value"></param>
        public override void Write(float value)
        {
            if (!_baseMode)
            {
                int si = (int)this.BaseStream.Position;
                _baseMode = true;
                try
                {
                    base.Write(value);
                }
                finally
                {
                    _baseMode = false;
                }
                this.write(si, (int)this.BaseStream.Position);
            }
            else
                base.Write(value);
        }

        /// <summary>
        /// Write an unsigned long Value
        /// </summary>
        /// <param name="value"></param>
        public override void Write(ulong value)
        {
            if (!_baseMode)
            {
                int si = (int)this.BaseStream.Position;
                _baseMode = true;
                try
                {
                    base.Write(value);
                }
                finally
                {
                    _baseMode = false;
                }
                this.write(si, (int)this.BaseStream.Position);
            }
            else
                base.Write(value);
        }

        /// <summary>
        /// Write a long Value
        /// </summary>
        /// <param name="value"></param>
        public override void Write(long value)
        {
            int si = (int)this.BaseStream.Position;
            base.Write(value);
            this.write(si, (int)this.BaseStream.Position);
        }

        /// <summary>
        /// Write an unsigned int Value
        /// </summary>
        /// <param name="value"></param>
        public override void Write(uint value)
        {
            int si = (int)this.BaseStream.Position;
            base.Write(value);
            this.write(si, (int)this.BaseStream.Position);
        }

        /// <summary>
        /// Write an int Value
        /// </summary>
        /// <param name="value"></param>
        public override void Write(int value)
        {
            int si = (int)this.BaseStream.Position;
            base.Write(value);
            this.write(si, (int)this.BaseStream.Position);
        }

        /// <summary>
        /// Write an unsigned short Value
        /// </summary>
        /// <param name="value"></param>
        public override void Write(ushort value)
        {
            int si = (int)this.BaseStream.Position;
            base.Write(value);
            this.write(si, (int)this.BaseStream.Position);
        }

        /// <summary>
        /// Write short Value
        /// </summary>
        /// <param name="value"></param>
        public override void Write(short value)
        {
            int si = (int)this.BaseStream.Position;
            base.Write(value);
            this.write(si, (int)this.BaseStream.Position);
        }

        /// <summary>
        /// Write a decimal Value
        /// </summary>
        /// <param name="value"></param>
        public override void Write(decimal value)
        {
            int si = (int)BaseStream.Position;
            base.Write(value);
            write(si, (int)BaseStream.Position);
        }

        /// <summary>
        /// Write a double Value
        /// </summary>
        /// <param name="Value"></param>
        public override void Write(double Value)
        {
            int si = (int)this.BaseStream.Position;
            base.Write(Value);
            this.write(si, (int)this.BaseStream.Position);
        }

        /// <summary>
        /// Write a char array
        /// </summary>
        /// <param name="chars"></param>
        /// <param name="index"></param>
        /// <param name="count"></param>
        public override void Write(char[] chars, int index, int count)
        {
            int si = (int)this.BaseStream.Position;
            base.Write(chars, index, count);
            this.write(si, (int)this.BaseStream.Position);
        }

        /// <summary>
        /// Write a char array
        /// </summary>
        /// <param name="chars"></param>
        public override void Write(char[] chars)
        {
            int si = (int)BaseStream.Position;
            base.Write(chars);
            write(si, (int)BaseStream.Position);
        }

        /// <summary>
        /// Write a char
        /// </summary>
        /// <param name="ch"></param>
        public override void Write(char ch)
        {
            int si = (int)BaseStream.Position;
            base.Write(ch);
            write(si, (int)BaseStream.Position);
        }

        /// <summary>
        /// Write a byte array
        /// </summary>
        /// <param name="buffer"></param>
        /// <param name="index"></param>
        /// <param name="count"></param>
        public override void Write(byte[] buffer, int index, int count)
        {
            int si = (int)this.BaseStream.Position;
            base.Write(buffer, index, count);
            this.write(si, (int)this.BaseStream.Position);
        }

        /// <summary>
        /// Write a byte array
        /// </summary>
        /// <param name="buffer"></param>
        public override void Write(byte[] buffer)
        {
            int si = (int)this.BaseStream.Position;
            base.Write(buffer);
            this.write(si, (int)this.BaseStream.Position);
        }

        /// <summary>
        /// Write a signed byte
        /// </summary>
        /// <param name="value"></param>
        public override void Write(sbyte value)
        {
            int si = (int)this.BaseStream.Position;
            base.Write(value);
            this.write(si, (int)this.BaseStream.Position);
        }

        /// <summary>
        /// Write a byte
        /// </summary>
        /// <param name="value"></param>
        public override void Write(byte value)
        {
            if (!_baseMode)
            {
                int si = (int)this.BaseStream.Position;
                _baseMode = true;
                try
                {
                    base.Write(value);
                }
                finally
                {
                    _baseMode = false;
                }
                this.write(si, (int)this.BaseStream.Position);
            }
            else
                base.Write(value);
        }

        /// <summary>
        /// Write a boolean Value
        /// </summary>
        /// <param name="value"></param>
        public override void Write(bool value)
        {
            int si = (int)this.BaseStream.Position;
            base.Write(value);
            this.write(si, (int)this.BaseStream.Position);
        }

        internal void write(int startIndex, int endIndex)
        {
            write(_buffer.GetBuffer(), startIndex, (int)(endIndex - startIndex));
        }

        private void write(byte[] srcData, int index, int size)
        {
            if (DataBlockPosition + size <= DataBlock.SizeOccupied)
            {
                //** overwrite data in Stream Position..
                Array.Copy(srcData, index, DataBlock.Data, DataBlockPosition, size);
                DataBlockPosition += size;
            }
            else if (DataBlock.SizeAvailable > 0)
            {
                int BytesToCopy = size;
                if (BytesToCopy <= DataBlock.SizeAvailable)
                {
                    Array.Copy(srcData, index, DataBlock.Data, DataBlockPosition, BytesToCopy);
                    DataBlockPosition += BytesToCopy;
                    DataBlock.SizeOccupied += BytesToCopy;
                }
                else
                {
                    // Write Data spanning multiple blocks
                    int BytesToWrite = DataBlock.SizeAvailable;
                    Array.Copy(srcData, index, DataBlock.Data, DataBlockPosition, BytesToWrite);
                    DataBlockPosition = DataBlock.Data.Length;
                    DataBlock.SizeOccupied = DataBlock.Data.Length;
                    write(srcData, index + BytesToWrite, BytesToCopy - BytesToWrite);
                }
            }
            else
            {
                _dataBlock = DataBlock.Extend();
                DataBlockPosition = 0;
                _logicalPosition += OutStream.Position;
                OutStream.SetLength(0);
                OutStream.Seek(0, SeekOrigin.Begin);
                do
                {
                    if (_dataBlock.Data.Length < size)
                    {
                        Array.Copy(srcData, index, _dataBlock.Data, 0, _dataBlock.Data.Length);
                        index += _dataBlock.Data.Length;
                        _dataBlock.SizeOccupied = _dataBlock.Data.Length;
                        size -= _dataBlock.Data.Length;
                        _dataBlock.Extend();
                        _dataBlock = _dataBlock.Next;
                    }
                    else
                    {
                        Array.Copy(srcData, index, _dataBlock.Data, 0, size);
                        _dataBlock.SizeOccupied = size;
                        DataBlockPosition = size;
                        break;
                    }
                } while (size > 0);
            }
        }

        /// <summary>
        /// Serialize an Object to Xml. NOTE: object should be Xml Serializable
        /// </summary>
        /// <param name="serializer"></param>
        /// <param name="value"></param>
        public void WriteAsXml(XmlSerializer serializer, object value)
        //System.Text.Encoding Encoding)
        {
            if (value == null)
                throw new ArgumentNullException("value");

            long currPos = BaseStream.Position;
            base.Write(0);
            long sm = BaseStream.Position;
            serializer.Serialize(BaseStream, value);
            long cp = BaseStream.Position;
            BaseStream.Seek(currPos, SeekOrigin.Begin);
            base.Write((int)(cp - sm));
            BaseStream.Seek(cp, SeekOrigin.Begin);
            write((int)currPos, (int)cp); //(int)BaseStream.Position);
        }

        public void WriteRawData(byte[] rawData)
        {
            if (rawData == null)
                throw new ArgumentNullException("rawData");
            WriteRawData(rawData, 0, rawData.Length);
        }
        /// <summary>
        /// Serialize an Object to Xml. NOTE: object should be Xml Serializable
        /// </summary>
        /// <param name="serializer"></param>
        /// <param name="value"></param>
        public void WriteRawData(byte[] rawData, int index, int count)
        {
            if (rawData == null)
                throw new ArgumentNullException("rawData");
            long currPos = BaseStream.Position;
            base.Write(0);
            long sm = BaseStream.Position;
            BaseStream.Write(rawData, index, count);
            long cp = BaseStream.Position;
            BaseStream.Seek(currPos, SeekOrigin.Begin);
            base.Write((int)(cp - sm));
            BaseStream.Seek(cp, SeekOrigin.Begin);
            write((int)currPos, (int)cp);
        }


        /// <summary>
        /// Serialize an Object to the target DataBlock
        /// </summary>
        /// <param name="file"></param>
        /// <param name="value"></param>
        /// <param name="dataBlock"></param>
        public void WriteObject(File.IFile file, object value, Sop.DataBlock dataBlock)
        {
            if (value == null)
                throw new ArgumentNullException("value");

            this.DataBlock = dataBlock;
            if (value is IInternalPersistent)
                ((IInternalPersistent)value).Pack(file, this);
            else
            {
                _serializer.Serialize(this.BaseStream, value);
                this.Write(_buffer.GetBuffer(), 0, (int)this.BaseStream.Position);
            }
        }

        /// <summary>
        /// Moves the Stream Pointer to a given Offset relative to the Origin
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="origin"></param>
        /// <returns></returns>
        public override long Seek(int offset, SeekOrigin origin)
        {
            if (origin == SeekOrigin.Begin)
                DataBlockPosition = offset;
            else if (origin == SeekOrigin.End)
            {
                if (offset == 0)
                    DataBlockPosition = DataBlock.Data.Length - 1;
            }
            else
                DataBlockPosition += offset;
            return base.Seek(offset, origin);
        }

        /// <summary>
        /// get/set the target DataBlock. All Data writes will
        /// write to the target Block(s). OnDiskBinaryWriter takes
        /// care of dividing the data for write across 1 or more
        /// DataBlocks as needed.
        /// </summary>
        public Sop.DataBlock DataBlock
        {
            get { return _dataBlock; }
            set
            {
                _dataBlock = value;
                DataBlockPosition = 0;
                _logicalPosition = 0;
                OutStream.SetLength(value.Length);
                OutStream.Seek(0, SeekOrigin.Begin);
            }
        }
        /// <summary>
        /// Index in Data Block where next item will be written.
        /// </summary>
        public int DataBlockPosition { get; private set; }

        /// <summary>
        /// Returns the actual byte location of the stream pointer in the logical stream.
        ///
        /// NOTE: BaseStream.Position can return different value than LogicalPosition as
        /// this implementation resets/empties the BaseStream at times necessary to conserve
        /// memory. Intent is to write data to the target DataBlock(s) and it's discretion of
        /// code whether to truncate the BaseStream or not to conserve memory.
        ///
        /// LogicalPosition then serves the purpose of tracking and returning the actual
        /// total count of bytes written to the "stream".
        /// </summary>
        public long LogicalPosition
        {
            get
            {
                return _logicalPosition + OutStream.Position;
            }
            private set
            {
                _logicalPosition = value;
            }
        }
        private long _logicalPosition;

        private Sop.DataBlock _dataBlock;

        private readonly System.Runtime.Serialization.Formatters.Binary.BinaryFormatter _serializer =
            new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();

        private readonly MemoryStream _buffer;
        private bool _baseMode;
    }
}
