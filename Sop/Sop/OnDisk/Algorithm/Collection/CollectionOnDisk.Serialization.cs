// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.IO;
using System.Collections.Generic;
using System.Runtime.Serialization.Formatters.Binary;
using Sop.Collections.BTree;
using Sop.OnDisk.Algorithm.BTree;
using Sop.OnDisk.Algorithm.SortedDictionary;
using Sop.OnDisk.IO;
using Sop.Persistence;
using BTreeAlgorithm = Sop.OnDisk.Algorithm.BTree.BTreeAlgorithm;

namespace Sop.OnDisk.Algorithm.Collection
{
    internal abstract partial class CollectionOnDisk
    {
        internal static void WriteSimpleType(object value, BinaryWriter writer)
        {
            TypeCode tc = Type.GetTypeCode(value.GetType());
            writer.Write((short) tc);

            switch (tc)
            {
                case TypeCode.Single:
                    writer.Write((float) value);
                    break;
                case TypeCode.Double:
                    writer.Write((double) value);
                    break;
                case TypeCode.Int16:
                    writer.Write((short) value);
                    break;
                case TypeCode.UInt16:
                    writer.Write((ushort) value);
                    break;
                case TypeCode.Int32:
                    writer.Write((int) value);
                    break;
                case TypeCode.UInt32:
                    writer.Write((uint) value);
                    break;
                case TypeCode.Int64:
                    writer.Write((long) value);
                    break;
                case TypeCode.UInt64:
                    writer.Write((ulong) value);
                    break;
                case TypeCode.Boolean:
                    writer.Write((bool) value);
                    break;
                case TypeCode.Byte:
                    writer.Write((byte) value);
                    break;
                case TypeCode.SByte:
                    writer.Write((sbyte) value);
                    break;
                case TypeCode.Char:
                    writer.Write((char) value);
                    break;
                case TypeCode.Decimal:
                    writer.Write((decimal) value);
                    break;
                case TypeCode.DateTime:
                    writer.Write(((DateTime) value).Ticks);
                    break;
                default:
                    throw new SopException(string.Format("Unsupported simple type '{0}'", tc));
            }
        }

        private static object ReadSimpleType(BinaryReader reader)
        {
            var tc = (TypeCode) reader.ReadInt16();
            switch (tc)
            {
                case TypeCode.Single:
                    return reader.ReadSingle();
                case TypeCode.Double:
                    return reader.ReadDouble();
                case TypeCode.Int16:
                    return reader.ReadInt16();
                case TypeCode.UInt16:
                    return reader.ReadUInt16();
                case TypeCode.Int32:
                    return reader.ReadInt32();
                case TypeCode.UInt32:
                    return reader.ReadUInt32();
                case TypeCode.Int64:
                    return reader.ReadInt64();
                case TypeCode.UInt64:
                    return reader.ReadUInt64();
                case TypeCode.Boolean:
                    return reader.ReadBoolean();
                case TypeCode.Byte:
                    return reader.ReadByte();
                case TypeCode.SByte:
                    return reader.ReadSByte();
                case TypeCode.Char:
                    return reader.ReadChar();
                case TypeCode.Decimal:
                    return reader.ReadDecimal();
                case TypeCode.DateTime:
                    return new DateTime(reader.ReadInt64());
            }
            return null;
        }

        internal static bool CompareSimpleType(object a, object b)
        {
            if (a is float && b is float)
                return !(Math.Abs((float) a - (float) b) > 0);
            if (a is double && b is double)
                return !(Math.Abs((double)a - (double)b) > 0);

            if (a is short && b is short)
                return (short) a == (short) b;
            if (a is ushort && b is ushort)
                return (ushort) a == (ushort) b;
            if (a is bool && b is bool)
                return (bool) a == (bool) b;
            if (a is byte && b is byte)
                return (byte) a == (byte) b;
            if (a is sbyte && b is sbyte)
                return (sbyte) a == (sbyte) b;
            if (a is char && b is char)
                return (char) a == (char) b;
            if (a is decimal && b is decimal)
                return (decimal) a == (decimal) b;
            if (a is int && b is int)
                return (int) a == (int) b;
            if (a is uint && b is uint)
                return (uint) a == (uint) b;
            if (a is long && b is long)
                return (long) a == (long) b;
            if (a is ulong && b is ulong)
                return (ulong) a == (ulong) b;
            if (a is DateTime && b is DateTime)
                return (DateTime) a == (DateTime) b;
            return false;
        }

        /// <summary>
        /// Check whether a given object 'o' is of SOP simple type.
        /// SOP simple types are:
        ///     float, short/ushort, double, bool, byte/sbyte/char, decimal, int/uint, long/ulong, datetime
        /// </summary>
        /// <param name="o"></param>
        /// <returns></returns>
        public static bool IsSimpleType(object o)
        {
            return o is float ||
                    o is short ||
                    o is ushort ||
                    o is double ||
                    o is bool ||
                    o is byte ||
                    o is sbyte ||
                    o is char ||
                    o is decimal ||
                    o is int ||
                    o is uint ||
                    o is long ||
                    o is ulong ||
                    o is DateTime;
        }
        /// <summary>
        /// Check whether a given type 't' is an SOP simple type.
        /// SOP simple types are:
        ///     float, short/ushort, double, bool, byte/sbyte/char, decimal, int/uint, long/ulong, datetime
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        public static bool IsSimpleType(Type t)
        {
            return t == typeof (float) ||
                   t == typeof (short) ||
                   t == typeof (ushort) ||
                   t == typeof (double) ||
                   t == typeof (bool) ||
                   t == typeof (byte) ||
                   t == typeof (sbyte) ||
                   t == typeof (char) ||
                   t == typeof (decimal) ||
                   t == typeof (int) ||
                   t == typeof (uint) ||
                   t == typeof (long) ||
                   t == typeof (ulong) ||
                   t == typeof (DateTime);
        }

        /// <summary>
        /// Read/Deserialize Persisted object
        /// </summary>
        /// <param name="parent"> </param>
        /// <param name="reader"></param>
        /// <param name="itemType"> </param>
        /// <param name="deSerializedObject">DeSerializedObject will contain the Deserialized object (returns true), 
        /// else the size of the Custom Persisted object (returns false).
        /// </param>
        /// <returns>true if Deserialized, 
        ///         false if End of Stream,
        ///         null if object was Custom Persisted and was not read</returns>
        public static bool? ReadPersistentData(
            IInternalPersistent parent,
            BinaryReader reader,
            ref object deSerializedObject,
            ItemType itemType = ItemType.Default
            )
        {
            if (reader.PeekChar() == -1)
            {
                //** end of stream, no more object to deserialize...
                deSerializedObject = 0;
                return false;
            }
            var file = (File.File) InternalPersistent.GetParent(parent, typeof (File.File));
            var t = (PersistenceType) reader.ReadByte();
            int size;
            switch (t)
            {
                case PersistenceType.Custom:
                    //** get the type of object to be deserialized
                    int objectTypeId = reader.ReadInt32();
                    //** get the size of object
                    size = reader.ReadInt32();
                    if (size > 0)
                    {
                        if (deSerializedObject == null)
                            deSerializedObject = file.Server.TypeStore.CreateInstance(file.Transaction, objectTypeId,
                                                                                      new[]
                                                                                          {
                                                                                              new KeyValuePair
                                                                                                  <string, object>(
                                                                                                  "File", file)
                                                                                          });
                        if (deSerializedObject is IInternalPersistent)
                        {
                            if (deSerializedObject is ICollectionOnDisk &&
                                ((ICollectionOnDisk) deSerializedObject).File == null)
                                ((ICollectionOnDisk) deSerializedObject).File = file;
                            else if (deSerializedObject is File.File)
                            {
                                if (parent is ICollectionOnDisk)
                                {
                                    //((File.File)deSerializedObject).Profile = new Profile(file.Profile);
                                    ((File.File)deSerializedObject).Profile.DataBlockSize = file.Profile.DataBlockSize;
                                }
                                if (((File.File) deSerializedObject).Parent == null)
                                {
                                    if (parent is ICollectionOnDisk)
                                        ((File.File) deSerializedObject).Parent = (ICollectionOnDisk) parent;
                                    ((File.File) deSerializedObject).Server = file.Server;
                                }
                            }
                            if (deSerializedObject is SortedDictionaryOnDisk)
                            {
                                var bTreeAlgorithm = parent as BTreeAlgorithm;
                                if (bTreeAlgorithm != null)
                                {
                                    if ((bTreeAlgorithm).onInnerMemberKeyPack != null)
                                        ((SortedDictionaryOnDisk) deSerializedObject).BTreeAlgorithm.onKeyPack =
                                            (bTreeAlgorithm).onInnerMemberKeyPack;
                                    if ((bTreeAlgorithm).onInnerMemberKeyUnpack != null)
                                        ((SortedDictionaryOnDisk) deSerializedObject).BTreeAlgorithm.onKeyUnpack =
                                            (bTreeAlgorithm).onInnerMemberKeyUnpack;

                                    if ((bTreeAlgorithm).onInnerMemberValuePack != null)
                                        ((SortedDictionaryOnDisk) deSerializedObject).BTreeAlgorithm.onValuePack =
                                            (bTreeAlgorithm).onInnerMemberValuePack;
                                    if ((bTreeAlgorithm).onInnerMemberValueUnpack != null)
                                        ((SortedDictionaryOnDisk) deSerializedObject).BTreeAlgorithm.onValueUnpack =
                                            (bTreeAlgorithm).onInnerMemberValueUnpack;
                                }
                                var sdod = (SortedDictionaryOnDisk) deSerializedObject;
                                sdod.DataAddress = reader.ReadInt64();
                                sdod.Open();
                            }
                            else
                                ((IInternalPersistent) deSerializedObject).Unpack(parent, reader);
                            return true;
                        }
                        //** Object(with ObjectTypeID) is User defined Type. Caller code should DeSerialize it...
                        deSerializedObject = null;
                        return null;
                    }
                    if (size == 0)
                        return true;
                    //** end of stream, no more object to deserialize...
                    deSerializedObject = 0;
                    return false;
                case PersistenceType.SimpleType:
                    deSerializedObject = ReadSimpleType(reader);
                    break;
                case PersistenceType.String:
                    deSerializedObject = reader.ReadString();
                    break;
                case PersistenceType.ByteArray:
                    size = reader.ReadInt32();
                    deSerializedObject = reader.ReadBytes(size);
                    break;
                case PersistenceType.BinarySerialized:
                    var ser =
                        new BinaryFormatter();
                    deSerializedObject = ser.Deserialize(reader.BaseStream);
                    break;
                case PersistenceType.Null:
                    deSerializedObject = null;
                    break;
            }
            return true;
        }

        public static void WritePersistentData(
            IInternalPersistent parent,
            object objectToSerialize,
            BinaryWriter writer)
        {
            WritePersistentData(parent, objectToSerialize, writer, Sop.Collections.BTree.ItemType.Default);
        }

        /// <summary>
        /// Serialize Object in preparation for saving to disk/virtual store
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="objectToSerialize"></param>
        /// <param name="writer"></param>
        /// <param name="itemType"> </param>
        public static void WritePersistentData(
            IInternalPersistent parent,
            object objectToSerialize,
            BinaryWriter writer,
            ItemType itemType
            )
        {
            if (objectToSerialize != null)
            {
                long proxyRealDataAddress = 0;
                #region process proxy object 
                if (objectToSerialize is Sop.IProxyObject)
                {
                    if (((Sop.IProxyObject)objectToSerialize).RealObject != null)
                    {
                        long.TryParse(objectToSerialize.ToString(), out proxyRealDataAddress);
                        objectToSerialize = ((Sop.IProxyObject) objectToSerialize).RealObject;
                    }
                    else
                    {
                        string s = objectToSerialize.ToString();
                        long dataAddress;
                        if (long.TryParse(s, out dataAddress))
                        {
                            // save object's value
                            //** save persistence method used so we can deserialize
                            writer.Write((byte)PersistenceType.Custom);

                            //** save Value type so we can deserialize to the same type.
                            const int objectTypeId = (int)BuiltinTypes.SortedDictionaryOnDisk;
                            writer.Write(objectTypeId);

                            //** reserve space for the data size!
                            writer.Write(40);
                            writer.Write(dataAddress);
                            // set to null to prevent code
                            objectToSerialize = null;
                        }
                        else
                        {
                            throw new InvalidOperationException(
                                string.Format("Unknown 'objectToSerialize' ({0}) type.",
                                              objectToSerialize.GetType().ToString()));
                        }
                    }
                }
                #endregion
                if (objectToSerialize is IInternalPersistent)
                {
                    // save object's value
                    //** save persistence method used so we can deserialize
                    writer.Write((byte) PersistenceType.Custom);

                    var file = (File.IFile) InternalPersistent.GetParent(parent, typeof (File.File));
                    //** save Value type so we can deserialize to the same type.
                    int objectTypeId = file.Server.TypeStore.RegisterType(objectToSerialize);
                    writer.Write(objectTypeId);

                    //** save data value size and data value
                    int posn = 0;
                    Sop.DataBlock posnBlock = null;
                    if (objectToSerialize is BTreeNodeOnDisk)
                    {
                        posn = (int) writer.BaseStream.Position;
                        posnBlock = ((OnDiskBinaryWriter) writer).DataBlock;
                    }
                    //** reserve space for the data size!
                    writer.Write(40);

                    if (objectToSerialize is SortedDictionaryOnDisk)
                    {
                        var sdod = (SortedDictionaryOnDisk) objectToSerialize;
                        if (proxyRealDataAddress > 0)
                            writer.Write(proxyRealDataAddress);
                        else
                        {
                            if (sdod.DataAddress == -1)
                                sdod.Flush();
                            writer.Write(sdod.DataAddress);
                        }
                    }
                    else
                        ((IInternalPersistent)objectToSerialize).Pack(parent, writer);

                    //** save data value size and data value
                    var bTreeNodeOnDisk = objectToSerialize as BTreeNodeOnDisk;
                    if (bTreeNodeOnDisk != null)
                    {
                        int cm = (bTreeNodeOnDisk).DiskBuffer.CountMembers(true);
                        if (cm > byte.MaxValue)
                            cm = byte.MaxValue;
                        posnBlock.Data[posn] = (byte) cm;
                    }
                }
                else if (IsSimpleType(objectToSerialize))
                {
                    writer.Write((byte) PersistenceType.SimpleType);
                    WriteSimpleType(objectToSerialize, writer);
                }
                else if (objectToSerialize is string)
                {
                    writer.Write((byte) PersistenceType.String);
                    var s = (string) objectToSerialize;
                    writer.Write(s);
                }
                else if (objectToSerialize is byte[])
                {
                    writer.Write((byte) PersistenceType.ByteArray);
                    var rawBytes = (byte[]) objectToSerialize;
                    writer.Write(rawBytes.Length);
                    writer.Write(rawBytes);
                }
                else if (itemType == ItemType.Key &&
                         parent is BTreeAlgorithm &&
                         ((BTreeAlgorithm) parent).onKeyPack != null)
                {
                    // save object's value
                    //** save persistence method used so we can deserialize
                    writer.Write((byte) PersistenceType.Custom);
                    //** save Value type so we can deserialize to the same type.
                    var file = (File.IFile) InternalPersistent.GetParent(parent, typeof (File.File));
                    int objectTypeId = file.Server.TypeStore.RegisterType(objectToSerialize);
                    writer.Write(objectTypeId);
                    //** reserve space for the data size!
                    writer.Write(40);
                    //((Algorithm.BTreeAlgorithm)Parent).onKeyPack(Writer, ObjectToSerialize);

                    int sizeOccupied = ((OnDiskBinaryWriter) writer).DataBlock.SizeOccupied;
                    var db = ((OnDiskBinaryWriter) writer).DataBlock;

                    int posn = (int) writer.BaseStream.Position;
                    ((BTreeAlgorithm) parent).onKeyPack(writer, objectToSerialize);

                    //** write to SOP disk buffer if data wasn't written to it yet...
                    if (db == ((OnDiskBinaryWriter) writer).DataBlock &&
                        sizeOccupied == ((OnDiskBinaryWriter) writer).DataBlock.SizeOccupied)
                        ((OnDiskBinaryWriter) writer).write(posn, (int) writer.BaseStream.Position);
                }
                else if (itemType == ItemType.Value &&
                         parent is BTreeAlgorithm &&
                         ((BTreeAlgorithm) parent).onValuePack != null)
                {
                    // save object's value
                    //** save persistence method used so we can deserialize
                    writer.Write((byte) PersistenceType.Custom);
                    //** save Value type so we can deserialize to the same type.
                    var file = (File.IFile) InternalPersistent.GetParent(parent, typeof (File.File));
                    int objectTypeId = file.Server.TypeStore.RegisterType(objectToSerialize);
                    writer.Write(objectTypeId);
                    //** reserve space for the data size!
                    writer.Write(40);
                    //((Algorithm.BTreeAlgorithm)Parent).onValuePack(Writer, ObjectToSerialize);

                    int sizeOccupied = ((OnDiskBinaryWriter) writer).DataBlock.SizeOccupied;
                    Sop.DataBlock db = ((OnDiskBinaryWriter) writer).DataBlock;

                    int posn = (int) writer.BaseStream.Position;
                    ((BTreeAlgorithm) parent).onValuePack(writer, objectToSerialize);
                    //** write to SOP disk buffer if data wasn't written to it yet...
                    if (db == ((OnDiskBinaryWriter) writer).DataBlock &&
                        sizeOccupied == ((OnDiskBinaryWriter) writer).DataBlock.SizeOccupied)
                        ((OnDiskBinaryWriter) writer).write(posn, (int) writer.BaseStream.Position);
                }
                else if (objectToSerialize is IPersistent)
                {
                    // save object's value
                    //** save persistence method used so we can deserialize
                    writer.Write((byte) PersistenceType.Custom);
                    //** save Value type so we can deserialize to the same type.
                    var file = (File.IFile) InternalPersistent.GetParent(parent, typeof (File.File));
                    int objectTypeId = file.Server.TypeStore.RegisterType(objectToSerialize);
                    writer.Write(objectTypeId);
                    //** reserve space for the data size!
                    writer.Write(40);

                    //** save data value size and data value
                    int posn = (int) writer.BaseStream.Position;
                    Sop.DataBlock posnBlock = ((OnDiskBinaryWriter) writer).DataBlock;
                    ((IPersistent) objectToSerialize).Pack(writer);
                    int hintSize = ((IPersistent) objectToSerialize).HintSizeOnDisk;
                    if (hintSize > 0)
                    {
                        int sizeWritten = posnBlock.GetSizeOccupied(posn);
                        if (hintSize > sizeWritten)
                        {
                            var b = new byte[hintSize - sizeWritten];
                            writer.Write(b);
                        }
                    }
                }
                else if (objectToSerialize != null)
                {
                    // expects Data to be Binary Serializable
                    writer.Write((byte) PersistenceType.BinarySerialized);
                    int si = (int) writer.BaseStream.Position;
                    var ser = new BinaryFormatter();
                    ser.Serialize(writer.BaseStream, objectToSerialize);
                    ((OnDiskBinaryWriter) writer).write(si, (int) writer.BaseStream.Position);
                }
            }
            else
                writer.Write((byte) PersistenceType.Null);

            //** mark unused next block as unoccupied so it and its linked blocks can be recycled..
            var w = writer as OnDiskBinaryWriter;
            if (w == null) return;
            if (w.DataBlock.SizeAvailable > 0 && w.DataBlock.Next != null && w.DataBlock.Next.SizeOccupied > 0)
                w.DataBlock.Next.SizeOccupied = 0;
        }

        internal static PersistenceType GetPersistenceType(
            IInternalPersistent parent,
            object objectToSerialize,
            ItemType itemType)
        {
            if (objectToSerialize == null)
                return PersistenceType.Null;
            if (objectToSerialize is Sop.IProxyObject)
                objectToSerialize = ((Sop.IProxyObject) objectToSerialize).RealObject;
            if (objectToSerialize is IInternalPersistent)
                return PersistenceType.Custom;
            if (IsSimpleType(objectToSerialize))
                return PersistenceType.SimpleType;
            if (objectToSerialize is string)
                return PersistenceType.String;
            if (objectToSerialize is byte[])
                return PersistenceType.ByteArray;
            if (itemType == ItemType.Key &&
                parent is BTreeAlgorithm &&
                ((BTreeAlgorithm) parent).onKeyPack != null)
                return PersistenceType.Custom;
            if (itemType == ItemType.Value &&
                parent is BTreeAlgorithm &&
                ((BTreeAlgorithm) parent).onValuePack != null)
                return PersistenceType.Custom;
            if (objectToSerialize is IPersistent)
                return PersistenceType.Custom;
            return PersistenceType.BinarySerialized;
        }
    }
}
