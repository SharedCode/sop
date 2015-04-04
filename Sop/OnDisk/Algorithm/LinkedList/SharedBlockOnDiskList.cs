// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.Collections.Generic;
using Sop.Mru;

namespace Sop.OnDisk.Algorithm.LinkedList
{
    internal class SharedBlockOnDiskList : LinkedListOnDisk
    {
        internal SharedBlockOnDiskList()
        {
        }

        public SharedBlockOnDiskList(File.IFile container) : base(container)
        {
        }

        public SharedBlockOnDiskList(File.IFile container, 
            params KeyValuePair<string, object>[] extraParams)
            : base(container, string.Empty, extraParams)
        {
        }

        /// <summary>
        /// Shared Block on Disk purifies data and only saves one
        /// copy of Meta data in it.
        /// </summary>
        /// <param name="biod"></param>
        /// <param name="db"></param>
        protected override void PurifyMeta(LinkedItemOnDisk biod, Sop.DataBlock db)
        {
            if (db.SizeOccupied > 0)
            {
                //** purify data by separating meta data from it so meta data won't be serialized more than once
                int metaDataSize = LinkedListOnDisk.SizeOfMetaData(this);
                int newSize = db.SizeOccupied - metaDataSize;
                byte[] newData = new byte[newSize];
                Array.Copy(db.Data, metaDataSize, newData, 0, newSize);

                //** assign data to block for serialization
                biod.Data = newData;
            }
        }
    }
}