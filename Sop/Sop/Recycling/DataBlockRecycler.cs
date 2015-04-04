// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Sop.OnDisk;

namespace Sop.Recycling
{
    /// <summary>
    /// DataBlock specific recycler.
    /// </summary>
    internal class DataBlockRecycler : Recycler<DataBlock>
    {
        public DataBlockRecycler(int capacity) : base(capacity) { }

        public void PreAllocateBlocks(DataBlockSize size)
        {
            if (!Enabled) return;
            int c = (int)(Capacity * .2);
            if (c < 10) c = 10;
            for (int i = 0; i < c; i++)
            {
                DataBlock d = new DataBlock(size);
                d.Orphaned = true;
                Recycle(d);
            }
        }

        public override bool Recycle(DataBlock data)
        {
            if (!Enabled || data == null) return false;
            if (!data.Orphaned) return false;
            DataBlock d = data, p = null;
            while (d != null)
            {
                d.Initialize();
                if (Count < Capacity) RecycleBin[d] = 0;
                // break the chain, block needs to be recycled individually.
                if (p != null) p.Next = null;
                p = d;
                d = d.Next;
            }
            if (p != null) p.Next = null;
            return true;
        }
    }
}
