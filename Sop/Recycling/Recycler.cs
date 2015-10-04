// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Sop.OnDisk;

namespace Sop.Recycling
{
    /// <summary>
    /// General purpose object recycler.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal class Recycler<T> : IRecycler<T>
    {
        public Recycler(int capacity)
        {
            //RecycleBin = new Dictionary<T, byte>(capacity);
            if (!Enabled) return;
            Capacity = capacity;
        }
        virtual public bool Recycle(T data)
        {
            if (!Enabled) return false;
            if (Count >= Capacity) return false;
            if (data is IRecyclable)
                ((IRecyclable)data).Initialize();
            RecycleBin[data] = 0;
            return true;
        }
        public void Recycle(ICollection<T> data)
        {
            if (!Enabled) return;
            if (Count >= Capacity) return;
            foreach (var d in data)
            {
                Recycle(d);
            }
        }
        public T[] GetRecycledObject(int count)
        {
            if (!Enabled) return null;
            if (RecycleBin.Count == 0 || RecycleBin.Count < count) return null;
            T[] r = new T[count];
            int i = 0;
            foreach (var k in RecycleBin.Keys)
            {
                r[i++] = k;
                if (i >= count) break;
            }
            for (i = 0; i < count; i++)
                RecycleBin.Remove(r[i]);
            return r;
        }
        public T GetRecycledObject()
        {
            if (!Enabled) return default(T);
            if (RecycleBin.Count == 0) return default(T);
            foreach (var k in RecycleBin.Keys)
            {
                RecycleBin.Remove(k);
                return k;
            }
            return default(T);
        }
        public int Capacity { get; private set; }

        protected bool Enabled
        {
            get
            {
                return RecycleBin != null;
            }
        }

        public int Count
        {
            get
            {
                if (RecycleBin == null) return 0;
                return RecycleBin.Count;
            }
        }
        protected Dictionary<T, byte> RecycleBin;
    }
}
