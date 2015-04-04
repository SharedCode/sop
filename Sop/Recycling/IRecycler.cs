// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Sop.Recycling
{
    internal interface IRecycler<T>
    {
        int Capacity { get; }
        int Count { get; }
        void Recycle(ICollection<T> data);
        bool Recycle(T data);
        T GetRecycledObject();
    }
}
