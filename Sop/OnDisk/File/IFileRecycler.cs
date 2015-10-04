// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;
using Sop.OnDisk.DataBlock;

namespace Sop.OnDisk.File
{
    internal interface IFileRecycler : IDisposable, Algorithm.Collection.ICollectionOnDisk
    {
        long DataAddress { get; set; }
        void Add(DeletedBlockInfo value);
        DeletedBlockInfo GetTop();
        void SetTop(DeletedBlockInfo value);
        void RemoveTop();
        DeletedBlockInfo Get(long DataAddress);
        void Remove(long DataAddress);
    }
}