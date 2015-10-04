// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;
using System.Collections.Generic;
using System.Text;

namespace Sop.OnDisk.DataBlock
{
    /// <summary>
    /// Data Block Recycler interface declares the methods
    /// necessary for implementation of a Data Block recycler
    /// algorithm
    /// </summary>
    internal interface IDataBlockRecycler : Algorithm.Collection.ICollectionOnDisk, IDisposable
    {
        /// <summary>
        /// Address on Disk of the Data Block Recycler
        /// </summary>
        long DataAddress { get; set; }

        /// <summary>
        /// true means the recycler manages a Collection On Disk's
        /// list of deleted blocks
        /// </summary>
        bool IsDeletedBlocksList { get; set; }

        /// <summary>
        /// Add a freed up segment on disk to the list.
        /// NOTE: you can implement to detect abd combine 
        /// contiguous freed up segments
        /// into a bigger segment as necessary to reduce the 
        /// number of items managed
        /// </summary>
        /// <param name="dataAddress"></param>
        /// <param name="dataSize"></param>
        void AddAvailableBlock(long dataAddress, long dataSize);

        /// <summary>
        /// Retrieves a freed up data block for allocation by the requesting code
        /// </summary>
        /// <param name="isRequesterRecycler"></param>
        /// <param name="requestedBlockSize"></param>
        /// <param name="dataAddress"></param>
        /// <param name="dataSize"></param>
        /// <returns></returns>
        bool GetAvailableBlock(bool isRequesterRecycler, int requestedBlockSize, out long dataAddress, out long dataSize);

        /// <summary>
        /// Updates a freed up data segment either to reduce or expand it
        /// </summary>
        /// <param name="availableBlockAddress"></param>
        /// <param name="availableBlockNewAddress"></param>
        /// <param name="availableBlockNewSize"></param>
        /// <returns></returns>
        bool SetAvailableBlock(long availableBlockAddress, long availableBlockNewAddress, long availableBlockNewSize);

        /// <summary>
        /// Remove a freed up data segment in the list.
        /// NOTE: caller code upon taking ownership (for allocation) 
        /// of a specific data segment can invoke this method to remove the segment from the list 
        /// </summary>
        /// <param name="dataAddress"></param>
        void RemoveAvailableBlock(long dataAddress);

        /// <summary>
        /// Clear the freed up segments list
        /// </summary>
        void Clear();

        /// <summary>
        /// For now, Delete is synonymous to Clear
        /// </summary>
        void Delete();
    }
}