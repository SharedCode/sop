// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using Sop.Mru;

namespace Sop.OnDisk.DataBlock
{
    /// <summary>
    /// Data Block Driver interface extends IDataBlockFactory
    /// and adds data block management functions within a collection
    /// of a File.
    /// </summary>
    internal interface IDataBlockDriver : IDisposable
    {
        /// <summary>
        /// Instantiate a new Data block of a given size
        /// </summary>
        /// <param name="blockSize"></param>
        /// <returns></returns>
        Sop.DataBlock CreateBlock(DataBlockSize blockSize);

        /// <summary>
        /// Returns the effective ID of the block. It can be the DataAddress
        /// or it can be a handle/reference to the block. DataBlockDriver
        /// implementation drives what GetID and SetID usage is
        /// </summary>
        /// <param name="block"></param>
        /// <returns></returns>
        long GetId(Sop.DataBlock block);

        /// <summary>
        /// Update the effective ID of the block with a given Value
        /// </summary>
        /// <param name="block"></param>
        /// <param name="value"></param>
        void SetId(Sop.DataBlock block, long value);

        /// <summary>
        /// true if block was modified, false otherwise
        /// </summary>
        bool GetIsDirty(Sop.DataBlock block);

        /// <summary>
        /// Mark block as modified or not
        /// </summary>
        /// <param name="targetBlock"></param>
        /// <param name="newValue"></param>
        void SetIsDirty(Sop.DataBlock targetBlock, bool newValue);

        /// <summary>
        /// Block Recycler.
        /// </summary>
        Recycling.IRecycler<Sop.DataBlock> BlockRecycler { get; }

        /// <summary>
        /// Delete all of collection's data blocks and send them for recycling
        /// </summary>
        /// <param name="parent"></param>
        void Delete(Algorithm.Collection.ICollectionOnDisk parent);

        /// <summary>
        /// Move the current item pointer to the 1st item.
        /// The collection dictates which item is considered 1st according to
        /// its sort order, if it has sort order
        /// </summary>
        /// <param name="parent"></param>
        /// <returns></returns>
        bool MoveFirst(Algorithm.Collection.ICollectionOnDisk parent);

        /// <summary>
        /// Move the current item pointer to the last item.
        /// </summary>
        /// <param name="parent"></param>
        /// <returns></returns>
        bool MoveLast(Algorithm.Collection.ICollectionOnDisk parent);

        /// <summary>
        /// Move the current item pointer to the next item.
        /// </summary>
        /// <param name="parent"></param>
        /// <returns></returns>
        bool MoveNext(Algorithm.Collection.ICollectionOnDisk parent);

        /// <summary>
        /// Move the current item pointer to the item specified by DataAddress of a given Sop.DataBlock
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="dataBlock"></param>
        /// <returns></returns>
        bool MoveTo(Algorithm.Collection.ICollectionOnDisk parent, Sop.DataBlock dataBlock);

        /// <summary>
        /// Move the current item pointer to the item specified by DataAddress
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="address"></param>
        /// <returns></returns>
        bool MoveTo(Algorithm.Collection.ICollectionOnDisk parent, long address);

        /// <summary>
        /// Read the block from disk
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="address"></param>
        /// <param name="getForRemoval"></param>
        /// <returns></returns>
        Sop.DataBlock ReadBlockFromDisk(Algorithm.Collection.ICollectionOnDisk parent, long address, bool getForRemoval);

        /// <summary>
        /// Read the block from disk
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="getForRemoval"></param>
        /// <returns></returns>
        Sop.DataBlock ReadBlockFromDisk(Algorithm.Collection.ICollectionOnDisk parent, bool getForRemoval);

        /// <summary>
        /// Remove the block from disk and sends it for recycling
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="block"></param>
        /// <param name="clearBlock"></param>
        void Remove(Algorithm.Collection.ICollectionOnDisk parent, Sop.DataBlock block, bool clearBlock = true);

        /// <summary>
        /// Remove the block from disk and sends it for recycling
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="block"></param>
        /// <param name="isCollectionBlock"> </param>
        /// <param name="addToMru"></param>
        void SetDiskBlock(Algorithm.Collection.ICollectionOnDisk parent, Sop.DataBlock block,
                          bool isCollectionBlock, bool addToMru = true);

        /// <summary>
        /// Bulk write a given set of Blocks
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="blocks"></param>
        /// <param name="clear"> </param>
        /// <returns></returns>
        int WriteBlocksToDisk(Algorithm.Collection.ICollectionOnDisk parent,
                              System.Collections.Generic.IDictionary<long, Sop.DataBlock> blocks,
                              bool clear);

        /// <summary>
        /// get/set the HeaderData
        /// </summary>
        HeaderData HeaderData { get; set; }

    }
}