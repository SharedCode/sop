// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using Sop.Mru;
using System.Collections.Generic;
using System.Threading.Tasks;

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
        /// Read a set of Data Blocks information from disk.
        /// This will only read the entire linked blocks' information
        /// of a given Data Block on a given Address or Data offset in file.
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="dataAddress"></param>
        /// <returns></returns>
        List<Sop.DataBlock.Info> ReadBlockInfoFromDisk(Algorithm.Collection.ICollectionOnDisk parent, long dataAddress);

        /// <summary>
        /// Asynchronous Read of Data Blocks from disk given a set of block information specifying the Blocks' Addresses
        /// and their sizes. Each item whose data blocks are completely read will be allowed to get processed via the read callback.
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="items">BTree Items to read </param>
        /// <param name="readCallback">callback to process each completed BTree Item's blocks reading. Parameter denotes 
        /// the index of this item in the List.</param>
        /// <returns></returns>
        void ReadBlockFromDisk(Algorithm.Collection.ICollectionOnDisk parent, 
            List<Algorithm.BTree.BTreeItemOnDisk> items, System.Func<int, bool> readCallback);

        /// <summary>
        /// Remove the block from disk and sends it for recycling
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="block"></param>
        /// <param name="clearBlock"></param>
        /// <returns>true if successfully deleted,
        /// false otherwise, e.g. - there is no Trash Bin.</returns>
        bool Remove(Algorithm.Collection.ICollectionOnDisk parent, Sop.DataBlock block, bool clearBlock = true);

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