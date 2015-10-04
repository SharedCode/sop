// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using Sop.OnDisk.DataBlock;
using Sop.OnDisk.File;

namespace Sop.OnDisk.Algorithm.BTree
{
    internal class FileRecycler : OnDisk.ConcurrentWrappers.ConcurrentBTreeAlgorithm, IFileRecycler
    {
        public FileRecycler(File.IFile file)
            : base(file, new BTreeDefaultComparer())
        {
        }

        public void Add(DeletedBlockInfo value)
        {
            Locker.Lock();
            if (Search(value.StartBlockAddress))
            {
                Locker.Unlock();
                return;
            }

            if (Log.Logger.Instance.IsVerboseEnabled)
                Log.Logger.Instance.Log(Log.LogLevels.Verbose, "FileRecycler.Add: {0}", value.ToString());

            var itm = new BTreeItemOnDisk(DataBlockSize, value.StartBlockAddress, value.EndBlockAddress);
            base.Add(itm);
            Locker.Unlock();
        }

        public DeletedBlockInfo GetTop()
        {
            Locker.Lock();
            if (!MoveFirst())
            {
                Locker.Unlock();
                return null;
            }
            var dbi = new DeletedBlockInfo
                          {StartBlockAddress = (long) CurrentKey, EndBlockAddress = (long) CurrentValue};
            Locker.Unlock();
            return dbi;
        }

        public void SetTop(DeletedBlockInfo value)
        {
            Locker.Lock();
            if (!MoveFirst())
            {
                Locker.Unlock();
                return;
            }
            Remove();
            Add(value);
            Locker.Unlock();
        }

        public void RemoveTop()
        {
            Locker.Lock();
            if (MoveFirst()) Remove();
            Locker.Unlock();
        }

        public DeletedBlockInfo Get(long dataAddress)
        {
            Locker.Lock();
            if (Search(dataAddress))
            {
                var dbi = new DeletedBlockInfo
                              {StartBlockAddress = (long) CurrentKey, EndBlockAddress = (long) CurrentValue};
                Locker.Unlock();
                return dbi;
            }
            Locker.Unlock();
            return null;
        }

        public void Remove(long dataAddress)
        {
            Locker.Lock();
            if (Search(dataAddress))
                Remove();
            Locker.Unlock();
        }
    }
}