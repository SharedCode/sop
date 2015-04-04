using System;
using System.Collections;
using Sop.Collections.BTree;
using Sop.OnDisk.DataBlock;
using Sop.Persistence;

namespace Sop.Mru
{
    using OnDisk;

    //** NOTE: Classes below are for SOP internal use only.
    /// <summary>
    /// For MruManager's internal use only.
    /// </summary>
    internal class InternalMruManager : System.Collections.Generic.IEnumerable<Node>
    {
        #region Enumerator

        public System.Collections.Generic.IEnumerator<Node> GetEnumerator()
        {
            return new Iterator(this);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return new Iterator(this);
        }

        #endregion

        public int Count = 0;
        public int MinCapacity = DefaultMinCapacity;
        public int MaxCapacity = DefaultMaxCapacity;

        public const int DefaultMinCapacity = 3000;
        public const int DefaultMaxCapacity = 5000;

        private Node _head = null;
        private Node _tail = null;

        public InternalMruManager(int minCapacity, int maxCapacity)
        {
            Collection = null;
            if (minCapacity < maxCapacity)
            {
                this.MinCapacity = minCapacity;
                this.MaxCapacity = maxCapacity;
                RemovedObjects = new Collections.Generic.SortedDictionary<object, object>();
            }
            else
                throw new SopException(
                    string.Format("Minimum Capacity '{0}' can't be >= MaxCapacity '{1}'",
                                  minCapacity, maxCapacity)
                    );
        }

        /// <summary>
        /// Generate prune event
        /// </summary>
        public bool GeneratePruneEvent = true;

        public InternalMruManager()
        {
            Collection = null;
            RemovedObjects = new Collections.Generic.SortedDictionary<object, object>();
        }

        public void Clear()
        {
            this._head = null;
            this._tail = null;
            Count = 0;
        }

        public Node AddInHead(MruItem data)
        {
            return AddInHead(data, false);
        }

        public Node AddInHead(MruItem data, bool addInCache)
        {
            Node n = new Node(data);
            this.AddInHead(n, addInCache);
            return n;
        }

        public void AddInHead(Node dataNode)
        {
            AddInHead(dataNode, false);
        }

        public void AddInHead(Node dataNode, bool addInCache)
        {
            if (!addInCache && GeneratePruneEvent && DataDriver != null)
            {
                if (MaxCapacity > 0 && Count >= MaxCapacity)
                {
                    if (RemovedObjects.Count == 0)
                    {
                        Log.Logger.Instance.Log(Log.LogLevels.Information, "InternalMruManager.AddInHead: pruning enter.");
                        //RemovedObjects.Clear();
                        for (int i = MaxCapacity; i > MinCapacity; i--)
                            this.RemoveInTail(true);
                        Log.Logger.Instance.Log(Log.LogLevels.Information, "InternalMruManager.AddInHead: pruning exit.");
                        RemovedObjects.Clear();
                    }
                }
            }
            Node n = dataNode;
            n.Next = _head;
            n.Previous = null;
            if (_head != null)
                _head.Previous = n;
            else
                _tail = n;
            _head = n;
            Count++;
        }

        public MruItem PeekInTail()
        {
            return _tail != null ? _tail.Data : null;
        }

        public Node AddInTail(MruItem data)
        {
            Node n = null;
            if (_tail == null)
                return AddInHead(data);
            n = new Node(data);
            n.Previous = _tail;
            _tail.Next = n;
            _tail = n;
            Count++;
            return n;
        }

        public MruItem RemoveInTail(bool moveToRemoveList)
        {
            if (_tail != null)
            {
                Count--;
                MruItem d = _tail.Data;
                if (_tail.Previous != null)
                {
                    _tail.Previous.Next = null;
                    _tail = _tail.Previous;
                }
                else
                {
                    _tail = null;
                    _head = null;
                }
                //if (Collection != null)
                //{
                    if (d.Value is Sop.DataBlock)
                    {
                        if (((DataBlock)d.Value).IsDirty)
                        {
                            if (moveToRemoveList && DataDriver != null && !RemovedObjects.ContainsKey(d.Key))
                                RemovedObjects.Add(d.Key, d.Value);
                        }
                    }
                    else if (moveToRemoveList && d.Value != null)
                    {
                        if (!RemovedObjects.ContainsKey(d.Key))
                        {
                            if (d.Value is IInternalPersistent)
                            {
                                if (((IInternalPersistent)d.Value).IsDirty)
                                    RemovedObjects.Add(d.Key, d.Value);
                                else
                                {
                                    //// auto dispose non-dirty item to help GC!!
                                    //if (d.Key is IDisposable)
                                    //    ((IDisposable)d.Key).Dispose();
                                    //if (d.Value is IDisposable)
                                    //    ((IDisposable)d.Value).Dispose();
                                }
                            }
                            else
                                RemovedObjects.Add(d.Key, d.Value);
                        }
                    }
                //}
                return d;
            }
            return null;
        }

        internal SaveTypes SaveState = SaveTypes.Default;

        public void SaveRemovedBlocks()
        {
            if (RemovedObjects.Count <= 0) return;
            if (Collection == null) return;
            if ((SaveState & SaveTypes.DataPoolInMaxCapacity) == SaveTypes.DataPoolInMaxCapacity)
                return;
            Log.Logger.Instance.Log(Log.LogLevels.Information, "InternalMruManager.SaveRemoveBlocks: OnMaxCapacity(RemovedObjects.Values) enter.");
            SaveState |= SaveTypes.DataPoolInMaxCapacity;
            bool wasSaved = Collection.OnMaxCapacity(RemovedObjects.Values) > 0;
            SaveState ^= SaveTypes.DataPoolInMaxCapacity;
            Log.Logger.Instance.Log(Log.LogLevels.Information, "InternalMruManager.SaveRemoveBlocks: OnMaxCapacity(RemovedObjects.Values) exit.");
            return;
        }

        public Collections.Generic.SortedDictionary<object, object> RemovedObjects;

        public void RemoveNode(Node node)
        {
            RemoveNode(node, false);
        }

        public void RemoveNode(Node node, bool removeFromCache)
        {
            if (node == null)
                throw new SopException("Node not in List.");
            if (node.Next != null)
                node.Next.Previous = node.Previous;
            else
                _tail = node.Previous;
            if (node.Previous != null)
                node.Previous.Next = node.Next;
            else
                _head = node.Next;
            Count--;
        }

        internal object DataDriver = null;
        internal IMruClient Collection { get; set; }

        /// <summary>
        /// Internal MRU Manager Iterator.
        /// </summary>
        internal class Iterator : System.Collections.Generic.IEnumerator<Node>
        {
            public Iterator(InternalMruManager collection)
            {
                if (collection == null)
                    throw new ArgumentNullException("collection");
                this._collection = collection;
                Current = collection._head;
            }

            private InternalMruManager _collection;

            public Node Current { get; private set; }

            public void Dispose()
            {
                Current = null;
                _collection = null;
            }

            object IEnumerator.Current
            {
                get { return (Node)Current; }
            }

            public bool MoveNext()
            {
                if (_wasReset)
                {
                    Current = _collection._head;
                    _wasReset = false;
                    return true;
                }
                if (Current != null)
                    Current = Current.Next;
                return Current != null;
            }

            public void Reset()
            {
                _wasReset = true;
                Current = null;
            }

            private bool _wasReset = true;
        }
    }
}