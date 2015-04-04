using System;
using System.Collections;
using Sop.Collections.Generic;
using Sop.Persistence;

namespace Sop.Mru.Generic
{
    //** NOTE: Classes below are for SOP internal use only.

    internal class InternalMruManager<TKey, TValue> : System.Collections.Generic.IEnumerable<Node<TKey, TValue>>
    {
        internal class Iterator : System.Collections.Generic.IEnumerator<Node<TKey, TValue>>
        {
            public Iterator(InternalMruManager<TKey, TValue> collection)
            {
                if (collection == null)
                    throw new ArgumentNullException("collection");
                this._collection = collection;
                Current = collection._head;
            }

            public void Dispose()
            {
                _collection = null;
                Current = null;
            }

            private InternalMruManager<TKey, TValue> _collection;

            public Node<TKey, TValue> Current { get; private set; }

            object IEnumerator.Current
            {
                get { return Current; }
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

        #region Enumerator

        public System.Collections.Generic.IEnumerator<Node<TKey, TValue>> GetEnumerator()
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

        private Node<TKey, TValue> _head = null;
        private Node<TKey, TValue> _tail = null;

        public InternalMruManager(int minCapacity, int maxCapacity)
        {
            Collection = null;
            if (minCapacity < maxCapacity)
            {
                this.MinCapacity = minCapacity;
                this.MaxCapacity = maxCapacity;
                RemovedObjects = new SortedDictionary<TKey, TValue>();
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
            RemovedObjects = new SortedDictionary<TKey, TValue>();
        }

        public void Clear()
        {
            this._head = null;
            this._tail = null;
            Count = 0;
        }

        public Node<TKey, TValue> AddInHead(MruItem<TKey, TValue> data)
        {
            return AddInHead(data, false);
        }

        public Node<TKey, TValue> AddInHead(MruItem<TKey, TValue> data, bool addInCache)
        {
            var n = new Node<TKey, TValue>(data);
            this.AddInHead(n, addInCache);
            return n;
        }

        public void AddInHead(Node<TKey, TValue> dataNode)
        {
            AddInHead(dataNode, false);
        }

        public void AddInHead(Node<TKey, TValue> dataNode, bool addInCache)
        {
            if (!addInCache && GeneratePruneEvent /*&& DataDriver != null*/)
            {
                if (MaxCapacity > 0 && Count >= MaxCapacity)
                {
                    if (RemovedObjects.Count == 0)
                    {
                        Log.Logger.Instance.Log(Log.LogLevels.Information, "Generic.InternalMruManager.AddInHead: pruning enter.");
                        //RemovedObjects.Clear();
                        for (int i = MaxCapacity; i > MinCapacity; i--)
                            this.RemoveInTail(true);
                        Log.Logger.Instance.Log(Log.LogLevels.Information, "Generic.InternalMruManager.AddInHead: pruning exit.");
                        SaveRemovedBlocks();
                    }
                }
            }
            Node<TKey, TValue> n = dataNode;
            n.Next = _head;
            n.Previous = null;
            if (_head != null)
                _head.Previous = n;
            else
                _tail = n;
            _head = n;
            Count++;
        }

        public MruItem<TKey, TValue> PeekInTail()
        {
            return _tail != null ? _tail.Data : null;
        }

        public Node<TKey, TValue> AddInTail(MruItem<TKey, TValue> data)
        {
            Node<TKey, TValue> n = null;
            if (_tail == null)
                return AddInHead(data);
            n = new Node<TKey, TValue>(data);
            n.Previous = _tail;
            _tail.Next = n;
            _tail = n;
            Count++;
            return n;
        }

        public MruItem<TKey, TValue> RemoveInTail(bool moveToRemoveList)
        {
            if (_tail != null)
            {
                Count--;
                MruItem<TKey, TValue> d = _tail.Data;
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
                if (d.Value is Sop.DataBlock)
                {
                    //** check whether this needs to be supported..
                    //if (((DataBlock)d.Value).IsDirty)
                    //{
                    //    if (MoveToRemoveList && DataDriver != null && !RemovedObjects.ContainsKey(d.Value))
                    //        RemovedObjects.Add(d.Value, d.Value);
                    //}
                }
                else if (moveToRemoveList)
                {
                    if (!RemovedObjects.ContainsKey(d.Key))
                    {
                        if (d.Value is IInternalPersistent)
                        {
                            if (((IInternalPersistent)d.Value).IsDirty)
                                RemovedObjects.Add(d.Key, d.Value);
                        }
                        else
                            RemovedObjects.Add(d.Key, d.Value);
                    }
                }
                return d;
            }
            return null;
        }

        internal Sop.Mru.SaveTypes SaveState = Sop.Mru.SaveTypes.Default;

        public void SaveRemovedBlocks()
        {
            if (RemovedObjects.Count <= 0) return;
            bool WasSaved = false;
            if (Collection != null)
            {
                if ((SaveState & Sop.Mru.SaveTypes.DataPoolInMaxCapacity) != Sop.Mru.SaveTypes.DataPoolInMaxCapacity)
                {
                    Log.Logger.Instance.Log(Log.LogLevels.Information, "GenericInternalMruManager.SaveRemoveBlocks: OnMaxCapacity(RemovedObjects.Values) enter.");
                    SaveState |= Sop.Mru.SaveTypes.DataPoolInMaxCapacity;
                    WasSaved = ((IMruClient)Collection).OnMaxCapacity(RemovedObjects.Values) > 0;
                    SaveState ^= Sop.Mru.SaveTypes.DataPoolInMaxCapacity;
                    if (WasSaved)
                        RemovedObjects.Clear();
                    Log.Logger.Instance.Log(Log.LogLevels.Information, "GenericInternalMruManager.SaveRemoveBlocks: OnMaxCapacity(RemovedObjects.Values) exit.");
                }
                return;
            }
            RemovedObjects.Clear();
        }

        public Collections.Generic.ISortedDictionary<TKey, TValue> RemovedObjects;

        public void RemoveNode(Node<TKey, TValue> node)
        {
            if (node != null)
            {
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
            else
                throw new SopException("Node not in List.");
        }

        //internal IDataBlockDriver DataDriver = null;
        internal IMruClient Collection { get; set; }
    }
}