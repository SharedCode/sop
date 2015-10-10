// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;
using System.Collections.Generic;
using System.Text;
using Sop.Collections.BTree;
using Sop.OnDisk.Algorithm.Collection;
using Sop.OnDisk.Algorithm.SortedDictionary;
using Sop.Persistence;
using Sop.Mru.Generic;
using Sop.Collections;
using BTreeAlgorithm = Sop.OnDisk.Algorithm.BTree.BTreeAlgorithm;

namespace Sop
{
    using Synchronization;
    using SpecializedDataStore;

    /// <summary>
    /// Store Factory is the hub for creation or retrieval
    /// of an object Store (sorted dictionary on disk).
    /// 
    /// Currently supported Serialization techniques are:
    /// * Basic (simple) type Serialization.
    ///     e.g. - int, float, char, string, double, long, etc... 
    ///     are serialized using respective method of the Binary Writer.
    /// * Basic with POCO Xml Serialization. Basic types as listed above are serialized 
    ///     using Binary Writer and POCO using Xml Serializer.
    /// * Basic with POCO SOP.IPersistent implements. Basic types are
    ///     serialized using Binary Writer and POCO using code implementation of
    ///     Pack/Unpack methods of the IPersistent interface.
    /// </summary>
    public class StoreFactory : IStoreFactory
    {
        public StoreFactory()
        {
            AutoDisposeItem = true;
        }
        /// <summary>
        /// Auto Dispose Store when it gets removed from B-Tree Cache.
        /// Defaults to false as StoreFactory Getter by default provides
        /// MRU caching of stores and it manages auto-dispose of stores least
        /// used.
        /// </summary>
        public bool AutoDisposeItem { get; set; }

        /// <summary>
        /// Create/Get a general purpose Data Store. Key and/or Value types can be any of the following:
        ///     - simple type, e.g. - int, short, string, char, float, etc...
        ///     - Xml serializable object.
        /// NOTE: simple type data are stored in non-Xml format for space optimization reasons.
        /// 
        /// NOTE 2: this is a multi-thread safe method.
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="createIfNotExist"></param>
        /// <param name="container"></param>
        /// <param name="comparer"></param>
        /// <param name="name"></param>
        /// <param name="isDataInKeySegment"> </param>
        /// <param name="mruManaged">true means the returned store's lifetime is managed by StoreFactory using MRU algorithm.
        /// i.e. - least recently used store(s) will be most likely auto-disposed when number of stores accessed by code reached MRU maximum threshold count.</param>
        /// <returns></returns>
        public ISortedDictionary<TKey, TValue> Get<TKey, TValue>(object container, string name,
                                                                 IComparer<TKey> comparer = null, bool createIfNotExist = true,
                                                                 bool isDataInKeySegment = true, bool mruManaged = true, bool isUnique = false)
        {
            // assign the current default Value Unpack delegate
            BTreeAlgorithm.CurrentOnValueUnpack =
                SpecializedStoreBase.Collection_XmlSerOnUnpack;
            var resolvedContainer = GetContainer(container);
            var r2 = CreateDictionary<GeneralPurpose<TKey, TValue>, TKey, TValue>(createIfNotExist, resolvedContainer,
                                                                                  name,
                                                                                  containerDod =>
                                                                                  {
                                                                                      var r =
                                                                                          new GeneralPurpose
                                                                                              <TKey,
                                                                                                  TValue
                                                                                                  >(
                                                                                              resolvedContainer,
                                                                                              comparer,
                                                                                              name,
                                                                                              DataStoreType.SopOndisk,
                                                                                              null,
                                                                                              isDataInKeySegment);
                                                                                      containerDod.
                                                                                          SetCurrentValueInMemoryData
                                                                                          (r);
                                                                                      return r;
                                                                                  }, mruManaged);
            // assign the current default Value Unpack delegate
            BTreeAlgorithm.CurrentOnValueUnpack = null;
            if (r2 != null)
                ((SortedDictionaryOnDisk)((SpecializedStoreBase)(object)r2).Collection).IsUnique = isUnique;
            return r2;
        }
        /// <summary>
        /// Returns true if Store with name is found in the Container, otherwise false.
        /// </summary>
        /// <param name="container"></param>
        /// <param name="name"></param>
        /// <returns></returns>
        public bool Contains(object container, string name)
        {
            if (container == null)
                throw new ArgumentNullException("container");
            if (string.IsNullOrEmpty(name))
                throw new ArgumentNullException("name");

            var containerDod = (SortedDictionaryOnDisk)GetContainer(container);
            ((ISynchronizer)containerDod.SyncRoot).Lock();
            bool r = containerDod.Contains(name);
            ((ISynchronizer)containerDod.SyncRoot).Unlock();
            return r;
        }

        /// <summary>
        /// Remove a Data Store from its container.
        /// NOTE: this is thread-safe.
        /// </summary>
        /// <param name="container">data store container</param>
        /// <param name="name">name of the Data Store to remove</param>
        public void Remove(object container, string name)
        {
            if (container == null)
                throw new ArgumentNullException("container");
            if (string.IsNullOrEmpty(name))
                throw new ArgumentNullException("name");

            var containerDod = (SortedDictionaryOnDisk)GetContainer(container);
            ((ISynchronizer)containerDod.SyncRoot).Lock();

            bool storeDeleted = true;
            // delete store. NOTE: Delete will also remove itself from the container store.
            var store = containerDod[name];
            if (store is SpecializedStoreBase)
                ((SpecializedStoreBase)store).Collection.Delete();
            else if (store is SortedDictionaryOnDisk)
                ((SortedDictionaryOnDisk)store).Delete();
            else
                storeDeleted = false;

            ((ISynchronizer)containerDod.SyncRoot).Unlock();
            if (!storeDeleted)
            {
                string s = string.Format("Can't Remove Store {0} from container.", name);
                Log.Logger.Instance.Log(Log.LogLevels.Information, s);
                throw new SopException(s);
            }
        }

        /// <summary>
        /// Create/Get Data Store for both IPersistent Typed Key and Value
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="createIfNotExist"></param>
        /// <param name="container"></param>
        /// <param name="comparer"></param>
        /// <param name="name"></param>
        /// <param name="isDataInKeySegment"> </param>
        /// <param name="mruManaged"> </param>
        /// <returns></returns>
        public ISortedDictionary<TKey, TValue> GetPersistent<TKey, TValue>(object container, string name,
                                                                                      System.Collections.Generic.IComparer<TKey> comparer = null,
                                                                                      bool createIfNotExist = true, bool isDataInKeySegment = true,
                                                                                      bool mruManaged = true, bool isUnique = false)
            where TKey : IPersistent, new()
            where TValue : IPersistent, new()
        {
            BTreeAlgorithm.CurrentOnValueUnpack =
                PersistentTypeKeyValue<TKey, TValue>.Collection_OnValueUnpack;

            //BTreeAlgorithm.CurrentOnValueUnpack =
            //    PersistentTypeKeyValue<TKey, TValue>.Collection_OnKeyUnpack;
            var resolvedContainer = GetContainer(container);
            var r2 = CreateDictionary<PersistentTypeKeyValue<TKey, TValue>, TKey, TValue>(createIfNotExist, resolvedContainer,
                                                                                        name,
                                                                                        containerDod =>
                                                                                        {
                                                                                            var r =
                                                                                                new PersistentTypeKeyValue
                                                                                                    <TKey, TValue>(
                                                                                                    resolvedContainer,
                                                                                                    comparer,
                                                                                                    name,
                                                                                                    DataStoreType.SopOndisk,
                                                                                                    null,
                                                                                                    isDataInKeySegment);
                                                                                            containerDod.SetCurrentValueInMemoryData
                                                                                                (r);
                                                                                            return r;
                                                                                        }, mruManaged);
            BTreeAlgorithm.CurrentOnValueUnpack = null;
            if (r2 != null)
                ((SortedDictionaryOnDisk)((SpecializedStoreBase)(object)r2).Collection).IsUnique = isUnique;
            return r2;
        }

        /// <summary>
        /// Create/Get Data Store for IPersistent typed Key and "Simple typed" Value.
        /// NOTE: Simple type means one of the integer, numeric(decimals,...), char data types, byte array
        /// or a string
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="createIfNotExist"></param>
        /// <param name="container"></param>
        /// <param name="comparer"></param>
        /// <param name="name"></param>
        /// <param name="isDataInKeySegment"> </param>
        /// <param name="mruManaged"> </param>
        /// <returns></returns>
        public ISortedDictionary<TKey, TValue> GetPersistentKey<TKey, TValue>(object container, string name,
                                                                                      System.Collections.Generic.IComparer<TKey> comparer = null,
                                                                                      bool createIfNotExist = true, bool isDataInKeySegment = true,
                                                                                      bool mruManaged = true, bool isUnique = false)
            where TKey : IPersistent, new()
        {
            //if (!CollectionOnDisk.IsSimpleType(typeof(TValue)))
            //    throw new ArgumentException(string.Format("Type of TValue ({0}) isn't an SOP simple type.",
            //                                              typeof(TValue)));

            BTreeAlgorithm.CurrentOnValueUnpack =
                PersistentTypeKeySimpleValue<TKey, TValue>.Collection_OnKeyUnpack;
            var resolvedContainer = GetContainer(container);
            var r2=  CreateDictionary<PersistentTypeKeySimpleValue<TKey, TValue>, TKey, TValue>(createIfNotExist,
                                                                                              resolvedContainer, name,
                                                                                              containerDod =>
                                                                                              {
                                                                                                  var
                                                                                                      r =
                                                                                                          new PersistentTypeKeySimpleValue
                                                                                                              <TKey,
                                                                                                                  TValue
                                                                                                                  >(
                                                                                                              resolvedContainer,
                                                                                                              comparer,
                                                                                                              name,
                                                                                                              DataStoreType
                                                                                                                  .
                                                                                                                  SopOndisk,
                                                                                                              null,
                                                                                                              isDataInKeySegment);
                                                                                                  containerDod.
                                                                                                      SetCurrentValueInMemoryData
                                                                                                      (r);
                                                                                                  return r;
                                                                                              }, mruManaged);
            if (r2 != null)
                ((SortedDictionaryOnDisk)((SpecializedStoreBase)(object)r2).Collection).IsUnique = isUnique;
            return r2;
        }

        /// <summary>
        /// Create/Get Data Store for IPersistent typed Value and "Simple typed" Key.
        /// NOTE: Simple type means one of the integer, numeric(decimals,...), char data types, byte array
        /// or a string
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="createIfNotExist"></param>
        /// <param name="container"></param>
        /// <param name="comparer"></param>
        /// <param name="name"></param>
        /// <param name="isDataInKeySegment"> </param>
        /// <returns></returns>
        public ISortedDictionary<TKey, TValue> GetPersistentValue<TKey, TValue>(object container, string name,
                                                                                      System.Collections.Generic.IComparer<TKey> comparer = null,
                                                                                      bool createIfNotExist = true, bool isDataInKeySegment = true,
                                                                                      bool mruManaged = true, bool isUnique = false)
            where TValue : IPersistent, new()
        {
            BTreeAlgorithm.CurrentOnValueUnpack =
                PersistentTypeValueSimpleKey<TKey, TValue>.Collection_OnValueUnpack;
            var resolvedContainer = GetContainer(container);
            var r2 = CreateDictionary<PersistentTypeValueSimpleKey<TKey, TValue>, TKey, TValue>(createIfNotExist,
                                                                                              resolvedContainer, name,
                                                                                              containerDod =>
                                                                                              {
                                                                                                  var
                                                                                                      r =
                                                                                                          new PersistentTypeValueSimpleKey
                                                                                                              <TKey,
                                                                                                                  TValue
                                                                                                                  >(
                                                                                                              resolvedContainer,
                                                                                                              comparer,
                                                                                                              name,
                                                                                                              DataStoreType
                                                                                                                  .
                                                                                                                  SopOndisk,
                                                                                                              null,
                                                                                                              isDataInKeySegment);
                                                                                                  containerDod.
                                                                                                      SetCurrentValueInMemoryData
                                                                                                      (r);
                                                                                                  return r;
                                                                                              }, mruManaged);
            BTreeAlgorithm.CurrentOnValueUnpack = null;
            if (r2 != null)
                ((SortedDictionaryOnDisk)((SpecializedStoreBase)(object)r2).Collection).IsUnique = isUnique;
            return r2;
        }

        /// <summary>
        /// Returns a valid container of correct type
        /// </summary>
        /// <param name="container"></param>
        /// <returns></returns>
        public ISortedDictionaryOnDisk GetContainer(object container)
        {
            if (container == null)
                throw new ArgumentNullException("container");
            if (container is ISortedDictionaryOnDisk)
                return (ISortedDictionaryOnDisk)container;
            if (container is IProxyObject &&
                ((IProxyObject)container).RealObject is ISortedDictionaryOnDisk)
                return (ISortedDictionaryOnDisk)((IProxyObject)container).RealObject;
            if (container is ObjectServer)
                return ((ObjectServer)container).SystemFile.Store;
            throw new ArgumentException(
                "container type isn't supported. Only Sop...SortedDictionaryOnDisk or Sop.ISortedDictionary types are allowed.");
        }

        private T CreateDictionary<T, TKey, TValue>(bool createIfNotExist, object container, string name,
                                                    CreateDictionaryDelegate<T> createDelegate, bool mruManaged = true)
            where T : class, ISortedDictionary<TKey, TValue>
        {
            var containerDod = (SortedDictionaryOnDisk)GetContainer(container);
            if (createIfNotExist && containerDod.File.Server.ReadOnly)
            {
                // override create flag if Server is in readonly mode...
                createIfNotExist = false;
                //throw new ArgumentException("createIfNotExist can't be true in Server ReadOnly mode.");
            }

            T r = default(T);

            // check whether Data Store is in MRU cache of opened stores...
            string storeName = SpecializedStoreBase.FormatStoreName(containerDod.ToString(), name);
            object store = OpenedStores[storeName];
            // let casting throw an exception, 'don't handle it & let caller code to manage it... (todo: revisit if there is benefit to handle such exception??)
            if (store != null && !((T)store).IsDisposed) return (T)store;

            // Data Store is NOT in MRU cache, try to get it from the container OR create it if it doesn't exist...
            ((ISynchronizer)containerDod.SyncRoot).Lock();
            bool found = containerDod.Contains(name);
            if (found)
            {
                var cv = containerDod.CurrentValue;
                if (cv is ISortedDictionary<TKey, TValue>)
                    r = (T)cv;
                if (containerDod.CurrentValue is ISortedDictionaryOnDisk)
                    r = createDelegate(containerDod);
            }
            if (r == null)
            {
                if (createIfNotExist)
                {
                    r = createDelegate(containerDod);
                    if (containerDod.File.Server.Profile.TrackStoreTypes != null &&
                        (r != null &&
                         name != OnDisk.ObjectServer.StoreTypesLiteral &&
                         containerDod.File.Server.Profile.TrackStoreTypes.Value))
                    {
                        // log create store info
                        var storeLog = containerDod.File.Server.StoreTypes;
                        storeLog.Add(storeName, string.Format("{0}", typeof(T)));
                    }
                }
                if (r == null)
                {
                    ((ISynchronizer)containerDod.SyncRoot).Unlock();
                    return null;
                }
            }
            r.AutoDispose = AutoDisposeItem;
            ((SortedDictionaryOnDisk)((SpecializedStoreBase)(object)r).Collection).UniqueStoreName = storeName;
            ((ISynchronizer)containerDod.SyncRoot).Unlock();
            if (mruManaged)
                OpenedStores.Add(storeName, (SpecializedStoreBase)(object)r);
            else
                OpenedStores.Remove(storeName);
            return r;
        }

        /// <summary>
        /// Maximum number of Opened Data Stores in-memory.
        /// NOTE: When total number of opened and in-memory Stores 
        /// reach this amount, StoreFactory will start to auto-dispose
        /// least used opened Stores in-memory in order to maintain 
        /// memory/resource consumption within reasonable levels.
        /// </summary>
        public static int MaxStoreInstancePoolCount
        {
            get { return _maxOpenStoreInMemoryCount; }
            set
            {
                if (value < 4)
                {
                    string errMsg = "Minimum MaxOpenStoreInMemoryCount is 4.";
                    Log.Logger.Instance.Log(Log.LogLevels.Error, errMsg);
                    throw new ArgumentException(errMsg);
                }
                _maxOpenStoreInMemoryCount = value;
                OpenedStores.MaxCapacity = value;
                OpenedStores.MinCapacity = (int)(value * .75);
            }
        }
        private static int _maxOpenStoreInMemoryCount = DefaultMaxOpenStoreInMemoryCount;

        /// <summary>
        /// Default maximum number of Stores to be kept opened in memory by StoreFactory.
        /// </summary>
        public const int DefaultMaxOpenStoreInMemoryCount = 25;


        // todo: future task, move this into the core... (when Server is disposed, its Stores should auto-remove themselves from OpenedStores).
        internal static void RemoveServerStoresInMru(IObjectServer server)
        {
            Mru.Generic.ConcurrentMruManager<string, SpecializedStoreBase> openedStores =
                (Mru.Generic.ConcurrentMruManager<string, SpecializedStoreBase>)OpenedStores;
            lock (openedStores.Locker)
            {
                if (!openedStores.MruManager.CacheCollection.MoveFirst()) return;
                List<KeyValuePair<string, ISortedDictionaryOnDisk>> storesForDispose =
                    new List<KeyValuePair<string, ISortedDictionaryOnDisk>>(openedStores.Count);
                do
                {
                    if (openedStores.MruManager.CacheCollection.CurrentValue.Value.Collection != null)
                    {
                        if (openedStores.MruManager.CacheCollection.CurrentValue.Value.Collection.File == null)
                        {
                            Log.Logger.Instance.Warning("Collection {0} has no File assigned.",
                                openedStores.MruManager.CacheCollection.CurrentValue.Value.Collection.Name ?? "");
                        }
                        else if (openedStores.MruManager.CacheCollection.CurrentValue.Value.Collection.File.Server != server)
                            continue;
                    }
                    storesForDispose.Add(
                        new KeyValuePair<string, ISortedDictionaryOnDisk>(
                            openedStores.MruManager.CacheCollection.CurrentKey,
                            openedStores.MruManager.CacheCollection.CurrentValue.Value.Collection));
                } while (openedStores.MruManager.CacheCollection.MoveNext());
                // remove Stores identified to belong to the server received as parameter...
                foreach (var kvp in storesForDispose)
                {
                    openedStores.MruManager.Remove(kvp.Key);
                }
            }
        }

        /// <summary>
        /// Opened Stores MRU cache.
        /// </summary>
        internal static readonly Mru.Generic.IMruManager<string, SpecializedStoreBase> OpenedStores =
            new ConcurrentMruManager<string, SpecializedStoreBase>(MaxStoreInstancePoolCount - 5,
                                                                               MaxStoreInstancePoolCount);

        private delegate T CreateDictionaryDelegate<out T>(SortedDictionaryOnDisk container);
    }
}

