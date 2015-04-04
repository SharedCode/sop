using Sop.Persistence;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sop
{
    /// <summary>
    /// Store Navigator is a helper class that is used to retrieve Store
    /// as referenced by the store URI path parameter.
    /// 
    /// Sample valid Store Uri path formats:
    ///     "SystemFile/Store1"                     - implicitly references SystemFile/Store/Store1
    ///     "SystemFile/Store2/Store2.1"            - implicitly references SystemFile/Store/Store2/Store2.1,
    ///                                               Store2 is implicitly typed to be PersistentTypeValueSimpleKey as derived from the Store usage,
    ///                                               i.e. - it is a Store containing elements with Key of string type and Value of a sub-Store.
    ///                                               
    ///     "File1/Store1/Store1.1"                 - implicitly references File1/Store/Store1/Store1.1.
    ///                                               Ensure File1 File object was added to the ObjectServer before making this call, 
    ///                                               otherwise an exception will be thrown.
    ///     "File1/Store1/Store1.2"
    ///     "File2/Store1/Store1.2/Store1.2.1"
    ///     
    /// </summary>
    public class StoreNavigator : Sop.IStoreNavigator
    {
        private Sop.IObjectServer _server;
        /// <summary>
        /// StoreNavigator Constructor expecting Object Server argument.
        /// </summary>
        /// <param name="server"></param>
        public StoreNavigator(Sop.IObjectServer server)
        {
            if (server == null)
                throw new ArgumentNullException("server");
            _server = server;
        }
        /// <summary>
        /// Retrieves the raw (unwrapped), object Typed Key/Value Store as referenced by storePath.
        /// </summary>
        /// <param name="storePath"></param>
        /// <returns>ISortedDictionaryOnDisk object</returns>
        public ISortedDictionaryOnDisk GetUnwrappedStore(string storePath)
        {
            string s;
            ISortedDictionaryOnDisk container = getContainer(storePath, out s, false);
            // if storePath references the File object, 'just return container as it should be the File's default Store.
            if (s == storePath)
                return container;
            if (container == null || !container.Contains(s)) return null;
            var v = container.GetValue(s, null);
            var sf = new Sop.StoreFactory();
            container = sf.GetContainer(v);
            if (container == null)
                throw new SopException(string.Format("Can't recreate Store {0}", s));
            return container;
        }

        /// <summary>
        /// Navigate, instantiate and return the Store as referenced by the storePath.
        /// Following conditions apply:
        /// - if any element in the storePath is not found and can't be created even if flag
        /// is set to true, this throws exception.
        /// - if any element in the storePath is not found and create flag is false, this returns null.
        /// 
        /// Supported Key and Value data types are Simple and Xml Serializable, i.e. - Key can be either and so is Value.
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="storePath"></param>
        /// <param name="storeConfig">specify the Store configuration parameters. If file already exists, this will be ignored
        /// as the Configuration info will be read from the Disk.</param>
        /// <param name="fileConfig">if storePath references a File that hasn't been created yet, fileConfig 
        /// will be used to configure the File when it is auto-created.</param>
        /// <returns></returns>
        public ISortedDictionary<TKey, TValue> GetStore<TKey, TValue>(
            string storePath, StoreParameters<TKey> storeConfig = null, Profile fileConfig = null)
        {
            string s;
            ISortedDictionaryOnDisk container = getContainerWithRootStoreCheck(storePath, out s, config: fileConfig);
            if (container == null)
                throw new ArgumentException(string.Format("Can't get a valid Store Container from storePath {0}.", storePath));
            Sop.IStoreFactory sf = new Sop.StoreFactory();
            ISortedDictionary<TKey, TValue> store;
            if (storeConfig == null)
                store = sf.Get<TKey, TValue>(container, s);
            else
            {
                store = sf.Get<TKey, TValue>(container, s,
                    storeConfig.StoreKeyComparer,
                    storeConfig.CreateStoreIfNotExist,
                    storeConfig.IsDataInKeySegment,
                    storeConfig.MruManaged,
                    storeConfig.IsUnique);
                if (store != null)
                    store.AutoFlush = storeConfig.AutoFlush;
            }
            if (store != null)
                ((Sop.SpecializedDataStore.SimpleKeyValue<TKey, TValue>)store).Path = storePath;
            return store;
        }
        public ISortedDictionary<TKey, TValue> GetStore<TKey, TValue>(object container, string storeName,
            StoreParameters<TKey> storeConfig = null)
        {
            Sop.IStoreFactory sf = new Sop.StoreFactory();
            ISortedDictionary<TKey, TValue> store;
            if (storeConfig == null)
                store = sf.Get<TKey, TValue>(container, storeName);
            else
            {
                store = sf.Get<TKey, TValue>(container, storeName,
                    storeConfig.StoreKeyComparer, storeConfig.CreateStoreIfNotExist,
                    storeConfig.IsDataInKeySegment, storeConfig.MruManaged, storeConfig.IsUnique);
                if (store != null)
                    store.AutoFlush = storeConfig.AutoFlush;
            }
            return store;
        }

        /// <summary>
        /// Navigate, instantiate and return the Store as referenced by the storePath.
        /// Following conditions apply:
        /// - if any element in the storePath is not found and can't be created even if flag
        /// is set to true, this throws exception.
        /// - if any element in the storePath is not found and create flag is false, this returns null.
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="storePath"></param>
        /// <param name="createStoreIfNotExist"></param>
        /// <param name="storeKeyComparer"></param>
        /// <param name="isDataInKeySegment"></param>
        /// <returns></returns>
        public ISortedDictionary<TKey, TValue> GetStorePersistentKey<TKey, TValue>(
            string storePath, StoreParameters<TKey> storeConfig = null, Profile fileConfig = null)
            where TKey : IPersistent, new()
        {
            string s;
            ISortedDictionaryOnDisk container = getContainerWithRootStoreCheck(storePath, out s, config: fileConfig);
            if (container == null)
                throw new ArgumentException(string.Format("Can't get a valid Store Container from storePath {0}.", storePath));
            Sop.IStoreFactory sf = new Sop.StoreFactory();
            ISortedDictionary<TKey, TValue> store;
            if (storeConfig == null)
                store = sf.Get<TKey, TValue>(container, s);
            else
            {
                store = sf.GetPersistentKey<TKey, TValue>(container, s,
                    storeConfig.StoreKeyComparer, storeConfig.CreateStoreIfNotExist,
                    storeConfig.IsDataInKeySegment, storeConfig.MruManaged, storeConfig.IsUnique);
                if (store != null)
                {
                    ((Sop.SpecializedDataStore.SimpleKeyValue<TKey, TValue>)store).Path = storePath;
                    store.AutoFlush = storeConfig.AutoFlush;
                }
            }
            return store;
        }
        public ISortedDictionary<TKey, TValue> GetStorePersistentKey<TKey, TValue>(object container, string storeName,
            StoreParameters<TKey> storeConfig = null)
            where TKey : IPersistent, new()
        {
            Sop.IStoreFactory sf = new Sop.StoreFactory();
            ISortedDictionary<TKey, TValue> store;
            if (storeConfig == null)
                store = sf.Get<TKey, TValue>(container, storeName);
            else
            {
                store = sf.GetPersistentKey<TKey, TValue>(container, storeName,
                    storeConfig.StoreKeyComparer, storeConfig.CreateStoreIfNotExist,
                    storeConfig.IsDataInKeySegment, storeConfig.MruManaged, storeConfig.IsUnique);
                if (store != null)
                    store.AutoFlush = storeConfig.AutoFlush;
            }
            return store;
        }
        /// <summary>
        /// Get the File given a store URI path.
        /// </summary>
        /// <param name="storePath"></param>
        /// <param name="validPath">true means storePath is valid, false otherwise.</param>
        /// <returns></returns>
        public bool TryParse(string storePath, out string[] storePathParts)
        {
            if (string.IsNullOrWhiteSpace(storePath))
                throw new ArgumentNullException("storePath");
            string[] parts = storePath.Split(new char[] { storeUriSeparator }, StringSplitOptions.RemoveEmptyEntries);
            storePathParts = null;
            if (parts == null || parts.Length == 0)
                return false;
            storePathParts = parts;
            return true;
        }

        #region Get Container
        private ISortedDictionaryOnDisk getContainerWithRootStoreCheck(string storePath, out string storeName,
            bool createStoreIfNotExist = true, Profile config = null)
        {
            ISortedDictionaryOnDisk container = getContainer(storePath, out storeName, config: config);
            //if (storeName == storePath)
            //    throw new SopException(string.Format("{0} refers to the File's root Store and it can't be cast as a strongly typed Store.", storePath));
            return container;
        }
        private const char storeUriSeparator = '/';
        private ISortedDictionaryOnDisk getContainer(string storePath, out string storeName, bool createStoreIfNotExist = true, Profile config = null)
        {
            if (string.IsNullOrWhiteSpace(storePath))
                throw new ArgumentNullException("storePath");
            storeName = null;
            string[] parts = storePath.Split(new char[] { storeUriSeparator }, StringSplitOptions.RemoveEmptyEntries);
            if (parts == null || parts.Length == 0)
                return null;
            var f = _server.GetFile(parts[0]);
            if (f == null)
            {
                // if storePath is a Store name (no File in path), simply return the SystemFile's default Store...
                if (parts.Length == 1)
                {
                    storeName = parts[0];
                    return _server.SystemFile.Store;
                }
                else if (!createStoreIfNotExist)
                    return null;
                else
                    // auto create the File if createStoreIfNotExist is true...
                    f = _server.FileSet.Add(parts[0], profile: config);
            }
            ISortedDictionaryOnDisk container = f.Store;
            for (int i = 1; i < parts.Length - 1; i++)
            {
                if (container.Contains(parts[i]))
                {
                    container = (ISortedDictionaryOnDisk)container.GetValue(parts[i], null);
                    if (container == null)
                        throw new SopException(string.Format("Can't recreate Store {0}", parts[i]));
                    continue;
                }
                if (!createStoreIfNotExist)
                    return null;
                container = CreateCollection(container, parts[i]);
            }
            storeName = parts[parts.Length - 1];
            return container;
        }
        #endregion

        /// <summary>
        /// Get a Store with Value object implementing the SOP IPersistent interface.
        /// Key is expected to be one of the simple types.
        /// 
        /// Navigate, instantiate and return the Store as referenced by the storePath.
        /// Following conditions apply:
        /// - if any element in the storePath is not found and can't be created even if flag
        /// is set to true, this throws exception.
        /// - if any element in the storePath is not found and create flag is false, this returns null.
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="storePath"></param>
        /// <returns></returns>
        public ISortedDictionary<TKey, TValue> GetStorePersistentValue<TKey, TValue>(
            string storePath, StoreParameters<TKey> storeConfig = null, Profile fileConfig = null)
            where TValue : IPersistent, new()
        {
            string s;
            ISortedDictionaryOnDisk container = getContainerWithRootStoreCheck(storePath, out s, config: fileConfig);
            if (container == null)
                throw new ArgumentException(string.Format("Can't get a valid Store Container from storePath {0}.", storePath));
            Sop.IStoreFactory sf = new Sop.StoreFactory();
            ISortedDictionary<TKey, TValue> store;
            if (storeConfig == null)
                return container.Locker.Invoke(() => { return sf.Get<TKey, TValue>(container, s); });
            else
            {
                return container.Locker.Invoke(() =>
                {
                    store = sf.GetPersistentValue<TKey, TValue>(container, s,
                    storeConfig.StoreKeyComparer, storeConfig.CreateStoreIfNotExist,
                    storeConfig.IsDataInKeySegment, storeConfig.MruManaged, storeConfig.IsUnique);
                    if (store != null)
                    {
                        ((Sop.SpecializedDataStore.SimpleKeyValue<TKey, TValue>)store).Path = storePath;
                        store.AutoFlush = storeConfig.AutoFlush;
                    }
                    return store;
                });
            }
        }
        /// <summary>
        /// Get a Store with Value object implementing the SOP IPersistent interface.
        /// Key is expected to be one of the simple types.
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="container"></param>
        /// <param name="storeName"></param>
        /// <param name="storeConfig"></param>
        /// <returns></returns>
        public ISortedDictionary<TKey, TValue> GetStorePersistentValue<TKey, TValue>(
            object container, string storeName, StoreParameters<TKey> storeConfig = null)
            where TValue : IPersistent, new()
        {
            var sf = new Sop.StoreFactory();
            var cont = sf.GetContainer(container);
            ISortedDictionary<TKey, TValue> store;
            if (storeConfig == null)
                return cont.Locker.Invoke(() => { return sf.Get<TKey, TValue>(container, storeName); });
            else
            {
                return cont.Locker.Invoke(() =>
                {
                    store = sf.GetPersistentValue<TKey, TValue>(container, storeName,
                        storeConfig.StoreKeyComparer, storeConfig.CreateStoreIfNotExist,
                        storeConfig.IsDataInKeySegment, storeConfig.MruManaged, storeConfig.IsUnique);
                    if (store != null)
                        store.AutoFlush = storeConfig.AutoFlush;
                    return store;
                });
            }
        }
        /// <summary>
        /// Navigate, instantiate and return the Store as referenced by the storePath.
        /// Following conditions apply:
        /// - if any element in the storePath is not found and can't be created even if flag
        /// is set to true, this throws exception.
        /// - if any element in the storePath is not found and create flag is false, this returns null.
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="storePath"></param>
        /// <param name="createStoreIfNotExist"></param>
        /// <param name="storeKeyComparer"></param>
        /// <param name="isDataInKeySegment"></param>
        /// <returns></returns>
        public ISortedDictionary<TKey, TValue> GetStorePersistent<TKey, TValue>(
            string storePath, StoreParameters<TKey> storeConfig = null, Profile fileConfig = null)
            where TKey : IPersistent, new()
            where TValue : IPersistent, new()
        {
            string s;
            ISortedDictionaryOnDisk container = getContainerWithRootStoreCheck(storePath, out s, config : fileConfig);
            if (container == null)
                throw new ArgumentException(string.Format("Can't get a valid Store Container from storePath {0}.", storePath));
            Sop.IStoreFactory sf = new Sop.StoreFactory();
            ISortedDictionary<TKey, TValue> store;
            if (storeConfig == null)
                return container.Locker.Invoke(() => { return sf.Get<TKey, TValue>(container, s); });
            else
            {
                return container.Locker.Invoke(() =>
                {
                    store = sf.GetPersistent<TKey, TValue>(container, s,
                    storeConfig.StoreKeyComparer, storeConfig.CreateStoreIfNotExist,
                    storeConfig.IsDataInKeySegment, storeConfig.MruManaged, storeConfig.IsUnique);
                    if (store != null)
                    {
                        ((Sop.SpecializedDataStore.SimpleKeyValue<TKey, TValue>)store).Path = storePath;
                        store.AutoFlush = storeConfig.AutoFlush;
                    }
                    return store;
                });
            }
        }
        public ISortedDictionary<TKey, TValue> GetStorePersistent<TKey, TValue>(object container,
            string storeName, StoreParameters<TKey> storeConfig = null)
            where TKey : IPersistent, new()
            where TValue : IPersistent, new()
        {
            if (container == null)
                throw new ArgumentNullException("container");
            var sf = new Sop.StoreFactory();
            var cont = sf.GetContainer(container);
            ISortedDictionary<TKey, TValue> store;
            if (storeConfig == null)
                return cont.Locker.Invoke(() => { return sf.Get<TKey, TValue>(container, storeName); });
            else
            {
                return cont.Locker.Invoke(() =>
                {
                    store = sf.GetPersistent<TKey, TValue>(container, storeName,
                        storeConfig.StoreKeyComparer, storeConfig.CreateStoreIfNotExist,
                        storeConfig.IsDataInKeySegment, storeConfig.MruManaged, storeConfig.IsUnique);
                    if (store != null)
                        store.AutoFlush = storeConfig.AutoFlush;
                    return store;
                });
            }
        }
        /// <summary>
        /// Returns true if Store with path is found in the ObjectServer, otherwise false.
        /// </summary>
        /// <param name="storePath">Store path to check.</param>
        public bool Contains(string storePath)
        {
            string s;
            ISortedDictionaryOnDisk container = getContainerWithRootStoreCheck(storePath, out s);
            if (container == null)
                throw new ArgumentException(string.Format("Can't get a valid Store Container from storePath {0}.", storePath));
            Sop.IStoreFactory sf = new Sop.StoreFactory();
            return container.Locker.Invoke(() => { return sf.Contains(container, s); });
        }
        /// <summary>
        /// Remove a Data Store referenced by storePath.
        /// </summary>
        /// <param name="storePath"></param>
        public void Remove(string storePath)
        {
            string s;
            ISortedDictionaryOnDisk container = getContainerWithRootStoreCheck(storePath, out s);
            if (container == null)
                throw new ArgumentException(string.Format("Can't get a valid Store Container from storePath {0}.", storePath));
            Sop.IStoreFactory sf = new Sop.StoreFactory();
            container.Locker.Invoke(() => { sf.Remove(container, s); });
        }

        private ISortedDictionaryOnDisk CreateCollection(ISortedDictionaryOnDisk container, string storeName)
        {
            return container.Locker.Invoke(() =>
            {
                ISortedDictionaryOnDisk o;
                if (container.Transaction != null)
                    o = ((Transaction.ITransactionLogger)container.Transaction).CreateCollection(
                        container.File, null, storeName, true);
                else
                    o = OnDisk.ObjectServer.CreateDictionaryOnDisk(
                        ((OnDisk.Algorithm.SortedDictionary.ISortedDictionaryOnDisk)container).File, null, storeName, true);
                o.Open();
                o.Flush();
                container.Add(o.Name, o);
                o.Container = container;
                return o;
            });
        }
    }
}