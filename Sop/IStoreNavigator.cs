using System;
namespace Sop
{
    /// <summary>
    /// Store Navigator Interface.
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
    interface IStoreNavigator
    {
        ISortedDictionary<TKey, TValue> GetStore<TKey, TValue>(object container, string storeName, StoreParameters<TKey> storeConfig = null);
        ISortedDictionary<TKey, TValue> GetStore<TKey, TValue>(string storePath, StoreParameters<TKey> storeConfig = null, Profile fileConfig = null);
        ISortedDictionary<TKey, TValue> GetStorePersistent<TKey, TValue>(string storePath, StoreParameters<TKey> storeConfig = null, Profile fileConfig = null)
            where TKey : Sop.Persistence.IPersistent, new()
            where TValue : Sop.Persistence.IPersistent, new();
        ISortedDictionary<TKey, TValue> GetStorePersistentKey<TKey, TValue>(string storePath, StoreParameters<TKey> storeConfig = null, Profile fileConfig = null)
            where TKey : Sop.Persistence.IPersistent, new();
        ISortedDictionary<TKey, TValue> GetStorePersistentValue<TKey, TValue>(string storePath, StoreParameters<TKey> storeConfig = null, Profile fileConfig = null) 
            where TValue : Sop.Persistence.IPersistent, new();
        ISortedDictionaryOnDisk GetUnwrappedStore(string storePath);
        void Remove(string storePath);
    }
}
