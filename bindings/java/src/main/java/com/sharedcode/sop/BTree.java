package com.sharedcode.sop;

import com.sun.jna.ptr.PointerByReference;
import com.sun.jna.ptr.LongByReference;
import com.sun.jna.Pointer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class BTree<K, V> implements AutoCloseable {
    private final Context ctx;
    private final String id;
    private final String transactionId;
    private final boolean isPrimitiveKey;
    private final Class<K> keyType;
    private final Class<V> valueType;
    private final ObjectMapper mapper;

    BTree(Context ctx, String id, String transactionId, boolean isPrimitiveKey, Class<K> keyType, Class<V> valueType) {
        this.ctx = ctx;
        this.id = id;
        this.transactionId = transactionId;
        this.isPrimitiveKey = isPrimitiveKey;
        this.keyType = keyType;
        this.valueType = valueType;
        this.mapper = new ObjectMapper();
    }

    /**
     * Creates a new B-Tree.
     *
     * @param ctx The context.
     * @param name The name of the B-Tree.
     * @param tx The transaction.
     * @param options The B-Tree options.
     * @param keyType The class of the key.
     * @param valueType The class of the value.
     * @return The created B-Tree.
     * @throws SopException If an error occurs.
     */
    public static <K, V> BTree<K, V> create(Context ctx, String name, Transaction tx, BTreeOptions options, Class<K> keyType, Class<V> valueType) throws SopException {
        if (options == null) options = new BTreeOptions(name);
        options.transactionId = tx.getId();
        
        boolean isPrimitive = isPrimitive(keyType);
        options.isPrimitiveKey = isPrimitive;

        try {
            String payload = new ObjectMapper().writeValueAsString(options);
            
            String res = SopUtils.manageDatabase(ctx.getId(), SopLibrary.NewBtree, tx.getDatabase().getId(), payload);
            
            if (res != null) {
                return new BTree<>(ctx, res, tx.getId(), isPrimitive, keyType, valueType);
            } else {
                throw new SopException("Result is null");
            }
        } catch (JsonProcessingException e) {
            throw new SopException("Failed to serialize options", e);
        }
    }

    /**
     * Opens an existing B-Tree.
     *
     * @param ctx The context.
     * @param name The name of the B-Tree.
     * @param tx The transaction.
     * @param keyType The class of the key.
     * @param valueType The class of the value.
     * @return The opened B-Tree.
     * @throws SopException If an error occurs.
     */
    public static <K, V> BTree<K, V> open(Context ctx, String name, Transaction tx, Class<K> keyType, Class<V> valueType) throws SopException {
        BTreeOptions options = new BTreeOptions(name);
        options.transactionId = tx.getId();
        
        boolean isPrimitive = isPrimitive(keyType);
        options.isPrimitiveKey = isPrimitive;

        try {
            String payload = new ObjectMapper().writeValueAsString(options);
            
            String res = SopUtils.manageDatabase(ctx.getId(), SopLibrary.OpenBtree, tx.getDatabase().getId(), payload);
            
            if (res != null) {
                return new BTree<>(ctx, res, tx.getId(), isPrimitive, keyType, valueType);
            } else {
                throw new SopException("Result is null");
            }
        } catch (JsonProcessingException e) {
            throw new SopException("Failed to serialize options", e);
        }
    }

    private static boolean isPrimitive(Class<?> type) {
        return type.isPrimitive() || type == String.class || Number.class.isAssignableFrom(type) || Boolean.class.isAssignableFrom(type);
    }

    /**
     * Adds a key-value pair to the B-Tree.
     *
     * @param key The key to add.
     * @param value The value to add.
     * @return True if the add was successful, false otherwise.
     * @throws SopException If an error occurs.
     */
    public boolean add(K key, V value) throws SopException {
        return manage(BTreeAction.Add.value, new Item<>(key, value));
    }

    /**
     * Adds an item to the B-Tree.
     *
     * @param item The item to add.
     * @return True if the add was successful, false otherwise.
     * @throws SopException If an error occurs.
     */
    public boolean add(Item<K, V> item) throws SopException {
        return manage(BTreeAction.Add.value, item);
    }

    /**
     * Adds a list of items to the B-Tree.
     *
     * @param items The list of items to add.
     * @return True if the add was successful, false otherwise.
     * @throws SopException If an error occurs.
     */
    public boolean add(List<Item<K, V>> items) throws SopException {
        return manage(BTreeAction.Add.value, items);
    }

    /**
     * Adds a key-value pair to the B-Tree if it does not already exist.
     *
     * @param key The key to add.
     * @param value The value to add.
     * @return True if the add was successful, false otherwise.
     * @throws SopException If an error occurs.
     */
    public boolean addIfNotExist(K key, V value) throws SopException {
        return manage(BTreeAction.AddIfNotExist.value, new Item<>(key, value));
    }

    /**
     * Adds an item to the B-Tree if it does not already exist.
     *
     * @param item The item to add.
     * @return True if the add was successful, false otherwise.
     * @throws SopException If an error occurs.
     */
    public boolean addIfNotExist(Item<K, V> item) throws SopException {
        return manage(BTreeAction.AddIfNotExist.value, item);
    }

    /**
     * Updates the current key with a new value.
     *
     * @param item The item containing the new value.
     * @return True if the update was successful, false otherwise.
     * @throws SopException If an error occurs.
     */
    public boolean updateCurrentKey(Item<K, V> item) throws SopException {
        return manage(BTreeAction.UpdateCurrentKey.value, item);
    }

    /**
     * Adds a list of items to the B-Tree if they do not already exist.
     *
     * @param items The list of items to add.
     * @return True if the add was successful, false otherwise.
     * @throws SopException If an error occurs.
     */
    public boolean addIfNotExist(List<Item<K, V>> items) throws SopException {
        return manage(BTreeAction.AddIfNotExist.value, items);
    }

    /**
     * Updates a key-value pair in the B-Tree.
     *
     * @param key The key to update.
     * @param value The new value.
     * @return True if the update was successful, false otherwise.
     * @throws SopException If an error occurs.
     */
    public boolean update(K key, V value) throws SopException {
        return manage(BTreeAction.Update.value, new Item<>(key, value));
    }

    /**
     * Updates an item in the B-Tree.
     *
     * @param item The item to update.
     * @return True if the update was successful, false otherwise.
     * @throws SopException If an error occurs.
     */
    public boolean update(Item<K, V> item) throws SopException {
        return manage(BTreeAction.Update.value, item);
    }

    /**
     * Updates a list of items in the B-Tree.
     *
     * @param items The list of items to update.
     * @return True if the update was successful, false otherwise.
     * @throws SopException If an error occurs.
     */
    public boolean update(List<Item<K, V>> items) throws SopException {
        return manage(BTreeAction.Update.value, items);
    }

    /**
     * Inserts or updates a key-value pair in the B-Tree.
     *
     * @param key The key to upsert.
     * @param value The value to upsert.
     * @return True if the upsert was successful, false otherwise.
     * @throws SopException If an error occurs.
     */
    public boolean upsert(K key, V value) throws SopException {
        return manage(BTreeAction.Upsert.value, new Item<>(key, value));
    }

    /**
     * Inserts or updates an item in the B-Tree.
     *
     * @param item The item to upsert.
     * @return True if the upsert was successful, false otherwise.
     * @throws SopException If an error occurs.
     */
    public boolean upsert(Item<K, V> item) throws SopException {
        return manage(BTreeAction.Upsert.value, item);
    }

    /**
     * Inserts or updates a list of items in the B-Tree.
     *
     * @param items The list of items to upsert.
     * @return True if the upsert was successful, false otherwise.
     * @throws SopException If an error occurs.
     */
    public boolean upsert(List<Item<K, V>> items) throws SopException {
        return manage(BTreeAction.Upsert.value, items);
    }

    /**
     * Updates the key of an item in the B-Tree.
     *
     * @param item The item with the new key.
     * @return True if the update was successful, false otherwise.
     * @throws SopException If an error occurs.
     */
    public boolean updateKey(Item<K, V> item) throws SopException {
        return manage(BTreeAction.UpdateKey.value, item);
    }

    /**
     * Updates the keys of a list of items in the B-Tree.
     *
     * @param items The list of items with new keys.
     * @return True if the update was successful, false otherwise.
     * @throws SopException If an error occurs.
     */
    public boolean updateKey(List<Item<K, V>> items) throws SopException {
        return manage(BTreeAction.UpdateKey.value, items);
    }

    /**
     * Removes an item from the B-Tree by its key.
     *
     * @param key The key of the item to remove.
     * @return True if the removal was successful, false otherwise.
     * @throws SopException If an error occurs.
     */
    public boolean remove(K key) throws SopException {
        List<K> keys = new ArrayList<>();
        keys.add(key);
        return manageRaw(BTreeAction.Remove.value, keys);
    }

    /**
     * Removes multiple items from the B-Tree by their keys.
     *
     * @param keys The list of keys of the items to remove.
     * @return True if the removal was successful, false otherwise.
     * @throws SopException If an error occurs.
     */
    public boolean remove(List<K> keys) throws SopException {
        return manageRaw(BTreeAction.Remove.value, keys);
    }

    /**
     * Finds an item in the B-Tree by its key.
     *
     * @param key The key to search for.
     * @return True if the item was found, false otherwise.
     * @throws SopException If an error occurs.
     */
    public boolean find(K key) throws SopException {
        ManageBtreePayload<K, V> payload = new ManageBtreePayload<>();
        payload.items = new ArrayList<>();
        payload.items.add(new Item<>(key, null));
        
        try {
            String payloadJson = mapper.writeValueAsString(payload);
            Pointer p = SopLibrary.INSTANCE.navigateBtree(ctx.getId(), BTreeAction.Find.value, getMetaJson(), payloadJson);
            String result = SopUtils.fromPointer(p);
            return checkBooleanResult(result);
        } catch (JsonProcessingException e) {
            throw new SopException("Failed to serialize payload", e);
        }
    }

    /**
     * Finds an item in the B-Tree by its key and ID.
     *
     * @param key The key to search for.
     * @param id The ID to search for.
     * @return True if the item was found, false otherwise.
     * @throws SopException If an error occurs.
     */
    public boolean findWithId(K key, String id) throws SopException {
        ManageBtreePayload<K, V> payload = new ManageBtreePayload<>();
        payload.items = new ArrayList<>();
        Item<K, V> item = new Item<>(key, null);
        item.id = id;
        payload.items.add(item);
        
        try {
            Pointer p = SopLibrary.INSTANCE.navigateBtree(ctx.getId(), BTreeAction.FindWithID.value, getMetaJson(), mapper.writeValueAsString(payload));
            String result = SopUtils.fromPointer(p);
            return checkBooleanResult(result);
        } catch (JsonProcessingException e) {
            throw new SopException("Failed to serialize payload", e);
        }
    }

    /**
     * Moves the cursor to the first item in the B-Tree.
     *
     * @return True if successful, false otherwise.
     * @throws SopException If an error occurs.
     */
    public boolean moveToFirst() throws SopException {
        return navigate(BTreeAction.First.value);
    }

    /**
     * Moves the cursor to the last item in the B-Tree.
     *
     * @return True if successful, false otherwise.
     * @throws SopException If an error occurs.
     */
    public boolean moveToLast() throws SopException {
        return navigate(BTreeAction.Last.value);
    }

    /**
     * Moves the cursor to the next item in the B-Tree.
     *
     * @return True if successful, false otherwise.
     * @throws SopException If an error occurs.
     */
    public boolean moveToNext() throws SopException {
        return navigate(BTreeAction.Next.value);
    }

    /**
     * Moves the cursor to the previous item in the B-Tree.
     *
     * @return True if successful, false otherwise.
     * @throws SopException If an error occurs.
     */
    public boolean moveToPrevious() throws SopException {
        return navigate(BTreeAction.Previous.value);
    }

    /**
     * Gets the number of items in the B-Tree.
     *
     * @return The number of items.
     * @throws SopException If an error occurs.
     */
    public long count() throws SopException {
        LongByReference countRef = new LongByReference();
        PointerByReference errorRef = new PointerByReference();
        
        SopLibrary.INSTANCE.getBtreeItemCountOut(getMetaJson(), countRef, errorRef);
        SopUtils.checkError(errorRef.getValue());
        
        return countRef.getValue();
    }

    /**
     * Gets the current key at the cursor position.
     *
     * @return The current item (key only).
     * @throws SopException If an error occurs.
     */
    public Item<K, V> getCurrentKey() throws SopException {
        // GetCurrentKey in C# passes PagingInfo as payload (even if empty).
        PagingInfo pagingInfo = new PagingInfo();
        String json = get(BTreeAction.GetCurrentKey.value, pagingInfo);
        
        if (json == null || json.isEmpty()) return null;
        
        try {
            com.fasterxml.jackson.databind.JavaType itemType = mapper.getTypeFactory().constructParametricType(Item.class, keyType, valueType);
            com.fasterxml.jackson.databind.JavaType listType = mapper.getTypeFactory().constructCollectionType(List.class, itemType);
            List<Item<K, V>> items = mapper.readValue(json, listType);
            if (items != null && !items.isEmpty()) {
                return items.get(0);
            }
        } catch (JsonProcessingException e) {
            throw new SopException("Failed to deserialize items", e);
        }
        return null;
    }

    /**
     * Gets the current value at the cursor position.
     *
     * @return The current item (key and value).
     * @throws SopException If an error occurs.
     */
    public Item<K, V> getCurrentValue() throws SopException {
        PagingInfo pagingInfo = new PagingInfo();
        String json = get(BTreeAction.GetCurrentValue.value, pagingInfo);
        
        if (json == null || json.isEmpty()) return null;
        
        try {
            com.fasterxml.jackson.databind.JavaType itemType = mapper.getTypeFactory().constructParametricType(Item.class, keyType, valueType);
            com.fasterxml.jackson.databind.JavaType listType = mapper.getTypeFactory().constructCollectionType(List.class, itemType);
            List<Item<K, V>> items = mapper.readValue(json, listType);
            if (items != null && !items.isEmpty()) {
                return items.get(0);
            }
        } catch (JsonProcessingException e) {
            throw new SopException("Failed to deserialize items", e);
        }
        return null;
    }

    /**
     * Gets a list of keys from the B-Tree based on the paging info.
     *
     * @param pagingInfo The paging information.
     * @return A list of items containing keys.
     * @throws SopException If an error occurs.
     */
    public List<Item<K, V>> getKeys(PagingInfo pagingInfo) throws SopException {
        String json = get(BTreeAction.GetKeys.value, pagingInfo);
        if (json == null) return null;
        
        try {
            com.fasterxml.jackson.databind.JavaType itemType = mapper.getTypeFactory().constructParametricType(Item.class, keyType, valueType);
            com.fasterxml.jackson.databind.JavaType listType = mapper.getTypeFactory().constructCollectionType(List.class, itemType);
            return mapper.readValue(json, listType);
        } catch (JsonProcessingException e) {
            throw new SopException("Failed to deserialize keys", e);
        }
    }

    /**
     * Gets a list of values from the B-Tree for the given keys.
     *
     * @param keys The list of keys to retrieve values for.
     * @return A list of items containing keys and values.
     * @throws SopException If an error occurs.
     */
    public List<Item<K, V>> getValues(List<Item<K, V>> keys) throws SopException {
        ManageBtreePayload<K, V> payload = new ManageBtreePayload<>();
        payload.items = keys;
        
        String json = get(BTreeAction.GetValues.value, payload);
        if (json == null) return null;
        
        try {
            com.fasterxml.jackson.databind.JavaType itemType = mapper.getTypeFactory().constructParametricType(Item.class, keyType, valueType);
            com.fasterxml.jackson.databind.JavaType listType = mapper.getTypeFactory().constructCollectionType(List.class, itemType);
            return mapper.readValue(json, listType);
        } catch (JsonProcessingException e) {
            throw new SopException("Failed to deserialize values", e);
        }
    }

    /**
     * Moves the cursor to the first item in the B-Tree.
     *
     * @return True if successful, false otherwise.
     * @throws SopException If an error occurs.
     */
    public boolean first() throws SopException {
        return navigate(BTreeAction.First.value);
    }

    /**
     * Moves the cursor to the next item in the B-Tree.
     *
     * @return True if successful, false otherwise.
     * @throws SopException If an error occurs.
     */
    public boolean next() throws SopException {
        return navigate(BTreeAction.Next.value);
    }

    /**
     * Moves the cursor to the previous item in the B-Tree.
     *
     * @return True if successful, false otherwise.
     * @throws SopException If an error occurs.
     */
    public boolean previous() throws SopException {
        return navigate(BTreeAction.Previous.value);
    }

    /**
     * Moves the cursor to the last item in the B-Tree.
     *
     * @return True if successful, false otherwise.
     * @throws SopException If an error occurs.
     */
    public boolean last() throws SopException {
        return navigate(BTreeAction.Last.value);
    }

    private boolean navigate(int action) throws SopException {
        Pointer p = SopLibrary.INSTANCE.navigateBtree(ctx.getId(), action, getMetaJson(), null);
        String result = SopUtils.fromPointer(p);
        return checkBooleanResult(result);
    }

    private boolean checkBooleanResult(String result) throws SopException {
        if (result == null) {
            throw new SopException("Unexpected null result from navigation");
        }
        if ("true".equalsIgnoreCase(result)) {
            return true;
        }
        if ("false".equalsIgnoreCase(result)) {
            return false;
        }
        throw new SopException(result);
    }

    private String get(int action, Object payloadObj) throws SopException {
        PointerByReference resultRef = new PointerByReference();
        PointerByReference errorRef = new PointerByReference();
        
        try {
            String payloadJson = mapper.writeValueAsString(payloadObj);
            
            SopLibrary.INSTANCE.getFromBtreeOut(ctx.getId(), action, getMetaJson(), payloadJson, resultRef, errorRef);
            SopUtils.checkError(errorRef.getValue());
            Pointer p = resultRef.getValue();
            String res = SopUtils.fromPointer(p);
            return res;
        } catch (JsonProcessingException e) {
            throw new SopException("Failed to serialize payload", e);
        }
    }

    private boolean manage(int action, Item<K, V> item) throws SopException {
        return manage(action, Collections.singletonList(item));
    }

    private boolean manage(int action, List<Item<K, V>> items) throws SopException {
        ManageBtreePayload<K, V> payload = new ManageBtreePayload<>();
        payload.items = items;
        return manageRaw(action, payload);
    }

    private boolean manageRaw(int action, Object payloadObj) throws SopException {
        try {
            String payloadJson = mapper.writeValueAsString(payloadObj);
            Pointer p = SopLibrary.INSTANCE.manageBtree(ctx.getId(), action, getMetaJson(), payloadJson);
            String res = SopUtils.fromPointer(p);
            if (res == null) {
                 return false; // Or throw? C# throws if error, returns bool if success/fail
            }
            if ("true".equalsIgnoreCase(res)) return true;
            if ("false".equalsIgnoreCase(res)) return false;
            
            throw new SopException(res);
        } catch (JsonProcessingException e) {
            throw new SopException("Failed to serialize payload", e);
        }
    }

    private String getMetaJson() {
        ManageBtreeMetaData meta = new ManageBtreeMetaData();
        meta.btreeId = id;
        meta.transactionId = transactionId;
        meta.isPrimitiveKey = isPrimitiveKey;
        try {
            return mapper.writeValueAsString(meta);
        } catch (JsonProcessingException e) {
            // Should not happen for simple POJO
            throw new RuntimeException("Failed to serialize metadata", e);
        }
    }

    @Override
    public void close() {
        // No-op
    }
}

