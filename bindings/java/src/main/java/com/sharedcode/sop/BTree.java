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

    public boolean add(K key, V value) throws SopException {
        return manage(BTreeAction.Add.value, new Item<>(key, value));
    }

    public boolean add(Item<K, V> item) throws SopException {
        return manage(BTreeAction.Add.value, item);
    }

    public boolean add(List<Item<K, V>> items) throws SopException {
        return manage(BTreeAction.Add.value, items);
    }

    public boolean addIfNotExist(K key, V value) throws SopException {
        return manage(BTreeAction.AddIfNotExist.value, new Item<>(key, value));
    }

    public boolean addIfNotExist(Item<K, V> item) throws SopException {
        return manage(BTreeAction.AddIfNotExist.value, item);
    }

    public boolean updateCurrentKey(Item<K, V> item) throws SopException {
        return manage(BTreeAction.UpdateCurrentKey.value, item);
    }

    public boolean addIfNotExist(List<Item<K, V>> items) throws SopException {
        return manage(BTreeAction.AddIfNotExist.value, items);
    }

    public boolean update(K key, V value) throws SopException {
        return manage(BTreeAction.Update.value, new Item<>(key, value));
    }

    public boolean update(Item<K, V> item) throws SopException {
        return manage(BTreeAction.Update.value, item);
    }

    public boolean update(List<Item<K, V>> items) throws SopException {
        return manage(BTreeAction.Update.value, items);
    }

    public boolean upsert(K key, V value) throws SopException {
        return manage(BTreeAction.Upsert.value, new Item<>(key, value));
    }

    public boolean upsert(Item<K, V> item) throws SopException {
        return manage(BTreeAction.Upsert.value, item);
    }

    public boolean upsert(List<Item<K, V>> items) throws SopException {
        return manage(BTreeAction.Upsert.value, items);
    }

    public boolean updateKey(Item<K, V> item) throws SopException {
        return manage(BTreeAction.UpdateKey.value, item);
    }

    public boolean updateKey(List<Item<K, V>> items) throws SopException {
        return manage(BTreeAction.UpdateKey.value, items);
    }

    public boolean remove(K key) throws SopException {
        List<K> keys = new ArrayList<>();
        keys.add(key);
        return manageRaw(BTreeAction.Remove.value, keys);
    }

    public boolean remove(List<K> keys) throws SopException {
        return manageRaw(BTreeAction.Remove.value, keys);
    }

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

    public boolean moveToFirst() throws SopException {
        return navigate(BTreeAction.First.value);
    }

    public boolean moveToLast() throws SopException {
        return navigate(BTreeAction.Last.value);
    }

    public boolean moveToNext() throws SopException {
        return navigate(BTreeAction.Next.value);
    }

    public boolean moveToPrevious() throws SopException {
        return navigate(BTreeAction.Previous.value);
    }

    public long count() throws SopException {
        LongByReference countRef = new LongByReference();
        PointerByReference errorRef = new PointerByReference();
        
        SopLibrary.INSTANCE.getBtreeItemCountOut(getMetaJson(), countRef, errorRef);
        SopUtils.checkError(errorRef.getValue());
        
        return countRef.getValue();
    }

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

    public boolean first() throws SopException {
        return navigate(BTreeAction.First.value);
    }

    public boolean next() throws SopException {
        return navigate(BTreeAction.Next.value);
    }

    public boolean previous() throws SopException {
        return navigate(BTreeAction.Previous.value);
    }

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

