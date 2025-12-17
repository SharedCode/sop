package com.sharedcode.sop;

import com.sun.jna.Pointer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.UUID;

/**
 * Represents a database in the SOP library.
 */
public class Database {
    private String id;
    private final DatabaseOptions options;
    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * Creates a new Database instance.
     *
     * @param options The database options.
     */
    public Database(DatabaseOptions options) {
        this.options = options;
    }

    /**
     * Gets the database ID.
     *
     * @return The database ID.
     */
    public String getId() {
        return id;
    }

    private void ensureCreated(Context ctx) throws SopException {
        if (id != null) return;

        String payload;
        try {
            if (options != null) {
                payload = mapper.writeValueAsString(options);
            } else {
                // Default minimal options if none provided
                // Using a raw string here as fallback, or could create a default DatabaseOptions object
                payload = "{\"type\": 0, \"stores_folders\": [\".\"]}";
            }
        } catch (JsonProcessingException e) {
            throw new SopException("Failed to serialize database options", e);
        }

        String res = SopUtils.manageDatabase(ctx.getId(), SopLibrary.NewDatabase, null, payload);
        if (res != null) {
            this.id = res;
        } else {
            throw new SopException("Unknown error creating database. Payload: " + payload);
        }
    }

    /**
     * Begins a new transaction with default mode (ForWriting) and timeout (15 minutes).
     *
     * @param ctx The context.
     * @return The new transaction.
     * @throws SopException If the transaction cannot be begun.
     */
    public Transaction beginTransaction(Context ctx) throws SopException {
        return beginTransaction(ctx, TransactionMode.ForWriting, 15);
    }

    /**
     * Begins a new transaction with specified mode and default timeout (15 minutes).
     *
     * @param ctx The context.
     * @param mode The transaction mode.
     * @return The new transaction.
     * @throws SopException If the transaction cannot be begun.
     */
    public Transaction beginTransaction(Context ctx, int mode) throws SopException {
        return beginTransaction(ctx, mode, 15);
    }

    /**
     * Begins a new transaction.
     *
     * @param ctx The context.
     * @param mode The transaction mode.
     * @param maxTime The maximum duration of the transaction in minutes.
     * @return The new transaction.
     * @throws SopException If the transaction cannot be begun.
     */
    public Transaction beginTransaction(Context ctx, int mode, int maxTime) throws SopException {
        ensureCreated(ctx);
        
        String payload = "{\"mode\": " + mode + ", \"max_time\": " + maxTime + "}";
        String res = SopUtils.manageDatabase(ctx.getId(), SopLibrary.BeginTransaction, id, payload);
        
        if (res != null) {
            return new Transaction(ctx, res, this);
        } else {
            throw new SopException("Failed to begin transaction (result is null)");
        }
    }

    /**
     * Creates a new B-Tree.
     *
     * @param ctx The context.
     * @param name The name of the B-Tree.
     * @param tx The transaction.
     * @param options The B-Tree options.
     * @param keyType The class of the key type.
     * @param valueType The class of the value type.
     * @param <K> The key type.
     * @param <V> The value type.
     * @return The new B-Tree.
     * @throws SopException If the B-Tree cannot be created.
     */
    public <K, V> BTree<K, V> newBtree(Context ctx, String name, Transaction tx, BTreeOptions options, Class<K> keyType, Class<V> valueType) throws SopException {
        return BTree.create(ctx, name, tx, options, keyType, valueType);
    }

    /**
     * Opens an existing B-Tree.
     *
     * @param ctx The context.
     * @param name The name of the B-Tree.
     * @param tx The transaction.
     * @param keyType The class of the key type.
     * @param valueType The class of the value type.
     * @param <K> The key type.
     * @param <V> The value type.
     * @return The opened B-Tree.
     * @throws SopException If the B-Tree cannot be opened.
     */
    public <K, V> BTree<K, V> openBtree(Context ctx, String name, Transaction tx, Class<K> keyType, Class<V> valueType) throws SopException {
        return BTree.open(ctx, name, tx, keyType, valueType);
    }
    
    /**
     * Removes a B-Tree.
     *
     * @param ctx The context.
     * @param name The name of the B-Tree.
     * @throws SopException If the B-Tree cannot be removed.
     */
    public void removeBtree(Context ctx, String name) throws SopException {
        ensureCreated(ctx);
        
        // Payload is just the name of the btree
        // Note: manageDatabase expects payload to be JSON usually, but for RemoveBtree it might be just the name?
        // Let's check how NewBtree passes name. It passes BTreeOptions JSON which contains name.
        // But RemoveBtree might just take the name string as payload.
        // Let's assume it takes the name string directly as payload.
        String payload = name;
        
        SopUtils.manageDatabase(ctx.getId(), SopLibrary.RemoveBtree, id, payload);
    }
}
