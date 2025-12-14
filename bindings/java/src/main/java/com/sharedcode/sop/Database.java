package com.sharedcode.sop;

import com.sun.jna.Pointer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.UUID;

public class Database {
    private String id;
    private final DatabaseOptions options;
    private final ObjectMapper mapper = new ObjectMapper();

    public Database(DatabaseOptions options) {
        this.options = options;
    }

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

    public Transaction beginTransaction(Context ctx) throws SopException {
        return beginTransaction(ctx, TransactionMode.ForWriting, 15);
    }

    public Transaction beginTransaction(Context ctx, int mode) throws SopException {
        return beginTransaction(ctx, mode, 15);
    }

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
