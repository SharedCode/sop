package com.sharedcode.sop;

import com.sun.jna.Pointer;

public class Transaction implements AutoCloseable {
    private final Context ctx;
    private final String id;
    private final Database db;
    private boolean active;

    Transaction(Context ctx, String id, Database db) {
        this.ctx = ctx;
        this.id = id;
        this.db = db;
        this.active = true;
    }

    public String getId() {
        return id;
    }

    public Context getContext() {
        return ctx;
    }
    
    public Database getDatabase() {
        return db;
    }

    public void commit() throws SopException {
        if (!active) return;
        
        Pointer p = SopLibrary.INSTANCE.manageTransaction(ctx.getId(), SopLibrary.Commit, id);
        SopUtils.checkError(p);
        active = false;
    }

    public void rollback() throws SopException {
        if (!active) return;

        Pointer p = SopLibrary.INSTANCE.manageTransaction(ctx.getId(), SopLibrary.Rollback, id);
        SopUtils.checkError(p);
        active = false;
    }

    @Override
    public void close() {
        if (active) {
            try {
                rollback();
            } catch (SopException e) {
                // Suppress in close
            }
        }
    }
}
