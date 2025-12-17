package com.sharedcode.sop;

import com.sun.jna.Pointer;

/**
 * Represents a transaction in the SOP library.
 */
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

    /**
     * Gets the transaction ID.
     *
     * @return The transaction ID.
     */
    public String getId() {
        return id;
    }

    /**
     * Gets the context associated with the transaction.
     *
     * @return The context.
     */
    public Context getContext() {
        return ctx;
    }
    
    /**
     * Gets the database associated with the transaction.
     *
     * @return The database.
     */
    public Database getDatabase() {
        return db;
    }

    /**
     * Commits the transaction.
     *
     * @throws SopException If an error occurs.
     */
    public void commit() throws SopException {
        if (!active) return;
        
        Pointer p = SopLibrary.INSTANCE.manageTransaction(ctx.getId(), SopLibrary.Commit, id);
        SopUtils.checkError(p);
        active = false;
    }

    /**
     * Rolls back the transaction.
     *
     * @throws SopException If an error occurs.
     */
    public void rollback() throws SopException {
        if (!active) return;

        Pointer p = SopLibrary.INSTANCE.manageTransaction(ctx.getId(), SopLibrary.Rollback, id);
        SopUtils.checkError(p);
        active = false;
    }

    /**
     * Closes the transaction, rolling it back if it is still active.
     */
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
