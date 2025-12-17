package com.sharedcode.sop;

/**
 * Represents a context for SOP operations.
 * <p>
 * The context is used to manage the lifecycle of operations and handle errors.
 * It implements {@link AutoCloseable} to ensure resources are released.
 */
public class Context implements AutoCloseable {
    private final long id;
    private final SopLibrary lib;

    /**
     * Creates a new context.
     */
    public Context() {
        this.lib = SopLibrary.INSTANCE;
        this.id = lib.createContext();
        System.out.println("Context created with ID: " + this.id);
    }

    /**
     * Gets the context ID.
     *
     * @return The context ID.
     */
    public long getId() {
        return id;
    }

    /**
     * Closes the context and releases resources.
     */
    @Override
    public void close() {
        lib.removeContext(id);
    }
}
