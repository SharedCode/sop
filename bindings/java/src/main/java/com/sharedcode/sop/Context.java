package com.sharedcode.sop;

public class Context implements AutoCloseable {
    private final long id;
    private final SopLibrary lib;

    public Context() {
        this.lib = SopLibrary.INSTANCE;
        this.id = lib.createContext();
        System.out.println("Context created with ID: " + this.id);
    }

    public long getId() {
        return id;
    }

    @Override
    public void close() {
        lib.removeContext(id);
    }
}
