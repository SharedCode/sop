package com.sharedcode.sop;

import com.sun.jna.Pointer;

public class Redis {
    /**
     * Initializes the global shared Redis connection.
     * @param url The Redis connection string (e.g., "redis://localhost:6379")
     * @throws SopException if initialization fails
     */
    public static void initialize(String url) throws SopException {
        Pointer resPtr = SopLibrary.INSTANCE.openRedisConnection(url);
        String error = SopUtils.fromPointer(resPtr);
        if (error != null && !error.isEmpty()) {
            throw new SopException(error);
        }
    }

    /**
     * Closes the global shared Redis connection.
     * @throws SopException if closing fails
     */
    public static void close() throws SopException {
        Pointer resPtr = SopLibrary.INSTANCE.closeRedisConnection();
        String error = SopUtils.fromPointer(resPtr);
        if (error != null && !error.isEmpty()) {
            throw new SopException(error);
        }
    }
}
