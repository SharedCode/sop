package com.sharedcode.sop;

import com.sun.jna.Pointer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

public class Cassandra {
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Initializes the global shared Cassandra connection.
     * @param config The Cassandra configuration
     * @throws SopException if initialization fails
     */
    public static void initialize(CassandraConfig config) throws SopException {
        try {
            String json = mapper.writeValueAsString(config);
            Pointer resPtr = SopLibrary.INSTANCE.openCassandraConnection(json);
            String error = SopUtils.fromPointer(resPtr);
            if (error != null && !error.isEmpty()) {
                throw new SopException(error);
            }
        } catch (JsonProcessingException e) {
            throw new SopException("Failed to serialize Cassandra config", e);
        }
    }

    /**
     * Closes the global shared Cassandra connection.
     * @throws SopException if closing fails
     */
    public static void close() throws SopException {
        Pointer resPtr = SopLibrary.INSTANCE.closeCassandraConnection();
        String error = SopUtils.fromPointer(resPtr);
        if (error != null && !error.isEmpty()) {
            throw new SopException(error);
        }
    }
}
