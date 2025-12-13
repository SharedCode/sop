package com.sharedcode.sop;

import com.fasterxml.jackson.annotation.JsonValue;

public enum DatabaseType {
    Standalone(0),
    Clustered(1);

    private final int value;

    DatabaseType(int value) {
        this.value = value;
    }

    @JsonValue
    public int getValue() {
        return value;
    }
}
