package com.sharedcode.sop;

public class SopException extends Exception {
    public SopException(String message) {
        super(message);
    }

    public SopException(String message, Throwable cause) {
        super(message, cause);
    }
}
