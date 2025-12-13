package com.sharedcode.sop;

import com.sun.jna.Pointer;

public class Logger {
    public static void configure(int level, String logPath) throws SopException {
        if (logPath == null) logPath = "";
        Pointer resPtr = SopLibrary.INSTANCE.manageLogging(level, logPath);
        String error = SopUtils.fromPointer(resPtr);
        if (error != null && !error.isEmpty()) {
            throw new SopException(error);
        }
    }
}
