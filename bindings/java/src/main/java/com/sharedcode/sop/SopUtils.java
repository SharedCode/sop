package com.sharedcode.sop;

import com.sun.jna.Pointer;
import com.sun.jna.Memory;
import java.nio.charset.StandardCharsets;

class SopUtils {
    static String fromPointer(Pointer p) {
        if (p == null) return null;
        try {
            return p.getString(0, StandardCharsets.UTF_8.name());
        } finally {
            // We must free the string returned by Go C-shared library
            SopLibrary.INSTANCE.freeString(p);
        }
    }

    static Pointer toPointer(String s) {
        if (s == null) return null;
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        // Allocate memory for string + null terminator
        Memory m = new Memory(b.length + 1);
        m.write(0, b, 0, b.length);
        m.setByte(b.length, (byte)0);
        return m;
    }
    
    static void checkError(Pointer p) throws SopException {
        if (p != null) {
            String errorMsg = fromPointer(p);
            throw new SopException(errorMsg);
        }
    }
}
