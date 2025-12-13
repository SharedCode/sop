package com.sharedcode.sop;

import java.io.File;

public class BaseTest {
    static {
        // Set JNA library path to where libjsondb.dylib is located
        File libDir = new File("../../bindings/main");
        if (!libDir.exists()) {
            // Try relative to project root if running from IDE or different CWD
            libDir = new File("bindings/main");
        }
        // Also try absolute path if we know where we are (workspace root is /Users/grecinto/sop)
        if (!libDir.exists()) {
             libDir = new File("/Users/grecinto/sop/bindings/main");
        }

        if (libDir.exists()) {
            System.setProperty("jna.library.path", libDir.getAbsolutePath());
            // System.out.println("Set jna.library.path to: " + libDir.getAbsolutePath());
        } else {
            System.err.println("Could not find bindings/main directory for JNA library path.");
        }
    }
}
