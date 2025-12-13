package com.sharedcode.sop;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

public interface SopLibrary extends Library {
    SopLibrary INSTANCE = Native.load("jsondb", SopLibrary.class);

    // Memory Management
    void freeString(Pointer cString);

    // Context Management
    long createContext();
    void removeContext(long ctxId);
    Pointer contextError(long ctxId);

    // Database Management
    Pointer manageDatabase(long ctxId, int action, String targetID, String payload);

    // Transaction Management
    Pointer manageTransaction(long ctxId, int action, String payload);

    // B-Tree Management
    Pointer manageBtree(long ctxId, int action, String payload, String payload2);

    // B-Tree Navigation & Retrieval
    void getFromBtreeOut(long ctxId, int action, String payload, String payload2, PointerByReference result, PointerByReference error);
    Pointer navigateBtree(long ctxId, int action, String payload, String payload2);
    Pointer isUniqueBtree(String payload);
    void getBtreeItemCountOut(String payload, com.sun.jna.ptr.LongByReference count, PointerByReference error);

    // Redis Management
    Pointer openRedisConnection(String uri);
    Pointer closeRedisConnection();

    // Cassandra Management
    Pointer openCassandraConnection(String payload);
    Pointer closeCassandraConnection();

    // Logging Management
    Pointer manageLogging(int level, String logPath);

    // Constants for Actions (matching Go definitions)
    // Database Actions
    int NewDatabase = 1;
    int BeginTransaction = 2;
    int NewBtree = 3;
    int OpenBtree = 4;
    int OpenModelStore = 5;
    int OpenVectorStore = 6;
    int OpenSearch = 7;
    int RemoveBtree = 8;
    int RemoveModelStore = 9;
    int RemoveVectorStore = 10;
    int RemoveSearch = 11;

    // Transaction Actions
    int NewTransaction = 1;
    int Begin = 2;
    int Commit = 3;
    int Rollback = 4;

    // B-Tree Actions
    int Add = 1;
    int AddIfNotExist = 2;
    int Update = 3;
    int Upsert = 4;
    int Remove = 5;
    int Find = 6;
    int FindWithID = 7;
    int GetItems = 8;
    int GetValues = 9;
    int GetKeys = 10;
    int First = 11;
    int Last = 12;
    int IsUnique = 13;
    int Count = 14;
    int GetStoreInfo = 15;
    int UpdateKey = 16;
    int UpdateCurrentKey = 17;
    int GetCurrentKey = 18;
    int Next = 19;
    int Previous = 20;
}
