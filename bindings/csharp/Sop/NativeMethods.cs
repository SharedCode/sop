using System;
using System.Runtime.InteropServices;

namespace Sop;

internal static class NativeMethods
{
    // NOTE: The library name must match the output of the Go build.
    // For now, we assume 'libjsondb' which the runtime will resolve to 
    // libjsondb.dll, libjsondb.so, or libjsondb.dylib
    private const string LibName = "libjsondb";

    [DllImport(LibName, EntryPoint = "openRedisConnection", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr OpenRedisConnection(byte[] uri);

    [DllImport(LibName, EntryPoint = "closeRedisConnection", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr CloseRedisConnection();

    [DllImport(LibName, EntryPoint = "openCassandraConnection", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr OpenCassandraConnection(byte[] payload);

    [DllImport(LibName, EntryPoint = "closeCassandraConnection", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr CloseCassandraConnection();

    [DllImport(LibName, EntryPoint = "manageLogging", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ManageLogging(int level, byte[] logPath);

    [DllImport(LibName, EntryPoint = "freeString", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void FreeString(IntPtr str);

    [DllImport(LibName, EntryPoint = "createContext", CallingConvention = CallingConvention.Cdecl)]
    internal static extern long CreateContext();

    [DllImport(LibName, EntryPoint = "removeContext", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void RemoveContext(long ctxId);

    [DllImport(LibName, EntryPoint = "manageTransaction", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ManageTransaction(long ctxId, int action, byte[] payload);

    [DllImport(LibName, EntryPoint = "manageDatabase", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ManageDatabase(long ctxId, int action, byte[] targetId, byte[] payload);

    [DllImport(LibName, EntryPoint = "manageBtree", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ManageBtree(long ctxId, int action, byte[] metadata, byte[] payload);

    [DllImport(LibName, EntryPoint = "getFromBtreeOut", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void GetFromBtree(long ctxId, int action, byte[] metadata, byte[] payload, out IntPtr result, out IntPtr error);

    [DllImport(LibName, EntryPoint = "getBtreeItemCountOut", CallingConvention = CallingConvention.Cdecl)]
    internal static extern void GetBtreeItemCount(byte[] metadata, out long count, out IntPtr error);

    [DllImport(LibName, EntryPoint = "manageSearch", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ManageSearch(long ctxId, int action, byte[] targetId, byte[] payload);

    [DllImport(LibName, EntryPoint = "manageVectorDB", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ManageVectorDB(long ctxId, int action, byte[] targetId, byte[] payload);

    [DllImport(LibName, EntryPoint = "manageModelStore", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr ManageModelStore(long ctxId, int action, byte[] targetId, byte[] payload);

    [DllImport(LibName, EntryPoint = "navigateBtree", CallingConvention = CallingConvention.Cdecl)]
    internal static extern IntPtr NavigateBtree(long ctxId, int action, byte[] metadata, byte[] payload);
}
