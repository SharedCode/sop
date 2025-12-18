using System;
using System.Runtime.InteropServices;

namespace Sop;

/// <summary>
/// Provides methods to manage the global Redis connection.
/// Note: In newer versions of SOP, Redis configuration can be passed directly via DatabaseOptions,
/// allowing for different Redis connections per Database instance.
/// This static class is maintained for backward compatibility or global initialization.
/// </summary>
public static class Redis
{
    /// <summary>
    /// Initializes the global shared Redis connection.
    /// </summary>
    public static void Initialize(string url)
    {
        var resPtr = NativeMethods.OpenRedisConnection(Interop.ToBytes(url));
        var res = Interop.FromPtr(resPtr);
        if (res != null) throw new SopException(res);
    }

    /// <summary>
    /// Closes the global shared Redis connection.
    /// </summary>
    public static void Close()
    {
        var resPtr = NativeMethods.CloseRedisConnection();
        var res = Interop.FromPtr(resPtr);
        if (res != null) throw new SopException(res);
    }
}
