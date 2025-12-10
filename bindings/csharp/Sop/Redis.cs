using System;
using System.Runtime.InteropServices;

namespace Sop;

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
