using System;
using System.Runtime.InteropServices;
using System.Text;

namespace Sop;

/// <summary>
/// Exception thrown by the Sop library.
/// </summary>
public class SopException : Exception
{
    /// <summary>
    /// Initializes a new instance of the SopException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    public SopException(string message) : base(message) { }
}

internal static class Interop
{
    public static byte[] ToBytes(string str)
    {
        if (str == null) return null;
        var bytes = Encoding.UTF8.GetBytes(str);
        var result = new byte[bytes.Length + 1];
        Array.Copy(bytes, result, bytes.Length);
        result[bytes.Length] = 0;
        return result;
    }

    public static string FromPtr(IntPtr ptr, bool free = true)
    {
        if (ptr == IntPtr.Zero) return null;
        try
        {
            // Read UTF-8 string from pointer
            // Since .NET Standard 2.0 / .NET Core 3.1 doesn't have Marshal.PtrToStringUTF8 easily accessible in all versions,
            // we'll do a manual read.
            int len = 0;
            while (Marshal.ReadByte(ptr, len) != 0) len++;
            byte[] buffer = new byte[len];
            Marshal.Copy(ptr, buffer, 0, len);
            return Encoding.UTF8.GetString(buffer);
        }
        finally
        {
            if (free) NativeMethods.FreeString(ptr);
        }
    }
}

/// <summary>
/// Represents a connection context to the Sop library.
/// </summary>
public class Context : IDisposable
{
    internal long Id { get; private set; }

    /// <summary>
    /// Initializes a new instance of the Context class.
    /// </summary>
    public Context()
    {
        Id = NativeMethods.CreateContext();
    }

    /// <summary>
    /// Disposes the context and releases resources.
    /// </summary>
    public void Dispose()
    {
        if (Id != 0)
        {
            NativeMethods.RemoveContext(Id);
            Id = 0;
        }
    }
}
