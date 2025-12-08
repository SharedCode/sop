using System;

namespace Sop;

public enum LogLevel
{
    Debug = 0,
    Info = 1,
    Warn = 2,
    Error = 3
}

public static class Logger
{
    public static void Configure(LogLevel level, string logPath = "")
    {
        var resPtr = NativeMethods.ManageLogging((int)level, Interop.ToBytes(logPath ?? ""));
        var res = Interop.FromPtr(resPtr);
        if (res != null) throw new SopException(res);
    }
}
