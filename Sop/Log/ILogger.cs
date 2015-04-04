using System;

namespace Sop.Log
{
    /// <summary>
    /// General purpose Logger Interface.
    /// NOTE: this is mainly used for integrating to a log framework, e.g. - nLog or simple text logger,...
    /// </summary>
    public interface ILogger
    {
        /// <summary>
        /// Name of this Logger.
        /// </summary>
        string Name { get; }
        /// <summary>
        /// Log an exception given log level (Error) with option for formatting and logging additional log text.
        /// </summary>
        /// <param name="logLevel"></param>
        /// <param name="exc"></param>
        /// <param name="messageFormat"></param>
        /// <param name="messageArgs"></param>
        /// <returns></returns>
        string Log(LogLevels logLevel, Exception exc, string messageFormat, params object[] messageArgs);
        /// <summary>
        /// Log some text and returns its formatted string log value.
        /// </summary>
        /// <param name="logLevel"></param>
        /// <param name="messageFormat"></param>
        /// <param name="messageArgs"></param>
        /// <returns></returns>
        string Log(LogLevels logLevel, string messageFormat, params object[] messageArgs);
        /// <summary>
        /// Determines whether a log level is enabled or not.
        /// </summary>
        /// <param name="logLevel"></param>
        /// <returns></returns>
        bool IsLogLevelEnabled(LogLevels logLevel);

        /// <summary>
        /// Log Level.
        /// </summary>
        LogLevels? LogLevel { get; set; }
    }
}
