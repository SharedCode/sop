using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Sop.Log
{
    /// <summary>
    /// Logger wrapper. This utilizes passed in ILogger instance
    /// for actual logging.
    /// </summary>
    public class Logger : ILogger
    {
        /// <summary>
        /// Constructor that uses TextLogger for actual logging of messages.
        /// </summary>
        /// <param name="logName"></param>
        public Logger(string logName) : this(new TextLogger(logName)) { }

        /// <summary>
        /// Constructor expecting a logger instance for doing actual logging.
        /// </summary>
        /// <param name="realLogger"></param>
        public Logger(ILogger realLogger)
        {
            if (realLogger == null)
                throw new ArgumentNullException("realLogger");

            RealLogger = realLogger;
        }

        /// <summary>
        /// Default Log Directory.
        /// </summary>
        public static string DefaultLogDirectory
        {
            get
            {
                return Utility.GenericLogger.DefaultLogDirectory;
            }
            set
            {
                Utility.GenericLogger.DefaultLogDirectory = value;
            }
        }

        /// <summary>
        /// LogLevel set.
        /// </summary>
        public LogLevels? LogLevel { get; set; }

        /// <summary>
        /// Singleton instance of Logger.
        /// </summary>
        public static Logger Instance
        {
            get
            {
                if (_instance == null)
                {
                    lock (_locker)
                    {
                        if (_instance == null)
                            _instance = new Logger("SopLog");
                    }
                }
                return _instance;
            }
        }
        private static object _locker = new object();
        private static Logger _instance;


        #region Log methods
        /// <summary>
        /// Log exception using default log level (error).
        /// </summary>
        /// <param name="exc">exception info of which will be logged.</param>
        /// <returns>Actual formatted text that got logged.</returns>
        public string Log(Exception exc)
        {
            return Log(LogLevels.Error, exc);
        }
        /// <summary>
        /// Log exception with given log level.
        /// </summary>
        /// <param name="logLevel">Log Level</param>
        /// <param name="exc">exception to log</param>
        /// <returns>Actual formatted text that got logged.</returns>
        public string Log(LogLevels logLevel, Exception exc)
        {
            return Log(logLevel, exc, "");
        }
        /// <summary>
        /// Log exception on default log level (error) with add'l formatable text msg and optional messageArgs.
        /// </summary>
        /// <param name="exc">exception to be logged.</param>
        /// <param name="messageFormat">Text message containing formatting section.</param>
        /// <param name="messageArgs">Arguments to be rendered/formatted part of the Text message.</param>
        /// <returns>Actual formatted text that got logged.</returns>
        public string Log(Exception exc, string messageFormat, params object[] messageArgs)
        {
            return Log(LogLevels.Error, messageFormat, messageArgs);
        }
        /// <summary>
        /// Log exception on given log level with add'l formatable text msg and optional messageArgs.
        /// </summary>
        /// <param name="logLevel">Log Level</param>
        /// <param name="exc">exception to be logged.</param>
        /// <param name="messageFormat">Text message containing formatting section.</param>
        /// <param name="messageArgs">Arguments to be rendered/formatted part of the Text message.</param>
        /// <returns>Actual formatted text that got logged.</returns>
        public string Log(LogLevels logLevel, Exception exc, string messageFormat, params object[] messageArgs)
        {
            if (!IsLogLevelEnabled(logLevel)) return string.Empty;

            var msg = format(exc, messageFormat, messageArgs);
            if (msg == null)
                throw new ArgumentNullException("messageFormat",
                                                "Log failed as there is no data to write, params are null.");

#if (DEBUG)
            var str = RealLogger.Log(logLevel, exc, messageFormat, messageArgs);
            Console.WriteLine(str);
            return str;
#else
            return RealLogger.Log(logLevel, exc, messageFormat, messageArgs);
#endif
        }
        /// <summary>
        /// Log a formatable text and optional messageArgs.
        /// </summary>
        /// <param name="logLevel">Log Level</param>
        /// <param name="messageFormat">Text message containing formatting section.</param>
        /// <param name="messageArgs">Arguments to be rendered/formatted part of the Text message.</param>
        /// <returns>Actual formatted text that got logged.</returns>
        public string Log(LogLevels logLevel, string messageFormat, params object[] messageArgs)
        {
            //** prevent to do anything if not logging for the target log level...
            if (!IsLogLevelEnabled(logLevel)) return string.Empty;

            var msg = format(messageFormat, messageArgs);
            if (msg == null)
                throw new ArgumentNullException("messageFormat",
                                                "Log failed as there is no data to write, params are null.");
#if (DEBUG)
            var str = RealLogger.Log(logLevel, messageFormat, messageArgs);
            Console.WriteLine(str);
            return str;
#else
            return RealLogger.Log(logLevel, messageFormat, messageArgs);
#endif
        }
        #endregion

        #region Verbose convenience log methods
        /// <summary>
        /// Log exception in verbose log level.
        /// </summary>
        /// <param name="exc">exception to be logged.</param>
        /// <returns>Actual formatted text that got logged.</returns>
        public string Verbose(Exception exc)
        {
            return Log(LogLevels.Verbose, exc);
        }
        /// <summary>
        /// Log exception in verbose log level with add'l formatable text msg and optional messageArgs.
        /// </summary>
        /// <param name="exc">exception to be logged.</param>
        /// <param name="messageFormat">Text message containing formatting section.</param>
        /// <param name="messageArgs">Arguments to be rendered/formatted part of the Text message.</param>
        /// <returns>Actual formatted text that got logged.</returns>
        public string Verbose(Exception exc, string messageFormat, params object[] messageArgs)
        {
            return Log(LogLevels.Verbose, exc, messageFormat, messageArgs);
        }
        /// <summary>
        /// Log formatable text msg and optional messageArgs in verbose log level.
        /// </summary>
        /// <param name="messageFormat">Text message containing formatting section.</param>
        /// <param name="messageArgs">Arguments to be rendered/formatted part of the Text message.</param>
        /// <returns>Actual formatted text that got logged.</returns>
        public string Verbose(string messageFormat, params object[] messageArgs)
        {
            return Log(LogLevels.Verbose, messageFormat, messageArgs);
        }
        #endregion
        #region Error convenience log methods
        /// <summary>
        /// Log exception with error log level.
        /// </summary>
        /// <param name="exc">exception to be logged.</param>
        /// <returns>Actual formatted text that got logged.</returns>
        public string Error(Exception exc)
        {
            return Log(LogLevels.Error, exc);
        }
        /// <summary>
        /// Log exception on error log level with add'l formatable text msg and optional messageArgs.
        /// </summary>
        /// <param name="exc">exception to be logged.</param>
        /// <param name="messageFormat">Text message containing formatting section.</param>
        /// <param name="messageArgs">Arguments to be rendered/formatted part of the Text message.</param>
        /// <returns>Actual formatted text that got logged.</returns>
        public string Error(Exception exc, string messageFormat, params object[] messageArgs)
        {
            return Log(LogLevels.Error, exc, messageFormat, messageArgs);
        }
        /// <summary>
        /// Log formatable text msg and optional messageArgs on error log level.
        /// </summary>
        /// <param name="messageFormat">Text message containing formatting section.</param>
        /// <param name="messageArgs">Arguments to be rendered/formatted part of the Text message.</param>
        /// <returns>Actual formatted text that got logged.</returns>
        public string Error(string messageFormat, params object[] messageArgs)
        {
            return Log(LogLevels.Error, messageFormat, messageArgs);
        }
        #endregion
        #region Information convenience log methods
        /// <summary>
        /// Log exception on Information log level.
        /// </summary>
        /// <param name="exc">exception to be logged.</param>
        /// <returns>Actual formatted text that got logged.</returns>
        public string Information(Exception exc)
        {
            return Log(LogLevels.Information, exc);
        }
        /// <summary>
        /// Log exception on Information log level with add'l formatable text msg and optional messageArgs.
        /// </summary>
        /// <param name="exc">exception to be logged.</param>
        /// <param name="messageFormat">Text message containing formatting section.</param>
        /// <param name="messageArgs">Arguments to be rendered/formatted part of the Text message.</param>
        /// <returns>Actual formatted text that got logged.</returns>
        public string Information(Exception exc, string messageFormat, params object[] messageArgs)
        {
            return Log(LogLevels.Information, exc, messageFormat, messageArgs);
        }
        /// <summary>
        /// Log formatable text msg and optional messageArgs on Information log level.
        /// </summary>
        /// <param name="messageFormat">Text message containing formatting section.</param>
        /// <param name="messageArgs">Arguments to be rendered/formatted part of the Text message.</param>
        /// <returns>Actual formatted text that got logged.</returns>
        public string Information(string messageFormat, params object[] messageArgs)
        {
            return Log(LogLevels.Information, messageFormat, messageArgs);
        }
        #endregion
        #region Fatal convenience log methods
        /// <summary>
        /// Log exception on Fatal log level.
        /// </summary>
        /// <param name="exc"></param>
        /// <returns>Actual formatted text that got logged.</returns>
        public string Fatal(Exception exc)
        {
            return Log(LogLevels.Fatal, exc);
        }
        /// <summary>
        /// Log exception, formatable text msg and optional messageArgs on Fatal log level.
        /// </summary>
        /// <param name="exc">exception to be logged.</param>
        /// <param name="messageFormat">Text message containing formatting section.</param>
        /// <param name="messageArgs">Arguments to be rendered/formatted part of the Text message.</param>
        /// <returns>Actual formatted text that got logged.</returns>
        public string Fatal(Exception exc, string messageFormat, params object[] messageArgs)
        {
            return Log(LogLevels.Fatal, exc, messageFormat, messageArgs);
        }
        /// <summary>
        /// Log formatable text msg and optional messageArgs on Fatal log level.
        /// </summary>
        /// <param name="messageFormat">Text message containing formatting section.</param>
        /// <param name="messageArgs">Arguments to be rendered/formatted part of the Text message.</param>
        /// <returns>Actual formatted text that got logged.</returns>
        public string Fatal(string messageFormat, params object[] messageArgs)
        {
            return Log(LogLevels.Fatal, messageFormat, messageArgs);
        }
        #endregion
        #region Warning convenience log methods
        /// <summary>
        /// Log exception on Warning log level.
        /// </summary>
        /// <param name="exc">exception to be logged.</param>
        /// <returns>Actual formatted text that got logged.</returns>
        public string Warning(Exception exc)
        {
            return Log(LogLevels.Warning, exc);
        }
        /// <summary>
        /// Log exception, formatable text msg and optional messageArgs on Warning log level.
        /// </summary>
        /// <param name="exc">exception to be logged.</param>
        /// <param name="messageFormat">Text message containing formatting section.</param>
        /// <param name="messageArgs">Arguments to be rendered/formatted part of the Text message.</param>
        /// <returns>Actual formatted text that got logged.</returns>
        public string Warning(Exception exc, string messageFormat, params object[] messageArgs)
        {
            return Log(LogLevels.Warning, exc, messageFormat, messageArgs);
        }
        /// <summary>
        /// Log formatable text msg and optional messageArgs on Warning log level.
        /// </summary>
        /// <param name="messageFormat">Text message containing formatting section.</param>
        /// <param name="messageArgs">Arguments to be rendered/formatted part of the Text message.</param>
        /// <returns>Actual formatted text that got logged.</returns>
        public string Warning(string messageFormat, params object[] messageArgs)
        {
            return Log(LogLevels.Warning, messageFormat, messageArgs);
        }
        #endregion

        #region Log Formatting functions
        /// <summary>
        /// Utility function for Formatting exception, text msg and optional messageArgs.
        /// </summary>
        /// <param name="exc">exception to be logged.</param>
        /// <param name="messageFormat">Text message containing formatting section.</param>
        /// <param name="messageArgs">Arguments to be rendered/formatted part of the Text message.</param>
        /// <returns>Actual formatted text that got logged.</returns>
        public static string Format(Exception exc, string messageFormat, params object[] messageArgs)
        {
            if (exc == null)
                throw new ArgumentNullException("exc");
            if (string.IsNullOrEmpty(messageFormat))
                messageFormat = exc.Message;
            var data = Format(messageFormat, messageArgs);
            return Format("{0}, exception: {1}", data, exc.ToString());
        }
        /// <summary>
        /// Utility function for Formatting text msg and optional messageArgs.
        /// </summary>
        /// <param name="messageFormat">Text message containing formatting section.</param>
        /// <param name="messageArgs">Arguments to be rendered/formatted part of the Text message.</param>
        /// <returns>Actual formatted text that got logged.</returns>
        public static string Format(string messageFormat, params object[] messageArgs)
        {
            string data;
            if (messageFormat != null && messageArgs != null && messageArgs.Length > 0)
                data = string.Format(messageFormat, messageArgs);
            else
                data = messageFormat;
            return data;
        }
        /// <summary>
        /// Utility function for Formatting exception, text msg and optional messageArgs.
        /// </summary>
        /// <param name="target">Target StringBuilder where we append message and their optional arguments.</param>
        /// <param name="exc">exception to be logged.</param>
        /// <param name="messageFormat">Text message containing formatting section.</param>
        /// <param name="messageArgs">Arguments to be rendered/formatted part of the Text message.</param>
        /// <returns>Actual formatted text that got logged.</returns>
        public static void Format(StringBuilder target, Exception exc, string messageFormat, params object[] messageArgs)
        {
            if (target == null)
                throw new ArgumentNullException("target");
            if (exc == null)
                throw new ArgumentNullException("exc");
            Format(target, messageFormat, messageArgs);
            target.AppendFormat(", exception: {0}", exc.ToString());
        }
        /// <summary>
        /// Utility function for Formatting text msg and optional messageArgs.
        /// </summary>
        /// <param name="target">Target StringBuilder where we append message and their optional arguments.</param>
        /// <param name="messageFormat">Text message containing formatting section.</param>
        /// <param name="messageArgs">Arguments to be rendered/formatted part of the Text message.</param>
        /// <returns>Actual formatted text that got logged.</returns>
        public static void Format(StringBuilder target, string messageFormat, params object[] messageArgs)
        {
            if (target == null)
                throw new ArgumentNullException("target");
            if (messageFormat != null && messageArgs != null && messageArgs.Length > 0)
                target.AppendFormat(messageFormat, messageArgs);
            else
                target.AppendFormat(messageFormat);
        }
        #endregion

        /// <summary>
        /// Is given log level logging enabled.
        /// </summary>
        /// <param name="logLevel">Log Level</param>
        /// <returns>true if Log Level is enabled, otherwise false.</returns>
        public bool IsLogLevelEnabled(LogLevels logLevel)
        {
            return RealLogger.IsLogLevelEnabled(logLevel);
        }
        /// <summary>
        /// Is Verbose logging enabled.
        /// </summary>
        public bool IsVerboseEnabled
        {
            get
            {
                return IsLogLevelEnabled(LogLevels.Verbose);
            }
        }
        /// <summary>
        /// Is Error logging enabled.
        /// </summary>
        public bool IsErrorEnabled
        {
            get
            {
                return IsLogLevelEnabled(LogLevels.Error);
            }
        }
        /// <summary>
        /// Is Fatal logging enabled.
        /// </summary>
        public bool IsFatalEnabled
        {
            get
            {
                return IsLogLevelEnabled(LogLevels.Fatal);
            }
        }
        /// <summary>
        /// Is Information logging enabled.
        /// </summary>
        public bool IsInformationEnabled
        {
            get
            {
                return IsLogLevelEnabled(LogLevels.Information);
            }
        }
        /// <summary>
        /// Is Warning logging enabled.
        /// </summary>
        public bool IsWarningEnabled
        {
            get
            {
                return IsLogLevelEnabled(LogLevels.Warning);
            }
        }

        /// <summary>
        /// Name of this Logger.
        /// </summary>
        public string Name
        {
            get
            {
                return RealLogger.Name;
            }
        }

        /// <summary>
        /// Returns the Log context data dictionary.
        /// All entries (ie - both Name and Value pair) in the log context dictionary 
        /// will be written as prefix for each log Log Facade generates. It can be useful
        /// for things like easily specifying Logged in user info that makes it to the
        /// logs the application is generating.
        /// </summary>
        public IDictionary<string, string> LogContextData
        {
            get { return _logContextData; }
        }

        private string format(Exception exc, string messageFormat, params object[] messageArgs)
        {
            var sb = RenderContext();
            if (sb.Length > 0)
                sb.AppendFormat("| ");
            Format(sb, exc, messageFormat, messageArgs);
            return sb.ToString();
        }
        private string format(string messageFormat, params object[] messageArgs)
        {
            var sb = RenderContext();
            if (sb.Length > 0)
                sb.AppendFormat("| ");
            Format(sb, messageFormat, messageArgs);
            return sb.ToString();
        }
        private StringBuilder RenderContext()
        {
            //** set capacity to 50 characters to prevent re-allocation if not set...
            var sb = new StringBuilder(50);
            if (LogContextData.Count > 0)
            {
                foreach (var kvp in LogContextData)
                {
                    sb.AppendFormat(sb.Length > 0 ? ", {0}: {1}" : "{0}: {1}", kvp.Key ?? "", kvp.Value ?? "");
                }
            }
            return sb;
        }

        /// <summary>
        /// Real logger instance.
        /// </summary>
        internal readonly ILogger RealLogger;

        private readonly Dictionary<string, string> _logContextData = new Dictionary<string, string>();
    }
}
