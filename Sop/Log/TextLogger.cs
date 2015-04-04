using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Sop.Log
{
    public class TextLogger : ILogger, IDisposable
    {
        private Utility.GenericLogger _logger;
        public TextLogger(string logName)
        {
            Name = logName;
            string logFilename = logName;
            var fi = new System.IO.FileInfo(logName);
            if (System.IO.Directory.Exists(fi.DirectoryName))
                logFilename = fi.FullName;
            else
                logFilename = string.Format("{0}{2}{1}", Utility.GenericLogger.DefaultLogDirectory, logName, System.IO.Path.DirectorySeparatorChar);

            if (Utility.Utility.FileExists(logFilename))
                Utility.Utility.FileDelete(logFilename);
            _logger = new Utility.GenericLogger(logFilename);
        }
        public string Name { get; private set; }

        /// <summary>
        /// Dispose this Logger.
        /// </summary>
        public void Dispose()
        {
            if (_logger != null)
            {
                _logger.Dispose();
                _logger = null;
            }
        }

        public string Log(LogLevels logLevel, Exception exc, string messageFormat, params object[] messageArgs)
        {
            if (!IsLogLevelEnabled(logLevel)) return string.Empty;

            var msg = Logger.Format(exc, messageFormat, messageArgs);
            if (msg == null)
                throw new ArgumentNullException("messageFormat",
                                                "Log failed as there is no data to write, params are null.");
            var prependTime = string.Format("{0}:{1}:{2}", DateTime.Now, logLevel, msg);
            _logger.LogLine(prependTime);
            return msg;
        }

        public string Log(LogLevels logLevel, string messageFormat, params object[] messageArgs)
        {
            if (!IsLogLevelEnabled(logLevel)) return string.Empty;

            var msg = Logger.Format(messageFormat, messageArgs);
            if (msg == null)
                throw new ArgumentNullException("messageFormat",
                                                "Log failed as there is no data to write, params are null.");
            var prependTime = string.Format("{0}:{1}:{2}", DateTime.Now, logLevel, msg);
            _logger.LogLine(prependTime);
            return msg;
        }

        public LogLevels? LogLevel { get; set; }

        public bool IsLogLevelEnabled(LogLevels logLevel)
        {
            if (LogLevel == null) return false;
            return (int)LogLevel.Value <= (int)logLevel;
        }
    }
}
