using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Sop.Log
{
    public class TextLogger : ILogger
    {
        private Utility.GenericLogger _logger;
        public TextLogger(string logName)
        {
            Name = logName;
            string logFilename = string.Format("{0}\\{1}", Utility.GenericLogger.DefaultLogDirectory, logName);
            if (Utility.Utility.FileExists(logFilename))
                Utility.Utility.FileDelete(logFilename);
            _logger = new Utility.GenericLogger(logFilename);
        }
        public string Name { get; private set; }

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

        public bool IsLogLevelEnabled(LogLevels logLevel)
        {
            if (Logger.Instance.LogLevel == null) return false;
            return (int)Logger.Instance.LogLevel.Value <= (int)logLevel;
        }
    }
}
