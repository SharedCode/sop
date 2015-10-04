// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;
using System.IO;
using System.Reflection;

namespace Sop.Utility
{
    /// <summary>
    /// Generic logger is used by Transaction Manager in
    /// logging transaction activities.
    /// </summary>
    public class GenericLogger : IDisposable
    {
        /// <summary>
        /// Logger Constructor
        /// </summary>
        /// <param name="filename">Filename of the log file</param>
        public GenericLogger(string filename = null)
        {
            if (string.IsNullOrEmpty(filename) && !string.IsNullOrEmpty(DefaultLogFilename))
                filename = DefaultLogFilename = DefaultLogDirectory + System.IO.Path.DirectorySeparatorChar + "BTreeLog";

            LogFilename = filename;
            try
            {
                _fs = new FileStream(LogFilename,
                                              FileMode.OpenOrCreate,
                                              FileAccess.Write, //ReadWrite,
                                              FileShare.ReadWrite);
            }
            catch (Exception exc)
            {
                throw new Log.LogException(Sop.Log.Logger.Format(exc, "Can't create a log file for filename: {0}", LogFilename));
            }
            Writer = new StreamWriter(_fs);
        }

        /// <summary>
        /// Default Log Directory.
        /// </summary>
        public static string DefaultLogDirectory
        {
            get
            {
                if (_logDirectory == null)
                {
                    var s = Path.GetDirectoryName(Assembly.GetAssembly(typeof(GenericLogger)).CodeBase);
                    _logDirectory = new Uri(s).LocalPath;
                }
                return _logDirectory;
            }
            set
            {
                if (value != null)
                {
                    if (!System.IO.Directory.Exists(value))
                    {
                        try
                        {
                            // try to create default directory received...
                            System.IO.Directory.CreateDirectory(value);
                        }
                        catch
                        {
                            string s = string.Format("{0}{2}{1}", System.Environment.CurrentDirectory, value, System.IO.Path.DirectorySeparatorChar);
                            if (!System.IO.Directory.Exists(s))
                            {
                                try
                                {
                                    System.IO.Directory.CreateDirectory(s);
                                    // do nothing when successfully creating directory specified...
                                }
                                catch (Exception exc)
                                {
                                    throw new Sop.Log.LogException(string.Format("Error encountered while setting LogDirectory to {0}.", value), exc);
                                }
                            }
                        }
                    }
                }
                _logDirectory = value;
            }
        }
        private static string _logDirectory;

        /// <summary>
        /// Dispose the Logger
        /// </summary>
        public void Dispose()
        {
            if (Writer != null)
            {
                Writer.Dispose();
                _fs.Dispose();
                Writer = null;
                _fs = null;
            }
        }

        /// <summary>
        /// Close the log file
        /// </summary>
        public void Close()
        {
            Writer.Close();
            _fs.Close();
        }

        /// <summary>
        /// Log a text given Format and Arguments
        /// </summary>
        /// <param name="format"></param>
        /// <param name="args"></param>
        public void Log(string format, params object[] args)
        {
            string s;
            if (args != null && args.Length > 0)
                s = string.Format(format, args);
            else
                s = format;
            lock (_locker)
            {
                Writer.Write(s);
                Writer.Flush();
            }
        }


        /// <summary>
        /// Log an empty line
        /// </summary>
        public void LogLine()
        {
            LogLine("");
        }

        /// <summary>
        /// Log a line of text
        /// </summary>
        /// <param name="format"></param>
        /// <param name="args"></param>
        public void LogLine(string format, params object[] args)
        {
            string s;
            if (args != null && args.Length > 0)
                s = string.Format(format, args);
            else
                s = (format);
            lock (_locker)
            {
                Writer.WriteLine(s);
                Writer.Flush();
            }
        }

        internal static string DefaultLogFilename;

        /// <summary>
        /// Log filename
        /// </summary>
        public readonly string LogFilename;

        private readonly object _locker = new object();

        internal StreamWriter Writer;
        private FileStream _fs;
    }
}