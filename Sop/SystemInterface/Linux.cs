

#if (_Linux)

using System;
using System.IO;
using System.Security;
using Mono.Unix.Native;
using System.Diagnostics;
using System.Collections.Generic;
using System.Text;

namespace Sop.SystemInterface
{

    /// <summary>
    /// System I/O interface methods.
    /// </summary>
    public class SystemInterface : Linux
    {
    }

    /// <summary>
    /// Linux System interface.
    /// </summary>
    public class Linux : ISystemInterface
    {
		private long _memorySize;
        public long GetMemorySize ()
		{
			if (_memorySize == 0)
			{
				using (var pc = new PerformanceCounter ("Mono Memory", "Total Physical Memory"))
				{
					_memorySize = pc.RawValue;
				}
			}
			return _memorySize;
        }

        /// <summary>
        /// Encoding defaults to UTF7 on Linux systems.
        /// </summary>
        public System.Text.Encoding DefaultEncoding
        {
            get
            {
                return System.Text.Encoding.UTF7;
            }
        }

		private int _maxStdio;
        public int GetMaxStdio ()
		{
			if (_maxStdio == 0) 
            {
                string output = RunExternalExe("cat", "/proc/sys/fs/file-max");
				int i;
				if (int.TryParse(output, out i))
    				_maxStdio = i;
				else
                    // should not occur, but just in case, set 800 max openable file streams...
					_maxStdio = 800;
			}
			return _maxStdio;
		}
        public int SetMaxStdio(int newMax)
        {
            return GetMaxStdio();
            //_setmaxstdio(newMax);
        }

        private string RunExternalExe(string filename, string arguments = null)
        {
            var process = new Process();
            process.StartInfo.FileName = filename;
            if (!string.IsNullOrEmpty(arguments))
            {
                process.StartInfo.Arguments = arguments;
            }
            process.StartInfo.CreateNoWindow = true;
            process.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;
            process.StartInfo.UseShellExecute = false;
            process.StartInfo.RedirectStandardError = true;
            process.StartInfo.RedirectStandardOutput = true;
            var stdOutput = new StringBuilder();
            process.OutputDataReceived += (sender, args) => stdOutput.Append(args.Data);
            string stdError = null;
            try
            {
                process.Start();
                process.BeginOutputReadLine();
                stdError = process.StandardError.ReadToEnd();
                process.WaitForExit();
            }
            catch (Exception e)
            {
                throw new Exception("OS error while executing " + Format(filename, arguments) + ": " + e.Message, e);
            }
            if (process.ExitCode == 0)
            {
                return stdOutput.ToString();
            }
            else
            {
                var message = new StringBuilder();
                if (!string.IsNullOrEmpty(stdError))
                {
                    message.AppendLine(stdError);
                }
                if (stdOutput.Length != 0)
                {
                    message.AppendLine("Std output:");
                    message.AppendLine(stdOutput.ToString());
                }
                throw new Exception(Format(filename, arguments) + " finished with exit code = " + process.ExitCode + ": " + message);
            }
        }

        private string Format(string filename, string arguments)
        {
            return "'" + filename + ((string.IsNullOrEmpty(arguments)) ? string.Empty : " " + arguments) + "'";
        }

        public int GetDriveSectorSize (string path)
		{
            // just return 512 for linux systems...
            // Linux UnbufferedOpen make get drive sector size use insignificant...
            return 512;
        }

        public FileStream UnbufferedOpen(string path, 
		                                 FileMode mode, 
		                                 FileAccess acc, 
		                                 FileShare share, 
		                                 bool sequential, 
		                                 bool async, 
		                                 int blockSize)
        {
			FileOptions options = FileOptions.WriteThrough; //.RandomAccess;
			if (async) options = FileOptions.Asynchronous;
			// NOTE: experiment using 4096 buffersize as shown in Jim Grey's Sequential & Performance File programming sample code...
			if (blockSize < 4096) blockSize = 4096;
			return new FileStream(path, mode, acc, share, blockSize, options);
        }
    }
}

#endif
