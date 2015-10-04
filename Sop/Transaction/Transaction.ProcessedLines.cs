// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.IO;

namespace Sop.Transaction
{
    using OnDisk;

    internal class ProcessedLinesHandler : IDisposable
    {
        public ProcessedLinesHandler(string filename)
        {
            if (Sop.Utility.Utility.FileExists(filename))
            {
                var sr = new System.IO.StreamReader(filename);
                using (sr)
                {
                    if (!sr.EndOfStream)
                    {
                        string line = sr.ReadLine();
                        if (int.TryParse(line, out _processedLines))
                        {
                            if (sr.EndOfStream)
                                _processedLines--;
                            else
                            {
                                line = sr.ReadLine();
                                if (line != "Success")
                                    _processedLines--;
                            }
                        }
                    }
                }
            }
            _fs = new System.IO.FileStream(filename, System.IO.FileMode.OpenOrCreate);
            this._filename = filename;
            _sw = new System.IO.StreamWriter(_fs);
            _sw.AutoFlush = true;
        }

        private readonly string _filename;

        public void Dispose()
        {
            if (_fs != null)
            {
                _sw.Dispose();
                _sw = null;
                _fs.Dispose();
                _fs = null;
                try
                {
                    System.IO.File.Delete(_filename);
                }
                catch
                {
                } //** ignore if can't remove the processed lines handler internal file as it is internal use only,
                //** it is a must for the transaction to complete, this is ignorable (red herring if occurred)...
            }
        }

        public void MarkLineSuccess()
        {
            _sw.WriteLine("Success");
        }

        private System.IO.FileStream _fs;
        private System.IO.StreamWriter _sw;

        public int ProcessedLines
        {
            get { return _processedLines; }
            set
            {
                _processedLines = value;
                _sw.BaseStream.Seek(0, System.IO.SeekOrigin.Begin);
                _sw.WriteLine(_processedLines.ToString(CultureInfo.InvariantCulture));
                long l = _sw.BaseStream.Position;
                _sw.WriteLine("       ");
                _sw.BaseStream.Seek(l, System.IO.SeekOrigin.Begin);
            }
        }

        private int _processedLines = 0;
    }
}