// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;

namespace Sop.Utility
{
    /// <summary>
    /// Utility contains standalone unclassified methods providing
    /// useful functionalities. Currently contains mostly File I/O
    /// shortcut/wrapper functions.
    /// </summary>
    internal class Utility
    {
        public static bool HasRequiredDirectoryAccess(string filename)
        {
            try
            {
                System.IO.FileInfo fi = new System.IO.FileInfo(filename);

                if (!System.IO.Directory.Exists(fi.DirectoryName))
                    System.IO.Directory.CreateDirectory(fi.DirectoryName);
                string tempFile;
                int i = 0;
                do
                {
                    tempFile = string.Format("{0}{3}{1}{2}", fi.DirectoryName, i++, fi.Name, System.IO.Path.DirectorySeparatorChar);
                } while (System.IO.File.Exists(tempFile));
                System.IO.FileStream fs = new System.IO.FileStream(tempFile, System.IO.FileMode.Create,
                                                                   System.IO.FileAccess.ReadWrite);
                fs.Dispose();
                FileDelete(tempFile);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        public static bool HasRequiredAccess(string filename)
        {
            if (!System.IO.File.Exists(filename))
                return true;
            try
            {
                using (System.IO.FileStream fs = new System.IO.FileStream(filename, System.IO.FileMode.Open,
                                                                          System.IO.FileAccess.ReadWrite))
                {
                    return true;
                }
            }
            catch (Exception)
            {
                return false;
            }
        }

        public static bool FileExists(string filename)
        {
            return System.IO.File.Exists(filename);
        }

        public static bool FileExists(IObjectServer server, string filename)
        {
            string s = ((OnDisk.ObjectServer) server).NormalizePath(filename);
            return System.IO.File.Exists(s);
        }

        public static bool FileExists(string serverRootPath, string filename)
        {
            string s = OnDisk.ObjectServer.NormalizePath(serverRootPath, filename);
            return System.IO.File.Exists(s);
        }

        public static void FileMove(string source, string target)
        {
            System.IO.File.Move(source, target);
        }

        public static void FileDelete(string filename)
        {
            if (System.IO.File.Exists(filename))
                System.IO.File.Delete(filename);
        }
    }
}