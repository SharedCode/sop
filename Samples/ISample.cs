using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sop.Samples
{
    public interface ISample
    {
        void DeleteDataFolder(string serverFilename);
    }
    public abstract class Sample : ISample
    {
        public void DeleteDataFolder(string serverFilename)
        {
            var folderPath = Sop.ObjectServer.GetFullFolderPath(serverFilename);
            if (System.IO.Directory.Exists(folderPath))
                System.IO.Directory.Delete(folderPath, true);
        }

        public static string GetRootPath(string serverSystemFilename)
        {
            if (string.IsNullOrEmpty(serverSystemFilename))
                return string.Empty;
            var path = System.IO.Path.GetDirectoryName(serverSystemFilename);
            if (path == string.Empty)
                path = System.Environment.CurrentDirectory;
            path += System.IO.Path.DirectorySeparatorChar;
            return path;
        }
    }
}
