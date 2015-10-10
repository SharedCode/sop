using System;

namespace Sop.Samples
{
	public class ManageMultipleFiles : Sample
	{
		/// <summary>
		/// Demo to show how to manage multiple Files of a given ObjectServer
		/// </summary>
		public void Run()
		{
			Console.WriteLine("{0}: ManageMultipleFiles demo started...", DateTime.Now);
            const int FileCount = 20;
			if (Server.FileSet.Count == 0)
			{
                //Log.Logger.Instance.LogLevel = Log.LogLevels.Verbose;

				Server.FileSet.AutoDisposeItem = true;
				for (int i = 0; i < FileCount; i++)
				{
					string s = string.Format("File{0}", i);
					IFile f = Server.FileSet.Add(s);
					f.Store.Add("Foo2" + i, "Bar2");
                    f.Store.Add("Foot" + i, "Bart");
                    // Dispose auto flushes the Store..
                    f.Dispose();
                }
			}
			else
			{
                int i = 0;
				//** iterate thru all Files in Server.FileSet
				foreach (IFile f in Server.FileSet)
				{
                    i++;
                    if (f.Store.Count != 2)
                        throw new Exception(string.Format("Failed, Store has {0} members, expected two.", f.Store.Count));
					f.Store.MoveFirst();
                    while (true)
                    {
                        if (f.Store.EndOfTree())
                            break;
                        Console.WriteLine(f.Store.CurrentKey as string + f.Store.CurrentValue as string);
                        f.Store.MoveNext();
                    }
				}
                if (i != FileCount)
                {
                    throw new Exception(string.Format("Failed, File Count {0}, expected {1}.", i, FileCount));
                }
            }
            Server.Commit();
            Server.Dispose();
            server = null;
			Console.WriteLine("{0}: ManageMultipleFiles demo ended...", DateTime.Now);
		}

		Sop.IObjectServer Server
		{
			get
			{
				if (server == null)
					server = Sop.ObjectServer.OpenWithTransaction(ServerFilename);
				return server;
			}
		}

        public const string ServerFilename = "SopBin\\OServer.dta";

        Sop.IObjectServer server;
	}
}
