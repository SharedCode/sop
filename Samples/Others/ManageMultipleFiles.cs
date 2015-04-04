using System;

namespace Sop.Samples
{
	public class ManageMultipleFiles
	{
		/// <summary>
		/// Demo to show how to manage multiple Files of a given ObjectServer
		/// </summary>
		public void Run()
		{
			Console.WriteLine("{0}: ManageMultipleFiles demo started...", DateTime.Now);

			if (Server.FileSet.Count == 0)
			{
                //Log.Logger.Instance.LogLevel = Log.LogLevels.Verbose;

				Server.FileSet.AutoDisposeItem = true;
				for (int i = 0; i < 20; i++)
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
				//** iterate thru all Files in Server.FileSet
				foreach (IFile f in Server.FileSet)
				{
					f.Store.MoveFirst();
                    while (true)
                    {
                        if (f.Store.EndOfTree())
                            break;
                        Console.WriteLine(f.Store.CurrentKey as string + f.Store.CurrentValue as string);
                        f.Store.MoveNext();
                    }
				}
			}
            Server.Commit();
			Console.WriteLine("{0}: ManageMultipleFiles demo ended...", DateTime.Now);
		}

		Sop.IObjectServer Server
		{
			get
			{
				string ServerFilename = "SopBin\\OServer.dta";
				if (server == null)
					server = Sop.ObjectServer.OpenWithTransaction(ServerFilename);
				return server;
			}
		}
		Sop.IObjectServer server;
	}
}
