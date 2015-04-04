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
				Server.FileSet.AutoDisposeItem = true;
				for (int i = 0; i < 50; i++)
				{
					string s = string.Format("File{0}", i);
					IFile f = Server.FileSet.Add(s);
					f.Store.Add("Foo", "Bar");
					f.Flush();
					//** Dispose file to offload from memory
					f.Dispose();
					if (i > 10)
					{
						//** now, delete the File 5 Files ago...
						s = string.Format("File{0}", i - 5);
						f = Server.FileSet[s];
						if (f != null)
						{
							//** dispose from memory
							f.Dispose();
							//** remove from FileSet
							Server.FileSet.Remove(s);
							//** delete the physical file from File System
							//** NOTE: deleting file physically isn't a rollback-able action, once done it's done
							//** Transaction Rollback will not restore it.
							System.IO.File.Delete(f.Filename);
						}
					}
				}
			}
			else
			{
				//** iterate thru all Files in Server.FileSet
				foreach (IFile f in Server.FileSet)
				{
					f.Store.MoveFirst();
					Console.WriteLine(f.Store.CurrentKey as string + f.Store.CurrentValue as string);
				}
			}
			Server.SystemFile.Store.Transaction.Commit();
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
