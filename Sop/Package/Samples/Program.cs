using System;
using System.Collections.Generic;
using System.Text;

namespace Sop.Samples
{
	public class Program
	{
		public static void Main()
		{
            PeopleDirectoryWithUpdateLargeDBLitePersistence largeDb = new PeopleDirectoryWithUpdateLargeDBLitePersistence();
            largeDb.Run();

			#region Running Other Samples
            //PeopleDirectoryWithMediumDB mdb = new PeopleDirectoryWithMediumDB();
            //mdb.Run();

            //BayWind bw = new BayWind();
            //bw.Run();
            //return;
			//Collection400 c400 = new Collection400();
			//c400.Run();

			//NestedSortedDictionary nsd = new NestedSortedDictionary();
			//nsd.Run();

			//VirtualCache.Run();
			//VirtualCacheGenerics.Run();

			//IterateDescendingOrder ido = new IterateDescendingOrder();
			//ido.Run();
            //PeopleDirectoryAsIPersistent pd = new PeopleDirectoryAsIPersistent();
            //pd.Run();
            #endregion

            Console.WriteLine("Press any key to exit the App.");
			Console.ReadLine();
		}
	}
}
