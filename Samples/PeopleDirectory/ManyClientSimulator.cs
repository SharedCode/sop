using System;
using System.Collections.Generic;

namespace Sop.Samples
{
	public class ManyClientSimulator : Sample
    {
		#region Record definitions & key comparers
		public class PersonKey
		{
			public string FirstName;
			public string LastName;
		}
		public class Person
		{
			public PersonKey Key;
			public int PersonID;
			public string PhoneNumber;
		}
		public class PersonComparer : IComparer<PersonKey>
		{
			public int Compare(PersonKey x, PersonKey y)
			{
				int i = string.Compare(x.FirstName, y.FirstName);
				if (i == 0)
					i = string.Compare(x.LastName, y.LastName);
				return i;
			}
		}
		#endregion

        /// <summary>
		/// Sample code for managing "high volume (5,000,000: 2.5 mil people & 2.5 mil people names)"
		/// in a very decent amount of time... Inserts: around 20 minutes on a fairly equipped 2 yr old laptop.
		/// </summary>
		public void Run()
        {
            using (var Server = new ObjectServer(ServerFilename, true))
            {
                IStoreFactory sf = new StoreFactory();

            }
        }

        private void AddItems(IObjectServer server, IStoreFactory sf)
        {
            var PeopleStore = sf.Get<long, Person>(server.SystemFile.Store, "People");

        }

        public const string ServerFilename = "SopBin\\OServer.dta";
	}
}
