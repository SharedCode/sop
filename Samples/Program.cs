using System;
using System.Collections.Generic;
using System.Text;

namespace Sop.Samples
{
    /// <summary>
    /// Main program for the SOP demo apps.
    /// Most or all of these demo apps generate SOP database file(s) within the build directory.
    /// Thus, when switching to run a demo app from another, it is wise to delete the SOP related 
    /// database files/folders on target build directory. This will ensure the new demo app
    /// you would want to run will run properly.
    /// 
    /// Feel free to read the code on these demo apps and learn some nifty tricks how to use SOP
    /// API. Two very useful demo apps are the "VirtualCache" and "VirtualCacheMemoryExtenderMultipleClients" apps.
    /// 
    /// "VirtualCache" app shows how to use the new SOP VirtualCache which is a .Net 4.5 ObjectCache
    /// implementation. This implementation uses SOP to virtualize the RAM & Disk in order to provide
    /// a high speed, lightweight caching solution. The application is very basic and showcase simple
    /// use of the .Net ObectCache interface for managing/accessing the cache.
    /// 
    /// "VirtualCacheMemoryExtenderMultipleClients" app is the multi-threaded version of the "VirtualCache". 
    /// i.e. - this spawns multiple threads simulating concurrent clients' access to a server piece which uses
    /// the SOP VirtualCache for caching each of the clients' submitted/requested data set.
    /// </summary>
	public class Program
	{
        enum DemoType
        {
            ObjectDirectory,
            ObjectDirectoryLINQ,
            PeopleDirectoryWithBigData,
            PeopleDirectoryXmlSerializableObject,
            PeopleDirectoryWithUpdateLargeDBLitePersistence,
            NestedSortedDictionary,
            PeopleDirectoryWithMediumDB,
            BayWind,
            Store400,
            MemoryExtender,
            MemoryExtenderGenerics,
            IterateDescendingOrder,
            ManageMultipleFiles,
            PeopleDirectoryAsIPersistent,
            PeopleDirectory,
            PeopleDirectoryWithUpdate,
            PeopleDirectoryWithBlobData,
            PeopleDirectoryWithBlobDataUpdate,
            PeopleDirectoryWithBlobDataDelete,
            PeopleDirectoryWithBlobDataNull,
            PeopleDirectoryWithBlobDataAddUniqueCheck,
            PeopleDirectoryWithBlobDataQueryFunc,
            PeopleDirectoryLargeDB,
            //RenameStore,
            RenameItemKeysOfStore,
            VirtualCacheMemoryCacheCompare,
            VirtualCacheDemo,
            PeopleDirectoryWithUpdateDelete,
            VirtualCacheThreaded,
            //VirtualCacheWithBackgroundRefreshDemo,
            VirtualCacheMemoryExtenderReCreate,
            VirtualCacheMemoryExtenderMultipleClients,
            ManyClientSimulator,
            OneHundredMillionInserts
        };
		public static void Main()
		{
            var demo = DemoType.OneHundredMillionInserts;
                //.PeopleDirectoryLargeDB;
                //.Store400;
                //.VirtualCacheMemoryExtenderMultipleClients;    //VirtualCacheMemoryExtenderReCreate;
            dynamic pd = null;
            switch(demo)
            {
                case DemoType.VirtualCacheMemoryExtenderMultipleClients:
                    pd = new VirtualCacheMemoryExtenderMultipleClients();
                    break;
                case DemoType.VirtualCacheThreaded:
                    pd = new VirtualCacheThreaded();
                    break;
                //case DemoType.VirtualCacheWithBackgroundRefreshDemo:
                //    pd = new VirtualCacheWithBackgroundRefreshDemo();
                //    break;
                case DemoType.VirtualCacheMemoryExtenderReCreate:
                    pd = new VirtualCacheMemoryExtenderReCreate();
                    break;
                case DemoType.PeopleDirectoryWithUpdateDelete:
                    pd = new PeopleDirectoryWithUpdateDelete();
                    break;
                case DemoType.VirtualCacheDemo:
                    pd = new VirtualCacheDemo();
                    break;
                case DemoType.VirtualCacheMemoryCacheCompare:
                    pd = new VirtualCacheMemoryCacheCompare();
                    break;
                case DemoType.RenameItemKeysOfStore:
                    pd = new RenameItemsOfStore();
                    break;
                case DemoType.ObjectDirectory:
                    pd = new ObjectDirectory();
                    break;
                case DemoType.ObjectDirectoryLINQ:
                    pd = new ObjectDirectoryLINQ();
                    break;
                case DemoType.PeopleDirectoryWithBigData:
                    pd = new PeopleDirectoryWithBigData();
                    break;
                //NOTE: Store rename is not supported in 4.7.
                //case DemoType.RenameStore:
                //    pd = new RenameStore();
                //    break;
                case DemoType.PeopleDirectoryXmlSerializableObject:
                    pd = new PeopleDirectoryXmlSerializableObject();
                    break;
                case DemoType.PeopleDirectoryWithBlobDataQueryFunc:
                    pd = new PeopleDirectoryWithBlobDataQueryFunc();
                    break;
                case DemoType.PeopleDirectoryWithBlobDataAddUniqueCheck:
                    pd = new PeopleDirectoryWithBlobDataAddUniqueCheck();
                    break;
                case DemoType.PeopleDirectoryWithBlobDataNull:
                    pd = new PeopleDirectoryWithBlobDataNull();
                    break;
                case DemoType.PeopleDirectoryWithUpdateLargeDBLitePersistence:
                    pd = new PeopleDirectoryWithUpdateLargeDBLitePersistence();
                    break;
                case DemoType.NestedSortedDictionary:
                    pd = new NestedSortedDictionary();
                    break;
                case DemoType.PeopleDirectoryWithMediumDB:
                    pd = new PeopleDirectoryWithMediumDB();
                    break;
                case DemoType.BayWind:
                    pd = new BayWind();
                    break;
                case DemoType.Store400:
                    pd = new Store400();
                    break;
                case DemoType.MemoryExtender:
                    pd = new MemoryExtender();
                    break;
                case DemoType.MemoryExtenderGenerics:
                    pd = new MemoryExtenderGenerics();
                    break;
                case DemoType.IterateDescendingOrder:
                    pd = new IterateDescendingOrder();
                    break;
                case DemoType.ManageMultipleFiles:
                    pd = new ManageMultipleFiles();
                    break;
                case DemoType.PeopleDirectoryAsIPersistent:
                    pd = new PeopleDirectoryAsIPersistent();
                    break;
                case DemoType.PeopleDirectory:
                    pd = new PeopleDirectory();
                    break;
                case DemoType.PeopleDirectoryWithUpdate:
                    pd = new PeopleDirectoryWithUpdate();
                    break;
                case DemoType.PeopleDirectoryWithBlobData:
                    pd = new PeopleDirectoryWithBlobData();
                    break;
                case DemoType.PeopleDirectoryWithBlobDataUpdate:
                    pd = new PeopleDirectoryWithBlobDataUpdate();
                    break;
                case DemoType.PeopleDirectoryWithBlobDataDelete:
                    pd = new PeopleDirectoryWithBlobDataDelete();
                    break;
                case DemoType.PeopleDirectoryLargeDB:
                    pd = new PeopleDirectoryLargeDB();
                    break;
                case DemoType.ManyClientSimulator:
                    pd = new ManyClientSimulator();
                    pd.DeleteDataFolder(ManyClientSimulator.ServerFilename);
                    // simulate numerous parallel clients accessing the same Store.
                    // this demonstrates multi-reader, single writer SOP Store feature.
                    pd.ThreadCount = 500;
                    pd.DataInsertionThreadCount = 150;
                    //pd.ThreadCount = 5;
                    //pd.DataInsertionThreadCount = 2;
                    pd.Threaded = true;
                    break;
                case DemoType.OneHundredMillionInserts:
                    pd = new OneHundredMillionInserts();
                    pd.DeleteDataFolder(OneHundredMillionInserts.ServerFilename);
                    pd.Insert = true;
                    pd.Run();
                    pd.Insert = false;
                    break;
            }
            pd.Run();

            Console.WriteLine("Press any key to exit the App.");
			Console.ReadLine();
		}
	}
}
