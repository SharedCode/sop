using System;
using System.Text;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Sop.Samples;

namespace SopClientTests
{
    /// <summary>
    /// Summary description for UnitTest1
    /// </summary>
    [TestClass]
    public class AcceptanceTests
    {
        [TestMethod]
        public void PeopleDirectoryWithUpdateDeleteScenarioTest()
        {
            var pd = new PeopleDirectoryWithUpdateDelete();
            // Populate will Insert, Update, Delete people directory records.
            pd.Run();
            // Read All records.
            pd.Run();
            // Delete SOP data folder now that we're done.
            pd.DeleteDataFolder(PeopleDirectoryWithUpdateDelete.ServerFilename);
        }
        [TestMethod]
        public void PeopleDirectoryXmlSerializableObjectScenarioTest()
        {
            var pd = new PeopleDirectoryXmlSerializableObject();
            // Populate will Insert, Update, Delete people directory records.
            pd.Run();
            // Read All records.
            pd.Run();
            // Delete SOP data folder now that we're done.
            pd.DeleteDataFolder(PeopleDirectoryXmlSerializableObject.ServerFilename);
        }
        [TestMethod]
        public void ManageMultipleFilesScenarioTest()
        {
            var pd = new ManageMultipleFiles();
            // Populate will Insert, Update, Delete people directory records.
            pd.Run();
            // Read All records.
            pd.Run();
            // Delete SOP data folder now that we're done.
            pd.DeleteDataFolder(ManageMultipleFiles.ServerFilename);
        }

        [TestMethod]
        public void Store400ScenarioTest()
        {
            var pd = new Store400();
            // Populate will Insert, Update, Delete people directory records.
            pd.Run();
            // Read All records.
            pd.Run();
            // Delete SOP data folder now that we're done.
            pd.DeleteDataFolder(Store400.ServerFilename);
        }
    }
}
