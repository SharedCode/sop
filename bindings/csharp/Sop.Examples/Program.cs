using System;
using System.Collections.Generic;

namespace Sop.Examples;

class Program
{
    static void Main(string[] args)
    {
        Console.WriteLine("SOP C# Examples");
        Console.WriteLine("=================");
        Console.WriteLine("1. Basic B-Tree Operations (CRUD)");
        Console.WriteLine("2. Complex Keys & Index Specification");
        Console.WriteLine("3. Metadata 'Ride-on' Keys (High Performance)");
        Console.WriteLine("4. B-Tree Paging & Navigation");
        Console.WriteLine("5. Vector Search (AI/RAG Example)");
        Console.WriteLine("6. Model Store (Machine Learning)");
        Console.WriteLine("7. Logging Demo");
        Console.WriteLine("8. Batched B-Tree Operations");
        Console.WriteLine("9. Cassandra Initialization Demo");
        Console.WriteLine("10. Text Search Demo");
        Console.WriteLine("0. Exit");
        Console.WriteLine("=================");

        while (true)
        {
            Console.Write("\nEnter example number to run: ");
            var input = Console.ReadLine();

            if (input == "0") break;

            try
            {
                switch (input)
                {
                    case "1":
                        BtreeBasic.Run();
                        break;
                    case "2":
                        BtreeComplexKey.Run();
                        break;
                    case "3":
                        BtreeMetadata.Run();
                        break;
                    case "4":
                        BtreePaging.Run();
                        break;
                    case "5":
                        VectorSearchAI.Run();
                        break;
                    case "6":
                        ModelStoreSimple.Run();
                        break;
                    case "7":
                        LoggingDemo.Run();
                        break;
                    case "8":
                        BtreeBatched.Run();
                        break;
                    case "9":
                        CassandraDemo.Run();
                        break;
                    case "10":
                        TextSearchDemo.Run();
                        break;
                    default:
                        Console.WriteLine("Invalid option.");
                        break;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }
        }
    }
}
