using System;
using System.Threading.Tasks;
using Sop.Server;

namespace Sop.HttpServer;

class Program
{
    static async Task Main(string[] args)
    {
        await SopServer.RunAsync(args);
    }
}
