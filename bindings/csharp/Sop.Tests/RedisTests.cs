using System;
using Xunit;

namespace Sop.Tests;

public class RedisTests
{
    [Fact]
    public void TestRedisInitialization()
    {
        // Assuming a local redis is running, or just testing the binding call.
        // If no redis is running, this might fail or log a warning depending on Go implementation.
        
        try
        {
            Redis.Initialize("redis://localhost:6379/0");
            // If successful, great.
        }
        catch (SopException e)
        {
            // If it fails due to connection refused, that's expected if no redis is running.
            if (!e.Message.Contains("connection refused") && !e.Message.Contains("connect: connection refused"))
            {
                // If it's another error, re-throw
                throw;
            }
        }

        try
        {
            Redis.Close();
        }
        catch (SopException e)
        {
             // Ignore close errors if init failed
             if (!e.Message.Contains("connection refused"))
             {
                 throw;
             }
        }
    }
}
