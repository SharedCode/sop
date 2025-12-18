using System;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Sop.Server;

/// <summary>
/// Provides functionality to download and run the SOP HTTP Server (Data Management Console).
/// </summary>
public static class SopServer
{
    private const string GithubRepo = "sharedcode/sop";
    private const string BinaryName = "sop-httpserver";

    /// <summary>
    /// Downloads (if necessary) and runs the SOP HTTP Server.
    /// </summary>
    /// <param name="args">Command line arguments to pass to the server.</param>
    public static async Task RunAsync(string[] args)
    {
        try
        {
            var (osName, arch) = GetPlatformInfo();
            var binaryPath = GetBinaryPath();

            // Get version from the assembly
            var version = typeof(SopServer).Assembly.GetName().Version;
            var versionString = $"{version.Major}.{version.Minor}.{version.Build}";

            if (!File.Exists(binaryPath))
            {
                Console.WriteLine($"SOP Server binary not found at {binaryPath}");
                await DownloadBinary(versionString, osName, arch, binaryPath);
            }
            else
            {
                // Optional: Check if we should update? 
                // For now, we assume if it exists, it's fine. 
                // In a real scenario, we might check a version flag or hash.
            }

            Console.WriteLine($"Starting SOP Server (v{versionString})...");
            
            var psi = new ProcessStartInfo
            {
                FileName = binaryPath,
                UseShellExecute = false
            };

            if (args != null && args.Length > 0)
            {
                // netstandard2.0 compatible argument handling
                psi.Arguments = string.Join(" ", args);
            }
            
            using var process = Process.Start(psi);
            process?.WaitForExit();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
            throw;
        }
    }

    private static (string os, string arch) GetPlatformInfo()
    {
        string os = "";
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows)) os = "windows";
        else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux)) os = "linux";
        else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX)) os = "darwin";
        else throw new PlatformNotSupportedException("Unsupported OS");

        string arch = RuntimeInformation.ProcessArchitecture switch
        {
            Architecture.X64 => "amd64",
            Architecture.Arm64 => "arm64",
            _ => throw new PlatformNotSupportedException($"Unsupported Architecture: {RuntimeInformation.ProcessArchitecture}")
        };

        return (os, arch);
    }

    private static string GetBinaryPath()
    {
        var home = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        var sopDir = Path.Combine(home, ".sop", "bin");
        Directory.CreateDirectory(sopDir);

        var ext = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? ".exe" : "";
        return Path.Combine(sopDir, BinaryName + ext);
    }

    private static async Task DownloadBinary(string version, string os, string arch, string targetPath)
    {
        // Use the Sop4CS release tag format
        string tag = $"Sop4CS-v{version}"; 
        
        string baseUrl = $"https://github.com/{GithubRepo}/releases/download/{tag}";
        string filename = $"{BinaryName}-{os}-{arch}";
        if (os == "windows") filename += ".exe";

        string url = $"{baseUrl}/{filename}";

        Console.WriteLine($"Downloading SOP Data Management Console ({version}) from {url}...");

        using var client = new HttpClient();
        
        // Add a User-Agent header as GitHub API/Downloads often require it
        client.DefaultRequestHeaders.Add("User-Agent", "Sop4CS-Client");

        using var response = await client.GetAsync(url);
        response.EnsureSuccessStatusCode();

        using var fs = new FileStream(targetPath, FileMode.Create, FileAccess.Write, FileShare.None);
        await response.Content.CopyToAsync(fs);

        // Make executable on Unix-like systems
        if (os != "windows")
        {
            try 
            {
                // Use chmod for netstandard2.0 compatibility
                var chmod = new ProcessStartInfo
                {
                    FileName = "chmod",
                    Arguments = $"+x \"{targetPath}\"",
                    UseShellExecute = false,
                    CreateNoWindow = true
                };
                Process.Start(chmod)?.WaitForExit();
            }
            catch
            {
                // Ignore errors if chmod is not available
            }
        }
        
        Console.WriteLine("Download complete.");
    }
}
