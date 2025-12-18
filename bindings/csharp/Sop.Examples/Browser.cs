using System;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Sop.Examples;

public static class Browser
{
    private const string GithubRepo = "sharedcode/sop";
    private const string BinaryName = "sop-browser";
    // TODO: Sync this version with the build system
    private const string Version = "2.0.40"; 

    public static async Task Run()
    {
        try
        {
            var (osName, arch) = GetPlatformInfo();
            var binaryPath = GetBinaryPath();

            if (!File.Exists(binaryPath))
            {
                Console.WriteLine($"SOP Data Browser not found at {binaryPath}");
                await DownloadBinary(osName, arch, binaryPath);
            }

            Console.WriteLine("Starting SOP Data Browser...");
            
            var psi = new ProcessStartInfo
            {
                FileName = binaryPath,
                UseShellExecute = false
            };
            
            using var process = Process.Start(psi);
            process?.WaitForExit();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
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

    private static async Task DownloadBinary(string os, string arch, string targetPath)
    {
        // Use the Sop4CS release tag
        string tag = $"Sop4CS-v{Version}"; 
        
        string baseUrl = $"https://github.com/{GithubRepo}/releases/download/{tag}";
        string filename = $"{BinaryName}-{os}-{arch}";
        if (os == "windows") filename += ".exe";

        string url = $"{baseUrl}/{filename}";

        Console.WriteLine($"Downloading SOP Data Browser ({Version}) from {url}...");

        using var client = new HttpClient();
        var response = await client.GetAsync(url);
        response.EnsureSuccessStatusCode();

        using var fs = new FileStream(targetPath, FileMode.Create);
        await response.Content.CopyToAsync(fs);
        
        if (os != "windows")
        {
            try
            {
                Process.Start("chmod", $"+x {targetPath}")?.WaitForExit();
            }
            catch { /* ignore */ }
        }
        
        Console.WriteLine("Download complete.");
    }
}
