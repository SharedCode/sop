import os
import sys
import platform
import urllib.request
import subprocess
import stat
from pathlib import Path

# Try to import version from the package, fallback to latest if running standalone
try:
    from . import __version__
except ImportError:
    __version__ = "latest"

GITHUB_REPO = "sharedcode/sop"
BINARY_NAME = "sop-browser"

def get_platform_info():
    system = platform.system().lower()
    machine = platform.machine().lower()
    
    if system == "darwin":
        os_name = "darwin"
    elif system == "linux":
        os_name = "linux"
    elif system == "windows":
        os_name = "windows"
    else:
        raise RuntimeError(f"Unsupported operating system: {system}")

    if machine in ["x86_64", "amd64"]:
        arch = "amd64"
    elif machine in ["arm64", "aarch64"]:
        arch = "arm64"
    else:
        raise RuntimeError(f"Unsupported architecture: {machine}")
        
    return os_name, arch

def get_binary_path():
    home = Path.home()
    sop_dir = home / ".sop" / "bin"
    sop_dir.mkdir(parents=True, exist_ok=True)
    
    ext = ".exe" if platform.system() == "Windows" else ""
    return sop_dir / (BINARY_NAME + ext)

def download_binary(os_name, arch, target_path):
    # Use the package version to find the matching release
    # If version is 'latest', use the latest endpoint
    if __version__ == "latest":
        base_url = f"https://github.com/{GITHUB_REPO}/releases/latest/download"
    else:
        # Use a prefixed tag to distinguish from the main Go project releases (e.g. v5.2)
        tag = f"sop4py-v{__version__}"
        base_url = f"https://github.com/{GITHUB_REPO}/releases/download/{tag}"

    filename = f"{BINARY_NAME}-{os_name}-{arch}"
    if os_name == "windows":
        filename += ".exe"
        
    url = f"{base_url}/{filename}"
    
    print(f"Downloading SOP Data Browser ({__version__}) from {url}...")
    try:
        urllib.request.urlretrieve(url, target_path)
        print("Download complete.")
        
        # Make executable
        st = os.stat(target_path)
        os.chmod(target_path, st.st_mode | stat.S_IEXEC)
        
    except Exception as e:
        print(f"Error downloading binary: {e}")
        if target_path.exists():
            target_path.unlink()
        sys.exit(1)

def main():
    try:
        os_name, arch = get_platform_info()
        binary_path = get_binary_path()
        
        if not binary_path.exists():
            print(f"SOP Data Browser not found at {binary_path}")
            download_binary(os_name, arch, binary_path)
            
        print(f"Starting SOP Data Browser...")
        
        # Pass through any arguments
        args = [str(binary_path)] + sys.argv[1:]
        
        # Replace current process with the binary
        if platform.system() == "Windows":
            subprocess.run(args)
        else:
            os.execv(str(binary_path), args)
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
