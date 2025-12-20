import os
import re
from setuptools import setup, find_packages

# Read version from pyproject.toml
with open("pyproject.toml", "r") as f:
    content = f.read()
    match = re.search(r'version\s*=\s*"([^"]+)"', content)
    if match:
        version = match.group(1)
    else:
        raise RuntimeError("Could not find version in pyproject.toml")

# Map build platform to the specific library file
PLATFORM_LIB_MAP = {
    "macosx_10_9_x86_64": "libjsondb_amd64darwin.dylib",
    "macosx_11_0_arm64": "libjsondb_arm64darwin.dylib",
    "manylinux_2_17_x86_64": "libjsondb_amd64linux.so",
    "manylinux_2_17_aarch64": "libjsondb_arm64linux.so",
    "win_amd64": "libjsondb_amd64windows.dll",
}

# Get the target platform from env var, or default to None (sdist)
target_platform = os.environ.get("SOP_BUILD_PLATFORM")

package_data = {"sop": ["ai/README.md", "examples/*.md"]}

if target_platform:
    if target_platform in PLATFORM_LIB_MAP:
        lib_file = PLATFORM_LIB_MAP[target_platform]
        package_data["sop"].append(lib_file)
    else:
        print(f"Warning: Unknown platform {target_platform}. No binary included.")
else:
    # If no platform specified (e.g. sdist), include ALL binaries
    # This ensures the source distribution is complete
    package_data["sop"].extend(PLATFORM_LIB_MAP.values())
    # Also include headers
    package_data["sop"].extend([
        "libjsondb_amd64darwin.h",
        "libjsondb_arm64darwin.h",
        "libjsondb_amd64linux.h",
        "libjsondb_arm64linux.h",
        "libjsondb_amd64windows.h",
    ])

# Read README.md for long description
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="sop4py",
    version=version,
    description="Scalable Objects Persistence (SOP) V2 for Python. General Public Availability (GPA) Release",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Gerardo Recinto",
    author_email="gerardorecinto@yahoo.com",
    url="https://pypi.org/project/sop-python-beta-3",
    packages=find_packages(),
    package_data=package_data,
    include_package_data=False, # We are managing it manually
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    entry_points={
        "console_scripts": [
            "sop-httpserver=sop.httpserver:main",
            "sop-demo=sop.demo:main",
        ],
    },
)
