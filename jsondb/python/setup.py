from sop import __version__
from setuptools import setup

setup(
    name='sop',
    version=__version__,
    packages=['sop'],
    package_data={
        'sop': ['libjsondb_amd64darwin.dylib', 'libjsondb_amd64darwin.h'], # Relative path within your package
    },
    include_package_data=True, # Important to ensure package_data is included
)

# Sample cmdline to build wheel:
# python3 setup.py sdist bdist_wheel
