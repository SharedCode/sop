from sop import __version__
from setuptools import setup, find_packages

setup(
    name='sop',
    version=__version__,
    packages=['sop'],
    package_data={
        'sop': ['libjsondb_amd64darwin.dylib'], # Relative path within your package
    },
    include_package_data=True, # Important to ensure package_data is included
)
