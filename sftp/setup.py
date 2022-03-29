import os

from setuptools import setup, find_packages


package_root = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(package_root, 'VERSION')) as version_file:
    version = version_file.read().strip()

packages = [package for package in find_packages() if package.startswith('dataeng')]

setup(
    name='dataeng-utils-sftp',
    version=version,
    description=' Common SFTP Utilities library',
    packages=packages,
    install_requires=[
        "paramiko==2.10.1",
        "dataeng-utils-gcs >= 1.1.0, < 2.0dev"
    ]
)
