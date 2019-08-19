import os

from setuptools import setup, find_packages


package_root = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(package_root, 'VERSION')) as version_file:
    version = version_file.read().strip()

packages = [package for package in find_packages() if package.startswith('dataeng')]

setup(
    name='dataeng-utils-mysql',
    version=version,
    description=' Common MySQL Utilities library',
    packages=packages,
    install_requires=[
        "mysql_connector_python==8.0.13"
    ]
)
