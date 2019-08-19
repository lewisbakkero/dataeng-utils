import os

from setuptools import setup, find_packages


package_root = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(package_root, 'VERSION')) as version_file:
    version = version_file.read().strip()

packages = [package for package in find_packages() if package.startswith('dataeng')]

setup(
    name='dataeng-utils-bigquery',
    version=version,
    description='Common BigQuery Utilities library',
    packages=packages,
    install_requires=[
        "google-cloud-bigquery==1.9.0"
    ]
)
