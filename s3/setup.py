import os

from setuptools import setup, find_packages


package_root = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(package_root, 'VERSION')) as version_file:
    version = version_file.read().strip()

packages = [package for package in find_packages() if package.startswith('dataeng')]

setup(
    name='dataeng-utils-s3',
    version=version,
    description=' Common S3 Utilities library',
    packages=packages,
    install_requires=[
        "paramiko==2.4.0",
        "boto3==1.9.57",
        "dataeng-utils-data-type >= 1.1.0, < 2.0dev"
    ],
    tests_require=[
        "dataeng-utils-aws >= 1.1.0, < 2.0dev",
        "dataeng-utils-logging >= 1.1.0, < 2.0dev"
    ]
)
