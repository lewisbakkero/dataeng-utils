import os

from setuptools import setup, find_packages


package_root = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(package_root, 'VERSION')) as version_file:
    version = version_file.read().strip()

packages = [package for package in find_packages() if package.startswith('dataeng')]

setup(
    name='dataeng-utils-aws',
    version=version,
    description='Common AWS Utilities library',
    packages=packages,
    install_requires=[
        "boto3==1.9.57"
    ],
    tests_require=[
        "dataeng-utils-logging >= 1.1.0, < 2.0dev"
    ]
)
