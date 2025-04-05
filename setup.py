from setuptools import setup, find_packages

# This is a setup script for a Python package named "data-ingestion-framework".
# It uses setuptools to package the code and define its dependencies.
# The package is versioned at 0.1.0 and is authored by Junjie Wu.

setup(
    name='data-ingestion-framework',
    version='0.1.0',
    author='Junjie Wu',
    packages=find_packages(), 
    install_requires=[
        'pyspark', 
    ],
    entry_points={
        'console_scripts': [
            'data-ingestion-framework=src.main:main',
        ],
    },

)