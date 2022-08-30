from setuptools import setup, find_packages

setup(
    name='opera_sds_lambdas',
    version='0.0.1',
    packages=find_packages(),
    install_requires=[
        "pytest>=7.1.1",
        "pytest-mock>=3.8.2",
        "pytest-asyncio>=0.18.3",
        "pytest-cov>=3.0.0"
    ]
)
