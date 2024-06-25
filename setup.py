from setuptools import find_packages, setup

setup(
    name="data_ingestion",
    packages=find_packages(exclude=["data_ingestion_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "polars"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
