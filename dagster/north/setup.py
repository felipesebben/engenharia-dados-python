from setuptools import find_packages, setup

setup(
    name="north",
    packages=find_packages(exclude=["north_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
