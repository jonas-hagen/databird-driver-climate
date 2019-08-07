#!/usr/bin/env python
import setuptools

VERSION = "0.1.2"

with open("README.md", "r") as fh:
    fh.readline()  # do not include ribbons
    long_description = fh.read()

setuptools.setup(
    author="Jonas Hagen",
    author_email="jonas.hagen@iap.unibe.ch",
    classifiers=["Operating System :: OS Independent"],
    description="Data sources for climate and atmospheric research.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="MIT",
    maintainer="Jonas Hagen",
    maintainer_email="jonas.hagen@iap.unibe.ch",
    name="databird-driver-climate",
    packages=["databird_drivers.climate"],
    python_requires=">=3.5.*, <4",
    version=VERSION,
    url="https://github.com/jonas-hagen/databird-driver-climate",
    install_requires=[
        "databird",
        "cdsapi",
        "ecmwf-api-client",
        "pydap",
        "lxml",
        "xarray",
        "netcdf4",
    ],
)
