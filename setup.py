# Copyright (c) 2024 Disintar LLP Licensed under the Apache License Version 2.0

import os
from setuptools import setup, find_packages

with open(f"README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open(f"requirements.txt", encoding="utf-8") as fh:
    install_requires = fh.read().split('\n')

setup(
    name="tonemuso",
    version="0.0.0.0.1a0",
    author="Disintar LLP",
    author_email="andrey@head-labs.com",
    description="Emulate TON blockchain",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/disintar/ton-emuso",
    project_urls={
        "Bug Tracker": "https://github.com/disintar/ton-emuso/issues",
    },
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: Implementation :: PyPy',
    ],
    setup_requires=install_requires,
    install_requires=install_requires,
    python_requires=">3.8,<3.12",
    packages=find_packages(
        where='src',  # '.' by default
    ),
    package_dir={
        "": "src",
    },
    entry_points={
        'console_scripts': [
            'tonemuso = tonemuso.main:main',
        ],
    },
    include_package_data=True
)
