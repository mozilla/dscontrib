#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import datetime

from setuptools import find_packages, setup


setup(
    name="dscontrib",
    version=datetime.datetime.now().strftime("%Y%m%d%H%M%S"),
    author="Rob Hudson",
    author_email="robhudson@mozilla.com",
    description="A Python library for Mozilla Data Science code snippets",
    url="https://github.com/mozilla/dscontrib",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "numpy",
        "pandas",
        "scipy"
    ],
)
