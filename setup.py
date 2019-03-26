#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from setuptools import setup, find_packages


setup(
    name='dscontrib',
    use_scm_version=True,
    author='Rob Hudson',
    author_email='robhudson@mozilla.com',
    description='A Python library for Mozilla Data Science code snippets',
    url='https://github.com/mozilla/dscontrib',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
        'numpy',
        'pandas',
        'py4j',
        'pyarrow>=0.8.0',
        'pyspark',
        'scipy',
    ],
    setup_requires=[
        'setuptools-scm',
    ]
)
