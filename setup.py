#!/usr/bin/env python

import io

from setuptools import setup, find_packages

with io.open('README.rst') as f:
    readme = f.read()

setup(
    name='sqlakeyset',
    version='0.1.1579837191',
    url='https://github.com/djrobstep/sqlakeyset',
    description='offset-free paging for sqlalchemy',
    long_description=readme,
    author='Robert Lechte',
    author_email='robertlechte@gmail.com',
    install_requires=[
        'sqlalchemy',
        'python-dateutil'
    ],
    zip_safe=False,
    packages=find_packages(),
    classifiers=[
    ]
)
