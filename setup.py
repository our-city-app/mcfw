# -*- coding: utf-8 -*-
# !/usr/bin/env python

import os

from setuptools import setup

import fvfw


def path(p):
    return os.path.join(os.path.abspath(os.path.dirname(__file__)), p)


requirements = open('requirements.txt').read().splitlines()

setup(
    name='fvfw',
    version=fvfw.__version__,
    description='Internal framework for easy caching and data serialization in google appengine projects',
    classifiers=[
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Environment :: Web Environment',
        'Operating System :: OS Independent',
        'Topic :: Software Development :: Libraries',
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
    ],
    keywords=[],
    author='Fairville NV',
    author_email='',
    url='https://github.com/our-city-apps/fvfw',
    license='Apache 2.0',
    packages=['fvfw'],
    install_requires=requirements,
)
