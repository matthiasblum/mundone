#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from setuptools import setup, find_packages

from mundone import __version__


setup(
    name='mundone',
    description='Mundane task management',
    version=__version__,
    packages=find_packages()
)
