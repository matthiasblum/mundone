# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

from mundone import __version__


setup(
    name='mundone',
    description='Mundane task management',
    version=__version__,
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            "mundone-query = mundone.workflow:query_db",
        ]
    }
)
