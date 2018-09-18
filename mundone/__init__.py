#!/usr/bin/env python
# -*- coding: utf-8 -*-

__version_info__ = (0, 2, 2)
__version__ = '.'.join(map(str, __version_info__))

import logging

from .task import Task
from .workflow import Workflow

logger = logging.getLogger('mundone')
logger.setLevel(logging.INFO)
_ch = logging.StreamHandler()
_ch.setFormatter(
    logging.Formatter(
        fmt='%(asctime)s: %(message)s',
        datefmt='%y-%m-%d %H:%M:%S'
    )
)
logger.addHandler(_ch)

