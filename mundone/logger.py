#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

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
