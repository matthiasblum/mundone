# -*- coding: utf-8 -*-

__version_info__ = (0, 4, 11)
__version__ = '.'.join(map(str, __version_info__))

from .pool import Pool
from .task import Task, as_completed
from .workflow import Workflow
