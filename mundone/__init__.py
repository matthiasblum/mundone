__version_info__ = (0, 5, 0)
__version__ = '.'.join(map(str, __version_info__))

from .pool import Pool
from . import statuses
from .task import Task, as_completed
from .workflow import Workflow
