__version_info__ = (0, 6, 1)
__version__ = '.'.join(map(str, __version_info__))

from .pool import Pool
from . import states
from .task import Task, as_completed
from .workflow import Workflow
