# -*- coding: utf-8 -*-

import shutil
import time

from .logger import logger
from .task import mktemp, Task


class Batch(object):
    def __init__(self, tasks):
        if not isinstance(tasks, (list, tuple)):
            raise TypeError("Workflow() arg 1 must be a list or a tuple")
        elif not tasks:
            raise ValueError("Workflow() arg 1 cannot be empty")
        elif not all([isinstance(task, Task) for task in tasks]):
            raise TypeError("Workflow() arg 1 expects a sequence of Task objects")

        self.tasks = tasks
        self.dir = mktemp(isdir=True)

    def start(self):
        for task in self.tasks:
            task.run(self.dir)
            logger.info("{} is running".format(task))

        return self

    def wait(self, secs=60):
        n_done = 0
        logged = [False] * len(self.tasks)

        while n_done < len(logged):
            time.sleep(secs)
            results = []

            for i, task in enumerate(self.tasks):
                if task.is_terminated():
                    results.append(task.output.read())

                    if not logged[i]:
                        logged[i] = True
                        n_done += 1

                        if task.is_success():
                            logger.info("'{}' has been completed".format(task))
                        else:
                            logger.error("'{}' has failed".format(task))

        return self

    def is_success(self):
        return all([t.is_success() for t in self.tasks])

    def clean(self):
        shutil.rmtree(self.dir)

    @property
    def output(self):
        return [t.output.read() for t in self.tasks]

    @property
    def stdout(self):
        return [t.stdout for t in self.tasks]

    @property
    def stderr(self):
        return [t.stderr for t in self.tasks]
