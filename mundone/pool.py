# -*- coding: utf-8 -*-

import threading
from queue import Queue, Empty
from typing import List

from .task import Task


def _manager(src: Queue, dst: Queue, max_running: int, workdir: str):
    pending = []
    running = []

    while True:
        try:
            task = src.get(block=True, timeout=15)
        except Empty:
            pass
        else:
            if task is None:
                break

            if len(running) < max_running:
                task.start(dir=workdir)
                running.append(task)
                continue

            pending.append(task)
            t = _monitor(running, pending, max_running, workdir)
            pending, running, done = t
            while done:
                dst.put(done.pop(0))

    while pending or running:
        t = _monitor(running, pending, max_running, workdir)
        pending, running, done = t
        while done:
            dst.put(done.pop(0))

    dst.put(None)


def _monitor(running: List[Task], pending: List[Task], max_running: int,
             workdir: str):
    _running = []
    done = []
    for task in running:
        task.poll()

        if task.running():
            _running.append(task)
        else:  # Assume 'done'
            done.append(task)

    running = _running
    while len(running) < max_running:
        try:
            task = pending.pop(0)
        except IndexError:
            break
        else:
            task.start(dir=workdir)
            running.append(task)

    return pending, running, done


class Pool:
    def __init__(self, path: str, max_workers: int):
        self._dir = path
        self._max_workers = max_workers
        self._queue_in = Queue()
        self._queue_out = Queue()

        self._t = threading.Thread(target=_manager,
                                   args=(self._queue_in, self._queue_out,
                                         self._max_workers, self._dir))
        self._t.start()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def submit(self, task: Task):
        self._queue_in.put(task)

    def as_completed(self):
        self._queue_in.put(None)
        for task in iter(self._queue_out.get, None):
            yield task
