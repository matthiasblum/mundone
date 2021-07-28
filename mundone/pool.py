# -*- coding: utf-8 -*-

import threading
from queue import Queue, Empty
from typing import List

from .task import Task

_TASK_REQ = "--task--"
_PING_REQ = "--ping--"
_STOP_REQ = "--stop--"
_KILL_REQ = "--kill--"
_OVER_RES = "--over--"


def _manager(src: Queue, dst: Queue, max_running: int, workdir: str):
    pending = []
    running = []
    sentinel = None
    while True:
        try:
            action, task = src.get(block=True, timeout=15)
        except Empty:
            pass
        else:
            if action in (_STOP_REQ, _KILL_REQ):
                sentinel = action
                break
            elif action == _TASK_REQ:
                if len(running) < max_running:
                    task.start(dir=workdir)
                    running.append(task)
                    continue

                pending.append(task)

            t = _monitor(running, pending, max_running, workdir)
            pending, running, done = t
            while done:
                dst.put(done.pop(0))

            if action == _PING_REQ:
                dst.put(_OVER_RES)

    if sentinel == _KILL_REQ:
        for task in pending + running:
            task.terminate()
    else:
        while pending or running:
            t = _monitor(running, pending, max_running, workdir)
            pending, running, done = t
            while done:
                dst.put(done.pop(0))

    dst.put(_OVER_RES)


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
        self.terminate()

    def __del__(self):
        try:
            self.terminate()
        except Exception:
            pass

    def submit(self, task: Task):
        self._queue_in.put((_TASK_REQ, task))

    def as_completed(self, close: bool = True):
        if close:
            self._queue_in.put((_STOP_REQ, None))
        else:
            self._queue_in.put((_PING_REQ, None))

        for task in iter(self._queue_out.get, _OVER_RES):
            yield task

        if close:
            self._queue_in = None

    def terminate(self):
        if self._queue_in is not None:
            self._queue_in.put((_KILL_REQ, None))
            self._queue_in = None

            for _ in iter(self._queue_out.get, _OVER_RES):
                pass
