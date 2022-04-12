from threading import Thread
from queue import Queue, Empty

from .task import Task

_TASK_REQ = "--task--"
_PING_REQ = "--ping--"
_WAIT_REQ = "--wait--"
_KILL_REQ = "--kill--"
_OVER_RES = "--over--"


def _worker(src: Queue, dst: Queue):
    for task in iter(src.get, None):
        task.poll()
        dst.put(task)


def _manager(main_req: Queue, main_res: Queue, sec_req: Queue, sec_res: Queue,
             num_workers: int, max_running: int, workdir: str):
    pending = []
    running = []
    notify_when_done = False
    while True:
        try:
            action, task = main_req.get(block=True, timeout=15)
        except Empty:
            action = task = None

        if action == _TASK_REQ:
            if task.running():
                # Passed task is already running
                running.append(task)
            elif task.done():
                # Passed task is already done: send it back
                main_res.put(task)
            elif len(running) < max_running:
                # Pool not full: start task right away
                task.start(dir=workdir)
                running.append(task)
            else:
                # Pool full: task needs to wait
                pending.append(task)

            continue
        elif action == _WAIT_REQ:
            notify_when_done = True
        elif action == _KILL_REQ:
            for task in running + pending:
                task.terminate()

            break

        # Update running/pending/finished tasks
        for task in running:
            sec_req.put(task)

        tmp_running = []
        for _ in range(len(running)):
            task = sec_res.get()

            if task.running():
                tmp_running.append(task)
            else:
                main_res.put(task)

                # Replace by pending task
                if pending:
                    task = pending.pop(0)
                    task.start(dir=workdir)
                    tmp_running.append(task)

        running = tmp_running
        while pending and len(running) < max_running:
            task = pending.pop(0)
            task.start(dir=workdir)
            running.append(task)

        if action == _PING_REQ:
            main_res.put(_OVER_RES)
        elif notify_when_done and not pending and not running:
            # No more running/waiting tasks
            main_res.put(_OVER_RES)
            notify_when_done = False

    for _ in range(num_workers):
        sec_req.put(None)

    main_res.put(_OVER_RES)


class Pool:
    def __init__(self, path: str, max_running: int, kill_on_exit: bool = True,
                 threads: int = 4):
        self._dir = path
        self._max_running = max_running
        self._kill_on_exit = kill_on_exit

        self._main_req = Queue()
        self._main_res = Queue()
        self._sec_req = Queue()
        self._sec_res = Queue()

        self._workers = []
        for _ in range(max(1, threads - 1)):
            t = Thread(target=_worker,
                       args=(self._sec_req, self._sec_res))
            t.start()
            self._workers.append(t)

        self._manager = Thread(target=_manager,
                               args=(self._main_req, self._main_res,
                                     self._sec_req, self._sec_res,
                                     len(self._workers), self._max_running,
                                     self._dir))
        self._manager.start()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._kill_on_exit:
            self.terminate()

    def __del__(self):
        if self._kill_on_exit:
            try:
                self.terminate()
            except Exception:
                pass

    def submit(self, task: Task):
        self._main_req.put((_TASK_REQ, task))

    def as_completed(self, wait: bool = False):
        if wait:
            self._main_req.put((_WAIT_REQ, None))
        else:
            self._main_req.put((_PING_REQ, None))

        for task in iter(self._main_res.get, _OVER_RES):
            yield task

    def terminate(self):
        if self._main_req is not None:
            self._main_req.put((_KILL_REQ, None))
            self._main_req = None

            for _ in iter(self._main_res.get, _OVER_RES):
                pass
