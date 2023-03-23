import inspect
import os
import pickle
import shutil
import sys
import time
from datetime import datetime
from tempfile import mkdtemp
from typing import Callable

from mundone import executors, states


INPUT_FILE = "input.pickle"
OUTPUT_FILE = "output.log"
ERROR_FILE = "error.log"
RESULT_FILE = "output.pickle"


class Task:
    def __init__(self, fn: Callable, args: list | tuple | None = None,
                 kwargs: dict | None = None, **_kwargs):
        if not callable(fn):
            raise TypeError(f"'{fn}' is not callable")
        elif args is not None and not isinstance(args, (list, tuple)):
            raise TypeError("Task() arg 2 must be a list or a tuple")
        elif kwargs is not None and not isinstance(kwargs, dict):
            raise TypeError("Task() arg 3 must be a dict")

        self.id = None
        self.fn = fn
        self.args = args if args is not None else []
        self.kwargs = kwargs if kwargs is not None else {}

        self.name = str(_kwargs.get("name", fn.__name__))
        self.status = states.PENDING
        self.workdir = None

        self.create_time = datetime.now()
        self.submit_time = None
        self.start_time = None
        self.end_time = None
        self.unknown_since = None

        self.stdout = self.stderr = ""
        self.result = None

        if _kwargs.get("scheduler"):
            scheduler_obj = _kwargs["scheduler"]

            try:
                scheduler = scheduler_obj["type"].upper()
            except TypeError:
                raise TypeError("scheduler: must be a dictionary")
            except KeyError:
                raise KeyError("scheduler: 'type' is mandatory")
            except AttributeError:
                raise KeyError("scheduler: 'type' must be a string")

            if scheduler == "LSF":
                self.executor = executors.LsfExecutor(**scheduler_obj,
                                                      name=self.name)
            elif scheduler == "SLURM":
                self.executor = executors.SlurmExecutor(**scheduler_obj,
                                                        name=self.name)
            else:
                raise ValueError("scheduler: 'type' must be 'LSF' or 'SLURM'")
        else:
            self.executor = executors.LocalExecutor()

        self.requires = set()
        for arg in self.args:
            if isinstance(arg, TaskOutput):
                self.requires.add(arg.task_name)

        for arg in self.kwargs.values():
            if isinstance(arg, TaskOutput):
                self.requires.add(arg.task_name)

        requires = _kwargs.get("requires", [])
        if not isinstance(requires, (dict, list, set, tuple)):
            raise TypeError("'requires' must of one of these types: "
                            "dict, list, set, tuple")

        for item in set(requires):
            if isinstance(item, Task):
                self.requires.add(item.name)
            elif isinstance(item, str):
                self.requires.add(item)
            else:
                raise TypeError("'requires' must be a sequence "
                                "of strings or Tasks")

        self.add_random_suffix = _kwargs.get("random_suffix", True)
        self.keep_tmp = _kwargs.get("keep", False)

    def __repr__(self) -> str:
        return self.name

    @property
    def output(self):
        return TaskOutput(self)

    @property
    def state(self) -> str:
        if self.status == states.PENDING:
            return "pending"
        elif self.status == states.RUNNING:
            return "running"
        elif self.status == states.ERROR:
            return "failed"
        elif self.status == states.CANCELLED:
            return "cancelled"
        elif self.status == states.SUCCESS:
            return "done"

    @property
    def cputime(self) -> int | None:
        return self.executor.get_cpu_time(self.stdout)

    @property
    def maxmem(self) -> int | None:
        return self.executor.get_max_memory(self.stdout)

    def pack(self, workdir: str):
        if self.workdir is None:
            if self.add_random_suffix:
                self.workdir = mkdtemp(prefix=self.name, dir=workdir)
            else:
                self.workdir = os.path.join(workdir, self.name)

        try:
            os.makedirs(self.workdir)
        except FileExistsError:
            pass

        for file in [OUTPUT_FILE, ERROR_FILE, INPUT_FILE, RESULT_FILE]:
            try:
                os.unlink(os.path.join(self.workdir, file))
            except FileNotFoundError:
                pass

        with open(os.path.join(self.workdir, INPUT_FILE), "wb") as fh:
            module = inspect.getmodule(self.fn)
            module_path = module.__file__
            module_name = module.__name__

            for _ in range(len(module_name.split('.'))):
                module_path = os.path.dirname(module_path)

            p = pickle.dumps((self.fn, self.args, self.kwargs), protocol=0)
            if module_name == "__main__":
                module_name = os.path.basename(sys.argv[0]).rsplit('.', 1)[0]
                p = p.replace(b"(c__main__", f"(c{module_name}".encode())

            pickle.dump(module_path.encode(), fh)
            pickle.dump(module_name.encode(), fh)
            pickle.dump(p, fh)

    def is_ready(self) -> bool:
        args = []
        for arg in self.args:
            if isinstance(arg, TaskOutput):
                if arg.ready():
                    args.append(arg.read())
                else:
                    return False
            else:
                args.append(arg)

        kwargs = {}
        for key, arg in self.kwargs.items():
            if isinstance(arg, TaskOutput):
                if arg.ready():
                    kwargs[key] = arg.read()
                else:
                    return False
            else:
                kwargs[key] = arg

        self.args = args
        self.kwargs = kwargs
        return True

    def is_running(self) -> bool:
        return self.status == states.RUNNING

    def is_done(self) -> bool:
        return self.status in (states.SUCCESS, states.ERROR, states.CANCELLED)

    def is_successful(self) -> bool:
        return self.status == states.SUCCESS

    def start(self, **kwargs) -> bool:
        workdir = kwargs.get("dir", os.getcwd())
        # trust_scheduler = kwargs.get("trust_scheduler", True)

        if self.is_running() or not self.is_ready():
            return True

        self.pack(workdir)
        # self.trust_scheduler = trust_scheduler
        self.id = self.executor.submit(os.path.join(self.workdir, INPUT_FILE),
                                       os.path.join(self.workdir, RESULT_FILE),
                                       os.path.join(self.workdir, OUTPUT_FILE),
                                       os.path.join(self.workdir, ERROR_FILE))

        if self.id is None:
            time.sleep(3)
            return False

        self.status = states.RUNNING
        self.submit_time = datetime.now()
        self.start_time = self.end_time = None
        return True

    def terminate(self, force: bool = False):
        if self.is_done():
            return

        self.executor.kill(force)
        while not self.executor.ready_to_collect():
            time.sleep(1)

        self.collect()
        self.status = states.CANCELLED

    def clean(self, seconds: int = 30, max_attempts: int = 5):
        self.id = None

        if not self.keep_tmp:
            num_attempts = 0
            while True:
                num_attempts += 1
                try:
                    shutil.rmtree(self.workdir)
                except OSError:
                    if num_attempts == max_attempts:
                        raise
                    else:
                        time.sleep(seconds)
                else:
                    break

            self.workdir = None

    def wait(self, seconds: int = 10):
        while not self.is_done():
            self.poll()
            time.sleep(seconds)

    def poll(self):
        if self.status != states.RUNNING:
            return

        status = self.executor.poll()
        if status == states.NOT_FOUND:
            self.try_collect()
        elif (status in (states.SUCCESS, states.ERROR)
              and self.executor.ready_to_collect()):
            self.collect()
            self.status = status
        elif status == states.RUNNING:
            # Reset unknown status timer
            self.unknown_since = None
        elif status == states.UNKNOWN:
            now = datetime.now()
            if self.unknown_since is None:
                self.unknown_since = now
            elif (now - self.unknown_since).total_seconds() >= 3600:
                self.terminate(force=True)

    def collect(self) -> int | None:
        if self.workdir is None:
            # Task cancelled before started
            self.result = None
            self.end_time = datetime.now()
            return None

        try:
            with open(os.path.join(self.workdir, OUTPUT_FILE), "rt") as fh:
                self.stdout = fh.read()
        except FileNotFoundError:
            self.stdout = ""

        try:
            with open(os.path.join(self.workdir, ERROR_FILE), "rt") as fh:
                self.stderr = fh.read()
        except FileNotFoundError:
            self.stderr = ""

        returncode = None
        try:
            with open(os.path.join(self.workdir, RESULT_FILE), "rb") as fh:
                res = pickle.load(fh)
        except (FileNotFoundError, EOFError):
            """
            The file may not exist if the task is killed, or may be empty
            if the task failed.
            """
            self.result = None
            self.status = states.ERROR

            start_time, end_time = self.executor.get_times()
            self.start_time = start_time or self.start_time
            self.end_time = end_time or datetime.now()
        else:
            self.result = res[0]
            returncode = res[1]
            self.start_time = res[2]
            self.end_time = res[3]

        self.clean()
        return returncode

    def try_collect(self) -> bool:
        if self.workdir:
            result_file = os.path.join(self.workdir, RESULT_FILE)
            if os.path.isfile(result_file):
                returncode = self.collect()
                if returncode == 0:
                    self.status = states.SUCCESS
                else:
                    self.status = states.ERROR

                return True

        return False


class TaskOutput:
    def __init__(self, task: Task):
        self.task = task

    def ready(self) -> bool:
        self.task.poll()
        return self.task.is_successful()

    def read(self):
        return self.task.result

    @property
    def task_name(self) -> str:
        return self.task.name


def as_completed(tasks: list[Task], seconds: int = 10):
    while tasks:
        _tasks = []

        for t in tasks:
            t.poll()
            if t.is_done():
                yield t
            else:
                _tasks.append(t)

        tasks = _tasks
        if tasks:
            time.sleep(seconds)
