import inspect
import os
import pickle
import shutil
import sys
import time
from datetime import datetime
from subprocess import Popen
from tempfile import mkdtemp
from typing import Callable, Optional, Sequence

from . import lsf, runner, statuses


INPUT_FILE = "input.pickle"
OUTPUT_FILE = "output.log"
ERROR_FILE = "error.log"
RESULT_FILE = "output.pickle"


class Task:
    def __init__(self, fn: Callable, args: Optional[Sequence] = None,
                 kwargs: Optional[dict] = None, **_kwargs):
        if not callable(fn):
            raise TypeError(f"'{fn}' is not callable")
        elif args is not None and not isinstance(args, (list, tuple)):
            raise TypeError("Task() arg 2 must be a list or a tuple")
        elif kwargs is not None and not isinstance(kwargs, dict):
            raise TypeError("Task() arg 3 must be a dict")

        self.fn = fn
        self.args = args if args is not None else []
        self.kwargs = kwargs if kwargs is not None else {}

        self.name = str(_kwargs.get("name", fn.__name__))
        self.status = statuses.PENDING
        self.workdir = None

        # Local process
        self.proc = None
        self.file_handlers = None  # file handlers (stdout, stderr)

        # LSF/SLURM job
        self.jobid = None
        self.unknown_start = None  # first time job status is UNKWN

        self.stdout = ""
        self.stderr = ""
        self.result = None

        self.create_time = datetime.now()
        self.submit_time = None
        self.start_time = None
        self.end_time = None

        self.returncode = None
        self.trust_scheduler = True

        if _kwargs.get("scheduler"):
            if isinstance(_kwargs["scheduler"], dict):
                self.scheduler = _kwargs["scheduler"]
            else:
                self.scheduler = {}
        else:
            self.scheduler = None

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

    def __repr__(self) -> str:
        return self.name

    @property
    def output(self):
        return TaskOutput(self)

    @property
    def id(self) -> Optional[int]:
        if self.proc is not None:
            return self.proc.pid
        elif self.jobid is not None:
            return -self.jobid
        else:
            return None

    @property
    def state(self) -> str:
        if self.status == statuses.PENDING:
            return "pending"
        elif self.status == statuses.RUNNING:
            return "running"
        elif self.status == statuses.ERROR:
            return "failed"
        elif self.status == statuses.CANCELLED:
            return "cancelled"
        elif self.status == statuses.SUCCESS:
            return "done"

    @property
    def cputime(self) -> Optional[int]:
        if isinstance(self.scheduler, dict):
            scheduler = self.scheduler.get("type", "LSF")
            if scheduler == "LSF":
                return lsf.get_cpu_time(self.stdout)
            elif scheduler == "SLURM":
                raise NotImplementedError
            else:
                raise ValueError(scheduler)
        else:
            return None

    @property
    def maxmem(self) -> Optional[int]:
        if isinstance(self.scheduler, dict):
            scheduler = self.scheduler.get("type", "LSF")
            if scheduler == "LSF":
                return lsf.get_max_memory(self.stdout)
            elif scheduler == "SLURM":
                raise NotImplementedError
            else:
                raise ValueError(scheduler)
        else:
            return None

    def _pack(self, workdir: str):
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

    def ready(self) -> bool:
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

    def running(self) -> bool:
        return self.status == statuses.RUNNING

    def completed(self) -> bool:
        return self.status == statuses.SUCCESS

    def done(self) -> bool:
        return self.status in (statuses.SUCCESS, statuses.ERROR, statuses.CANCELLED)

    def start(self, **kwargs) -> bool:
        workdir = kwargs.get("dir", os.getcwd())
        trust_scheduler = kwargs.get("trust_scheduler", True)

        if self.running() or not self.ready():
            return True

        self._pack(workdir)
        output_file = os.path.join(self.workdir, OUTPUT_FILE)
        error_file = os.path.join(self.workdir, ERROR_FILE)
        input_file = os.path.join(self.workdir, INPUT_FILE)
        result_file = os.path.join(self.workdir, RESULT_FILE)
        self.trust_scheduler = trust_scheduler
        self.proc = self.jobid = self.file_handlers = None

        if isinstance(self.scheduler, dict):
            job_id = lsf.submit(name=self.name,
                                in_file=input_file,
                                out_file=result_file,
                                stdout_file=output_file,
                                stderr_file=error_file,
                                queue=self.scheduler.get("queue"),
                                project=self.scheduler.get("project"),
                                cpu=self.scheduler.get("cpu"),
                                mem=self.scheduler.get("mem"),
                                tmp=self.scheduler.get("tmp"),
                                scratch=self.scheduler.get("scratch"))

            if job_id is None:
                time.sleep(3)
                return False

            self.jobid = job_id
        else:
            cmd = [
                sys.executable,
                os.path.realpath(runner.__file__),
                input_file,
                result_file
            ]

            fh_out = open(output_file, "wt")
            fh_err = open(error_file, "wt")
            self.proc = Popen(cmd, stdout=fh_out, stderr=fh_err)
            self.file_handlers = (fh_out, fh_err)

        self.status = statuses.RUNNING  # actually not running: submitted
        self.submit_time = datetime.now()
        self.start_time = self.end_time = None
        return True

    def _clean(self, seconds: int = 30, max_attempts: int = 5):
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

    def wait(self, seconds: int = 10):
        while not self.done():
            self.poll()
            time.sleep(seconds)

    def poll(self):
        if self.status != statuses.RUNNING:
            return

        if self.proc is not None:
            returncode = self.proc.poll()
            if returncode is not None:
                self._collect()
                self.proc = None
                if returncode == 0:
                    self.status = statuses.SUCCESS
                else:
                    self.status = statuses.ERROR
        elif self.jobid is not None:
            found, status = lsf.check(self.jobid)

            if found:
                ok_or_err = [statuses.SUCCESS, statuses.ERROR]
                file = os.path.join(self.workdir, OUTPUT_FILE)
                if status in ok_or_err and lsf.is_ready_to_collect(file):
                    returncode = self._collect()
                    self.jobid = None

                    if self.trust_scheduler:
                        if status == statuses.SUCCESS:
                            self.status = statuses.SUCCESS
                        else:
                            self.status = statuses.ERROR
                    elif returncode == 0:
                        self.status = statuses.SUCCESS
                    else:
                        self.status = statuses.ERROR
                elif status == statuses.RUNNING:
                    # Reset unknown status timer
                    self.unknown_start = None
                elif status == statuses.UNKNOWN:
                    now = datetime.now()
                    if self.unknown_start is None:
                        self.unknown_start = now
                    elif (now - self.unknown_start).total_seconds() >= 3600:
                        self.terminate(force=True)
                elif status == statuses.ZOMBIE:
                    self.terminate(force=True)
            else:
                if not self._try_collect():
                    # Job does not exist and results not found: error
                    self.status = statuses.ERROR
        else:
            self._try_collect()

    def _try_collect(self) -> bool:
        if self.workdir and os.path.isfile(os.path.join(self.workdir,
                                                        RESULT_FILE)):
            returncode = self._collect()
            if returncode == 0:
                self.status = statuses.SUCCESS
            else:
                self.status = statuses.ERROR
            return True

        return False

    def _collect(self) -> Optional[int]:
        if self.workdir is None:
            # Task cancelled before started
            self.result = None
            self.end_time = datetime.now()
            return None
        elif self.file_handlers:
            fh_out, fh_err = self.file_handlers
            fh_out.close()
            fh_err.close()
            self.file_handlers = None

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
            self.status = statuses.ERROR

            if isinstance(self.scheduler, dict):
                self.start_time = lsf.get_start_time(self.stdout)
                end_time = lsf.get_end_time(self.stdout)
                self.end_time = end_time or datetime.now()
            else:
                self.end_time = datetime.now()
        else:
            self.result = res[0]
            returncode = res[1]
            self.start_time = res[2]
            self.end_time = res[3]

        self._clean()
        return returncode

    def terminate(self, force: bool = False):
        if self.done():
            return
        elif self.proc is not None:
            self.proc.kill()
        elif self.jobid is not None:
            lsf.kill(self.jobid, force=force)

            file = os.path.join(self.workdir, OUTPUT_FILE)
            while not lsf.is_ready_to_collect(file):
                time.sleep(1)

        self._collect()
        self.proc = self.jobid = None
        self.status = statuses.CANCELLED


class TaskOutput:
    def __init__(self, task: Task):
        self._task = task

    def ready(self) -> bool:
        self._task.poll()
        return self._task.done()

    def read(self):
        return self._task.result

    @property
    def task_name(self) -> str:
        return self._task.name


def as_completed(tasks: Sequence[Task], seconds: int = 10):
    while tasks:
        _tasks = []

        for t in tasks:
            t.poll()
            if t.done():
                yield t
            else:
                _tasks.append(t)

        tasks = _tasks
        if tasks:
            time.sleep(seconds)
